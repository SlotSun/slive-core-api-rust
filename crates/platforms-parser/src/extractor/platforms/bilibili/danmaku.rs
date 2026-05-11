//! Bilibili danmaku (chat) provider.
//!
//! Connects to Bilibili's WebSocket-based danmaku stream using a binary protocol
//! with Brotli/zlib-compressed payloads.
//!
//! Protocol overview:
//! 1. Resolve room_id (short IDs) via `room_init` API.
//! 2. Fetch token + host list via `getDanmuInfo` API.
//! 3. Open WebSocket to `wss://{host}/sub`.
//! 4. Send auth packet (op=7) with protover=3 (Brotli).
//! 5. Send heartbeat (op=2) every 30 seconds.
//! 6. Decode incoming binary frames – protover 3 packets are Brotli-compressed,
//!    protover 2 packets are zlib-compressed, protover 0 packets are raw JSON.
//! 7. Parse notification commands: `DANMU_MSG`, `SEND_GIFT`, `SUPER_CHAT_MESSAGE`,
//!    `ROOM_CHANGE`.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};

use serde_json::Value as JsonValue;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use crate::danmaku::error::{DanmakuError, Result};
use crate::danmaku::event::{DanmuControlEvent, DanmuItem};
use crate::danmaku::message::DanmuMessage;
use crate::danmaku::provider::{ConnectionConfig, DanmuConnection, DanmuProvider};
use crate::extractor::http_client::HttpClient;
use crate::extractor::platforms::bilibili::wbi;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default WebSocket endpoint when the API does not provide one.
const DEFAULT_WS_URL: &str = "wss://broadcastlv.chat.bilibili.com/sub";

/// Interval between heartbeat packets.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

/// Maximum time `receive` will wait for a message before returning `Ok(None)`.
const RECEIVE_TIMEOUT: Duration = Duration::from_millis(100);

/// Maximum time to wait for the initial WebSocket handshake.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Channel buffer size for decoded danmu items.
const MESSAGE_CHANNEL_SIZE: usize = 256;

/// Room init API for resolving short room IDs.
const ROOM_INIT_URL: &str = "https://api.live.bilibili.com/room/v1/Room/room_init?id={room_id}";

// ---------------------------------------------------------------------------
// Binary packet protocol helpers
// ---------------------------------------------------------------------------

/// Encode a binary packet with a 16-byte header.
///
/// Header layout (big-endian):
/// ```text
/// [0..4]  total packet length  (u32)
/// [4..6]  header length         (u16, always 16)
/// [6..8]  protocol version      (u16)
/// [8..12] operation code         (u32)
/// [12..16] sequence id           (u32, always 1)
/// ```
fn encode_packet(operation: u32, proto_ver: u16, body: &[u8]) -> Vec<u8> {
    let total_len = 16 + body.len();
    let mut packet = Vec::with_capacity(total_len);
    packet.extend_from_slice(&(total_len as u32).to_be_bytes());
    packet.extend_from_slice(&16u16.to_be_bytes());
    packet.extend_from_slice(&proto_ver.to_be_bytes());
    packet.extend_from_slice(&operation.to_be_bytes());
    packet.extend_from_slice(&1u32.to_be_bytes());
    packet.extend_from_slice(body);
    packet
}

/// Build an authentication packet (op=7, protover=3).
///
/// When `uid` is 0 the connection is anonymous.  Set `uid` to the user's
/// `DedeUserID` from the cookie when using an authenticated session.
fn build_auth_packet(room_id: u64, token: &str, uid: u64, buvid: &str) -> Vec<u8> {
    let body = serde_json::json!({
        "uid": uid,
        "roomid": room_id,
        "protover": 3,
        "buvid": buvid,
        "platform": "web",
        "type": 2,
        "key": token
    });
    let body_str = body.to_string();
    encode_packet(7, 1, body_str.as_bytes())
}

/// Build a heartbeat packet (op=2).
fn build_heartbeat_packet() -> Vec<u8> {
    encode_packet(2, 1, &[])
}

/// Decode one or more packets from a byte buffer.
///
/// Returns `(operation, proto_ver, body)` tuples.
fn decode_packets(data: &[u8]) -> Vec<(u32, u16, Vec<u8>)> {
    let mut packets = Vec::new();
    let mut offset = 0;
    while offset + 16 <= data.len() {
        let total_len =
            u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap_or([0; 4])) as usize;
        let header_len =
            u16::from_be_bytes(data[offset + 4..offset + 6].try_into().unwrap_or([0, 16])) as usize;
        let proto_ver =
            u16::from_be_bytes(data[offset + 6..offset + 8].try_into().unwrap_or([0; 2]));
        let operation =
            u32::from_be_bytes(data[offset + 8..offset + 12].try_into().unwrap_or([0; 4]));

        // Sanity checks.
        if total_len < header_len || total_len < 16 || offset + total_len > data.len() {
            break;
        }

        let body = data[offset + header_len..offset + total_len].to_vec();
        packets.push((operation, proto_ver, body));

        offset += total_len;
    }
    packets
}

// ---------------------------------------------------------------------------
// Decompression helpers
// ---------------------------------------------------------------------------

/// Decompress a Brotli-compressed body.
fn decompress_brotli(data: &[u8]) -> std::result::Result<Vec<u8>, String> {
    let mut output = Vec::new();
    brotli::Decompressor::new(std::io::Cursor::new(data), 4096)
        .read_to_end(&mut output)
        .map_err(|e| format!("brotli decompress: {e}"))?;
    Ok(output)
}

/// Decompress a zlib-compressed body.
fn decompress_zlib(data: &[u8]) -> std::result::Result<Vec<u8>, String> {
    use flate2::read::ZlibDecoder;
    let mut output = Vec::new();
    ZlibDecoder::new(data)
        .read_to_end(&mut output)
        .map_err(|e| format!("zlib decompress: {e}"))?;
    Ok(output)
}

// ---------------------------------------------------------------------------
// Notification parsing
// ---------------------------------------------------------------------------

/// Parse a raw JSON notification into zero or more [`DanmuItem`]s.
fn parse_notification_json(json: &JsonValue) -> Vec<DanmuItem> {
    let cmd = json.get("cmd").and_then(|v| v.as_str()).unwrap_or("");
    match cmd {
        "DANMU_MSG" => parse_danmu_msg(json)
            .map(DanmuItem::Message)
            .into_iter()
            .collect(),
        "SEND_GIFT" => parse_send_gift(json)
            .map(DanmuItem::Message)
            .into_iter()
            .collect(),
        "SUPER_CHAT_MESSAGE" => parse_super_chat_message(json)
            .map(DanmuItem::Message)
            .into_iter()
            .collect(),
        "ROOM_CHANGE" => parse_room_change(json)
            .map(DanmuItem::Control)
            .into_iter()
            .collect(),
        _ => {
            trace!(cmd = cmd, "Unhandled Bilibili notification command");
            vec![]
        }
    }
}

/// Recursively parse binary data into [`DanmuItem`]s, handling compression.
fn parse_notification_recursive(data: &[u8], proto_ver: u16) -> Vec<DanmuItem> {
    match proto_ver {
        // Raw JSON packets
        0 | 1 => {
            let packets = decode_packets(data);
            let mut items = Vec::new();
            for (op, pver, body) in packets {
                match op {
                    5 => {
                        // Notification – parse JSON body directly
                        if let Ok(json) = serde_json::from_slice::<JsonValue>(&body) {
                            items.extend(parse_notification_json(&json));
                        }
                    }
                    3 => {
                        // Heartbeat reply – popularity count (4 bytes)
                        if body.len() >= 4 {
                            let _popularity =
                                u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
                            trace!(popularity = _popularity, "Heartbeat reply");
                        }
                    }
                    8 => {
                        debug!("Auth reply received (op=8)");
                    }
                    _ => {
                        debug!(op = op, proto_ver = pver, "Unknown packet op");
                    }
                }
            }
            items
        }
        // Zlib compressed
        2 => match decompress_zlib(data) {
            Ok(decompressed) => parse_notification_recursive(&decompressed, 0),
            Err(e) => {
                warn!("Failed to decompress zlib data: {}", e);
                vec![]
            }
        },
        // Brotli compressed
        3 => {
            match decompress_brotli(data) {
                Ok(decompressed) => {
                    // After Brotli decompression the result may contain multiple
                    // packets (each with a 16-byte header). Parse them recursively
                    // as proto_ver 0 (raw).
                    parse_notification_recursive(&decompressed, 0)
                }
                Err(e) => {
                    warn!("Failed to decompress brotli data: {}", e);
                    vec![]
                }
            }
        }
        _ => {
            warn!(proto_ver = proto_ver, "Unknown protocol version");
            vec![]
        }
    }
}

// -- Individual command parsers ----------------------------------------------

/// Parse a `DANMU_MSG` command.
///
/// Structure: `info[1]` = content, `info[2][0]` = uid, `info[2][1]` = username,
/// `info[0][3]` = font color.
fn parse_danmu_msg(json: &JsonValue) -> Option<DanmuMessage> {
    let info = json.get("info")?.as_array()?;
    let content = info.get(1)?.as_str()?;
    let user_info = info.get(2)?.as_array()?;
    let uid = user_info.first()?.as_u64()?;
    let username = user_info.get(1)?.as_str()?;

    // info[0][3] = font color (0 or 0xFFFFFF = default/white)
    let color = info
        .first()
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.get(3))
        .and_then(|v| v.as_u64())
        .filter(|&c| c > 0 && c < 16_777_215);

    let color_hex = color.map(|c| format!("#{:06X}", c as u32));

    let mut msg = DanmuMessage::chat(
        Uuid::new_v4().to_string(),
        uid.to_string(),
        username,
        content,
    );

    if let Some(color) = color_hex {
        msg = msg.with_color(color);
    }

    Some(msg)
}

/// Parse a `SEND_GIFT` command.
///
/// Structure: `data.uname`, `data.action`, `data.giftName`, `data.num`.
fn parse_send_gift(json: &JsonValue) -> Option<DanmuMessage> {
    let data = json.get("data")?;
    let uid = data.get("uid")?.as_u64()?;
    let username = data.get("uname")?.as_str()?;
    let action = data.get("action").and_then(|v| v.as_str()).unwrap_or("");
    let gift_name = data.get("giftName").and_then(|v| v.as_str()).unwrap_or("?");
    let num = data.get("num").and_then(|v| v.as_u64()).unwrap_or(1);

    let content = format!("{} {} x{}", action, gift_name, num);

    let msg = DanmuMessage::gift(
        Uuid::new_v4().to_string(),
        uid.to_string(),
        username,
        gift_name,
        num as u32,
    )
    .with_metadata("action", serde_json::json!(action));

    // Override the default content with the action-prefixed version.
    let mut msg = msg;
    msg.content = content;

    Some(msg)
}

/// Parse a `SUPER_CHAT_MESSAGE` command.
///
/// Structure: `data.id`, `data.uid`, `data.user_info.uname`, `data.message`,
/// `data.price`, `data.time`.
fn parse_super_chat_message(json: &JsonValue) -> Option<DanmuMessage> {
    let data = json.get("data")?;
    let id = data.get("id")?.as_u64()?;
    let uid = data.get("uid")?.as_u64()?;
    let user_info = data.get("user_info")?;
    let username = user_info.get("uname")?.as_str()?;
    let message = data.get("message").and_then(|v| v.as_str()).unwrap_or("");
    let price = data.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let keep_time = data.get("time").and_then(|v| v.as_u64()).unwrap_or(0);

    let face_raw = user_info
        .get("face")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let face = if face_raw.is_empty() {
        String::new()
    } else {
        format!("{face_raw}@200w.jpg")
    };
    let bg_color = data
        .get("background_color")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let bg_bottom_color = data
        .get("background_bottom_color")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let msg = DanmuMessage::super_chat(
        id.to_string(),
        uid.to_string(),
        username,
        message,
        price as u64,
    )
    .with_super_chat_keep_time(keep_time)
    .with_metadata("price_exact", serde_json::json!(price))
    .with_metadata("face", serde_json::json!(face))
    .with_metadata("background_color", serde_json::json!(bg_color))
    .with_metadata("background_bottom_color", serde_json::json!(bg_bottom_color));

    Some(msg)
}

/// Parse a `ROOM_CHANGE` command as a [`DanmuControlEvent`].
///
/// Structure: `data.title`, `data.area_name`, `data.parent_area_name`.
fn parse_room_change(json: &JsonValue) -> Option<DanmuControlEvent> {
    let data = json.get("data")?;
    let title = data
        .get("title")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let area_name = data
        .get("area_name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let parent_area_name = data
        .get("parent_area_name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Some(DanmuControlEvent::RoomInfoChanged {
        title,
        category: area_name,
        parent_category: parent_area_name,
    })
}

// ---------------------------------------------------------------------------
// Internal connection state
// ---------------------------------------------------------------------------

/// Per-connection bookkeeping kept inside [`BilibiliDanmuProvider`].
struct BilibiliConnectionState {
    /// Receiver end of the decoded-message channel.
    message_rx: Arc<Mutex<mpsc::Receiver<DanmuItem>>>,
    /// Sender used to signal the background task to shut down.
    shutdown_tx: Option<mpsc::Sender<()>>,
    /// Background task handles (WebSocket reader/heartbeat).
    tasks: Vec<JoinHandle<()>>,
}

impl BilibiliConnectionState {
    /// Abort all spawned tasks immediately.
    fn abort_tasks(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }
}

impl Drop for BilibiliConnectionState {
    fn drop(&mut self) {
        self.abort_tasks();
    }
}

// ---------------------------------------------------------------------------
// Background WebSocket task
// ---------------------------------------------------------------------------

/// Percent-encode a string for URL use (Bilibili WBI compatible).
fn url_encode(s: &str) -> String {
    let mut encoded = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                encoded.push(c);
            }
            _ => {
                let mut buf = [0u8; 4];
                let s = c.encode_utf8(&mut buf);
                for b in s.bytes() {
                    encoded.push_str(&format!("%{:02X}", b));
                }
            }
        }
    }
    encoded
}

/// Long-running task that:
/// 1. Connects to the Bilibili danmaku WebSocket.
/// 2. Sends the auth packet (op=7).
/// 3. Sends periodic heartbeats (op=2).
/// 4. Decodes incoming frames and forwards [`DanmuItem`]s through `message_tx`.
async fn run_bilibili_ws_task(
    room_id: u64,
    token: String,
    ws_url: String,
    uid: u64,
    buvid: String,
    cookies: String,
    message_tx: mpsc::Sender<DanmuItem>,
    mut shutdown_rx: mpsc::Receiver<()>,
) {
    // --- Connect ----------------------------------------------------------
    let mut request = ws_url.into_client_request()
        .expect("failed to build WebSocket request");
    if !cookies.is_empty() {
        request.headers_mut().insert(
            "Cookie",
            http::HeaderValue::from_str(&cookies).unwrap_or(http::HeaderValue::from_static("")),
        );
    }
    let ws_stream = match tokio::time::timeout(CONNECT_TIMEOUT, connect_async(request)).await {
        Ok(Ok((stream, _response))) => stream,
        Ok(Err(e)) => {
            error!("Failed to connect to Bilibili WebSocket: {e}");
            return;
        }
        Err(_) => {
            error!("Timeout connecting to Bilibili WebSocket");
            return;
        }
    };

    let (mut ws_sink, mut ws_source) = ws_stream.split();
    info!(room_id = room_id, "Connected to Bilibili WebSocket");

    // --- Auth -------------------------------------------------------------
    let auth_packet = build_auth_packet(room_id, &token, uid, &buvid);
    if let Err(e) = ws_sink
        .send(Message::Binary(Bytes::from(auth_packet)))
        .await
    {
        error!("Failed to send auth packet: {e}");
        return;
    }
    debug!(room_id = room_id, "Sent auth packet");

    // --- Main loop --------------------------------------------------------
    let mut heartbeat_timer = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // `interval` fires immediately on the first tick – skip it.
    heartbeat_timer.tick().await;

    loop {
        tokio::select! {
            // Heartbeat
            _ = heartbeat_timer.tick() => {
                let heartbeat = build_heartbeat_packet();
                if let Err(e) = ws_sink.send(Message::Binary(Bytes::from(heartbeat))).await {
                    error!("Failed to send heartbeat: {e}");
                    break;
                }
                trace!(room_id = room_id, "Sent heartbeat");
            }

            // Incoming WebSocket frame
            msg_opt = ws_source.next() => {
                match msg_opt {
                    Some(Ok(Message::Binary(data))) => {
                        debug!(room_id = room_id, data_len = data.len(), "Received binary frame");
                        let packets = decode_packets(&data);
                        debug!(room_id = room_id, packet_count = packets.len(), "Decoded packets");
                        for (operation, proto_ver, body) in packets {
                            match operation {
                                5 => {
                                    // Notification – may be compressed
                                    let items = parse_notification_recursive(&body, proto_ver);
                                    for item in items {
                                        if message_tx.send(item).await.is_err() {
                                            debug!(room_id = room_id, "Message channel closed");
                                            return;
                                        }
                                    }
                                }
                                3 => {
                                    // Heartbeat reply – popularity count
                                    if body.len() >= 4 {
                                        let _popularity = u32::from_be_bytes([
                                            body[0], body[1], body[2], body[3],
                                        ]);
                                        trace!(popularity = _popularity, "Heartbeat reply");
                                    }
                                }
                                8 => {
                                    debug!(room_id = room_id, "Auth reply received (op=8)");
                                }
                                _ => {
                                    debug!(op = operation, "Unknown operation");
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!(room_id = room_id, "WebSocket closed by server");
                        let _ = message_tx
                            .send(DanmuItem::Control(DanmuControlEvent::StreamClosed {
                                message: Some("WebSocket closed by server".to_string()),
                                action: None,
                            }))
                            .await;
                        break;
                    }
                    Some(Err(e)) => {
                        error!(room_id = room_id, "WebSocket error: {e}");
                        break;
                    }
                    None => {
                        info!(room_id = room_id, "WebSocket stream ended");
                        break;
                    }
                    Some(Ok(msg)) => {
                        debug!(room_id = room_id, ?msg, "Received non-binary frame");
                    }
                }
            }

            // Shutdown signal from `disconnect()`
            _ = shutdown_rx.recv() => {
                debug!(room_id = room_id, "Shutdown signal received");
                let _ = ws_sink.close().await;
                return;
            }
        }
    }

    // Connection lost – make sure the socket is closed.
    let _ = ws_sink.close().await;
}

// ---------------------------------------------------------------------------
// BilibiliDanmuProvider
// ---------------------------------------------------------------------------

/// Platform-specific danmaku provider for Bilibili (哔哩哔哩).
pub struct BilibiliDanmuProvider {
    http: HttpClient,
    connections: tokio::sync::RwLock<HashMap<String, Arc<Mutex<BilibiliConnectionState>>>>,
}

impl BilibiliDanmuProvider {
    pub fn new() -> Self {
        Self {
            http: HttpClient::builder()
                .default_header("Referer", "https://live.bilibili.com")
                .build()
                .expect("failed to build HTTP client"),
            connections: tokio::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Resolve a (possibly short) room_id to the real room_id via `room_init`.
    async fn resolve_room_id(&self, room_id: &str) -> Result<u64> {
        let url = ROOM_INIT_URL.replace("{room_id}", room_id);
        let json: JsonValue = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| DanmakuError::connection(format!("room_init request failed: {e}")))?
            .json()
            .await
            .map_err(|e| DanmakuError::protocol(format!("room_init json error: {e}")))?;

        let code = json.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);
        if code != 0 {
            let msg = json
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            return Err(DanmakuError::connection(format!(
                "room_init error {}: {}",
                code, msg
            )));
        }

        json.pointer("/data/room_id")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| DanmakuError::protocol("room_init: missing room_id".to_string()))
    }

    /// Ensure the cookie string contains `buvid3`/`buvid4` (required for WBI-signed endpoints).
    async fn ensure_buvid(&self) -> Result<()> {
        let cookies = self.http.cookies();
        if cookies.contains("buvid3=") {
            return Ok(());
        }
        let url = "https://api.bilibili.com/x/frontend/finger/spi";
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| DanmakuError::connection(format!("ensure_buvid request failed: {e}")))?;
        let text = resp
            .text()
            .await
            .map_err(|e| DanmakuError::connection(format!("ensure_buvid read failed: {e}")))?;
        let json: JsonValue = serde_json::from_str(&text)
            .map_err(|e| DanmakuError::protocol(format!("ensure_buvid json error: {e}")))?;
        let b3 = json.pointer("/data/b_3").and_then(|v| v.as_str()).unwrap_or("");
        let b4 = json.pointer("/data/b_4").and_then(|v| v.as_str()).unwrap_or("");
        if !b3.is_empty() {
            let sep = if cookies.is_empty() { "" } else { ";" };
            self.http.set_cookies(&format!("{}{}buvid3={};buvid4={}", cookies, sep, b3, b4));
        }
        Ok(())
    }

    /// Fetch danmu token and host list from the `getDanmuInfo` API.
    ///
    /// Uses WBI signing for the request.
    async fn fetch_danmu_info(&self, room_id: u64) -> Result<(String, Vec<(String, u16)>)> {
        self.ensure_buvid().await?;

        // Build the full URL with params (same as Dart).
        let base = "https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo";
        let params = vec![
            ("id".to_string(), room_id.to_string()),
        ];
        let query: String = params
            .iter()
            .map(|(k, v)| format!("{}={}", url_encode(k), url_encode(v)))
            .collect::<Vec<_>>()
            .join("&");
        let url_with_params = format!("{}?{}", base, query);

        // Sign using the Dart-ported WBI algorithm.
        let signed_params = wbi::sign_url(&self.http, Some(&self.http.cookies()), &url_with_params)
            .await
            .map_err(|e| DanmakuError::connection(format!("Failed to sign WBI: {e}")))?;

        // Build final URL.
        let final_query = wbi::encode_query_string(&signed_params);
        let url = format!("{}?{}", base, final_query);

        debug!(url = %url, "getDanmuInfo WBI-signed request");

        let json: JsonValue = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| DanmakuError::connection(format!("getDanmuInfo request failed: {e}")))?
            .json()
            .await
            .map_err(|e| DanmakuError::protocol(format!("getDanmuInfo json error: {e}")))?;

        let code = json.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);
        if code != 0 {
            let msg = json
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            return Err(DanmakuError::connection(format!(
                "getDanmuInfo error {}: {}",
                code, msg
            )));
        }

        let data = json
            .get("data")
            .ok_or_else(|| DanmakuError::protocol("getDanmuInfo: missing data".to_string()))?;

        let token = data
            .get("token")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mut host_list: Vec<(String, u16)> = Vec::new();
        if let Some(hosts) = data.get("host_list").and_then(|v| v.as_array()) {
            for host in hosts {
                let hostname = host
                    .get("host")
                    .and_then(|v| v.as_str())
                    .unwrap_or("broadcastlv.chat.bilibili.com")
                    .to_string();
                let wss_port = host.get("wss_port").and_then(|v| v.as_u64()).unwrap_or(443) as u16;
                host_list.push((hostname, wss_port));
            }
        }

        Ok((token, host_list))
    }
}

/// Convenience factory used by the [`ProviderRegistry`](crate::danmaku::registry::ProviderRegistry).
pub fn create_bilibili_danmu_provider() -> BilibiliDanmuProvider {
    BilibiliDanmuProvider::new()
}

#[async_trait]
impl DanmuProvider for BilibiliDanmuProvider {
    fn platform(&self) -> &str {
        "bilibili"
    }

    fn supports_url(&self, url: &str) -> bool {
        url.contains("live.bilibili.com")
    }

    fn extract_room_id(&self, url: &str) -> Option<String> {
        let re = regex::Regex::new(r"(?:https?://)?(?:www\.)?live\.bilibili\.com/(\d+)").ok()?;
        re.captures(url)
            .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
    }

    async fn connect(&self, room_id: &str, config: ConnectionConfig) -> Result<DanmuConnection> {
        // Accept cookies from config for authenticated access.
        if let Some(cookies) = config.cookies {
            if !cookies.is_empty() {
                self.http.set_cookies(&cookies);
            }
        }

        // Ensure buvid cookies are set before any API calls.
        self.ensure_buvid().await?;

        // 1. Resolve room_id — skip if already numeric (extractor pre-resolves).
        let real_room_id = if room_id.chars().all(|c| c.is_ascii_digit()) {
            room_id.parse::<u64>().unwrap_or(0)
        } else {
            self.resolve_room_id(room_id).await?
        };

        // 2. Fetch danmaku token and host list.
        let (token, host_list) = self.fetch_danmu_info(real_room_id).await?;

        // 3. Determine WebSocket URL.
        let ws_url = if let Some((host, port)) = host_list.first() {
            format!("wss://{}:{}/sub", host, port)
        } else {
            DEFAULT_WS_URL.to_string()
        };

        // 4. Determine UID from cookies (DedeUserID) for authenticated connections.
        let cookies = self.http.cookies();
        let uid = if cookies.is_empty() {
            None
        } else {
            Some(cookies.as_str())
        }
        .and_then(|c| {
            c.split(';')
                .find(|p| p.trim().starts_with("DedeUserID="))
                .and_then(|p| p.trim().strip_prefix("DedeUserID="))
                .and_then(|v| v.parse::<u64>().ok())
        })
        .unwrap_or(0);

        // Extract buvid from cookies for auth packet
        let buvid = cookies
            .split(';')
            .find(|p| p.trim().starts_with("buvid3="))
            .and_then(|p| p.trim().strip_prefix("buvid3="))
            .unwrap_or("")
            .to_string();

        // 5. Set up channels and spawn the background task.
        let connection_id = format!("bilibili-{}-{}", real_room_id, Uuid::new_v4());
        let (message_tx, message_rx) = mpsc::channel(MESSAGE_CHANNEL_SIZE);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let handle = tokio::spawn(run_bilibili_ws_task(
            real_room_id,
            token,
            ws_url,
            uid,
            buvid,
            cookies,
            message_tx,
            shutdown_rx,
        ));

        let state = BilibiliConnectionState {
            message_rx: Arc::new(Mutex::new(message_rx)),
            shutdown_tx: Some(shutdown_tx),
            tasks: vec![handle],
        };

        self.connections
            .write()
            .await
            .insert(connection_id.clone(), Arc::new(Mutex::new(state)));

        let mut conn = DanmuConnection::new(connection_id, "bilibili", real_room_id.to_string());
        conn.set_connected();

        Ok(conn)
    }

    async fn disconnect(&self, connection: &mut DanmuConnection) -> Result<()> {
        if let Some(state_arc) = self.connections.write().await.remove(&connection.id) {
            let mut state = state_arc.lock().await;
            // Signal the background task to shut down gracefully.
            if let Some(tx) = state.shutdown_tx.take() {
                let _ = tx.try_send(());
            }
            // Abort any remaining tasks (e.g. if the shutdown signal was not received).
            state.abort_tasks();
        }
        connection.set_disconnected();
        Ok(())
    }

    async fn receive(&self, connection: &DanmuConnection) -> Result<Option<DanmuItem>> {
        // Look up the internal state for this connection.
        let state_arc = {
            let map = self.connections.read().await;
            map.get(&connection.id).cloned()
        };

        let Some(state_arc) = state_arc else {
            return Err(DanmakuError::connection("Connection not found"));
        };

        // Clone the Arc so we can release the state lock before awaiting.
        let message_rx = {
            let state = state_arc.lock().await;
            state.message_rx.clone()
        };

        // Try to receive a message with a timeout.
        let next = tokio::time::timeout(RECEIVE_TIMEOUT, async move {
            let mut rx = message_rx.lock().await;
            rx.recv().await
        })
        .await;

        match next {
            Ok(Some(msg)) => Ok(Some(msg)),
            Ok(None) => {
                // Channel closed – the background task has exited.
                let _ = self.connections.write().await.remove(&connection.id);
                Err(DanmakuError::connection("Message channel closed"))
            }
            Err(_) => Ok(None), // Timeout – no message available right now.
        }
    }
}
