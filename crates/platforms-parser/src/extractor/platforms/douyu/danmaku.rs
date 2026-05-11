//! Douyu danmaku (chat) provider.
//!
//! Connects to Douyu's WebSocket-based danmaku stream using the STT
//! (Serialized Tag Text) protocol.
//!
//! ## Protocol overview
//!
//! 1. Open a binary WebSocket to `wss://danmuproxy.douyu.com:8506/`.
//! 2. Send login request + join-group request immediately (no wait for response).
//! 3. Send keepalive heartbeats every 45 seconds: `type@=mrkl/`
//! 4. Decode incoming STT messages and dispatch by `type`.
//!
//! ## Binary framing
//!
//! Uses the [`super::stt`] module for packet encoding/decoding.
//! Client messages use magic number `0xb1020000` (689).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use rustc_hash::FxHashMap;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use crate::danmaku::error::{DanmakuError, Result};
use crate::danmaku::event::{DanmuControlEvent, DanmuItem};
use crate::danmaku::message::DanmuMessage;
use crate::danmaku::provider::{ConnectionConfig, DanmuConnection, DanmuProvider};

use super::stt;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Douyu danmaku WebSocket endpoint.
const DOUYU_WS_URL: &str = "wss://danmuproxy.douyu.com:8506/";

/// Interval between heartbeat packets.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(45);

/// Maximum time `receive` will wait for a message before returning `Ok(None)`.
const RECEIVE_TIMEOUT: Duration = Duration::from_millis(100);

/// Maximum time to wait for the initial WebSocket handshake.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Channel buffer size for decoded danmu items.
const MESSAGE_CHANNEL_SIZE: usize = 256;

// ---------------------------------------------------------------------------
// Protocol helpers using stt module
// ---------------------------------------------------------------------------

/// Build a login STT message for the given room.
fn encode_login(room_id: &str) -> Vec<u8> {
    let payload = format!("type@=loginreq/roomid@={}/", room_id);
    stt::create_packet(&payload).to_vec()
}

/// Build a join-group STT message.
fn encode_join_group(room_id: &str) -> Vec<u8> {
    let payload = format!("type@=joingroup/rid@={}/gid@=-9999/", room_id);
    stt::create_packet(&payload).to_vec()
}

/// Build a logout STT message.
fn encode_logout() -> Vec<u8> {
    stt::create_packet("type@=logout/").to_vec()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract face URL from Douyu STT `uat` field.
/// In STT wire format, `uat` uses `//` as array separator.
/// Index 1 is the avatar URL path (CDN-relative).
fn extract_douyu_face(inner: &FxHashMap<String, String>) -> String {
    if let Some(uat) = inner.get("uat") {
        let parts: Vec<&str> = uat.split("//").collect();
        if parts.len() > 1 && !parts[1].is_empty() {
            return format!("https://{}", parts[1].trim());
        }
    }
    String::new()
}

// ---------------------------------------------------------------------------
// Danmu message dispatching
// ---------------------------------------------------------------------------

/// Dispatch a decoded STT message map into zero or more [`DanmuItem`]s.
fn dispatch_stt_message(msg: FxHashMap<String, String>) -> Vec<DanmuItem> {
    let msg_type = match msg.get("type") {
        Some(t) => t.as_str(),
        None => return vec![],
    };

    match msg_type {
        // ---- Chat message ----
        "chatmsg" => {
            let uid = msg.get("uid").cloned().unwrap_or_default();
            let nn = msg.get("nn").cloned().unwrap_or_default();
            let txt = msg.get("txt").cloned().unwrap_or_default();
            let msg_id = Uuid::new_v4().to_string();

            if txt.is_empty() {
                return vec![];
            }

            let mut danmu = DanmuMessage::chat(msg_id, uid, nn, txt);

            // Extract color info if present, but skip black/dark colors.
            if let Some(col) = msg.get("col") {
                if let Ok(col_int) = col.parse::<i64>() {
                    if col_int > 0 {
                        let r = ((col_int >> 16) & 0xFF) as u8;
                        let g = ((col_int >> 8) & 0xFF) as u8;
                        let b = (col_int & 0xFF) as u8;
                        // Treat very dark colors as white.
                        if r < 0x40 && g < 0x40 && b < 0x40 {
                            danmu = danmu.with_color("#FFFFFF");
                        } else {
                            danmu = danmu.with_color(format!("#{:06X}", (col_int & 0xFFFFFF) as u32));
                        }
                    }
                }
            }

            // Extract badge / level info.
            if let Some(level) = msg.get("level") {
                danmu = danmu.with_metadata("level", serde_json::json!(level));
            }

            vec![DanmuItem::Message(danmu)]
        }

        // ---- Gift message (ignored) ----
        "dgb" => {
            vec![]
        }

        // ---- User enter (ignored) ----
        "uenter" => {
            vec![]
        }

        // ---- Room status change (ignored) ----
        "rss" => {
            vec![]
        }

        // ---- Super chat: 付费弹幕 ----
        // The `chatmsg` field is a nested STT object with nn/txt/ic.
        "comm_chatmsg" => {
            let msg_id = Uuid::new_v4().to_string();
            let price = msg
                .get("cprice")
                .and_then(|v| v.parse::<u64>().ok())
                .map(|v| v / 100)
                .unwrap_or(0);
            let keep_time = msg
                .get("cet")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            // `now` is the current timestamp in seconds.
            let now = msg
                .get("now")
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            let start_time = now;
            let end_time = now + keep_time as i64;

            // Parse nested chatmsg STT for user info and face (ic field).
            let (nn, txt, face) = msg
                .get("chatmsg")
                .map(|nested| {
                    let inner = stt::stt_decode(nested);
                    let nn = inner.get("nn").cloned().unwrap_or_default();
                    let txt = inner.get("txt").cloned().unwrap_or_default();
                    let ic = inner.get("ic").cloned().unwrap_or_default();
                    let face = if ic.is_empty() {
                        String::new()
                    } else {
                        format!("https://apic.douyucdn.cn/upload/{ic}_small.jpg")
                    };
                    (nn, txt, face)
                })
                .unwrap_or_default();

            let danmu = DanmuMessage::super_chat(msg_id, "", nn, txt, price)
                .with_super_chat_keep_time(keep_time)
                .with_metadata("sc_type", serde_json::json!("comm_chatmsg"))
                .with_metadata("face", serde_json::json!(face))
                .with_metadata("start_time", serde_json::json!(start_time))
                .with_metadata("end_time", serde_json::json!(end_time))
                .with_metadata("background_color", serde_json::json!("#c1c1ff"))
                .with_metadata(
                    "background_bottom_color",
                    serde_json::json!("#292a60"),
                );

            vec![DanmuItem::Message(danmu)]
        }

        // ---- Super chat: 高能弹幕 ----
        // `list` is a nested STT array; we parse the first entry.
        "voice_trlt" => {
            let msg_id = Uuid::new_v4().to_string();

            let (nn, content, price, keep_time, face, start_time, end_time) = msg
                .get("list")
                .map(|nested| {
                    let inner = stt::stt_decode(nested);
                    let nn = inner.get("un").cloned().unwrap_or_default();
                    let content = inner.get("content").cloned().unwrap_or_default();
                    let price = inner
                        .get("realPrice")
                        .and_then(|v| v.parse::<u64>().ok())
                        .map(|v| v / 100)
                        .unwrap_or(0);
                    let keep_time = inner
                        .get("etime")
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(0);
                    let face = extract_douyu_face(&inner);
                    let start_time = inner
                        .get("acptime")
                        .and_then(|v| v.parse::<i64>().ok())
                        .unwrap_or(0);
                    let end_time = inner
                        .get("etime")
                        .and_then(|v| v.parse::<i64>().ok())
                        .unwrap_or(0);
                    (nn, content, price, keep_time, face, start_time, end_time)
                })
                .unwrap_or_default();

            let danmu = DanmuMessage::super_chat(msg_id, "", nn, content, price)
                .with_super_chat_keep_time(keep_time)
                .with_metadata("sc_type", serde_json::json!("voice_trlt"))
                .with_metadata("face", serde_json::json!(face))
                .with_metadata("start_time", serde_json::json!(start_time))
                .with_metadata("end_time", serde_json::json!(end_time))
                .with_metadata("background_color", serde_json::json!("#ffffff"))
                .with_metadata(
                    "background_bottom_color",
                    serde_json::json!("#246488"),
                );

            vec![DanmuItem::Message(danmu)]
        }

        // ---- Login response (loginres) ----
        "loginres" => {
            debug!("Douyu login response received");
            vec![]
        }

        // ---- Keepalive response ----
        "keeplive" | "keepalive" | "mrkl" => {
            trace!("Douyu keepalive response received");
            vec![]
        }

        // ---- Other messages ----
        _ => {
            trace!(type = msg_type, "Unhandled Douyu STT message type");
            vec![]
        }
    }
}

// ---------------------------------------------------------------------------
// Internal connection state
// ---------------------------------------------------------------------------

/// Per-connection bookkeeping kept inside [`DouyuDanmuProvider`].
struct DouyuConnectionState {
    /// Receiver end of the decoded-message channel.
    message_rx: Arc<Mutex<mpsc::Receiver<DanmuItem>>>,
    /// Sender used to signal the background task to shut down.
    shutdown_tx: Option<mpsc::Sender<()>>,
    /// Background task handles (WebSocket reader/heartbeat).
    tasks: Vec<JoinHandle<()>>,
}

impl DouyuConnectionState {
    /// Abort all spawned tasks immediately.
    fn abort_tasks(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }
}

impl Drop for DouyuConnectionState {
    fn drop(&mut self) {
        self.abort_tasks();
    }
}

// ---------------------------------------------------------------------------
// Background WebSocket task
// ---------------------------------------------------------------------------

/// Helper to send a `StreamClosed` error through the channel.
async fn send_close_event(tx: &mpsc::Sender<DanmuItem>, msg: String) {
    let _ = tx
        .send(DanmuItem::Control(DanmuControlEvent::StreamClosed {
            message: Some(msg),
            action: None,
        }))
        .await;
}

/// Long-running task that:
/// 1. Connects to the Douyu danmaku WebSocket.
/// 2. Sends login request + join-group request immediately (no wait for response).
/// 3. Sends periodic heartbeats.
/// 4. Decodes incoming frames and forwards [`DanmuItem`]s through `message_tx`.
async fn run_douyu_ws_task(
    room_id: String,
    message_tx: mpsc::Sender<DanmuItem>,
    mut shutdown_rx: mpsc::Receiver<()>,
) {
    // --- Connect ----------------------------------------------------------
    debug!(room_id = %room_id, url = DOUYU_WS_URL, "Douyu WS task starting, connecting...");
    let ws_stream = match tokio::time::timeout(CONNECT_TIMEOUT, connect_async(DOUYU_WS_URL)).await {
        Ok(Ok((stream, _response))) => stream,
        Ok(Err(e)) => {
            let msg = format!("Failed to connect to Douyu WebSocket: {e}");
            error!("{msg}");
            send_close_event(&message_tx, msg).await;
            return;
        }
        Err(_) => {
            let msg = "Timeout connecting to Douyu WebSocket".to_string();
            error!("{msg}");
            send_close_event(&message_tx, msg).await;
            return;
        }
    };

    let (mut ws_sink, mut ws_source) = ws_stream.split();
    info!(room_id = %room_id, "Douyu WebSocket connected successfully");

    // --- Login + Join group (send both immediately, matching Dart behavior) ---
    let login_data = encode_login(&room_id);
    debug!(room_id = %room_id, len = login_data.len(), "Sending loginreq packet");
    if let Err(e) = ws_sink.send(Message::Binary(login_data.into())).await {
        let msg = format!("Failed to send login request: {e}");
        error!("{msg}");
        send_close_event(&message_tx, msg).await;
        return;
    }
    info!(room_id = %room_id, "Login request sent OK");

    let join_data = encode_join_group(&room_id);
    debug!(room_id = %room_id, len = join_data.len(), "Sending joingroup packet");
    if let Err(e) = ws_sink.send(Message::Binary(join_data.into())).await {
        let msg = format!("Failed to send join group: {e}");
        error!("{msg}");
        send_close_event(&message_tx, msg).await;
        return;
    }
    info!(room_id = %room_id, "Join group sent OK, entering main loop");

    // --- Main loop --------------------------------------------------------
    let mut heartbeat_timer = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // `interval` fires immediately on the first tick – skip it.
    heartbeat_timer.tick().await;

    loop {
        tokio::select! {
            // Heartbeat
            _ = heartbeat_timer.tick() => {
                if let Err(e) = ws_sink.send(Message::Binary(stt::HEARTBEAT.to_vec().into())).await {
                    error!("Failed to send heartbeat: {e}");
                    break;
                }
                debug!(room_id = %room_id, "Heartbeat sent");
            }

            // Incoming WebSocket frame
            msg_opt = ws_source.next() => {
                match msg_opt {
                    Some(Ok(Message::Binary(data))) => {
                        let bytes: &[u8] = &data;
                        debug!(room_id = %room_id, frame_len = bytes.len(), "Received binary frame");

                        // Use stt::parse_packets to extract all STT payloads.
                        let payloads = stt::parse_packets(bytes);
                        if payloads.is_empty() {
                            warn!(room_id = %room_id, frame_len = bytes.len(),
                                "No STT payloads parsed from binary frame");
                        }
                        for stt_payload in payloads {
                            let stt_map = stt::stt_decode(&stt_payload);
                            let msg_type = stt_map.get("type").map(|s| s.as_str()).unwrap_or("?");
                            debug!(room_id = %room_id, stt_type = msg_type, "Parsed STT message");

                            // Dispatch the message.
                            let items = dispatch_stt_message(stt_map);
                            for item in &items {
                                debug!(room_id = %room_id, ?item, "Dispatched danmu item");
                            }
                            for item in items {
                                if message_tx.send(item).await.is_err() {
                                    // Consumer dropped – exit quietly.
                                    debug!(room_id = %room_id, "Message channel closed");
                                    return;
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Text(text))) => {
                        debug!(room_id = %room_id, len = text.len(),
                            "Received text frame: {}", &text[..text.len().min(200)]);
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!(room_id = %room_id, "WebSocket closed by server");
                        send_close_event(&message_tx, "WebSocket closed by server".to_string()).await;
                        break;
                    }
                    Some(Err(e)) => {
                        error!(room_id = %room_id, "WebSocket error: {e}");
                        send_close_event(&message_tx, format!("WebSocket error: {e}")).await;
                        break;
                    }
                    None => {
                        info!(room_id = %room_id, "WebSocket stream ended");
                        break;
                    }
                    _ => {
                        // Ignore ping / pong / raw frame messages.
                    }
                }
            }

            // Shutdown signal from `disconnect()`
            _ = shutdown_rx.recv() => {
                debug!(room_id = %room_id, "Shutdown signal received");
                // Send a logout message before closing.
                let logout_data = encode_logout();
                let _ = ws_sink.send(Message::Binary(logout_data.into())).await;
                let _ = ws_sink.close().await;
                return;
            }
        }
    }

    // Connection lost – make sure the socket is closed.
    let _ = ws_sink.close().await;
}

// ---------------------------------------------------------------------------
// DouyuDanmuProvider
// ---------------------------------------------------------------------------

/// Platform-specific danmaku provider for Douyu (斗鱼).
pub struct DouyuDanmuProvider {
    connections: tokio::sync::RwLock<HashMap<String, Arc<Mutex<DouyuConnectionState>>>>,
}

impl DouyuDanmuProvider {
    pub fn new() -> Self {
        Self {
            connections: tokio::sync::RwLock::new(HashMap::new()),
        }
    }
}

/// Convenience factory used by the [`ProviderRegistry`](crate::danmaku::ProviderRegistry).
pub fn create_douyu_danmu_provider() -> DouyuDanmuProvider {
    DouyuDanmuProvider::new()
}

#[async_trait]
impl DanmuProvider for DouyuDanmuProvider {
    fn platform(&self) -> &str {
        "douyu"
    }

    fn supports_url(&self, url: &str) -> bool {
        url.contains("douyu.com")
    }

    fn extract_room_id(&self, url: &str) -> Option<String> {
        let re = regex::Regex::new(r"(?:https?://)?(?:www\.)?douyu\.com/(\d+)").ok()?;
        re.captures(url)
            .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
    }

    async fn connect(&self, room_id: &str, config: ConnectionConfig) -> Result<DanmuConnection> {
        let _ = config; // Douyu doesn't need extra config for danmaku.

        info!(room_id = %room_id, "DouyuDanmuProvider::connect called");

        // Set up channels and spawn the background task.
        let connection_id = format!("douyu-{}-{}", room_id, Uuid::new_v4());
        let (message_tx, message_rx) = mpsc::channel(MESSAGE_CHANNEL_SIZE);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let room_id_owned = room_id.to_string();
        let handle = tokio::spawn(run_douyu_ws_task(room_id_owned, message_tx, shutdown_rx));
        debug!(room_id = %room_id, connection_id = %connection_id, "Background WS task spawned");

        let state = DouyuConnectionState {
            message_rx: Arc::new(Mutex::new(message_rx)),
            shutdown_tx: Some(shutdown_tx),
            tasks: vec![handle],
        };

        self.connections
            .write()
            .await
            .insert(connection_id.clone(), Arc::new(Mutex::new(state)));

        let mut conn = DanmuConnection::new(connection_id, "douyu", room_id);
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
        trace!(connection_id = %connection.id, "receive() called");

        // Look up the internal state for this connection.
        let state_arc = {
            let map = self.connections.read().await;
            let found = map.get(&connection.id).cloned();
            trace!(
                connection_id = %connection.id,
                map_size = map.len(),
                found = found.is_some(),
                "Connection lookup"
            );
            found
        };

        let Some(state_arc) = state_arc else {
            warn!(connection_id = %connection.id, "Connection not found in map");
            return Err(DanmakuError::connection("Connection not found"));
        };

        // Check if the background task is still alive.
        {
            let state = state_arc.lock().await;
            for (i, task) in state.tasks.iter().enumerate() {
                trace!(
                    connection_id = %connection.id,
                    task_index = i,
                    task_finished = task.is_finished(),
                    "Background task status"
                );
            }
        }

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

        match &next {
            Ok(Some(msg)) => {
                debug!(connection_id = %connection.id, ?msg, "receive() got message");
            }
            Ok(None) => {
                warn!(connection_id = %connection.id, "receive() channel closed — background task exited");
            }
            Err(_) => {
                trace!(connection_id = %connection.id, "receive() timed out (normal)");
            }
        }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::danmaku::message::DanmuType;

    // ------------------------------------------------------------------
    // Message dispatch tests
    // ------------------------------------------------------------------

    #[test]
    fn test_dispatch_chatmsg() {
        let mut msg = FxHashMap::default();
        msg.insert("type".to_string(), "chatmsg".to_string());
        msg.insert("uid".to_string(), "12345".to_string());
        msg.insert("nn".to_string(), "TestUser".to_string());
        msg.insert("txt".to_string(), "Hello world!".to_string());

        let items = dispatch_stt_message(msg);
        assert_eq!(items.len(), 1);

        if let DanmuItem::Message(danmu) = &items[0] {
            assert_eq!(danmu.user_id, "12345");
            assert_eq!(danmu.username, "TestUser");
            assert_eq!(danmu.content, "Hello world!");
            assert_eq!(danmu.message_type, DanmuType::Chat);
        } else {
            panic!("Expected DanmuItem::Message");
        }
    }

    #[test]
    fn test_dispatch_chatmsg_empty_text() {
        let mut msg = FxHashMap::default();
        msg.insert("type".to_string(), "chatmsg".to_string());
        msg.insert("uid".to_string(), "12345".to_string());
        msg.insert("nn".to_string(), "TestUser".to_string());
        msg.insert("txt".to_string(), "".to_string());

        let items = dispatch_stt_message(msg);
        assert!(items.is_empty());
    }

    #[test]
    fn test_dispatch_gift_ignored() {
        let mut msg = FxHashMap::default();
        msg.insert("type".to_string(), "dgb".to_string());
        msg.insert("uid".to_string(), "12345".to_string());
        msg.insert("nn".to_string(), "GiftUser".to_string());
        msg.insert("gfid".to_string(), "42".to_string());
        msg.insert("gfcnt".to_string(), "5".to_string());
        msg.insert("gfn".to_string(), "火箭".to_string());

        let items = dispatch_stt_message(msg);
        assert!(items.is_empty());
    }

    #[test]
    fn test_dispatch_uenter_ignored() {
        let mut msg = FxHashMap::default();
        msg.insert("type".to_string(), "uenter".to_string());
        msg.insert("uid".to_string(), "12345".to_string());
        msg.insert("nn".to_string(), "NewUser".to_string());

        let items = dispatch_stt_message(msg);
        assert!(items.is_empty());
    }

    #[test]
    fn test_dispatch_comm_chatmsg() {
        let mut msg = FxHashMap::default();
        msg.insert("type".to_string(), "comm_chatmsg".to_string());
        // chatmsg is a nested STT string
        msg.insert(
            "chatmsg".to_string(),
            "nn@=大佬/txt@=加油！/ic@=avatar123/".to_string(),
        );
        msg.insert("cprice".to_string(), "500000".to_string());
        msg.insert("cet".to_string(), "60".to_string());

        let items = dispatch_stt_message(msg);
        assert_eq!(items.len(), 1);

        if let DanmuItem::Message(danmu) = &items[0] {
            assert_eq!(danmu.message_type, DanmuType::SuperChat);
            assert_eq!(danmu.username, "大佬");
            assert_eq!(danmu.content, "加油！");
            let meta = danmu.metadata.as_ref().unwrap();
            assert_eq!(meta.get("price").unwrap(), &serde_json::json!(5000));
            assert_eq!(meta.get("keep_time").unwrap(), &serde_json::json!(60));
            assert_eq!(
                meta.get("sc_type").unwrap(),
                &serde_json::json!("comm_chatmsg")
            );
        } else {
            panic!("Expected DanmuItem::Message");
        }
    }

    #[test]
    fn test_dispatch_voice_trlt() {
        let mut msg = FxHashMap::default();
        msg.insert("type".to_string(), "voice_trlt".to_string());
        // list is a nested STT string for the first entry
        msg.insert("list".to_string(), "un@=测试用户/content@=高能弹幕来了！/realPrice@=200000/etime@=1800000000/acptime@=1799999900/".to_string());

        let items = dispatch_stt_message(msg);
        assert_eq!(items.len(), 1);

        if let DanmuItem::Message(danmu) = &items[0] {
            assert_eq!(danmu.message_type, DanmuType::SuperChat);
            assert_eq!(danmu.username, "测试用户");
            assert_eq!(danmu.content, "高能弹幕来了！");
            let meta = danmu.metadata.as_ref().unwrap();
            assert_eq!(meta.get("price").unwrap(), &serde_json::json!(2000));
            assert_eq!(
                meta.get("sc_type").unwrap(),
                &serde_json::json!("voice_trlt")
            );
        } else {
            panic!("Expected DanmuItem::Message");
        }
    }

    #[test]
    fn test_dispatch_loginres() {
        let mut msg = FxHashMap::default();
        msg.insert("type".to_string(), "loginres".to_string());

        let items = dispatch_stt_message(msg);
        assert!(items.is_empty()); // loginres produces no user-facing messages
    }

    #[test]
    fn test_dispatch_unknown_type() {
        let mut msg = FxHashMap::default();
        msg.insert("type".to_string(), "something_unknown".to_string());

        let items = dispatch_stt_message(msg);
        assert!(items.is_empty());
    }

    // ------------------------------------------------------------------
    // Protocol helper tests
    // ------------------------------------------------------------------

    #[test]
    fn test_encode_login() {
        let frame = encode_login("12345");
        // Parse back using stt module
        let (payload, _) = stt::parse_packet(&frame).unwrap();
        let map = stt::stt_decode(&payload);
        assert_eq!(map.get("type").unwrap(), "loginreq");
        assert_eq!(map.get("roomid").unwrap(), "12345");
    }

    #[test]
    fn test_encode_join_group() {
        let frame = encode_join_group("12345");
        let (payload, _) = stt::parse_packet(&frame).unwrap();
        let map = stt::stt_decode(&payload);
        assert_eq!(map.get("type").unwrap(), "joingroup");
        assert_eq!(map.get("rid").unwrap(), "12345");
        assert_eq!(map.get("gid").unwrap(), "-9999");
    }

    #[test]
    fn test_encode_logout() {
        let frame = encode_logout();
        let (payload, _) = stt::parse_packet(&frame).unwrap();
        let map = stt::stt_decode(&payload);
        assert_eq!(map.get("type").unwrap(), "logout");
    }

    // ------------------------------------------------------------------
    // Provider method tests
    // ------------------------------------------------------------------

    #[test]
    fn test_extract_room_id_standard_url() {
        let provider = DouyuDanmuProvider::new();
        assert_eq!(
            provider.extract_room_id("https://www.douyu.com/12345"),
            Some("12345".to_string())
        );
    }

    #[test]
    fn test_extract_room_id_no_www() {
        let provider = DouyuDanmuProvider::new();
        assert_eq!(
            provider.extract_room_id("https://douyu.com/67890"),
            Some("67890".to_string())
        );
    }

    #[test]
    fn test_extract_room_id_no_protocol() {
        let provider = DouyuDanmuProvider::new();
        assert_eq!(
            provider.extract_room_id("www.douyu.com/111"),
            Some("111".to_string())
        );
    }

    #[test]
    fn test_extract_room_id_invalid_url() {
        let provider = DouyuDanmuProvider::new();
        assert_eq!(provider.extract_room_id("https://example.com/123"), None);
    }

    #[test]
    fn test_supports_url() {
        let provider = DouyuDanmuProvider::new();
        assert!(provider.supports_url("https://www.douyu.com/12345"));
        assert!(!provider.supports_url("https://www.huya.com/12345"));
    }

    #[test]
    fn test_platform() {
        let provider = DouyuDanmuProvider::new();
        assert_eq!(provider.platform(), "douyu");
    }
}
