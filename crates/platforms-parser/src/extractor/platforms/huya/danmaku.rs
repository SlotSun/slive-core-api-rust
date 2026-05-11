//! Huya danmaku (chat) provider.
//!
//! Connects to Huya's WebSocket-based danmaku stream using a TARS-encoded protocol.
//!
//! Protocol overview:
//! 1. Open a WebSocket to `wss://cdnws.api.huya.com`.
//! 2. Send a TARS-encoded join-room message (`encode_join_room`).
//! 3. Send a binary heartbeat every 60 seconds.
//! 4. Decode incoming binary frames through the TARS pipeline:
//!    `HuyaWsMessage` → `HYPushMessage` → dispatch by `uri`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
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
use crate::extractor::platforms::huya::tars::push_message::{
    HEARTBEAT_DATA, HYMessage, HYPushMessage, HuyaWsMessage, encode_join_room,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Huya danmaku WebSocket endpoint.
const HUYA_WS_URL: &str = "wss://cdnws.api.huya.com";

/// Interval between heartbeat packets.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

/// Maximum time `receive` will wait for a message before returning `Ok(None)`.
const RECEIVE_TIMEOUT: Duration = Duration::from_millis(100);

/// Maximum time to wait for the initial WebSocket handshake.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Channel buffer size for decoded danmu items.
const MESSAGE_CHANNEL_SIZE: usize = 256;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a Huya `fontColor` value to a `#RRGGBB` hex string.
///
/// Returns `None` when the value should be treated as the default (white).
///
/// Dart's `numberToColor` converts the int to a hex string and only parses
/// it when the string length is 6 or 8.  Small values like 0, 1, 255 produce
/// short hex strings (1-2 chars) and fall through to white.  We replicate
/// that behaviour by requiring the value to occupy at least 24 bits (>= 0x100000).
fn font_color_to_hex(color: i64) -> Option<String> {
    if color < 0x100000 {
        None
    } else {
        Some(format!("#{:06X}", (color & 0xFFFFFF) as u32))
    }
}

/// Decode a raw binary WebSocket frame into zero or more [`DanmuItem`]s.
///
/// The decoding pipeline is:
///
/// 1. [`HuyaWsMessage::decode`] – outer envelope (tag 0: type, tag 1: data).
/// 2. If `msg_type == 7`, decode the inner data as [`HYPushMessage`].
/// 3. Dispatch by `uri`:
///    - **1400** → chat message ([`HYMessage::decode`])
///    - **8006** → online viewer count (first big-endian `i32` in `msg`)
fn decode_huya_frame(data: &[u8]) -> std::result::Result<Vec<DanmuItem>, String> {
    // 1. Outer envelope
    let ws_msg =
        HuyaWsMessage::decode(data).map_err(|e| format!("HuyaWsMessage decode error: {e}"))?;

    if ws_msg.msg_type != 7 {
        // Non-push messages (e.g. heartbeat ack) are silently ignored.
        return Ok(vec![]);
    }

    // 2. Push message
    let push_msg = HYPushMessage::decode(&ws_msg.data)
        .map_err(|e| format!("HYPushMessage decode error: {e}"))?;

    // 3. Dispatch by URI
    match push_msg.uri {
        // ---- Chat message ----
        1400 => {
            let hy_msg = HYMessage::decode(&push_msg.msg)
                .map_err(|e| format!("HYMessage decode error: {e}"))?;

            let msg_id = Uuid::new_v4().to_string();
            let user_id = hy_msg.sender.uid.to_string();
            let username = hy_msg.sender.nick_name;
            let content = hy_msg.content;

            let mut danmu = DanmuMessage::chat(msg_id, user_id, username, content);

            if let Some(color) = font_color_to_hex(hy_msg.font_color) {
                danmu = danmu.with_color(color);
            }

            Ok(vec![DanmuItem::Message(danmu)])
        }

        // ---- Online viewer count (ignored) ----
        8006 => {
            Ok(vec![])
        }

        _ => {
            debug!(uri = push_msg.uri, "Unhandled Huya push message URI");
            Ok(vec![])
        }
    }
}

// ---------------------------------------------------------------------------
// Internal connection state
// ---------------------------------------------------------------------------

/// Per-connection bookkeeping kept inside [`HuyaDanmuProvider`].
struct HuyaConnectionState {
    /// Receiver end of the decoded-message channel.
    message_rx: Arc<Mutex<mpsc::Receiver<DanmuItem>>>,
    /// Sender used to signal the background task to shut down.
    shutdown_tx: Option<mpsc::Sender<()>>,
    /// Background task handles (WebSocket reader/heartbeat).
    tasks: Vec<JoinHandle<()>>,
}

impl HuyaConnectionState {
    /// Abort all spawned tasks immediately.
    fn abort_tasks(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }
}

impl Drop for HuyaConnectionState {
    fn drop(&mut self) {
        self.abort_tasks();
    }
}

// ---------------------------------------------------------------------------
// Background WebSocket task
// ---------------------------------------------------------------------------

/// Long-running task that:
/// 1. Connects to the Huya danmaku WebSocket.
/// 2. Sends the TARS-encoded join-room packet.
/// 3. Sends periodic heartbeats.
/// 4. Decodes incoming frames and forwards [`DanmuItem`]s through `message_tx`.
async fn run_huya_ws_task(
    room_id: String,
    ayyuid: i64,
    top_sid: i64,
    sub_sid: i64,
    message_tx: mpsc::Sender<DanmuItem>,
    mut shutdown_rx: mpsc::Receiver<()>,
) {
    // --- Connect ----------------------------------------------------------
    let ws_url = format!("{HUYA_WS_URL}/");
    let ws_stream = match tokio::time::timeout(CONNECT_TIMEOUT, connect_async(&ws_url)).await {
        Ok(Ok((stream, _response))) => stream,
        Ok(Err(e)) => {
            error!("Failed to connect to Huya WebSocket: {e}");
            return;
        }
        Err(_) => {
            error!("Timeout connecting to Huya WebSocket");
            return;
        }
    };

    let (mut ws_sink, mut ws_source) = ws_stream.split();
    info!(room_id = %room_id, "Connected to Huya WebSocket");

    // --- Join room --------------------------------------------------------
    match encode_join_room(ayyuid, top_sid, sub_sid) {
        Ok(join_data) => {
            if let Err(e) = ws_sink.send(Message::Binary(Bytes::from(join_data))).await {
                error!("Failed to send join room message: {e}");
                return;
            }
            debug!(room_id = %room_id, "Sent join room message");
        }
        Err(e) => {
            error!("Failed to encode join room message: {e}");
            return;
        }
    }

    // --- Main loop --------------------------------------------------------
    let mut heartbeat_timer = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // `interval` fires immediately on the first tick – skip it.
    heartbeat_timer.tick().await;

    loop {
        tokio::select! {
            // Heartbeat
            _ = heartbeat_timer.tick() => {
                if let Err(e) = ws_sink.send(Message::Binary(Bytes::from_static(HEARTBEAT_DATA))).await {
                    error!("Failed to send heartbeat: {e}");
                    break;
                }
                trace!(room_id = %room_id, "Sent heartbeat");
            }

            // Incoming WebSocket frame
            msg_opt = ws_source.next() => {
                match msg_opt {
                    Some(Ok(Message::Binary(data))) => {
                        match decode_huya_frame(&data) {
                            Ok(items) => {
                                for item in items {
                                    if message_tx.send(item).await.is_err() {
                                        // Consumer dropped – exit quietly.
                                        debug!(room_id = %room_id, "Message channel closed");
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to decode Huya message: {e}");
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!(room_id = %room_id, "WebSocket closed by server");
                        let _ = message_tx
                            .send(DanmuItem::Control(DanmuControlEvent::StreamClosed {
                                message: Some("WebSocket closed by server".to_string()),
                                action: None,
                            }))
                            .await;
                        break;
                    }
                    Some(Err(e)) => {
                        error!(room_id = %room_id, "WebSocket error: {e}");
                        break;
                    }
                    None => {
                        info!(room_id = %room_id, "WebSocket stream ended");
                        break;
                    }
                    _ => {
                        // Ignore text / ping / pong / raw frame messages.
                    }
                }
            }

            // Shutdown signal from `disconnect()`
            _ = shutdown_rx.recv() => {
                debug!(room_id = %room_id, "Shutdown signal received");
                let _ = ws_sink.close().await;
                return;
            }
        }
    }

    // Connection lost – make sure the socket is closed.
    let _ = ws_sink.close().await;
}

// ---------------------------------------------------------------------------
// HuyaDanmuProvider
// ---------------------------------------------------------------------------

/// Platform-specific danmaku provider for Huya (虎牙).
pub struct HuyaDanmuProvider {
    connections: tokio::sync::RwLock<HashMap<String, Arc<Mutex<HuyaConnectionState>>>>,
}

impl HuyaDanmuProvider {
    pub fn new() -> Self {
        Self {
            connections: tokio::sync::RwLock::new(HashMap::new()),
        }
    }
}

/// Convenience factory used by the [`ProviderRegistry`](crate::danmaku::ProviderRegistry).
pub fn create_huya_danmu_provider() -> HuyaDanmuProvider {
    HuyaDanmuProvider::new()
}

#[async_trait]
impl DanmuProvider for HuyaDanmuProvider {
    fn platform(&self) -> &str {
        "huya"
    }

    fn supports_url(&self, url: &str) -> bool {
        url.contains("huya.com")
    }

    fn extract_room_id(&self, url: &str) -> Option<String> {
        let re = regex::Regex::new(r"(?:https?://)?(?:www\.)?huya\.com/(\d+)").ok()?;
        re.captures(url)
            .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
    }

    async fn connect(&self, room_id: &str, config: ConnectionConfig) -> Result<DanmuConnection> {
        let extras = config.extras.unwrap_or_default();

        // Parse required parameters from `config.extras`.
        let ayyuid = extras
            .get("ayyuid")
            .and_then(|v| v.parse::<i64>().ok())
            .ok_or_else(|| {
                DanmakuError::connection("Missing or invalid 'ayyuid' in config.extras")
            })?;

        let top_sid = extras
            .get("top_sid")
            .and_then(|v| v.parse::<i64>().ok())
            .ok_or_else(|| {
                DanmakuError::connection("Missing or invalid 'top_sid' in config.extras")
            })?;

        let sub_sid = extras
            .get("sub_sid")
            .and_then(|v| v.parse::<i64>().ok())
            .ok_or_else(|| {
                DanmakuError::connection("Missing or invalid 'sub_sid' in config.extras")
            })?;

        // Set up channels and spawn the background task.
        let connection_id = format!("huya-{}-{}", room_id, Uuid::new_v4());
        let (message_tx, message_rx) = mpsc::channel(MESSAGE_CHANNEL_SIZE);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let room_id_owned = room_id.to_string();
        let handle = tokio::spawn(run_huya_ws_task(
            room_id_owned,
            ayyuid,
            top_sid,
            sub_sid,
            message_tx,
            shutdown_rx,
        ));

        let state = HuyaConnectionState {
            message_rx: Arc::new(Mutex::new(message_rx)),
            shutdown_tx: Some(shutdown_tx),
            tasks: vec![handle],
        };

        self.connections
            .write()
            .await
            .insert(connection_id.clone(), Arc::new(Mutex::new(state)));

        let mut conn = DanmuConnection::new(connection_id, "huya", room_id);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_font_color_to_hex_white_ignored() {
        assert_eq!(font_color_to_hex(0), None);
        assert_eq!(font_color_to_hex(-1), None);
        assert_eq!(font_color_to_hex(255), None);     // "ff" → too short → white
        assert_eq!(font_color_to_hex(0xFFFFF), None); // 5 hex digits → too short
    }

    #[test]
    fn test_font_color_to_hex_red() {
        assert_eq!(font_color_to_hex(0xFF0000), Some("#FF0000".to_string()));
        assert_eq!(font_color_to_hex(0x100000), Some("#100000".to_string()));
    }

    #[test]
    fn test_font_color_to_hex_green() {
        // 0x00FF00 = 65280, hex "ff00" (length 4) → Dart returns white
        assert_eq!(font_color_to_hex(0x00FF00), None);
        // 0x10FF00 >= 0x100000 → valid 6-digit color "#10FF00"
        assert_eq!(font_color_to_hex(0x10FF00), Some("#10FF00".to_string()));
    }

    #[test]
    fn test_font_color_to_hex_mixed() {
        assert_eq!(font_color_to_hex(0x123456), Some("#123456".to_string()));
    }

    #[test]
    fn test_font_color_to_hex_upper_bits_masked() {
        // Upper bits beyond 24 should be masked off.
        assert_eq!(font_color_to_hex(0x1_FF0000), Some("#FF0000".to_string()));
    }

    #[test]
    fn test_decode_non_push_message_returns_empty() {
        // A minimal TARS frame with msg_type != 7 should yield no items.
        // We construct a trivial binary that decodes to msg_type = 0.
        let mut ser = tars_codec::ser::TarsSerializer::new();
        ser.write_i32(0, 0).unwrap(); // tag 0 = msg_type 0
        let _ = ser.write_simple_list(1, &[]); // tag 1 = empty data
        let data = ser.into_bytes();

        let items = decode_huya_frame(&data).unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn test_extract_room_id_standard_url() {
        let provider = HuyaDanmuProvider::new();
        assert_eq!(
            provider.extract_room_id("https://www.huya.com/12345"),
            Some("12345".to_string())
        );
    }

    #[test]
    fn test_extract_room_id_no_www() {
        let provider = HuyaDanmuProvider::new();
        assert_eq!(
            provider.extract_room_id("https://huya.com/67890"),
            Some("67890".to_string())
        );
    }

    #[test]
    fn test_extract_room_id_no_protocol() {
        let provider = HuyaDanmuProvider::new();
        assert_eq!(
            provider.extract_room_id("www.huya.com/111"),
            Some("111".to_string())
        );
    }

    #[test]
    fn test_extract_room_id_invalid_url() {
        let provider = HuyaDanmuProvider::new();
        assert_eq!(provider.extract_room_id("https://example.com/123"), None);
    }

    #[test]
    fn test_supports_url() {
        let provider = HuyaDanmuProvider::new();
        assert!(provider.supports_url("https://www.huya.com/12345"));
        assert!(!provider.supports_url("https://www.douyu.com/12345"));
    }
}
