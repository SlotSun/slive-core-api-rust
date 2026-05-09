//! Douyin (抖音) danmaku provider.
//!
//! Connects to Douyin's WebSocket-based danmaku stream using Protocol Buffers.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use flate2::read::GzDecoder;
use futures::{SinkExt, StreamExt};
use md5::{Digest, Md5};
use prost::Message as ProstMessage;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::http;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use crate::danmaku::error::{DanmakuError, Result};
use crate::danmaku::event::{DanmuControlEvent, DanmuItem};
use crate::danmaku::message::{DanmuMessage, DanmuType};
use crate::danmaku::provider::{ConnectionConfig, DanmuConnection, DanmuProvider};
use crate::USER_AGENT;

// ===========================================================================
// WebSocket hosts
// ===========================================================================

const DOUYIN_WS_HOSTS: &[&str] = &[
    "wss://webcast100-ws-web-lq.douyin.com",
    "wss://webcast100-ws-web-hl.douyin.com",
    "wss://webcast100-ws-web-lf.douyin.com",
];

const DOUYIN_WS_PATH: &str = "/webcast/im/push/v2/";

// ===========================================================================
// Inline Protobuf types
// ===========================================================================

#[derive(Clone, PartialEq, ProstMessage)]
pub struct PushFrame {
    #[prost(uint64, tag = "1")]
    pub seq_id: u64,
    #[prost(uint64, tag = "2")]
    pub log_id: u64,
    #[prost(int32, tag = "3")]
    pub service: i32,
    #[prost(int32, tag = "4")]
    pub method: i32,
    #[prost(message, repeated, tag = "5")]
    pub headers: Vec<PushHeader>,
    #[prost(string, tag = "6")]
    pub payload_encoding: String,
    #[prost(string, tag = "7")]
    pub payload_type: String,
    #[prost(bytes, tag = "8")]
    pub payload: Vec<u8>,
    #[prost(string, tag = "9")]
    pub lod_id_new: String,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct PushHeader {
    #[prost(string, tag = "1")]
    pub key: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct DouyinResponse {
    #[prost(message, repeated, tag = "1")]
    pub messages: Vec<ImMessage>,
    #[prost(string, tag = "2")]
    pub cursor: String,
    #[prost(uint64, tag = "3")]
    pub fetch_interval: u64,
    #[prost(uint64, tag = "4")]
    pub now: u64,
    #[prost(bytes, tag = "5")]
    pub internal_ext: Vec<u8>,
    #[prost(int32, tag = "6")]
    pub fetch_type: i32,
    #[prost(map = "string, string", tag = "7")]
    pub route_params: HashMap<String, String>,
    #[prost(uint64, tag = "8")]
    pub heartbeat_duration: u64,
    #[prost(bool, tag = "9")]
    pub need_ack: bool,
    #[prost(string, tag = "10")]
    pub push_server: String,
    #[prost(string, tag = "11")]
    pub live_cursor: String,
    #[prost(bool, tag = "12")]
    pub history_no_more: bool,
    #[prost(string, tag = "13")]
    pub proxy_server: String,
    #[prost(string, tag = "14")]
    pub push_server_v2: String,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct ImMessage {
    #[prost(string, tag = "1")]
    pub method: String,
    #[prost(bytes, tag = "2")]
    pub payload: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub msg_id: u64,
    #[prost(int32, tag = "4")]
    pub msg_type: i32,
    #[prost(uint64, tag = "5")]
    pub offset: u64,
    #[prost(bool, tag = "6")]
    pub need_wrds_store: bool,
    #[prost(uint64, tag = "7")]
    pub wrds_version: u64,
    #[prost(string, tag = "8")]
    pub wrds_sub_key: String,
    #[prost(map = "string, string", tag = "9")]
    pub message_extra: HashMap<String, String>,
    #[prost(string, tag = "10")]
    pub tenant_id: String,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct Common {
    #[prost(string, tag = "1")]
    pub method: String,
    #[prost(uint64, tag = "2")]
    pub msg_id: u64,
    #[prost(uint64, tag = "3")]
    pub room_id: u64,
    #[prost(uint64, tag = "4")]
    pub create_time: u64,
    #[prost(int32, tag = "5")]
    pub monitor: i32,
    #[prost(bool, tag = "6")]
    pub is_show_msg: bool,
    #[prost(string, tag = "7")]
    pub describe: String,
    #[prost(message, optional, tag = "15")]
    pub user: Option<DataUser>,
    #[prost(message, optional, tag = "16")]
    pub room: Option<DataRoom>,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct DataUser {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(uint64, tag = "2")]
    pub short_id: u64,
    #[prost(string, tag = "3")]
    pub nickname: String,
    #[prost(int32, tag = "4")]
    pub gender: i32,
    #[prost(string, tag = "38")]
    pub display_id: String,
    #[prost(string, tag = "67")]
    pub web_rid: String,
    #[prost(string, tag = "1028")]
    pub id_str: String,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct DataRoom {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(string, tag = "2")]
    pub id_str: String,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct DataImage {
    #[prost(string, repeated, tag = "1")]
    pub url_list: Vec<String>,
    #[prost(string, tag = "2")]
    pub uri: String,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct DataGiftStruct {
    #[prost(message, optional, tag = "1")]
    pub image: Option<DataImage>,
    #[prost(string, tag = "2")]
    pub describe: String,
    #[prost(uint64, tag = "5")]
    pub id: u64,
    #[prost(int32, tag = "12")]
    pub diamond_count: i32,
    #[prost(string, tag = "16")]
    pub name: String,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct ChatMessage {
    #[prost(message, optional, tag = "1")]
    pub common: Option<Common>,
    #[prost(message, optional, tag = "2")]
    pub user: Option<DataUser>,
    #[prost(string, tag = "3")]
    pub content: String,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct GiftMessage {
    #[prost(message, optional, tag = "1")]
    pub common: Option<Common>,
    #[prost(uint64, tag = "2")]
    pub gift_id: u64,
    #[prost(uint64, tag = "3")]
    pub fan_ticket_count: u64,
    #[prost(uint64, tag = "4")]
    pub group_count: u64,
    #[prost(uint64, tag = "5")]
    pub repeat_count: u64,
    #[prost(message, optional, tag = "7")]
    pub user: Option<DataUser>,
    #[prost(message, optional, tag = "8")]
    pub to_user: Option<DataUser>,
    #[prost(int32, tag = "9")]
    pub repeat_end: i32,
    #[prost(message, optional, tag = "15")]
    pub gift: Option<DataGiftStruct>,
    #[prost(uint64, tag = "29")]
    pub total_count: u64,
    #[prost(uint64, tag = "44")]
    pub count: u64,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct MemberMessage {
    #[prost(message, optional, tag = "1")]
    pub common: Option<Common>,
    #[prost(message, optional, tag = "2")]
    pub user: Option<DataUser>,
    #[prost(uint64, tag = "3")]
    pub member_count: u64,
    #[prost(uint64, tag = "9")]
    pub enter_type: u64,
    #[prost(uint64, tag = "10")]
    pub action: u64,
    #[prost(string, tag = "11")]
    pub action_description: String,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct SocialMessage {
    #[prost(message, optional, tag = "1")]
    pub common: Option<Common>,
    #[prost(message, optional, tag = "2")]
    pub user: Option<DataUser>,
    #[prost(uint64, tag = "3")]
    pub share_type: u64,
    #[prost(uint64, tag = "4")]
    pub action: u64,
    #[prost(string, tag = "5")]
    pub share_target: String,
    #[prost(uint64, tag = "6")]
    pub follow_count: u64,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct ControlMessage {
    #[prost(uint64, tag = "2")]
    pub action: u64,
    #[prost(string, tag = "3")]
    pub tips: String,
}

#[derive(Clone, PartialEq, ProstMessage)]
pub struct RoomStatsMessage {
    #[prost(message, optional, tag = "1")]
    pub common: Option<Common>,
    #[prost(string, tag = "2")]
    pub display_short: String,
    #[prost(string, tag = "3")]
    pub display_middle: String,
    #[prost(string, tag = "4")]
    pub display_long: String,
    #[prost(uint64, tag = "5")]
    pub display_value: u64,
    #[prost(uint64, tag = "9")]
    pub total: u64,
}

// ===========================================================================
// Constants
// ===========================================================================

const HEARTBEAT: &[u8] = b":\x02hb";
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const RECEIVE_TIMEOUT: Duration = Duration::from_millis(100);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const MESSAGE_CHANNEL_SIZE: usize = 512;

const VERSION_CODE: &str = "180800";
const WEBCAST_SDK_VERSION: &str = "1.0.15";

// ===========================================================================
// Helpers
// ===========================================================================

/// Generate a random user unique ID (timestamp-based, like reference).
fn generate_user_unique_id() -> String {
    let base = 7300000000000000000u64;
    let range = 699999999999999999u64;
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    (base + (ts % range)).to_string()
}

/// MD5 hash returning 32-byte hex representation.
fn md5_hex(input: &str) -> [u8; 32] {
    let hash = Md5::digest(input.as_bytes());
    let mut result = [0u8; 32];
    for (i, byte) in hash.iter().enumerate() {
        let hi = byte >> 4;
        let lo = byte & 0x0f;
        result[i * 2] = if hi < 10 { b'0' + hi } else { b'a' + hi - 10 };
        result[i * 2 + 1] = if lo < 10 { b'0' + lo } else { b'a' + lo - 10 };
    }
    result
}

/// Get the ttwid cookie value (from global manager or default).
fn get_ttwid() -> String {
    crate::extractor::platforms::douyin::utils::GlobalTtwidManager::get_global_ttwid()
        .unwrap_or_else(|| crate::extractor::platforms::douyin::utils::DEFAULT_TTWID.to_string())
}

/// Build the WebSocket URL with signature.
fn build_ws_url(room_id: &str, user_unique_id: &str) -> String {
    // Build query params from common params + websocket-specific params.
    let common = crate::extractor::platforms::douyin::utils::get_common_params();
    let mut params: Vec<(&str, String)> = Vec::new();

    // Common params
    for (k, v) in &common {
        params.push((k, v.to_string()));
    }

    // WebSocket-specific params
    params.push(("version_code", VERSION_CODE.to_string()));
    params.push(("webcast_sdk_version", WEBCAST_SDK_VERSION.to_string()));
    params.push(("update_version_code", WEBCAST_SDK_VERSION.to_string()));
    params.push(("host", "https://live.douyin.com".to_string()));
    params.push(("did_rule", "3".to_string()));
    params.push(("identity", "audience".to_string()));
    params.push(("endpoint", "live_pc".to_string()));
    params.push(("need_persist_msg_count", "15".to_string()));
    params.push(("heartbeatDuration", "0".to_string()));
    params.push(("room_id", room_id.to_string()));
    params.push(("user_unique_id", user_unique_id.to_string()));

    // Build query string for URL
    let query: String = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    // Build signature input (comma-separated specific format)
    let signature_input = format!(
        "live_id=1,aid=6383,version_code={},webcast_sdk_version={},\
         room_id={},sub_room_id=,sub_channel_id=,did_rule=3,\
         user_unique_id={},device_platform=web,device_type=,ac=,identity=audience",
        VERSION_CODE, WEBCAST_SDK_VERSION, room_id, user_unique_id
    );

    let md5_hash = md5_hex(&signature_input);
    let signature_bytes =
        crate::extractor::platforms::douyin::signature::generate_xbogus(&md5_hash, 1);
    // SAFETY: result contains only ASCII from XBOGUS_ALPHABET
    let signature = unsafe { std::str::from_utf8_unchecked(&signature_bytes) };

    // Pick a random host
    use rand::seq::IndexedRandom;
    let host = DOUYIN_WS_HOSTS
        .choose(&mut rand::rng())
        .unwrap_or(&DOUYIN_WS_HOSTS[0]);

    format!("{host}{DOUYIN_WS_PATH}?{query}&signature={signature}")
}

/// Decompress a gzip-compressed byte slice.
fn gzip_decompress(data: &[u8]) -> std::result::Result<Vec<u8>, String> {
    let mut decoder = GzDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| format!("gzip decompression failed: {e}"))?;
    Ok(decompressed)
}

/// Build an ACK PushFrame to acknowledge receipt of messages.
fn build_ack_frame(log_id: u64, internal_ext: Vec<u8>) -> Vec<u8> {
    let frame = PushFrame {
        log_id,
        payload_type: "ack".to_string(),
        payload: internal_ext,
        ..Default::default()
    };
    let mut buf = Vec::new();
    frame.encode(&mut buf).expect("PushFrame encode failed");
    buf
}

fn user_id(user: &DataUser) -> String {
    if !user.id_str.is_empty() {
        user.id_str.clone()
    } else if user.id != 0 {
        user.id.to_string()
    } else {
        "0".to_string()
    }
}

fn user_name(user: &DataUser) -> String {
    if !user.nickname.is_empty() {
        user.nickname.clone()
    } else if !user.display_id.is_empty() {
        user.display_id.clone()
    } else {
        "unknown".to_string()
    }
}

// ===========================================================================
// Frame decoding
// ===========================================================================

struct DecodedFrame {
    items: Vec<DanmuItem>,
    ack_log_id: Option<u64>,
    internal_ext: Vec<u8>,
}

fn decode_douyin_frame(data: &[u8]) -> std::result::Result<DecodedFrame, String> {
    let frame = PushFrame::decode(Bytes::from(data.to_vec()))
        .map_err(|e| format!("PushFrame decode error: {e}"))?;

    // Check compression type from headers
    let compress_type = frame
        .headers
        .iter()
        .find(|h| h.key == "compress_type")
        .map(|h| h.value.as_str())
        .unwrap_or("gzip");

    let payload = if frame.payload.is_empty() {
        return Ok(DecodedFrame {
            items: vec![],
            ack_log_id: None,
            internal_ext: vec![],
        });
    } else if compress_type == "gzip" {
        gzip_decompress(&frame.payload)?
    } else {
        frame.payload.clone()
    };

    let response = DouyinResponse::decode(Bytes::from(payload))
        .map_err(|e| format!("Response decode error: {e}"))?;

    let ack_log_id = if response.need_ack {
        Some(frame.log_id)
    } else {
        None
    };
    let internal_ext = response.internal_ext.clone();

    let mut all_items = Vec::new();
    for im_msg in &response.messages {
        all_items.extend(decode_im_message(im_msg));
    }

    Ok(DecodedFrame {
        items: all_items,
        ack_log_id,
        internal_ext,
    })
}

fn decode_im_message(msg: &ImMessage) -> Vec<DanmuItem> {
    match msg.method.as_str() {
        "WebcastChatMessage" => {
            let Ok(chat) = ChatMessage::decode(Bytes::from(msg.payload.clone())) else {
                warn!("Failed to decode ChatMessage payload");
                return vec![];
            };
            let Some(user) = chat
                .user
                .as_ref()
                .or_else(|| chat.common.as_ref().and_then(|c| c.user.as_ref()))
            else {
                return vec![];
            };

            let id = if msg.msg_id != 0 {
                msg.msg_id.to_string()
            } else {
                Uuid::new_v4().to_string()
            };

            let danmu = DanmuMessage::chat(id, user_id(user), user_name(user), chat.content);
            vec![DanmuItem::Message(danmu)]
        }

        "WebcastGiftMessage" => {
            let Ok(gift) = GiftMessage::decode(Bytes::from(msg.payload.clone())) else {
                warn!("Failed to decode GiftMessage payload");
                return vec![];
            };
            let Some(user) = gift
                .user
                .as_ref()
                .or_else(|| gift.common.as_ref().and_then(|c| c.user.as_ref()))
            else {
                return vec![];
            };

            let gift_name = gift
                .gift
                .as_ref()
                .map(|g| {
                    if !g.name.is_empty() {
                        g.name.clone()
                    } else if !g.describe.is_empty() {
                        g.describe.clone()
                    } else {
                        format!("gift_{}", gift.gift_id)
                    }
                })
                .unwrap_or_else(|| format!("gift_{}", gift.gift_id));

            let gift_count = if gift.repeat_count > 0 {
                gift.repeat_count as u32
            } else if gift.count > 0 {
                gift.count as u32
            } else {
                1
            };

            let id = if msg.msg_id != 0 {
                msg.msg_id.to_string()
            } else {
                Uuid::new_v4().to_string()
            };

            let danmu =
                DanmuMessage::gift(id, user_id(user), user_name(user), &gift_name, gift_count);
            vec![DanmuItem::Message(danmu)]
        }

        "WebcastMemberMessage" => {
            let Ok(member) = MemberMessage::decode(Bytes::from(msg.payload.clone())) else {
                warn!("Failed to decode MemberMessage payload");
                return vec![];
            };
            let Some(user) = member
                .user
                .as_ref()
                .or_else(|| member.common.as_ref().and_then(|c| c.user.as_ref()))
            else {
                return vec![];
            };

            let id = if msg.msg_id != 0 {
                msg.msg_id.to_string()
            } else {
                Uuid::new_v4().to_string()
            };

            let content = if !member.action_description.is_empty() {
                member.action_description.clone()
            } else {
                format!("进入直播间 (在线 {})", member.member_count)
            };

            let danmu = DanmuMessage {
                id,
                user_id: user_id(user),
                username: user_name(user),
                content,
                color: None,
                timestamp: chrono::Utc::now(),
                message_type: DanmuType::UserJoin,
                metadata: None,
            };
            vec![DanmuItem::Message(danmu)]
        }

        "WebcastSocialMessage" => {
            let Ok(social) = SocialMessage::decode(Bytes::from(msg.payload.clone())) else {
                warn!("Failed to decode SocialMessage payload");
                return vec![];
            };
            let Some(user) = social
                .user
                .as_ref()
                .or_else(|| social.common.as_ref().and_then(|c| c.user.as_ref()))
            else {
                return vec![];
            };

            let id = if msg.msg_id != 0 {
                msg.msg_id.to_string()
            } else {
                Uuid::new_v4().to_string()
            };

            let action_str = match social.action {
                1 => "关注了主播",
                2 => "分享了直播间",
                _ => "进行了社交互动",
            };

            let danmu = DanmuMessage {
                id,
                user_id: user_id(user),
                username: user_name(user),
                content: action_str.to_string(),
                color: None,
                timestamp: chrono::Utc::now(),
                message_type: DanmuType::Follow,
                metadata: None,
            };
            vec![DanmuItem::Message(danmu)]
        }

        "WebcastControlMessage" => {
            let Ok(ctrl) = ControlMessage::decode(Bytes::from(msg.payload.clone())) else {
                warn!("Failed to decode ControlMessage payload");
                return vec![];
            };

            if ctrl.action == 3 {
                let tips = ctrl.tips.trim().to_string();
                let tips = (!tips.is_empty()).then_some(tips);
                vec![DanmuItem::Control(DanmuControlEvent::StreamClosed {
                    message: tips,
                    action: Some(ctrl.action),
                })]
            } else {
                vec![]
            }
        }

        "WebcastRoomStatsMessage" => {
            let Ok(stats) = RoomStatsMessage::decode(Bytes::from(msg.payload.clone())) else {
                return vec![];
            };

            let display = if !stats.display_middle.is_empty() {
                stats.display_middle.clone()
            } else if !stats.display_short.is_empty() {
                stats.display_short.clone()
            } else {
                stats.total.to_string()
            };

            let id = Uuid::new_v4().to_string();
            let danmu = DanmuMessage::chat(id, "0", "system", format!("在线: {}", display))
                .with_metadata("online_count", serde_json::json!(stats.total))
                .with_metadata("event_type", serde_json::json!("online_count"));

            vec![DanmuItem::Message(danmu)]
        }

        other => {
            trace!(method = other, "Skipping unhandled Douyin IM message");
            vec![]
        }
    }
}

// ===========================================================================
// Connection state
// ===========================================================================

struct DouyinConnectionState {
    message_rx: Arc<Mutex<mpsc::Receiver<DanmuItem>>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    tasks: Vec<JoinHandle<()>>,
}

impl DouyinConnectionState {
    fn abort_tasks(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }
}

impl Drop for DouyinConnectionState {
    fn drop(&mut self) {
        self.abort_tasks();
    }
}

// ===========================================================================
// Background WebSocket task
// ===========================================================================

async fn run_douyin_ws_task(
    room_id: String,
    user_unique_id: String,
    ttwid: String,
    message_tx: mpsc::Sender<DanmuItem>,
    mut shutdown_rx: mpsc::Receiver<()>,
) {
    let ws_url = build_ws_url(&room_id, &user_unique_id);
    debug!(room_id = %room_id, url = %ws_url, "Connecting to Douyin WebSocket");

    // Build HTTP request with proper headers (Origin, User-Agent, Cookie).
    let host = ws_url
        .split("wss://")
        .nth(1)
        .and_then(|s| s.split('/').next())
        .unwrap_or("webcast100-ws-web-lq.douyin.com");

    let request = match http::Request::builder()
        .uri(&ws_url)
        .header("Host", host)
        .header("User-Agent", USER_AGENT)
        .header("Origin", "https://live.douyin.com")
        .header("Cookie", format!("ttwid={ttwid}"))
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key(),
        )
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .body(())
    {
        Ok(r) => r,
        Err(e) => {
            error!(room_id = %room_id, "Failed to build WebSocket request: {e}");
            return;
        }
    };

    let ws_stream = match tokio::time::timeout(CONNECT_TIMEOUT, connect_async(request)).await {
        Ok(Ok((stream, _response))) => {
            info!(room_id = %room_id, "Connected to Douyin WebSocket");
            stream
        }
        Ok(Err(e)) => {
            error!(room_id = %room_id, "Failed to connect to Douyin WebSocket: {e}");
            return;
        }
        Err(_) => {
            error!(room_id = %room_id, "Timeout connecting to Douyin WebSocket");
            return;
        }
    };

    let (mut ws_sink, mut ws_source) = ws_stream.split();

    let mut heartbeat_timer = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat_timer.tick().await;

    loop {
        tokio::select! {
            _ = heartbeat_timer.tick() => {
                if let Err(e) = ws_sink.send(WsMessage::Binary(Bytes::from_static(HEARTBEAT))).await {
                    error!(room_id = %room_id, "Failed to send heartbeat: {e}");
                    break;
                }
                trace!(room_id = %room_id, "Sent heartbeat");
            }

            msg_opt = ws_source.next() => {
                match msg_opt {
                    Some(Ok(WsMessage::Binary(data))) => {
                        match decode_douyin_frame(&data) {
                            Ok(decoded) => {
                                if let Some(log_id) = decoded.ack_log_id {
                                    let ack = build_ack_frame(log_id, decoded.internal_ext);
                                    if let Err(e) = ws_sink.send(WsMessage::Binary(Bytes::from(ack))).await {
                                        warn!(room_id = %room_id, "Failed to send ACK: {e}");
                                    }
                                }
                                for item in decoded.items {
                                    if message_tx.send(item).await.is_err() {
                                        debug!(room_id = %room_id, "Message channel closed");
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(room_id = %room_id, "Failed to decode frame: {e}");
                            }
                        }
                    }
                    Some(Ok(WsMessage::Close(_))) => {
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
                    _ => {}
                }
            }

            _ = shutdown_rx.recv() => {
                debug!(room_id = %room_id, "Shutdown signal received");
                let _ = ws_sink.close().await;
                return;
            }
        }
    }

    let _ = ws_sink.close().await;
}

// ===========================================================================
// DouyinDanmuProvider
// ===========================================================================

pub struct DouyinDanmuProvider {
    connections: tokio::sync::RwLock<HashMap<String, Arc<Mutex<DouyinConnectionState>>>>,
}

impl DouyinDanmuProvider {
    pub fn new() -> Self {
        Self {
            connections: tokio::sync::RwLock::new(HashMap::new()),
        }
    }
}

pub fn create_douyin_danmu_provider() -> DouyinDanmuProvider {
    DouyinDanmuProvider::new()
}

#[async_trait]
impl DanmuProvider for DouyinDanmuProvider {
    fn platform(&self) -> &str {
        "douyin"
    }

    fn supports_url(&self, url: &str) -> bool {
        url.contains("douyin.com")
    }

    fn extract_room_id(&self, url: &str) -> Option<String> {
        let re = regex::Regex::new(r"(?:https?://)?(?:www\.)?live\.douyin\.com/(\d+)").ok()?;
        re.captures(url)
            .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
    }

    async fn connect(&self, room_id: &str, config: ConnectionConfig) -> Result<DanmuConnection> {
        let extras = config.extras.unwrap_or_default();

        // Numeric room_id from extras (set by extractor), fallback to web_rid.
        let numeric_room_id = extras
            .get("room_id")
            .cloned()
            .unwrap_or_else(|| room_id.to_string());

        let user_unique_id = extras
            .get("user_unique_id")
            .cloned()
            .unwrap_or_else(generate_user_unique_id);

        let ttwid = extras
            .get("ttwid")
            .cloned()
            .unwrap_or_else(get_ttwid);

        let connection_id = format!("douyin-{}-{}", room_id, Uuid::new_v4());
        let (message_tx, message_rx) = mpsc::channel(MESSAGE_CHANNEL_SIZE);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let handle = tokio::spawn(run_douyin_ws_task(
            numeric_room_id,
            user_unique_id,
            ttwid,
            message_tx,
            shutdown_rx,
        ));

        let state = DouyinConnectionState {
            message_rx: Arc::new(Mutex::new(message_rx)),
            shutdown_tx: Some(shutdown_tx),
            tasks: vec![handle],
        };

        self.connections
            .write()
            .await
            .insert(connection_id.clone(), Arc::new(Mutex::new(state)));

        let mut conn = DanmuConnection::new(connection_id, "douyin", room_id);
        conn.set_connected();
        Ok(conn)
    }

    async fn disconnect(&self, connection: &mut DanmuConnection) -> Result<()> {
        if let Some(state_arc) = self.connections.write().await.remove(&connection.id) {
            let mut state = state_arc.lock().await;
            if let Some(tx) = state.shutdown_tx.take() {
                let _ = tx.try_send(());
            }
            state.abort_tasks();
        }
        connection.set_disconnected();
        Ok(())
    }

    async fn receive(&self, connection: &DanmuConnection) -> Result<Option<DanmuItem>> {
        let state_arc = {
            let map = self.connections.read().await;
            map.get(&connection.id).cloned()
        };

        let Some(state_arc) = state_arc else {
            return Err(DanmakuError::connection("Connection not found"));
        };

        let message_rx = {
            let state = state_arc.lock().await;
            state.message_rx.clone()
        };

        let next = tokio::time::timeout(RECEIVE_TIMEOUT, async move {
            let mut rx = message_rx.lock().await;
            rx.recv().await
        })
        .await;

        match next {
            Ok(Some(msg)) => Ok(Some(msg)),
            Ok(None) => {
                let _ = self.connections.write().await.remove(&connection.id);
                Err(DanmakuError::connection("Message channel closed"))
            }
            Err(_) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_room_id() {
        let provider = DouyinDanmuProvider::new();
        assert_eq!(
            provider.extract_room_id("https://live.douyin.com/12345678"),
            Some("12345678".to_string())
        );
        assert_eq!(
            provider.extract_room_id("live.douyin.com/555"),
            Some("555".to_string())
        );
        assert_eq!(provider.extract_room_id("https://www.example.com"), None);
    }

    #[test]
    fn test_supports_url() {
        let provider = DouyinDanmuProvider::new();
        assert!(provider.supports_url("https://live.douyin.com/123"));
        assert!(!provider.supports_url("https://www.huya.com/123"));
    }

    #[test]
    fn test_heartbeat_bytes() {
        assert_eq!(HEARTBEAT, b":\x02hb");
    }

    #[test]
    fn test_md5_hex() {
        let result = md5_hex("test");
        assert_eq!(result.len(), 32);
        // Known MD5 of "test" = 098f6bcd4621d373cade4e832627b4f6
        let expected = b"098f6bcd4621d373cade4e832627b4f6";
        assert_eq!(&result, expected);
    }
}
