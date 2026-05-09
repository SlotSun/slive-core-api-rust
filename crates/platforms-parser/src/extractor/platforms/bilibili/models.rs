//! Bilibili-specific data models used by the extractor and danmaku provider.

use serde::{Deserialize, Serialize};

/// Play URL information cached in `LiveRoomDetail.data`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BilibiliPlayData {
    /// The resolved real room_id (not the short_id).
    pub room_id: u64,
}

/// Danmaku connection data cached in `LiveRoomDetail.danmaku_data`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BilibiliDanmakuData {
    /// The resolved real room_id.
    pub room_id: u64,
    /// Authentication token for the danmaku WebSocket.
    pub token: String,
    /// Available WebSocket hosts.
    pub host_list: Vec<BilibiliDanmuHost>,
}

/// A danmaku WebSocket host entry returned by `getDanmuInfo`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BilibiliDanmuHost {
    pub host: String,
    pub port: u16,
    pub wss_port: u16,
    pub ws_port: u16,
}
