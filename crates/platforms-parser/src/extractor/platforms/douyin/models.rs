//! Douyin-specific data models used by the extractor and danmaku provider.

use serde::{Deserialize, Serialize};

/// Stream line type (FLV or HLS).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DouyinStreamType {
    Flv,
    Hls,
}

/// A single CDN stream line offered by Douyin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DouyinStreamLine {
    /// Full stream URL.
    pub url: String,
    /// Quality label (e.g. "uhd", "hd", "sd", "origin").
    pub quality: String,
    /// Whether this is FLV or HLS.
    pub stream_type: DouyinStreamType,
}

/// Aggregated stream URL data extracted from the room-enter response.
///
/// Stored as the `data` field on `LiveRoomDetail` so that `get_play_qualities`
/// and `get_play_urls` can access it later.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DouyinUrlData {
    /// FLV stream lines (keyed by quality label).
    pub flv_urls: Vec<DouyinStreamLine>,
    /// HLS stream lines (keyed by quality label).
    pub hls_urls: Vec<DouyinStreamLine>,
}

/// Arguments passed to the danmaku WebSocket after room detail is fetched.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DouyinDanmakuData {
    /// Numeric room id (`id_str` from the room data).
    pub room_id: String,
    /// Web room id (the short id from the URL bar, e.g. `"1234567890"`).
    pub web_rid: String,
}
