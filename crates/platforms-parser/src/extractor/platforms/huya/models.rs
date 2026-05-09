//! Huya-specific data models used by the extractor and danmaku provider.

use serde::{Deserialize, Serialize};

/// Stream line type (FLV or HLS).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HuyaLineType {
    Flv,
    Hls,
}

/// A single CDN stream line offered by Huya.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HuyaLineModel {
    /// CDN base URL (e.g. `https://hw.flv.huya.com/src`)
    pub line: String,
    /// Whether this is FLV or HLS.
    pub line_type: HuyaLineType,
    /// Raw anti-code string for FLV streams.
    pub flv_anti_code: String,
    /// Raw anti-code string for HLS streams.
    pub hls_anti_code: String,
    /// The stream name (used to build the final URL).
    pub stream_name: String,
    /// CDN type identifier (e.g. "AL", "TX", "HW").
    pub cdn_type: String,
    /// Presenter / room owner UID.
    pub presenter_uid: i64,
}

/// A quality / bitrate option.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HuyaBitRateModel {
    /// Human-readable name (e.g. "原画", "蓝光4M", "超清").
    pub name: String,
    /// Bitrate value (`0` = 原画 / source).
    pub bit_rate: i32,
}

/// Aggregated stream URL data extracted from the room page.
///
/// Stored as the `data` field on `LiveRoomDetail` so that `get_play_qualities`
/// and `get_play_urls` can access it later.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HuyaUrlDataModel {
    /// All available CDN lines.
    pub lines: Vec<HuyaLineModel>,
    /// All available bitrate options.
    pub bit_rates: Vec<HuyaBitRateModel>,
    /// A pseudo-random UID used in anti-code calculation.
    pub uid: i64,
}

/// Arguments passed to the danmaku WebSocket after room detail is fetched.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HuyaDanmakuArgs {
    /// `yyid` from the room stream data (channel id).
    pub ayyuid: i64,
    /// Top-level channel id (`lChannelId`).
    pub top_sid: i64,
    /// Sub-channel id (`lSubChannelId`).
    pub sub_sid: i64,
}
