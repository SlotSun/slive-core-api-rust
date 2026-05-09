//! Core data models for live streaming platform extraction.
//!
//! These types mirror the Dart `LiveSite` interface and are shared across all platform
//! implementations.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Category types
// ---------------------------------------------------------------------------

/// A top-level live category (e.g. "Game", "Entertainment").
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveCategory {
    pub id: String,
    pub name: String,
    pub sub_categories: Vec<LiveSubCategory>,
}

/// A sub-category within a parent category.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveSubCategory {
    pub id: String,
    pub name: String,
    /// The id of the parent category, if any.
    pub parent_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Paginated results
// ---------------------------------------------------------------------------

/// Generic paginated result for category rooms / recommended rooms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveCategoryResult {
    pub has_more: bool,
    pub items: Vec<LiveRoomItem>,
}

/// Paginated search result for rooms.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveSearchRoomResult {
    pub has_more: bool,
    pub items: Vec<LiveRoomItem>,
}

/// Paginated search result for anchors / streamers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveSearchAnchorResult {
    pub has_more: bool,
    pub items: Vec<LiveAnchorItem>,
}

// ---------------------------------------------------------------------------
// Item types
// ---------------------------------------------------------------------------

/// A room (stream) listing item, typically shown in search / category results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveRoomItem {
    /// Platform-specific room id.
    pub room_id: String,
    /// Room / stream title.
    pub title: String,
    /// Thumbnail / cover image URL.
    pub cover: String,
    /// Current viewer count.
    pub online: u64,
    /// Streamer display name.
    pub user_name: String,
    /// Streamer avatar URL.
    pub user_avatar: String,
    /// Direct URL to the room page.
    pub url: String,
    /// Platform identifier (e.g. "huya", "bilibili").
    pub platform: String,
}

/// An anchor (streamer) listing item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveAnchorItem {
    pub user_id: String,
    pub user_name: String,
    pub user_avatar: String,
    pub room_id: Option<String>,
    pub is_live: bool,
    pub platform: String,
    /// URL to the anchor's room / profile page.
    pub url: String,
}

// ---------------------------------------------------------------------------
// Room detail
// ---------------------------------------------------------------------------

/// Detailed information about a single live room.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveRoomDetail {
    pub room_id: String,
    pub title: String,
    pub cover: String,
    /// Current viewer count.
    pub online: u64,
    /// Whether the room is currently live (`true`) or offline.
    pub status: bool,
    /// Direct URL to the room page.
    pub url: String,
    pub user_name: String,
    pub user_avatar: String,
    /// Platform identifier.
    pub platform: String,
    /// Platform-specific stream URL data (e.g. `HuyaUrlDataModel` serialized as JSON).
    pub data: Option<serde_json::Value>,
    /// Platform-specific danmaku connection data (e.g. `HuyaDanmakuArgs` serialized as JSON).
    pub danmaku_data: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Playback types
// ---------------------------------------------------------------------------

/// A single stream quality option offered by the platform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivePlayQuality {
    /// Human-readable quality label (e.g. "原画", "蓝光4M", "720p").
    pub quality: String,
    /// Opaque platform-specific data that `get_play_urls` needs.
    pub data: String,
}

/// The type of a media stream URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UrlType {
    Flv,
    M3u8,
    Other(String),
}

/// Resolved play URLs for a specific quality.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivePlayUrl {
    /// One or more stream URLs (CDN variants / fallbacks).
    pub urls: Vec<String>,
    /// The transport format.
    pub url_type: UrlType,
}

// ---------------------------------------------------------------------------
// Super-chat / paid messages
// ---------------------------------------------------------------------------

/// A super-chat (paid highlighted message) in the room.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveSuperChatMessage {
    pub id: String,
    pub user_id: String,
    pub user_name: String,
    pub content: String,
    /// Price in the platform's currency unit.
    pub price: f64,
    /// ISO-4217 currency code or platform-specific code.
    pub currency: String,
    /// How long the SC stays highlighted, in seconds.
    pub keep_time: u32,
    /// Unix timestamp (seconds) when the SC was sent.
    pub start_time: i64,
    /// Platform identifier.
    pub platform: String,
}
