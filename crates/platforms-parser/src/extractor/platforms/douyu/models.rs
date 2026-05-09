//! Douyu-specific data models used by the extractor and danmaku provider.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Play URL models
// ---------------------------------------------------------------------------

/// Aggregated play URL data extracted from the Douyu API response.
///
/// Stored as the `data` field on `LivePlayQuality` so that `get_play_urls`
/// can access rate and CDN info later.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DouyuPlayData {
    /// Rate value (0 = source / original).
    pub rate: i32,
    /// Available CDN identifiers (e.g. ["ws", "tct", "ws2"]).
    pub cdns: Vec<String>,
}

// ---------------------------------------------------------------------------
// Room detail API models
// ---------------------------------------------------------------------------

/// Partial model for the `betard` API response (`/betard/{room_id}`).
///
/// We only extract the fields we need from this large JSON payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DouyuRoomInfo {
    pub rid: Option<i64>,
    #[serde(default)]
    pub room_name: String,
    #[serde(default)]
    pub nickname: String,
    #[serde(default)]
    pub room_src: String,
    #[serde(default)]
    pub avatar: String,
    #[serde(default)]
    pub online: u64,
    /// 1 = live, 0 = offline.
    #[serde(default)]
    pub is_live: i32,
    #[serde(default)]
    pub cate_id: String,
    #[serde(default)]
    pub cate_name: String,
    #[serde(default)]
    pub owner_uid: u64,
}

// ---------------------------------------------------------------------------
// Search / category API models
// ---------------------------------------------------------------------------

/// Item in the Douyu room search response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DouyuSearchRoomItem {
    #[serde(default)]
    pub rid: i64,
    #[serde(default)]
    pub room_name: String,
    #[serde(default)]
    pub nick_name: String,
    #[serde(default)]
    pub room_src: String,
    #[serde(default)]
    pub avatar: String,
    #[serde(default)]
    pub online: u64,
}

/// Item in the Douyu category / recommend room list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DouyuRoomListItem {
    #[serde(default)]
    pub rid: i64,
    #[serde(default)]
    pub room_name: String,
    #[serde(default)]
    pub nickname: String,
    #[serde(default)]
    pub room_src: String,
    #[serde(default)]
    pub avatar: String,
    #[serde(default)]
    pub online: u64,
}

// ---------------------------------------------------------------------------
// Category models
// ---------------------------------------------------------------------------

/// A Douyu top-level category item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DouyuCategoryItem {
    #[serde(default)]
    pub cate_id: String,
    #[serde(default)]
    pub game_name: String,
    #[serde(default)]
    pub short_name: String,
}

/// A Douyu sub-category item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DouyuSubCategoryItem {
    #[serde(default)]
    pub cate_id: String,
    #[serde(default)]
    pub game_name: String,
    #[serde(default)]
    pub short_name: String,
    #[serde(default)]
    pub pic_url: String,
}
