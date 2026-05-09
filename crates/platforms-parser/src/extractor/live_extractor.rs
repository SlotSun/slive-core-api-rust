use async_trait::async_trait;

use super::error::ExtractorError;
use super::models::*;

/// Result alias for extractor operations.
pub type Result<T> = std::result::Result<T, ExtractorError>;

/// The central trait every live-streaming platform must implement.
///
/// Methods mirror the original Dart `LiveSite` abstract class. All methods are
/// `async` so they can perform HTTP requests, WebSocket handshakes, etc.
#[async_trait]
pub trait LiveExtractor: Send + Sync {
    /// Platform unique identifier (e.g. `"huya"`, `"bilibili"`).
    fn id(&self) -> &str;

    /// Human-readable platform name (e.g. `"虎牙"`, `"哔哩哔哩"`).
    fn name(&self) -> &str;

    /// Whether this extractor can handle the given URL.
    fn supports_url(&self, url: &str) -> bool;

    /// Try to extract a platform room-id from `url`.
    ///
    /// Returns `None` if the URL does not match this platform.
    fn extract_room_id(&self, url: &str) -> Option<String>;

    // ------------------------------------------------------------------
    // Discovery
    // ------------------------------------------------------------------

    /// Retrieve the top-level category list for the platform.
    async fn get_categories(&self) -> Result<Vec<LiveCategory>>;

    /// Search for rooms by `keyword`.
    async fn search_rooms(&self, keyword: &str, page: u32) -> Result<LiveSearchRoomResult>;

    /// Search for anchors / streamers by `keyword`.
    async fn search_anchors(&self, keyword: &str, page: u32) -> Result<LiveSearchAnchorResult>;

    /// List rooms inside a sub-category.
    async fn get_category_rooms(
        &self,
        category: &LiveSubCategory,
        page: u32,
    ) -> Result<LiveCategoryResult>;

    /// List recommended / featured rooms.
    async fn get_recommend_rooms(&self, page: u32) -> Result<LiveCategoryResult>;

    // ------------------------------------------------------------------
    // Room detail & playback
    // ------------------------------------------------------------------

    /// Fetch detailed information for a single room.
    async fn get_room_detail(&self, room_id: &str) -> Result<LiveRoomDetail>;

    /// Enumerate the available stream qualities for `detail`.
    async fn get_play_qualities(&self, detail: &LiveRoomDetail) -> Result<Vec<LivePlayQuality>>;

    /// Resolve the actual media stream URLs for a given quality.
    async fn get_play_urls(
        &self,
        detail: &LiveRoomDetail,
        quality: &LivePlayQuality,
    ) -> Result<LivePlayUrl>;

    /// Quick check: is the room currently live?
    async fn get_live_status(&self, room_id: &str) -> Result<bool>;

    // ------------------------------------------------------------------
    // Super-chat
    // ------------------------------------------------------------------

    /// Fetch recent super-chat (paid) messages for a room.
    async fn get_super_chat_messages(&self, room_id: &str) -> Result<Vec<LiveSuperChatMessage>>;

    // ------------------------------------------------------------------
    // Configuration (optional)
    // ------------------------------------------------------------------

    /// Set cookies for authenticated API access.
    ///
    /// Platforms that support authentication (e.g. Bilibili with SESSDATA,
    /// Douyin with ttwid) will use these cookies in their HTTP requests.
    /// Platforms that don't need cookies can ignore this call.
    ///
    /// Uses `&self` (not `&mut self`) so it works with `Arc<dyn LiveExtractor>`.
    fn set_cookies(&self, _cookies: &str) {
        // Default: no-op. Override in platforms that need cookies.
    }
}
