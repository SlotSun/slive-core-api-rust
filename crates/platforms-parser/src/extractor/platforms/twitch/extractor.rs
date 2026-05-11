use async_trait::async_trait;
use regex::Regex;

use serde::{Deserialize, Serialize};

use crate::extractor::error::ExtractorError;
use crate::extractor::http_client::HttpClient;
use crate::extractor::models::*;
use crate::extractor::{LiveExtractor, Result};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PLATFORM_ID: &str = "twitch";
const PLATFORM_NAME: &str = "Twitch";

const GQL_URL: &str = "https://gql.twitch.tv/gql";
const USHER_URL: &str = "https://usher.ttvnw.net/api/channel/hls";

/// Public client-id used by the Twitch web player.
const CLIENT_ID: &str = "kimne78kx3ncx6brgo4mv6wki5h1ko";

const SEARCH_ROWS: u32 = 20;
const CATEGORY_ROWS: u32 = 30;

/// Paths that should not be treated as channel names.
const RESERVED_PATHS: &[&str] = &[
    "directory",
    "settings",
    "subscriptions",
    "inventory",
    "wallet",
    "downloads",
    "store",
    "turbo",
    "prime",
    "jobs",
    "p",
    "videos",
    "clip",
    "events",
];

// ---------------------------------------------------------------------------
// Twitch-specific models
// ---------------------------------------------------------------------------

/// Stored in `LiveRoomDetail.data` so downstream methods can access channel info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TwitchRoomData {
    pub channel_login: String,
    pub display_name: String,
    pub user_id: String,
}

/// Stored in `LiveRoomDetail.danmaku_data` so the danmaku provider can connect.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TwitchDanmakuData {
    pub channel_login: String,
}

// ---------------------------------------------------------------------------
// GQL Response models
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct GqlResponse<T> {
    data: Option<T>,
}

#[derive(Debug, Deserialize)]
struct ChannelShellData {
    user: Option<ChannelShellUser>,
}

#[derive(Debug, Deserialize)]
struct ChannelShellUser {
    id: Option<String>,
    #[serde(rename = "displayName")]
    display_name: Option<String>,
    #[serde(rename = "profileImageURL")]
    profile_image_url: Option<String>,
    stream: Option<ChannelShellStream>,
}

#[derive(Debug, Deserialize)]
struct ChannelShellStream {
    #[serde(rename = "type")]
    stream_type: Option<String>,
    title: Option<String>,
    #[serde(rename = "viewersCount")]
    viewers_count: Option<u64>,
    #[serde(rename = "previewImageURL")]
    preview_image_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PlaybackAccessTokenData {
    #[serde(rename = "streamPlaybackAccessToken")]
    stream_playback_access_token: Option<PlaybackAccessToken>,
}

#[derive(Debug, Deserialize)]
struct PlaybackAccessToken {
    value: Option<String>,
    signature: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SearchResultsData {
    #[serde(rename = "searchFor")]
    search_for: Option<SearchForResult>,
}

#[derive(Debug, Deserialize)]
struct SearchForResult {
    channels: Option<SearchChannels>,
}

#[derive(Debug, Deserialize)]
struct SearchChannels {
    edges: Option<Vec<SearchChannelEdge>>,
    #[serde(rename = "pageInfo")]
    page_info: Option<PageInfo>,
}

#[derive(Debug, Deserialize)]
struct SearchChannelEdge {
    node: Option<SearchChannelNode>,
}

#[derive(Debug, Deserialize)]
struct SearchChannelNode {
    id: Option<String>,
    login: Option<String>,
    #[serde(rename = "displayName")]
    display_name: Option<String>,
    #[serde(rename = "profileImageURL")]
    profile_image_url: Option<String>,
    stream: Option<ChannelShellStream>,
}

#[derive(Debug, Deserialize)]
struct PageInfo {
    #[serde(rename = "hasNextPage")]
    has_next_page: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct DirectoryGameData {
    game: Option<GameNode>,
}

#[derive(Debug, Deserialize)]
struct GameNode {
    streams: Option<StreamConnection>,
}

#[derive(Debug, Deserialize)]
struct StreamConnection {
    edges: Option<Vec<StreamEdge>>,
    #[serde(rename = "pageInfo")]
    page_info: Option<PageInfo>,
}

#[derive(Debug, Deserialize)]
struct StreamEdge {
    node: Option<StreamNode>,
}

#[derive(Debug, Deserialize)]
struct StreamNode {
    title: Option<String>,
    #[serde(rename = "viewersCount")]
    viewers_count: Option<u64>,
    #[serde(rename = "previewImageURL")]
    preview_image_url: Option<String>,
    broadcaster: Option<BroadcasterNode>,
}

#[derive(Debug, Deserialize)]
struct BroadcasterNode {
    login: Option<String>,
    #[serde(rename = "displayName")]
    display_name: Option<String>,
    #[serde(rename = "profileImageURL")]
    profile_image_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RecommendedStreamsData {
    streams: Option<RecommendedStreamConnection>,
}

#[derive(Debug, Deserialize)]
struct RecommendedStreamConnection {
    edges: Option<Vec<StreamEdge>>,
    #[serde(rename = "pageInfo")]
    page_info: Option<PageInfo>,
}

#[derive(Debug, Deserialize)]
struct CategoriesData {
    #[serde(rename = "directoriesWithTags")]
    directories_with_tags: Option<DirectoriesConnection>,
}

#[derive(Debug, Deserialize)]
struct DirectoriesConnection {
    edges: Option<Vec<DirectoryEdge>>,
}

#[derive(Debug, Deserialize)]
struct DirectoryEdge {
    node: Option<DirectoryNode>,
}

#[derive(Debug, Deserialize)]
struct DirectoryNode {
    id: Option<String>,
    #[serde(rename = "displayName")]
    display_name: Option<String>,
}

// ---------------------------------------------------------------------------
// GQL query strings
// ---------------------------------------------------------------------------

const CHANNEL_SHELL_QUERY: &str = r#"
query ChannelShell($login: String!) {
  user(login: $login, lookupType: ALL) {
    id
    login
    displayName
    profileImageURL(width: 150)
    stream {
      id
      type
      title
      viewersCount
      previewImageURL(width: 1920, height: 1080)
    }
  }
}
"#;

const PLAYBACK_ACCESS_TOKEN_QUERY: &str = r#"
query PlaybackAccessToken_Template($login: String!, $isLive: Boolean!, $vodID: ID!, $isVod: Boolean!, $playerType: String!) {
  streamPlaybackAccessToken(channelName: $login, params: {platform: "web", playerBackend: "mediaplayer", playerType: $playerType}) @include(if: $isLive) {
    value
    signature
    __typename
  }
  videoPlaybackAccessToken(id: $vodID, params: {platform: "web", playerBackend: "mediaplayer", playerType: $playerType}) @include(if: $isVod) {
    value
    signature
    __typename
  }
}
"#;

const SEARCH_QUERY: &str = r#"
query SearchResultsPaginated($query: String!, $options: SearchOptions!) {
  searchFor(query: $query, options: $options) {
    channels {
      edges {
        node {
          id
          login
          displayName
          profileImageURL(width: 150)
          stream {
            id
            type
            title
            viewersCount
            previewImageURL(width: 1920, height: 1080)
          }
        }
      }
      pageInfo {
        hasNextPage
      }
    }
  }
}
"#;

const DIRECTORY_PAGE_GAME_QUERY: &str = r#"
query DirectoryPage_Game($name: String!, $options: ChannelOptions!, $limit: Int!, $cursor: Cursor) {
  game(name: $name) {
    displayName
    streams(first: $limit, after: $cursor, options: $options) {
      edges {
        node {
          id
          title
          viewersCount
          previewImageURL(width: 1920, height: 1080)
          broadcaster {
            id
            login
            displayName
            profileImageURL(width: 150)
          }
        }
      }
      pageInfo {
        hasNextPage
      }
    }
  }
}
"#;

const BROWSE_ALL_QUERY: &str = r#"
query BrowseAll_Directories($limit: Int!, $cursor: Cursor) {
  directoriesWithTags(first: $limit, after: $cursor) {
    edges {
      node {
        id
        displayName
        boxArtURL(width: 120, height: 160)
      }
    }
    pageInfo {
      hasNextPage
    }
  }
}
"#;

const RECOMMENDED_STREAMS_QUERY: &str = r#"
query BrowsePage_Popular($limit: Int!, $cursor: Cursor, $platformType: String!) {
  streams(first: $limit, after: $cursor, options: {platformType: $platformType, sort: VIEWER_COUNT}) {
    edges {
      node {
        id
        title
        viewersCount
        previewImageURL(width: 1920, height: 1080)
        broadcaster {
          id
          login
          displayName
          profileImageURL(width: 150)
        }
      }
    }
    pageInfo {
      hasNextPage
    }
  }
}
"#;

// ---------------------------------------------------------------------------
// Extractor
// ---------------------------------------------------------------------------

/// Twitch live streaming platform extractor.
pub struct TwitchExtractor {
    http: HttpClient,
    room_url_re: Regex,
}

impl TwitchExtractor {
    pub fn new() -> Self {
        Self {
            http: HttpClient::builder()
                .build()
                .expect("failed to build HTTP client"),
            room_url_re: Regex::new(r"(?:https?://)?(?:www\.)?twitch\.tv/(\w+)").unwrap(),
        }
    }

    // ------------------------------------------------------------------
    // GQL helpers
    // ------------------------------------------------------------------

    /// Send a GraphQL request to the Twitch GQL endpoint and return the JSON response.
    async fn gql_request<T: serde::de::DeserializeOwned>(
        &self,
        operation: &str,
        query: &str,
        variables: serde_json::Value,
    ) -> Result<T> {
        let body = serde_json::json!({
            "operationName": operation,
            "variables": variables,
            "query": query,
        });

        let text = self
            .http
            .request(reqwest::Method::POST, GQL_URL)
            .header("Client-Id", CLIENT_ID)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?
            .text()
            .await?;

        let gql_resp: GqlResponse<T> = serde_json::from_str(&text)
            .map_err(|e| ExtractorError::Other(format!("GQL JSON parse error: {e}")))?;

        gql_resp
            .data
            .ok_or_else(|| ExtractorError::Other("GQL response missing data field".into()))
    }

    /// Get the channel shell (user info + stream status) via GQL.
    async fn get_channel_shell(&self, channel: &str) -> Result<ChannelShellUser> {
        let variables = serde_json::json!({ "login": channel });
        let data: ChannelShellData = self
            .gql_request("ChannelShell", CHANNEL_SHELL_QUERY, variables)
            .await?;

        data.user.ok_or_else(|| ExtractorError::StreamerNotFound)
    }

    /// Get a PlaybackAccessToken for the given channel.
    async fn get_playback_access_token(&self, channel: &str) -> Result<(String, String)> {
        let variables = serde_json::json!({
            "login": channel,
            "isLive": true,
            "isVod": false,
            "vodID": "",
            "isVod": false,
            "playerType": "embed",
        });

        let data: PlaybackAccessTokenData = self
            .gql_request(
                "PlaybackAccessToken_Template",
                PLAYBACK_ACCESS_TOKEN_QUERY,
                variables,
            )
            .await?;

        let token = data
            .stream_playback_access_token
            .ok_or_else(|| ExtractorError::Other("missing streamPlaybackAccessToken".into()))?;

        let sig = token
            .signature
            .ok_or_else(|| ExtractorError::Other("missing token signature".into()))?;
        let value = token
            .value
            .ok_or_else(|| ExtractorError::Other("missing token value".into()))?;

        Ok((sig, value))
    }

    /// Fetch the M3U8 master playlist for a channel and parse it.
    async fn fetch_master_playlist(&self, channel: &str) -> Result<String> {
        let (sig, token) = self.get_playback_access_token(channel).await?;

        let url = format!(
            "{}/{}.m3u8?allow_source=true&allow_audio_only=true&sig={}&token={}",
            USHER_URL, channel, sig, token
        );

        let text = self.http.get_text(&url).await?;

        if text.is_empty() || !text.contains("#EXTM3U") {
            return Err(ExtractorError::NoStreamsFound);
        }

        Ok(text)
    }

    /// Parse an M3U8 master playlist and return (name, url) pairs for each variant.
    fn parse_master_playlist(playlist_text: &str) -> Vec<(String, String)> {
        let mut variants = Vec::new();

        let mut lines = playlist_text.lines().peekable();
        while let Some(line) = lines.next() {
            let line = line.trim();
            if line.starts_with("#EXT-X-STREAM-INF:") || line.starts_with("#EXT-X-MEDIA:") {
                // Extract NAME or RESOLUTION for quality label
                let name = Self::extract_quality_name(line);
                // The URL is on the next line
                if let Some(url_line) = lines.next() {
                    let url = url_line.trim();
                    if !url.is_empty() && !url.starts_with('#') {
                        variants.push((name, url.to_string()));
                    }
                }
            }
        }

        variants
    }

    /// Extract a human-readable quality name from an M3U8 tag line.
    fn extract_quality_name(line: &str) -> String {
        // Try NAME="..." from EXT-X-MEDIA
        if let Some(start) = line.find("NAME=\"") {
            let rest = &line[start + 6..];
            if let Some(end) = rest.find('"') {
                return rest[..end].to_string();
            }
        }

        // Try RESOLUTION from EXT-X-STREAM-INF
        if let Some(start) = line.find("RESOLUTION=") {
            let rest = &line[start + 11..];
            let resolution = rest.split(',').next().unwrap_or(rest).trim();
            if !resolution.is_empty() {
                // Convert "1920x1080" to "1080p"
                if let Some((_, h)) = resolution.split_once('x') {
                    return format!("{}p", h);
                }
                return resolution.to_string();
            }
        }

        "unknown".to_string()
    }
}

#[async_trait]
impl LiveExtractor for TwitchExtractor {
    fn id(&self) -> &str {
        PLATFORM_ID
    }

    fn name(&self) -> &str {
        PLATFORM_NAME
    }

    fn supports_url(&self, url: &str) -> bool {
        self.room_url_re.is_match(url)
    }

    fn extract_room_id(&self, url: &str) -> Option<String> {
        self.room_url_re
            .captures(url)
            .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
            .filter(|s| !RESERVED_PATHS.contains(&s.as_str()))
    }

    async fn get_categories(&self) -> Result<Vec<LiveCategory>> {
        let variables = serde_json::json!({
            "limit": 50,
            "cursor": "",
        });

        let data: CategoriesData = self
            .gql_request("BrowseAll_Directories", BROWSE_ALL_QUERY, variables)
            .await?;

        let directories = data
            .directories_with_tags
            .and_then(|d| d.edges)
            .unwrap_or_default();

        let mut categories = Vec::new();
        for edge in directories {
            if let Some(node) = edge.node {
                let id = node.id.unwrap_or_default();
                let name = node.display_name.unwrap_or_default();
                if !id.is_empty() && !name.is_empty() {
                    categories.push(LiveCategory {
                        id,
                        name,
                        sub_categories: vec![],
                    });
                }
            }
        }

        Ok(categories)
    }

    async fn search_rooms(&self, keyword: &str, page: u32) -> Result<LiveSearchRoomResult> {
        let cursor = if page > 1 {
            // Twitch uses cursor-based pagination; we encode page number in cursor.
            // For simplicity, we pass the cursor as base64-encoded page offset.
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                format!("{}", (page - 1) * SEARCH_ROWS),
            )
        } else {
            String::new()
        };

        let variables = serde_json::json!({
            "query": keyword,
            "options": {
                "shouldShowIndianJackpot": false,
                "sources": ["live_channel"],
                "limit": SEARCH_ROWS,
                "cursor": cursor,
            },
        });

        let data: SearchResultsData = self
            .gql_request("SearchResultsPaginated", SEARCH_QUERY, variables)
            .await?;

        let channels = data
            .search_for
            .and_then(|s| s.channels)
            .unwrap_or_else(|| SearchChannels {
                edges: None,
                page_info: None,
            });

        let has_more = channels
            .page_info
            .as_ref()
            .and_then(|p| p.has_next_page)
            .unwrap_or(false);

        let edges = channels.edges.unwrap_or_default();
        let mut items = Vec::new();

        for edge in edges {
            if let Some(node) = edge.node {
                let channel_login = node.login.clone().unwrap_or_default();
                let stream = node.stream;
                let is_live = stream
                    .as_ref()
                    .and_then(|s| s.stream_type.as_ref())
                    .map(|t| t == "live")
                    .unwrap_or(false);

                if !is_live {
                    continue;
                }

                let title = stream
                    .as_ref()
                    .and_then(|s| s.title.clone())
                    .unwrap_or_default();
                let cover = stream
                    .as_ref()
                    .and_then(|s| s.preview_image_url.clone())
                    .unwrap_or_default();
                let online = stream.as_ref().and_then(|s| s.viewers_count).unwrap_or(0);
                let user_name = node.display_name.unwrap_or_default();
                let user_avatar = node.profile_image_url.unwrap_or_default();

                items.push(LiveRoomItem {
                    room_id: channel_login.clone(),
                    title,
                    cover,
                    online,
                    user_name,
                    user_avatar,
                    url: format!("https://www.twitch.tv/{}", channel_login),
                    platform: PLATFORM_ID.to_string(),
                });
            }
        }

        Ok(LiveSearchRoomResult { has_more, items })
    }

    async fn search_anchors(&self, keyword: &str, page: u32) -> Result<LiveSearchAnchorResult> {
        let cursor = if page > 1 {
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                format!("{}", (page - 1) * SEARCH_ROWS),
            )
        } else {
            String::new()
        };

        let query = r#"
query SearchResultsPaginated($query: String!, $options: SearchOptions!) {
  searchFor(query: $query, options: $options) {
    channels {
      edges {
        node {
          id
          login
          displayName
          profileImageURL(width: 150)
          stream {
            id
            type
          }
        }
      }
      pageInfo {
        hasNextPage
      }
    }
  }
}
"#;

        let variables = serde_json::json!({
            "query": keyword,
            "options": {
                "sources": ["user"],
                "limit": SEARCH_ROWS,
                "cursor": cursor,
            },
        });

        let data: SearchResultsData = self
            .gql_request("SearchResultsPaginated", query, variables)
            .await?;

        let channels = data
            .search_for
            .and_then(|s| s.channels)
            .unwrap_or_else(|| SearchChannels {
                edges: None,
                page_info: None,
            });

        let has_more = channels
            .page_info
            .as_ref()
            .and_then(|p| p.has_next_page)
            .unwrap_or(false);

        let edges = channels.edges.unwrap_or_default();
        let mut items = Vec::new();

        for edge in edges {
            if let Some(node) = edge.node {
                let login = node.login.unwrap_or_default();
                let is_live = node
                    .stream
                    .as_ref()
                    .and_then(|s| s.stream_type.as_ref())
                    .map(|t| t == "live")
                    .unwrap_or(false);

                items.push(LiveAnchorItem {
                    user_id: node.id.unwrap_or_default(),
                    user_name: node.display_name.unwrap_or_default(),
                    user_avatar: node.profile_image_url.unwrap_or_default(),
                    room_id: if login.is_empty() {
                        None
                    } else {
                        Some(login.clone())
                    },
                    is_live,
                    platform: PLATFORM_ID.to_string(),
                    url: format!("https://www.twitch.tv/{}", login),
                });
            }
        }

        Ok(LiveSearchAnchorResult { has_more, items })
    }

    async fn get_category_rooms(
        &self,
        category: &LiveSubCategory,
        page: u32,
    ) -> Result<LiveCategoryResult> {
        let cursor = if page > 1 {
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                format!("{}", (page - 1) * CATEGORY_ROWS),
            )
        } else {
            String::new()
        };

        let variables = serde_json::json!({
            "name": category.name,
            "options": {
                "sort": "VIEWER_COUNT",
                "streamLanguages": ["EN"],
                "tags": [],
            },
            "limit": CATEGORY_ROWS,
            "cursor": cursor,
        });

        let data: DirectoryGameData = self
            .gql_request("DirectoryPage_Game", DIRECTORY_PAGE_GAME_QUERY, variables)
            .await?;

        let game = data.game.unwrap_or_else(|| GameNode { streams: None });

        let streams = game.streams.unwrap_or_else(|| StreamConnection {
            edges: None,
            page_info: None,
        });

        let has_more = streams
            .page_info
            .as_ref()
            .and_then(|p| p.has_next_page)
            .unwrap_or(false);

        let edges = streams.edges.unwrap_or_default();
        let mut items = Vec::new();

        for edge in edges {
            if let Some(node) = edge.node {
                let broadcaster = node.broadcaster.unwrap_or_else(|| BroadcasterNode {
                    login: None,
                    display_name: None,
                    profile_image_url: None,
                });

                let channel_login = broadcaster.login.unwrap_or_default();
                let user_name = broadcaster.display_name.unwrap_or_default();
                let user_avatar = broadcaster.profile_image_url.unwrap_or_default();

                items.push(LiveRoomItem {
                    room_id: channel_login.clone(),
                    title: node.title.unwrap_or_default(),
                    cover: node.preview_image_url.unwrap_or_default(),
                    online: node.viewers_count.unwrap_or(0),
                    user_name,
                    user_avatar,
                    url: format!("https://www.twitch.tv/{}", channel_login),
                    platform: PLATFORM_ID.to_string(),
                });
            }
        }

        Ok(LiveCategoryResult { has_more, items })
    }

    async fn get_recommend_rooms(&self, page: u32) -> Result<LiveCategoryResult> {
        let cursor = if page > 1 {
            base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                format!("{}", (page - 1) * CATEGORY_ROWS),
            )
        } else {
            String::new()
        };

        let variables = serde_json::json!({
            "limit": CATEGORY_ROWS,
            "cursor": cursor,
            "platformType": "web",
        });

        let data: RecommendedStreamsData = self
            .gql_request("BrowsePage_Popular", RECOMMENDED_STREAMS_QUERY, variables)
            .await?;

        let streams = data.streams.unwrap_or_else(|| RecommendedStreamConnection {
            edges: None,
            page_info: None,
        });

        let has_more = streams
            .page_info
            .as_ref()
            .and_then(|p| p.has_next_page)
            .unwrap_or(false);

        let edges = streams.edges.unwrap_or_default();
        let mut items = Vec::new();

        for edge in edges {
            if let Some(node) = edge.node {
                let broadcaster = node.broadcaster.unwrap_or_else(|| BroadcasterNode {
                    login: None,
                    display_name: None,
                    profile_image_url: None,
                });

                let channel_login = broadcaster.login.unwrap_or_default();
                let user_name = broadcaster.display_name.unwrap_or_default();
                let user_avatar = broadcaster.profile_image_url.unwrap_or_default();

                items.push(LiveRoomItem {
                    room_id: channel_login.clone(),
                    title: node.title.unwrap_or_default(),
                    cover: node.preview_image_url.unwrap_or_default(),
                    online: node.viewers_count.unwrap_or(0),
                    user_name,
                    user_avatar,
                    url: format!("https://www.twitch.tv/{}", channel_login),
                    platform: PLATFORM_ID.to_string(),
                });
            }
        }

        Ok(LiveCategoryResult { has_more, items })
    }

    async fn get_room_detail(&self, channel: &str) -> Result<LiveRoomDetail> {
        let user = self.get_channel_shell(channel).await?;

        let stream = user.stream;
        let is_live = stream
            .as_ref()
            .and_then(|s| s.stream_type.as_ref())
            .map(|t| t == "live")
            .unwrap_or(false);

        let title = stream
            .as_ref()
            .and_then(|s| s.title.clone())
            .unwrap_or_default();
        let cover = stream
            .as_ref()
            .and_then(|s| s.preview_image_url.clone())
            .unwrap_or_default();
        let online = stream.as_ref().and_then(|s| s.viewers_count).unwrap_or(0);

        let display_name = user.display_name.unwrap_or_else(|| channel.to_string());
        let user_avatar = user.profile_image_url.unwrap_or_default();
        let user_id = user.id.unwrap_or_default();

        let room_data = TwitchRoomData {
            channel_login: channel.to_string(),
            display_name: display_name.clone(),
            user_id,
        };

        let danmaku_data = TwitchDanmakuData {
            channel_login: channel.to_string(),
        };

        Ok(LiveRoomDetail {
            room_id: channel.to_string(),
            title,
            cover,
            online,
            status: is_live,
            url: format!("https://www.twitch.tv/{}", channel),
            user_name: display_name,
            user_avatar,
            platform: PLATFORM_ID.to_string(),
            data: serde_json::to_value(&room_data).ok(),
            danmaku_data: serde_json::to_value(&danmaku_data).ok(),
        })
    }

    async fn get_play_qualities(&self, detail: &LiveRoomDetail) -> Result<Vec<LivePlayQuality>> {
        let room_data: TwitchRoomData = detail
            .data
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .ok_or_else(|| {
                ExtractorError::Other("missing TwitchRoomData in room detail `data`".into())
            })?;

        let channel = &room_data.channel_login;
        let playlist_text = self.fetch_master_playlist(channel).await?;
        let variants = Self::parse_master_playlist(&playlist_text);

        if variants.is_empty() {
            return Err(ExtractorError::NoStreamsFound);
        }

        let qualities: Vec<LivePlayQuality> = variants
            .into_iter()
            .map(|(name, url)| LivePlayQuality {
                quality: name,
                data: url,
            })
            .collect();

        Ok(qualities)
    }

    async fn get_play_urls(
        &self,
        _detail: &LiveRoomDetail,
        quality: &LivePlayQuality,
    ) -> Result<LivePlayUrl> {
        // quality.data is the variant M3U8 playlist URL from get_play_qualities
        let url = quality.data.clone();

        if url.is_empty() {
            return Err(ExtractorError::NoStreamsFound);
        }

        Ok(LivePlayUrl {
            urls: vec![url],
            url_type: UrlType::M3u8,
            headers: None,
        })
    }

    async fn get_live_status(&self, channel: &str) -> Result<bool> {
        let user = self.get_channel_shell(channel).await?;

        let is_live = user
            .stream
            .as_ref()
            .and_then(|s| s.stream_type.as_ref())
            .map(|t| t == "live")
            .unwrap_or(false);

        Ok(is_live)
    }

    async fn get_super_chat_messages(&self, _room_id: &str) -> Result<Vec<LiveSuperChatMessage>> {
        // Twitch does not have super chat; it has "Bits" and "Hype Chat".
        // These come through the IRC/WebSocket chat, not a separate API.
        Ok(vec![])
    }
}
