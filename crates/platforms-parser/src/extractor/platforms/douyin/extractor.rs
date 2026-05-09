//! Douyin (抖音) live streaming platform extractor.
//!
//! Fetches room pages via the Douyin web API, extracts embedded stream data
//! (FLV / HLS URLs) and exposes quality / playback helpers.

use crate::extractor::error::ExtractorError;
use crate::extractor::http_client::HttpClient;
use crate::extractor::models::*;
use crate::extractor::platforms::douyin::abogus::ABogus;
use crate::extractor::platforms::douyin::models::*;
use crate::extractor::platforms::douyin::utils::GlobalTtwidManager;
use crate::extractor::{LiveExtractor, Result};
use async_trait::async_trait;
use std::sync::Mutex;
use regex::Regex;
use reqwest::header::{self, HeaderMap, HeaderValue};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use tokio::sync::OnceCell;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PLATFORM_ID: &str = "douyin";
const PLATFORM_NAME: &str = "抖音";

const BASE_URL: &str = "https://live.douyin.com";

/// Room enter API base URL — parameters are added at call time.
const ROOM_ENTER_URL: &str = "https://live.douyin.com/webcast/room/web/enter/";

/// Room info API for numeric room ids.
#[allow(dead_code)]
const ROOM_INFO_URL: &str = "https://webcast.amemv.com/webcast/room/reflow/info/";

/// Reflow URL for converting long roomId to webRid.
const REFLOW_URL: &str = "https://webcast.amemv.com/douyin/webcast/reflow/";

/// Category rooms API.
const CATEGORY_ROOMS_URL: &str = "https://live.douyin.com/webcast/web/partition/detail/room/v2/";

/// Recommended room feed.
const FEED_URL: &str = "https://live.douyin.com/webcast/feed/";

/// Room search API (www subdomain).
const SEARCH_ROOMS_URL: &str = "https://www.douyin.com/aweme/v1/web/live/search/";

/// Referer header sent with every request.
const REFERER: &str = "https://live.douyin.com";

// ---------------------------------------------------------------------------
// DouyinExtractor
// ---------------------------------------------------------------------------

/// Douyin (抖音) live streaming platform extractor.
pub struct DouyinExtractor {
    http: HttpClient,
    room_url_re: Regex,
    mobile_url_re: Regex,
    /// Cached ttwid, lazily fetched on first request.
    ttwid: OnceCell<String>,
    /// A-Bogus signer (mutex because `generate_abogus` mutates internal state).
    abogus: Mutex<ABogus>,
    /// Optional auth cookies (sessionid, uid_tt, etc.) for APIs that require login.
    auth_cookies: Mutex<Option<String>>,
}

impl DouyinExtractor {
    pub fn new() -> Self {
        let abogus = ABogus::new(None, Some(crate::USER_AGENT), None);

        Self {
            http: HttpClient::builder()
                .build()
                .expect("failed to build HTTP client"),
            room_url_re: Regex::new(r"(?:https?://)?(?:www\.)?live\.douyin\.com/(\d+)").unwrap(),
            mobile_url_re: Regex::new(r"(?:https?://)?(?:www\.)?v\.douyin\.com/\w+").unwrap(),
            ttwid: OnceCell::new(),
            abogus: Mutex::new(abogus),
            auth_cookies: Mutex::new(None),
        }
    }

    /// Set additional auth cookies (e.g. sessionid, uid_tt) for APIs that require login.
    pub fn with_auth_cookies(self, cookies: &str) -> Self {
        *self.auth_cookies.lock().unwrap() = Some(cookies.to_string());
        self
    }

    /// Build a signed URL with common params and A-Bogus signature.
    ///
    /// Mirrors the Dart `DouyinUtils.buildRequestUrl`.
    fn build_signed_url(&self, base_url: &str, params: &HashMap<&str, &str>) -> String {
        let mut all_params: HashMap<&str, &str> = HashMap::new();
        // Common params (matching Dart's buildRequestUrl)
        all_params.insert("aid", "6383");
        all_params.insert("compress", "gzip");
        all_params.insert("device_platform", "webapp");
        all_params.insert("browser_language", "zh-CN");
        all_params.insert("browser_platform", "Win32");
        all_params.insert("browser_name", "Edge");
        all_params.insert("browser_version", "125.0.0.0");
        all_params.insert("msToken", " "); // Dart generates a random msToken

        // Merge caller params (override common if needed)
        for (k, v) in params {
            all_params.insert(k, v);
        }

        // Build query string
        let query: String = all_params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        // Sign with A-Bogus
        let signed_query = self.abogus.lock().unwrap().generate_abogus(&query, "").0;

        // Build final URL
        if base_url.ends_with('?') || base_url.ends_with('/') {
            format!("{base_url}?{signed_query}")
        } else if base_url.contains('?') {
            format!("{base_url}&{signed_query}")
        } else {
            format!("{base_url}?{signed_query}")
        }
    }

    /// Convert a long roomId (>16 chars) to a webRid by fetching the reflow page.
    /// Short roomIds are already webRids and returned as-is.
    async fn get_web_rid(&self, room_id: &str) -> String {
        if room_id.len() <= 16 {
            return room_id.to_string();
        }
        let url = format!("{REFLOW_URL}{room_id}");
        let html = match self.http.get_text(&url).await {
            Ok(t) => t,
            Err(_) => return room_id.to_string(),
        };
        let re = Regex::new(r#"mysteryMan\":1,\"webRid\":\"([^\"]+)\",\"desensitizedNickname"#).unwrap();
        match re.captures(&html).and_then(|c| c.get(1)) {
            Some(m) => m.as_str().to_string(),
            None => room_id.to_string(),
        }
    }

    /// Ensure a ttwid cookie is available, fetching one if needed.
    async fn ensure_ttwid(&self) -> &str {
        self.ttwid
            .get_or_init(|| async {
                GlobalTtwidManager::ensure_global_ttwid(self.http.inner())
                    .await
                    .unwrap_or_else(|_| {
                        tracing::warn!("Failed to fetch ttwid, using default");
                        crate::extractor::platforms::douyin::utils::DEFAULT_TTWID.to_string()
                    })
            })
            .await
    }

    // ------------------------------------------------------------------
    // HTTP helpers
    // ------------------------------------------------------------------

    /// Extract `ttwid` from the response `set-cookie` headers.
    fn extract_ttwid(resp: &reqwest::Response) -> Option<String> {
        for value in resp.headers().get_all(header::SET_COOKIE).iter() {
            if let Ok(s) = value.to_str() {
                if s.starts_with("ttwid=") {
                    return s
                        .split(';')
                        .next()
                        .and_then(|kv| kv.strip_prefix("ttwid="))
                        .map(|v| v.to_string());
                }
            }
        }
        None
    }

    /// Build request headers with ttwid cookie (and optional auth cookies).
    /// Also sets the merged cookie on the shared HttpClient.
    /// Matches Dart `getRequestHeaders`: Authority, Referer, Cookie.
    async fn request_headers(&self) -> HeaderMap {
        let ttwid = self.ensure_ttwid().await;
        let auth = self.auth_cookies.lock().unwrap().clone();
        let cookie = match auth {
            Some(ref auth) => format!("ttwid={ttwid}; {auth}"),
            None => format!("ttwid={ttwid}"),
        };
        self.http.set_cookies(&cookie);
        let mut headers = HeaderMap::new();
        headers.insert(header::REFERER, HeaderValue::from_static(REFERER));
        headers
    }

    /// Fetch a JSON response from Douyin API (ttwid cookie, no ABogus).
    async fn fetch_json(&self, url: &str) -> Result<JsonValue> {
        let headers = self.request_headers().await;
        self.http.get_json_with_headers(url, &headers).await
    }

    /// Fetch the room-enter API via POST. Returns `(json, optional_ttwid_from_response)`.
    /// Note: This endpoint does NOT require A-Bogus signing.
    async fn fetch_room_enter(&self, web_rid: &str) -> Result<(JsonValue, Option<String>)> {
        let url = format!(
            "{ROOM_ENTER_URL}?aid=6383&app_name=douyin_web&live_id=1&device_platform=webapp&language=zh-CN&enter_from=web_live&browser_language=zh-CN&browser_platform=Win32&browser_name=Mozilla&browser_version=131.0.0.0"
        );
        let headers = self.request_headers().await;
        let resp = self
            .http
            .request(reqwest::Method::POST, &url)
            .headers(headers)
            .form(&[
                ("web_rid", web_rid),
                ("enter_from", "web_live"),
                ("action", "enter"),
                ("live_id", "1"),
                ("is_need_double_stream", "false"),
            ])
            .send()
            .await?;
        let ttwid = Self::extract_ttwid(&resp);
        let text = resp.text().await?;
        if text.is_empty() {
            return Err(ExtractorError::Other("empty response from Douyin API".into()));
        }
        let json: JsonValue = serde_json::from_str(&text)?;
        Ok((json, ttwid))
    }

    /// Debug: fetch raw room-enter response as string.
    pub async fn fetch_room_enter_debug(&self, web_rid: &str) -> Result<String> {
        let url = format!(
            "{ROOM_ENTER_URL}?aid=6383&app_name=douyin_web&live_id=1&device_platform=webapp&language=zh-CN&enter_from=web_live&browser_language=zh-CN&browser_platform=Win32&browser_name=Mozilla&browser_version=131.0.0.0"
        );
        let headers = self.request_headers().await;
        let resp = self
            .http
            .request(reqwest::Method::POST, &url)
            .headers(headers)
            .form(&[
                ("web_rid", web_rid),
                ("enter_from", "web_live"),
                ("action", "enter"),
                ("live_id", "1"),
                ("is_need_double_stream", "false"),
            ])
            .send()
            .await?;
        let text = resp.text().await?;
        Ok(text)
    }

    // ------------------------------------------------------------------
    // Data extraction helpers
    // ------------------------------------------------------------------

    /// Extract the first room data object from the room-enter response.
    ///
    /// The response shape is:
    /// `{ "data": { "data": [ { ...room... } ], "enter_room_id": ..., "user": {...} } }`
    fn first_room_data(json: &JsonValue) -> Option<&JsonValue> {
        json.get("data")
            .and_then(|d| d.get("data"))
            .and_then(|d| d.as_array())
            .and_then(|a| a.first())
    }

    /// Extract the numeric room id from the room data.
    fn room_id_from_data(room: &JsonValue) -> String {
        room.get("id_str")
            .and_then(|v| v.as_str())
            .or_else(|| room.get("id").and_then(|v| v.as_str()))
            .unwrap_or("0")
            .to_string()
    }

    /// Check whether a room is currently live.
    ///
    /// Douyin uses `status == 2` for live, `status == 4` for offline.
    fn is_room_live(room: &JsonValue) -> bool {
        room.get("status").and_then(|v| v.as_i64()).unwrap_or(0) == 2
    }

    /// Build danmaku connection data from the room-enter response.
    fn build_danmaku_data(room: &JsonValue, web_rid: &str) -> DouyinDanmakuData {
        let room_id_str = Self::room_id_from_data(room);
        DouyinDanmakuData {
            room_id: room_id_str,
            web_rid: web_rid.to_string(),
        }
    }

    // ------------------------------------------------------------------
    // Room item parsing
    // ------------------------------------------------------------------

    /// Parse a single room listing item from the Douyin room list response.
    ///
    /// Douyin returns room items in `data.data` arrays where each item has:
    /// `web_rid`, `room` (with `id_str`, `title`, `cover`, `user_count_str`),
    /// and `owner` (with `nickname`, `avatar_thumb`).
    fn parse_room_item_from_enter(item: &JsonValue) -> Option<LiveRoomItem> {
        let room = item.get("room")?;
        let room_id = room
            .get("id_str")
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .to_string();
        let web_rid = item.get("web_rid").and_then(|v| v.as_str()).unwrap_or("");

        let title = room
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let cover = room
            .get("cover")
            .and_then(|v| v.get("url_list"))
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let online = room
            .get("user_count_str")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let owner = item.get("owner").unwrap_or(&JsonValue::Null);
        let user_name = owner
            .get("nickname")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let user_avatar = owner
            .get("avatar_thumb")
            .and_then(|v| v.get("url_list"))
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Prefer web_rid for the URL, fall back to numeric id.
        let url_id = if !web_rid.is_empty() {
            web_rid
        } else {
            &room_id
        };

        Some(LiveRoomItem {
            room_id: url_id.to_string(),
            title,
            cover,
            online,
            user_name,
            user_avatar,
            url: format!("{}/{}", BASE_URL, url_id),
            platform: PLATFORM_ID.to_string(),
        })
    }

    /// Debug: fetch raw search response as string.
    pub async fn search_rooms_debug(&self, keyword: &str) -> Result<String> {
        let encoded_kw = percent_encoding::utf8_percent_encode(keyword, percent_encoding::NON_ALPHANUMERIC).to_string();
        let mut params = HashMap::new();
        params.insert("channel", "channel_pc_web");
        params.insert("search_channel", "aweme_live");
        params.insert("keyword", encoded_kw.as_str());
        params.insert("search_source", "switch_tab");
        params.insert("query_correct_type", "1");
        params.insert("is_filter_search", "0");
        params.insert("offset", "0");
        params.insert("count", "10");
        let url = self.build_signed_url(SEARCH_ROOMS_URL, &params);
        let headers = self.request_headers().await;
        self.http.get_text_with_headers(&url, &headers).await
    }
}

// ---------------------------------------------------------------------------
// LiveExtractor implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl LiveExtractor for DouyinExtractor {
    fn id(&self) -> &str {
        PLATFORM_ID
    }

    fn name(&self) -> &str {
        PLATFORM_NAME
    }

    fn set_cookies(&self, cookies: &str) {
        *self.auth_cookies.lock().unwrap() = Some(cookies.to_string());
    }

    fn supports_url(&self, url: &str) -> bool {
        self.room_url_re.is_match(url)
            || self.mobile_url_re.is_match(url)
            || url.contains("douyin.com")
    }

    fn extract_room_id(&self, url: &str) -> Option<String> {
        self.room_url_re
            .captures(url)
            .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
    }

    // ------------------------------------------------------------------
    // Discovery
    // ------------------------------------------------------------------

    async fn get_categories(&self) -> Result<Vec<LiveCategory>> {
        // Douyin categories are embedded in the homepage HTML (like Dart reference).
        let headers = self.request_headers().await;
        let html = self.http.get_text_with_headers("https://live.douyin.com/", &headers).await?;

        // Extract categoryData from RENDER_DATA script via regex.
        let re = regex::Regex::new(
            r#"\{\\"pathname\\":\\"\/\\",\\"categoryData.*?\],"#,
        )
        .map_err(|e| ExtractorError::Other(e.to_string()))?;

        let render_data = re
            .find(&html)
            .map(|m| {
                m.as_str()
                    .replace("\\\"", "\"")
                    .replace(r"\\", r"\")
                    .trim_end_matches("],")
                    .to_string()
            })
            .ok_or_else(|| ExtractorError::Other("categoryData not found in HTML".into()))?;

        let json: JsonValue =
            serde_json::from_str(&render_data).unwrap_or(JsonValue::Null);

        let category_arr = json
            .get("categoryData")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ExtractorError::Other("missing categoryData".into()))?;

        let mut categories: Vec<LiveCategory> = Vec::new();
        for item in category_arr {
            let partition = match item.get("partition") {
                Some(p) => p,
                None => continue,
            };
            let id_str = partition
                .get("id_str")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let ptype = partition
                .get("type")
                .and_then(|v| v.as_i64().map(|i| i.to_string()))
                .unwrap_or_default();
            let id = format!("{id_str},{ptype}");
            let name = partition
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let mut sub_categories: Vec<LiveSubCategory> = Vec::new();
            // Insert the category itself as the first sub-category.
            sub_categories.push(LiveSubCategory {
                id: id.clone(),
                name: name.clone(),
                parent_id: Some(id.clone()),
            });

            if let Some(sub_list) = item.get("sub_partition").and_then(|v| v.as_array()) {
                for sub in sub_list {
                    let sp = match sub.get("partition") {
                        Some(s) => s,
                        None => continue,
                    };
                    let sub_id_str = sp.get("id_str").and_then(|v| v.as_str()).unwrap_or("");
                    let sub_type = sp
                        .get("type")
                        .and_then(|v| v.as_i64().map(|i| i.to_string()))
                        .unwrap_or_default();
                    let sub_id = format!("{sub_id_str},{sub_type}");
                    let sub_name = sp.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    sub_categories.push(LiveSubCategory {
                        id: sub_id,
                        name: sub_name,
                        parent_id: Some(id.clone()),
                    });
                }
            }

            categories.push(LiveCategory {
                id,
                name,
                sub_categories,
            });
        }

        Ok(categories)
    }

    async fn search_rooms(&self, keyword: &str, page: u32) -> Result<LiveSearchRoomResult> {
        let offset = (page.saturating_sub(1)) * 10;
        let offset_str = offset.to_string();
        let encoded_kw = percent_encoding::utf8_percent_encode(keyword, percent_encoding::NON_ALPHANUMERIC).to_string();
        let count_str = "10";
        let mut params = HashMap::new();
        params.insert("channel", "channel_pc_web");
        params.insert("search_channel", "aweme_live");
        params.insert("keyword", &encoded_kw);
        params.insert("search_source", "switch_tab");
        params.insert("query_correct_type", "1");
        params.insert("is_filter_search", "0");
        params.insert("offset", &offset_str);
        params.insert("count", count_str);
        let url = self.build_signed_url(SEARCH_ROOMS_URL, &params);

        let json = self.fetch_json(&url).await?;

        let empty = vec![];
        let items_arr = json.get("data").and_then(|v| v.as_array()).unwrap_or(&empty);

        let mut items: Vec<LiveRoomItem> = Vec::new();
        for item in items_arr {
            let raw = item
                .get("lives")
                .and_then(|l| l.get("rawdata"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if raw.is_empty() {
                continue;
            }
            let room_json: JsonValue = match serde_json::from_str(raw) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let web_rid = room_json
                .get("owner")
                .and_then(|o| o.get("web_rid"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let title = room_json
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let cover = room_json
                .get("cover")
                .and_then(|v| v.get("url_list"))
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let user_name = room_json
                .get("owner")
                .and_then(|o| o.get("nickname"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let online = room_json
                .get("stats")
                .and_then(|s| s.get("total_user"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            items.push(LiveRoomItem {
                room_id: web_rid.clone(),
                title,
                cover,
                online,
                user_name,
                user_avatar: String::new(),
                url: format!("{BASE_URL}/{web_rid}"),
                platform: PLATFORM_ID.to_string(),
            });
        }

        Ok(LiveSearchRoomResult {
            has_more: items.len() >= 10,
            items,
        })
    }

    async fn search_anchors(&self, _keyword: &str, _page: u32) -> Result<LiveSearchAnchorResult> {
        Err(ExtractorError::Other(
            "抖音暂不支持搜索主播".into(),
        ))
    }

    async fn get_category_rooms(
        &self,
        category: &LiveSubCategory,
        page: u32,
    ) -> Result<LiveCategoryResult> {
        let ids: Vec<&str> = category.id.split(',').collect();
        let partition_id = ids.first().unwrap_or(&"");
        let partition_type = ids.get(1).unwrap_or(&"");
        let offset = (page.saturating_sub(1)) * 15;
        let offset_str = offset.to_string();
        let count_str = "15";
        let mut params = HashMap::new();
        params.insert("app_name", "douyin_web");
        params.insert("live_id", "1");
        params.insert("language", "zh-CN");
        params.insert("enter_from", "link_share");
        params.insert("count", count_str);
        params.insert("offset", &offset_str);
        params.insert("partition", partition_id);
        params.insert("partition_type", partition_type);
        params.insert("req_from", "2");
        let url = self.build_signed_url(CATEGORY_ROOMS_URL, &params);

        let json = self.fetch_json(&url).await?;

        let empty = vec![];
        let rooms = json
            .get("data")
            .and_then(|d| d.get("data"))
            .and_then(|v| v.as_array())
            .unwrap_or(&empty);

        let mut items: Vec<LiveRoomItem> = Vec::new();
        for r in rooms {
            if let Some(item) = Self::parse_room_item_from_enter(r) {
                items.push(item);
            }
        }

        Ok(LiveCategoryResult {
            has_more: items.len() >= 15,
            items,
        })
    }

    async fn get_recommend_rooms(&self, _page: u32) -> Result<LiveCategoryResult> {
        let url = format!(
            "{FEED_URL}?aid=6383&app_name=douyin_web&need_map=1&is_draw=1&inner_from_drawer=0&enter_source=web_homepage_hot_web_live_card&source_key=web_homepage_hot_web_live_card"
        );

        let json = self.fetch_json(&url).await?;

        let empty = vec![];
        let feed_arr = json
            .get("data")
            .and_then(|v| v.as_array())
            .unwrap_or(&empty);

        let mut items: Vec<LiveRoomItem> = Vec::new();
        for entry in feed_arr {
            let item = entry.get("data").unwrap_or(entry);
            let web_rid = item
                .get("owner")
                .and_then(|o| o.get("web_rid"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let title = item
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let cover = item
                .get("cover")
                .and_then(|v| v.get("url_list"))
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let user_name = item
                .get("owner")
                .and_then(|o| o.get("nickname"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let online = item
                .get("room_view_stats")
                .and_then(|s| s.get("display_value"))
                .and_then(|v| v.as_str().and_then(|s| s.parse::<u64>().ok()).or_else(|| v.as_u64()))
                .unwrap_or(0);

            items.push(LiveRoomItem {
                room_id: web_rid.clone(),
                title,
                cover,
                online,
                user_name,
                user_avatar: String::new(),
                url: format!("{BASE_URL}/{web_rid}"),
                platform: PLATFORM_ID.to_string(),
            });
        }

        Ok(LiveCategoryResult {
            has_more: items.len() >= 15,
            items,
        })
    }

    // ------------------------------------------------------------------
    // Room detail & playback
    // ------------------------------------------------------------------

    async fn get_room_detail(&self, room_id: &str) -> Result<LiveRoomDetail> {
        // Long roomId (>16 chars) needs conversion to webRid.
        let web_rid = self.get_web_rid(room_id).await;
        let (json, _ttwid) = self.fetch_room_enter(&web_rid).await?;

        // Response shape: { "data": { "data": [ { room... } ], "user": { user... } } }
        let data = json
            .get("data")
            .ok_or_else(|| ExtractorError::Other("missing `data` in room response".into()))?;

        let user_data = data.get("user").unwrap_or(&JsonValue::Null);

        // Room data may be empty when the streamer is offline.
        let room_opt = data
            .get("data")
            .and_then(|d| d.as_array())
            .and_then(|a| a.first());

        let (room, status) = match room_opt {
            Some(room) => (room, Self::is_room_live(room)),
            None => {
                // Offline: return user info only.
                let user_name = user_data
                    .get("nickname")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let user_avatar = user_data
                    .get("avatar_thumb")
                    .and_then(|v| v.get("url_list"))
                    .and_then(|v| v.as_array())
                    .and_then(|a| a.first())
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let _enter_room_id = data
                    .get("enter_room_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                return Ok(LiveRoomDetail {
                    room_id: web_rid.clone(),
                    title: String::new(),
                    cover: String::new(),
                    online: 0,
                    status: false,
                    url: format!("{BASE_URL}/{web_rid}"),
                    user_name,
                    user_avatar,
                    platform: PLATFORM_ID.to_string(),
                    data: None,
                    danmaku_data: None,
                });
            }
        };

        let owner = room.get("owner").unwrap_or(user_data);

        let title = room
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let cover = if status {
            room.get("cover")
                .and_then(|v| v.get("url_list"))
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        } else {
            String::new()
        };
        let online = room
            .get("room_view_stats")
            .and_then(|s| s.get("display_value"))
            .and_then(|v| v.as_str().and_then(|s| s.parse::<u64>().ok()).or_else(|| v.as_u64()))
            .unwrap_or(0);

        let user_name = owner.get("nickname").and_then(|v| v.as_str()).unwrap_or(
            user_data.get("nickname").and_then(|v| v.as_str()).unwrap_or("")
        ).to_string();
        let user_avatar = owner.get("avatar_thumb")
            .or_else(|| user_data.get("avatar_thumb"))
            .and_then(|v| v.get("url_list"))
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Store stream_url as platform-specific data (raw JSON).
        let stream_url = if status {
            room.get("stream_url").cloned().unwrap_or(JsonValue::Null)
        } else {
            JsonValue::Object(serde_json::Map::new())
        };
        // Numeric roomId from API (used for danmaku connection).
        let _numeric_room_id = Self::room_id_from_data(room);
        let danmaku_data = Self::build_danmaku_data(room, &web_rid);
        let danmaku_val = Some(serde_json::to_value(danmaku_data).unwrap_or_default());

        Ok(LiveRoomDetail {
            room_id: web_rid.clone(),
            title,
            cover,
            online,
            status,
            url: format!("{BASE_URL}/{web_rid}"),
            user_name,
            user_avatar,
            platform: PLATFORM_ID.to_string(),
            data: Some(stream_url),
            danmaku_data: danmaku_val,
        })
    }

    async fn get_play_qualities(&self, detail: &LiveRoomDetail) -> Result<Vec<LivePlayQuality>> {
        let stream_url = match &detail.data {
            Some(d) => d,
            None => return Ok(vec![LivePlayQuality { quality: "默认".into(), data: "默认".into() }]),
        };

        // Try live_core_sdk_data.pull_data first (new format).
        let sdk_qualities = stream_url
            .get("live_core_sdk_data")
            .and_then(|d| d.get("pull_data"))
            .and_then(|d| d.get("options"))
            .and_then(|d| d.get("qualities"))
            .and_then(|v| v.as_array());

        let stream_data_str = stream_url
            .get("live_core_sdk_data")
            .and_then(|d| d.get("pull_data"))
            .and_then(|d| d.get("stream_data"))
            .and_then(|v| v.as_str());

        let mut qualities: Vec<LivePlayQuality> = Vec::new();

        if let (Some(q_list), Some(sd_str)) = (sdk_qualities, stream_data_str) {
            // Parse stream_data JSON.
            let sd: JsonValue = serde_json::from_str(sd_str).unwrap_or(JsonValue::Null);
            let sd_map = sd.get("data").and_then(|v| v.as_object());

            for q in q_list {
                let _level = q.get("level").and_then(|v| v.as_i64()).unwrap_or(0);
                let name = q.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let sdk_key = q.get("sdk_key").and_then(|v| v.as_str()).unwrap_or("");

                let mut urls: Vec<String> = Vec::new();
                if let Some(map) = sd_map {
                    if let Some(flv) = map.get(sdk_key).and_then(|v| v.get("main")).and_then(|v| v.get("flv")).and_then(|v| v.as_str()) {
                        if !flv.is_empty() {
                            urls.push(flv.to_string());
                        }
                    }
                    if let Some(hls) = map.get(sdk_key).and_then(|v| v.get("main")).and_then(|v| v.get("hls")).and_then(|v| v.as_str()) {
                        if !hls.is_empty() {
                            urls.push(hls.to_string());
                        }
                    }
                }

                if !urls.is_empty() {
                    qualities.push(LivePlayQuality {
                        quality: name,
                        data: serde_json::to_string(&urls).unwrap_or_default(),
                    });
                }
            }
            qualities.sort_by(|a, b| {
                // Sort by level descending (higher quality first).
                let la = q_list.iter().find(|q| q.get("name").and_then(|v| v.as_str()) == Some(&a.quality)).and_then(|q| q.get("level")).and_then(|v| v.as_i64()).unwrap_or(0);
                let lb = q_list.iter().find(|q| q.get("name").and_then(|v| v.as_str()) == Some(&b.quality)).and_then(|q| q.get("level")).and_then(|v| v.as_i64()).unwrap_or(0);
                lb.cmp(&la)
            });
        }

        // Fallback: use flv_pull_url / hls_pull_url_map.
        if qualities.is_empty() {
            let flv_map = stream_url.get("flv_pull_url").and_then(|v| v.as_object());
            let hls_map = stream_url.get("hls_pull_url_map").and_then(|v| v.as_object());

            let mut all_urls: Vec<String> = Vec::new();
            if let Some(m) = flv_map {
                all_urls.extend(m.values().filter_map(|v| v.as_str().map(String::from)));
            }
            if let Some(m) = hls_map {
                all_urls.extend(m.values().filter_map(|v| v.as_str().map(String::from)));
            }

            if !all_urls.is_empty() {
                qualities.push(LivePlayQuality {
                    quality: "默认".into(),
                    data: serde_json::to_string(&all_urls).unwrap_or_default(),
                });
            }
        }

        if qualities.is_empty() {
            qualities.push(LivePlayQuality { quality: "默认".into(), data: "默认".into() });
        }

        Ok(qualities)
    }

    async fn get_play_urls(
        &self,
        _detail: &LiveRoomDetail,
        quality: &LivePlayQuality,
    ) -> Result<LivePlayUrl> {
        // quality.data is a JSON array of URLs (set by get_play_qualities).
        let urls: Vec<String> = serde_json::from_str(&quality.data).unwrap_or_default();
        if urls.is_empty() {
            return Err(ExtractorError::NoStreamsFound);
        }
        // Determine type from first URL.
        let url_type = if urls[0].contains(".flv") || urls[0].contains("flv_pull") {
            UrlType::Flv
        } else {
            UrlType::M3u8
        };
        Ok(LivePlayUrl { urls, url_type })
    }

    async fn get_live_status(&self, room_id: &str) -> Result<bool> {
        let (json, _) = self.fetch_room_enter(room_id).await?;

        let room = match Self::first_room_data(&json) {
            Some(r) => r,
            None => return Ok(false),
        };

        Ok(Self::is_room_live(room))
    }

    // ------------------------------------------------------------------
    // Super-chat (Douyin uses its own paid message system via danmaku)
    // ------------------------------------------------------------------

    async fn get_super_chat_messages(&self, _room_id: &str) -> Result<Vec<LiveSuperChatMessage>> {
        // Douyin paid messages (打赏/红包) are delivered through the
        // WebSocket danmaku stream as GiftMessage protobuf messages,
        // not through a separate REST API.
        Ok(vec![])
    }
}
