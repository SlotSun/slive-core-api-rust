//! Huya (虎牙) live streaming platform extractor.
//!
//! Ports the logic from the Dart `HuyaSite` class. Parses room pages via regex
//! to extract embedded JSON stream metadata, then builds play URLs through the
//! TARS-based CDN token API.

use parking_lot::Mutex;

use async_trait::async_trait;
use regex::Regex;
use serde_json::Value as JsonValue;

use crate::extractor::error::ExtractorError;
use crate::extractor::http_client::HttpClient;
use crate::extractor::models::*;
use crate::extractor::platforms::huya::anti_code::build_anti_code;
use crate::extractor::platforms::huya::models::*;
use crate::extractor::{LiveExtractor, Result};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PLATFORM_ID: &str = "huya";
const PLATFORM_NAME: &str = "虎牙";

const BASE_URL: &str = "https://www.huya.com";

/// Sub-categories for a given bussType.
const CATEGORY_SUB_URL: &str = "https://live.cdn.huya.com/liveconfig/game/bussLive?bussType={id}";

/// Paginated room list within a category.
const CATEGORY_ROOMS_URL: &str = "https://www.huya.com/cache.php?m=LiveList&do=getLiveListByPage&tagAll=0&gameId={id}&page={page}";

/// Paginated recommended room list (no gameId).
const RECOMMEND_ROOMS_URL: &str =
    "https://www.huya.com/cache.php?m=LiveList&do=getLiveListByPage&tagAll=0&page={page}";

/// Huya search API (shared between rooms and anchors).
const SEARCH_URL: &str = "https://search.cdn.huya.com/";

/// Top-level category list (hardcoded from the Huya website).
const CATEGORIES: &[(&str, &str)] = &[
    ("1", "网游"),
    ("2", "单机"),
    ("8", "娱乐"),
    ("3", "手游"),
];

/// Default quality list when `vMultiStreamInfo` is empty.
///
/// These are the standard Huya quality tiers, ordered from highest to lowest.
const DEFAULT_QUALITIES: &[(&str, i32)] = &[
    ("原画", 0),
    ("蓝光20M", 20000),
    ("蓝光10M", 10000),
    ("蓝光8M", 8000),
    ("蓝光4M", 4000),
    ("超清", 2000),
    ("高清", 1000),
    ("流畅", 500),
];

/// Rows per search page.
const SEARCH_ROWS: u32 = 20;

// ---------------------------------------------------------------------------
// Extractor
// ---------------------------------------------------------------------------

/// Huya (虎牙) live streaming platform extractor.
pub struct HuyaExtractor {
    http: HttpClient,
    room_url_re: Regex,
    sdk_ua: Mutex<String>,
}

impl HuyaExtractor {
    pub fn new() -> Self {
        Self {
            http: HttpClient::builder()
                .build()
                .expect("failed to build HTTP client"),
            room_url_re: Regex::new(r"(?:https?://)?(?:www\.)?huya\.com/(\d+)").unwrap(),
            sdk_ua: Mutex::new(String::new()),
        }
    }

    /// Set a custom User-Agent for all HTTP requests (e.g. HYSDK_UA).
    /// Also stores the value so it can be included in play URL headers.
    pub fn set_sdk_ua(&self, ua: &str) {
        *self.sdk_ua.lock() = ua.to_string();
        if let Err(e) = self.http.set_user_agent(ua) {
            tracing::warn!("Failed to set Huya SDK UA: {e}");
        }
    }

    /// Get the current SDK UA string (empty if not set).
    fn get_sdk_ua(&self) -> String {
        self.sdk_ua.lock().clone()
    }

    fn room_page_url(room_id: &str) -> String {
        format!("{}/{}", BASE_URL, room_id)
    }

    // ------------------------------------------------------------------
    // HTTP helpers
    // ------------------------------------------------------------------

    /// Fetch a URL and parse the response body as JSON.
    async fn fetch_json(&self, url: &str) -> Result<JsonValue> {
        self.http.get_json(url).await
    }

    // ------------------------------------------------------------------
    // HTML parsing helpers
    // ------------------------------------------------------------------

    /// Extract a JSON object from `html` that immediately follows `keyword`.
    ///
    /// This correctly handles nested braces, string literals and escape
    /// sequences by counting brace depth instead of relying on a fragile regex.
    fn extract_json_block(html: &str, keyword: &str) -> Option<JsonValue> {
        let start = html.find(keyword)?;
        let after = &html[start + keyword.len()..];
        let brace_offset = after.find('{')?;
        let rest = &after[brace_offset..];

        let mut depth: u32 = 0;
        let mut in_string = false;
        let mut escape_next = false;
        let mut end = 0usize;

        for (i, c) in rest.char_indices() {
            if escape_next {
                escape_next = false;
                continue;
            }
            if c == '\\' && in_string {
                escape_next = true;
                continue;
            }
            if c == '"' {
                in_string = !in_string;
                continue;
            }
            if in_string {
                continue;
            }
            match c {
                '{' => depth += 1,
                '}' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        end = i + c.len_utf8();
                        break;
                    }
                }
                _ => {}
            }
        }

        if end == 0 || end > rest.len() {
            return None;
        }

        let mut json_str = rest[..end].to_string();

        // Huya's embedded JS occasionally contains trailing commas which are
        // invalid JSON. Strip them before parsing.
        lazy_remove_trailing_commas(&mut json_str);

        serde_json::from_str(&json_str).ok()
    }

    /// Parse `TT_ROOM_DATA` and the `stream` block from a Huya room page.
    ///
    /// Returns `(room_data, stream_data)` on success.
    fn parse_room_page(html: &str) -> Option<(JsonValue, JsonValue)> {
        let room_data = Self::extract_json_block(html, "TT_ROOM_DATA")?;
        let stream_data = Self::extract_json_block(html, "stream:")?;
        Some((room_data, stream_data))
    }

    // ------------------------------------------------------------------
    // Stream data helpers
    // ------------------------------------------------------------------

    /// Build a [`HuyaUrlDataModel`] from the raw `stream` JSON block.
    fn build_url_data(stream_data: &JsonValue) -> Option<HuyaUrlDataModel> {
        let data = stream_data.get("data")?;
        let first = data.get(0)?;
        let stream_info_list = first.get("gameStreamInfoList")?;
        let multi_stream_info = first.get("vMultiStreamInfo");

        let mut lines: Vec<HuyaLineModel> = Vec::new();
        if let Some(list) = stream_info_list.as_array() {
            for info in list {
                let s_flv_url = str_field(info, "sFlvUrl");
                let s_hls_url = str_field(info, "sHlsUrl");
                let s_flv_anti_code = str_field(info, "sFlvAntiCode");
                let s_hls_anti_code = str_field(info, "sHlsAntiCode");
                let s_stream_name = str_field(info, "sStreamName");
                let s_cdn_type = str_field(info, "sCdnType");
                let l_channel_id = info.get("lChannelId").and_then(|v| v.as_i64()).unwrap_or(0);

                if !s_flv_url.is_empty() {
                    lines.push(HuyaLineModel {
                        line: s_flv_url,
                        line_type: HuyaLineType::Flv,
                        flv_anti_code: s_flv_anti_code.clone(),
                        hls_anti_code: s_hls_anti_code.clone(),
                        stream_name: s_stream_name.clone(),
                        cdn_type: s_cdn_type.clone(),
                        presenter_uid: l_channel_id,
                    });
                }
                if !s_hls_url.is_empty() {
                    lines.push(HuyaLineModel {
                        line: s_hls_url,
                        line_type: HuyaLineType::Hls,
                        flv_anti_code: s_flv_anti_code,
                        hls_anti_code: s_hls_anti_code,
                        stream_name: s_stream_name,
                        cdn_type: s_cdn_type,
                        presenter_uid: l_channel_id,
                    });
                }
            }
        }

        let mut bit_rates: Vec<HuyaBitRateModel> = Vec::new();
        if let Some(info_list) = multi_stream_info.and_then(|v| v.as_array()) {
            for info in info_list {
                let name = str_field(info, "sDisplayName");
                let bit_rate = info.get("iBitRate").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                bit_rates.push(HuyaBitRateModel { name, bit_rate });
            }
        }

        // Pseudo-random UID for anti-code calculation.
        let uid = rand::random::<i64>();

        Some(HuyaUrlDataModel {
            lines,
            bit_rates,
            uid,
        })
    }

    /// Extract danmaku connection arguments from the raw `stream` JSON block.
    ///
    /// Reads from the same paths as the Dart code:
    ///   - `ayyuid`  = `data[0].gameLiveInfo.yyid`
    ///   - `top_sid` = `data[0].gameStreamInfoList[0].lChannelId`
    ///   - `sub_sid` = `data[0].gameStreamInfoList[0].lSubChannelId`
    fn build_danmaku_args(stream_data: &JsonValue) -> HuyaDanmakuArgs {
        let first = stream_data.get("data").and_then(|v| v.get(0));

        // ayyuid comes from gameLiveInfo.yyid
        let ayyuid = first
            .and_then(|d| d.get("gameLiveInfo"))
            .and_then(|g| g.get("yyid"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        // top_sid and sub_sid come from gameStreamInfoList[0]
        let stream_info = first
            .and_then(|d| d.get("gameStreamInfoList"))
            .and_then(|v| v.get(0));

        let top_sid = stream_info
            .and_then(|s| s.get("lChannelId"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let sub_sid = stream_info
            .and_then(|s| s.get("lSubChannelId"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        HuyaDanmakuArgs {
            ayyuid,
            top_sid,
            sub_sid,
        }
    }

    // ------------------------------------------------------------------
    // Room-item parsing (shared between category / recommend results)
    // ------------------------------------------------------------------

    /// Parse a single room listing item from the `datas` array.
    fn parse_room_item(item: &JsonValue) -> Option<LiveRoomItem> {
        let room_id = match item.get("profileRoom")? {
            JsonValue::Number(n) => n.to_string(),
            JsonValue::String(s) => s.clone(),
            _ => return None,
        };
        let title = str_field(item, "introduction");
        let title = if title.is_empty() {
            str_field(item, "roomName")
        } else {
            title
        };
        let mut cover = str_field(item, "screenshot");
        if !cover.contains('?') {
            cover.push_str("?x-oss-process=style/w338_h190&");
        }
        let online = match item.get("totalCount") {
            Some(JsonValue::Number(n)) => n.as_u64().unwrap_or(0),
            Some(JsonValue::String(s)) => s.parse::<u64>().unwrap_or(0),
            _ => 0,
        };
        let user_name = str_field(item, "nick");

        Some(LiveRoomItem {
            room_id: room_id.clone(),
            title,
            cover,
            online,
            user_name,
            user_avatar: String::new(),
            url: format!("{}/{}", BASE_URL, room_id),
            platform: PLATFORM_ID.to_string(),
        })
    }

    /// Parse a paginated room list response (`{ data: { datas, page, totalPage } }`).
    fn parse_room_list(json: &JsonValue) -> Result<LiveCategoryResult> {
        let data = json
            .get("data")
            .ok_or_else(|| ExtractorError::Other("missing `data` in response".into()))?;

        let datas = data
            .get("datas")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ExtractorError::Other("missing `datas` in response".into()))?;

        let current_page = data.get("page").and_then(|v| v.as_u64()).unwrap_or(0);
        let total_page = data.get("totalPage").and_then(|v| v.as_u64()).unwrap_or(0);

        let items: Vec<LiveRoomItem> = datas.iter().filter_map(Self::parse_room_item).collect();
        let has_more = current_page < total_page;

        Ok(LiveCategoryResult { has_more, items })
    }
}

// ---------------------------------------------------------------------------
// Free helper functions
// ---------------------------------------------------------------------------

/// Extract a string field from a JSON value, returning an empty string on miss.
fn str_field(v: &JsonValue, key: &str) -> String {
    v.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

/// Extract the `gid` field from a Huya sub-category item.
///
/// The API may return `gid` as:
/// - A Map: `{"value": "123,456"}`  → take first comma-separated value
/// - A number (i64/f64) → convert to string
/// - A string → use directly
fn extract_gid(item: &JsonValue) -> Option<String> {
    let gid = item.get("gid")?;
    match gid {
        JsonValue::Object(map) => {
            // Map case: extract "value" field, take first comma-separated part
            let value = map.get("value").and_then(|v| v.as_str()).unwrap_or("");
            let first = value.split(',').next().unwrap_or("").trim();
            if first.is_empty() {
                None
            } else {
                Some(first.to_string())
            }
        }
        JsonValue::Number(n) => {
            // Number case: convert to string
            if let Some(i) = n.as_i64() {
                Some(i.to_string())
            } else if let Some(f) = n.as_f64() {
                Some((f as i64).to_string())
            } else {
                None
            }
        }
        JsonValue::String(s) => {
            // String case: use directly
            if s.is_empty() { None } else { Some(s.clone()) }
        }
        _ => None,
    }
}

/// Remove trailing commas before `}` and `]` in a JSON string (in-place).
///
/// Huya's embedded JavaScript occasionally uses trailing commas which are
/// invalid in strict JSON.
fn lazy_remove_trailing_commas(s: &mut String) {
    // Simple state-machine replacement: ",}" -> "}" and ",]" -> "]"
    // We iterate from the end so indices remain valid.
    let bytes = unsafe { s.as_mut_vec() };
    let mut i = bytes.len();
    while i >= 2 {
        i -= 1;
        if (bytes[i] == b'}' || bytes[i] == b']') && bytes[i - 1] == b',' {
            // Check the comma is not inside a string by counting preceding unescaped quotes.
            // This is a simplified heuristic that works for the known Huya payloads.
            let before = &bytes[..i - 1];
            let quote_count = before.iter().filter(|&&b| b == b'"').count();
            if quote_count % 2 == 0 {
                bytes.remove(i - 1);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// LiveExtractor implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl LiveExtractor for HuyaExtractor {
    fn id(&self) -> &str {
        PLATFORM_ID
    }

    fn name(&self) -> &str {
        PLATFORM_NAME
    }

    fn supports_url(&self, url: &str) -> bool {
        self.room_url_re.is_match(url) || url.contains("huya.com")
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
        let mut categories: Vec<LiveCategory> = Vec::with_capacity(CATEGORIES.len());

        for &(id, name) in CATEGORIES {
            let url = CATEGORY_SUB_URL.replace("{id}", id);
            let mut sub_categories: Vec<LiveSubCategory> = Vec::new();

            if let Ok(json) = self.fetch_json(&url).await {
                if let Some(data) = json.get("data").and_then(|v| v.as_array()) {
                    for item in data {
                        // gid can be a Map({"value": "123,456"}), i64, f64, or string.
                        // Dart code handles all these cases.
                        let gid = extract_gid(item);
                        let game_name = item.get("gameFullName").and_then(|v| v.as_str());

                        if let (Some(gid), Some(game_name)) = (gid, game_name) {
                            let pic = format!("https://huyaimg.msstatic.com/cdnimage/game/{}-MS.jpg", gid);
                            sub_categories.push(LiveSubCategory {
                                id: gid,
                                name: game_name.to_string(),
                                parent_id: Some(id.to_string()),
                                pic: Some(pic),
                            });
                        }
                    }
                }
            }

            categories.push(LiveCategory {
                id: id.to_string(),
                name: name.to_string(),
                sub_categories,
            });
        }

        Ok(categories)
    }

    async fn search_rooms(&self, keyword: &str, page: u32) -> Result<LiveSearchRoomResult> {
        let start = page.saturating_sub(1) * SEARCH_ROWS;
        let url = format!(
            "{}?m=Search&do=getSearchContent&q={}&uid=0&v=4&typ=-5&livestate=0&rows={}&start={}",
            SEARCH_URL, keyword, SEARCH_ROWS, start,
        );

        let json = self.fetch_json(&url).await?;

        let response = json
            .get("response")
            .ok_or_else(|| ExtractorError::Other("missing `response` in search result".into()))?;

        // Room search results are under key "3".
        let section = response
            .get("3")
            .ok_or_else(|| ExtractorError::Other("missing section `3` in search result".into()))?;

        let docs = section
            .get("docs")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ExtractorError::Other("missing `docs` in search result".into()))?;

        let num_found = section
            .get("numFound")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let mut items: Vec<LiveRoomItem> = Vec::with_capacity(docs.len());
        for doc in docs {
            let room_id = doc
                .get("yyid")
                .and_then(|v| v.as_i64())
                .map(|v| v.to_string())
                .unwrap_or_default();
            let title = str_field(doc, "game_introduction");
            let cover = str_field(doc, "game_screenshot");
            let online = doc
                .get("game_total_count")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let user_name = str_field(doc, "game_nick");

            items.push(LiveRoomItem {
                room_id: room_id.clone(),
                title,
                cover,
                online,
                user_name,
                user_avatar: String::new(),
                url: format!("{}/{}", BASE_URL, room_id),
                platform: PLATFORM_ID.to_string(),
            });
        }

        let has_more = (start as u64 + items.len() as u64) < num_found;

        Ok(LiveSearchRoomResult { has_more, items })
    }

    async fn search_anchors(&self, keyword: &str, page: u32) -> Result<LiveSearchAnchorResult> {
        let start = page.saturating_sub(1) * SEARCH_ROWS;
        let url = format!(
            "{}?m=Search&do=getSearchContent&q={}&uid=0&v=1&typ=-5&livestate=0&rows={}&start={}",
            SEARCH_URL, keyword, SEARCH_ROWS, start,
        );

        let json = self.fetch_json(&url).await?;

        let response = json
            .get("response")
            .ok_or_else(|| ExtractorError::Other("missing `response` in search result".into()))?;

        // Anchor search results are under key "1".
        let section = response
            .get("1")
            .ok_or_else(|| ExtractorError::Other("missing section `1` in search result".into()))?;

        let docs = section
            .get("docs")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ExtractorError::Other("missing `docs` in search result".into()))?;

        let num_found = section
            .get("numFound")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let mut items: Vec<LiveAnchorItem> = Vec::with_capacity(docs.len());
        for doc in docs {
            let room_id = doc
                .get("room_id")
                .and_then(|v| v.as_i64())
                .map(|v| v.to_string());
            let user_name = str_field(doc, "game_nick");
            let user_avatar = str_field(doc, "game_avatarUrl180");
            let is_live = doc.get("gameLiveOn").and_then(|v| v.as_i64()).unwrap_or(0) != 0;

            items.push(LiveAnchorItem {
                user_id: String::new(),
                user_name,
                user_avatar,
                room_id: room_id.clone(),
                is_live,
                platform: PLATFORM_ID.to_string(),
                url: room_id
                    .as_ref()
                    .map(|id| format!("{}/{}", BASE_URL, id))
                    .unwrap_or_default(),
            });
        }

        let has_more = (start as u64 + items.len() as u64) < num_found;

        Ok(LiveSearchAnchorResult { has_more, items })
    }

    async fn get_category_rooms(
        &self,
        category: &LiveSubCategory,
        page: u32,
    ) -> Result<LiveCategoryResult> {
        let url = CATEGORY_ROOMS_URL
            .replace("{id}", &category.id)
            .replace("{page}", &page.to_string());

        let json = self.fetch_json(&url).await?;
        Self::parse_room_list(&json)
    }

    async fn get_recommend_rooms(&self, page: u32) -> Result<LiveCategoryResult> {
        let url = RECOMMEND_ROOMS_URL.replace("{page}", &page.to_string());

        let json = self.fetch_json(&url).await?;
        Self::parse_room_list(&json)
    }

    // ------------------------------------------------------------------
    // Room detail & playback
    // ------------------------------------------------------------------

    async fn get_room_detail(&self, room_id: &str) -> Result<LiveRoomDetail> {
        let url = Self::room_page_url(room_id);
        let html = self.http.get_text(&url).await?;

        let (room_data, stream_data) = Self::parse_room_page(&html)
            .ok_or_else(|| ExtractorError::Other("failed to parse room page HTML".into()))?;

        // ----- Room status from TT_ROOM_DATA -----
        let state = room_data
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("OFF");
        let is_replay = room_data
            .get("isReplay")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let status = state == "ON" && !is_replay;

        // ----- Room metadata from stream data -> gameLiveInfo -----
        // The Dart code reads metadata from streamDataJson["gameLiveInfo"],
        // NOT from TT_ROOM_DATA.
        let game_live_info = stream_data
            .get("data")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|v| v.get("gameLiveInfo"))
            .cloned()
            .unwrap_or(JsonValue::Null);

        let title = str_field(&game_live_info, "introduction");
        let cover = str_field(&game_live_info, "screenshot");
        let user_name = str_field(&game_live_info, "nick");
        let user_avatar = str_field(&game_live_info, "avatar180");
        let online = game_live_info
            .get("totalCount")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        // ----- Stream data for playback -----
        let url_data = Self::build_url_data(&stream_data).ok_or_else(|| {
            ExtractorError::Other("failed to build url data from stream info".into())
        })?;
        let danmaku_args = Self::build_danmaku_args(&stream_data);

        let data = serde_json::to_value(&url_data).ok();
        let danmaku_data = serde_json::to_value(&danmaku_args).ok();

        Ok(LiveRoomDetail {
            room_id: room_id.to_string(),
            title,
            cover,
            online,
            status,
            url,
            user_name,
            user_avatar,
            platform: PLATFORM_ID.to_string(),
            data,
            danmaku_data,
        })
    }

    async fn get_play_qualities(&self, detail: &LiveRoomDetail) -> Result<Vec<LivePlayQuality>> {
        let url_data: HuyaUrlDataModel = detail
            .data
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .ok_or_else(|| {
                ExtractorError::Other("missing HuyaUrlDataModel in room detail `data`".into())
            })?;

        let bit_rates = if url_data.bit_rates.is_empty() {
            DEFAULT_QUALITIES
                .iter()
                .map(|(name, rate)| HuyaBitRateModel {
                    name: name.to_string(),
                    bit_rate: *rate,
                })
                .collect::<Vec<_>>()
        } else {
            url_data.bit_rates
        };

        let qualities: Vec<LivePlayQuality> = bit_rates
            .into_iter()
            .map(|br| LivePlayQuality {
                quality: br.name,
                data: br.bit_rate.to_string(),
            })
            .collect();

        Ok(qualities)
    }

    async fn get_play_urls(
        &self,
        detail: &LiveRoomDetail,
        quality: &LivePlayQuality,
    ) -> Result<LivePlayUrl> {
        let url_data: HuyaUrlDataModel = detail
            .data
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .ok_or_else(|| {
                ExtractorError::Other("missing HuyaUrlDataModel in room detail `data`".into())
            })?;

        let bit_rate: i32 = quality.data.parse().unwrap_or(0);

        let mut urls: Vec<String> = Vec::new();
        let mut url_type = UrlType::Flv;

        for line in &url_data.lines {
            // Use the anti-code from the stream data directly (WEB mode).
            // No need to call the WUP CDN Token API — the anti-code in the
            // HTML already contains `fm`, `wsTime`, `fs`, etc. and we just
            // need to compute `wsSecret` locally via `build_anti_code`.
            let raw_anti_code = match line.line_type {
                HuyaLineType::Flv => &line.flv_anti_code,
                HuyaLineType::Hls => &line.hls_anti_code,
            };
            if raw_anti_code.is_empty() {
                continue;
            }

            let anti_code = build_anti_code(&line.stream_name, line.presenter_uid, raw_anti_code);

            let suffix = match line.line_type {
                HuyaLineType::Flv => "flv",
                HuyaLineType::Hls => "m3u8",
            };

            let mut url = format!(
                "{}/{}.{}?{}&codec=264",
                line.line, line.stream_name, suffix, anti_code
            );

            if bit_rate > 0 {
                url.push_str(&format!("&ratio={}", bit_rate));
            }

            if urls.is_empty() {
                url_type = match line.line_type {
                    HuyaLineType::Flv => UrlType::Flv,
                    HuyaLineType::Hls => UrlType::M3u8,
                };
            }
            urls.push(url);
        }

        if urls.is_empty() {
            return Err(ExtractorError::NoStreamsFound);
        }

        let headers = {
            let ua = self.get_sdk_ua();
            if ua.is_empty() {
                None
            } else {
                Some(vec![("user-agent".to_string(), ua)])
            }
        };

        Ok(LivePlayUrl {
            urls,
            url_type,
            headers,
        })
    }

    async fn get_live_status(&self, room_id: &str) -> Result<bool> {
        let url = Self::room_page_url(room_id);
        let html = self.http.get_text(&url).await?;

        let (room_data, _stream_data) = Self::parse_room_page(&html)
            .ok_or_else(|| ExtractorError::Other("failed to parse room page HTML".into()))?;

        let state = room_data
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or("OFF");
        let is_replay = room_data
            .get("isReplay")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        Ok(state == "ON" && !is_replay)
    }

    // ------------------------------------------------------------------
    // Super-chat (Huya does not have super-chat in the Bilibili sense)
    // ------------------------------------------------------------------

    async fn get_super_chat_messages(&self, _room_id: &str) -> Result<Vec<LiveSuperChatMessage>> {
        Ok(vec![])
    }
}
