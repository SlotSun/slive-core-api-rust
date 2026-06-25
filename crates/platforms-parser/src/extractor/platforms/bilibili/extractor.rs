//! Bilibili (哔哩哔哩) live streaming platform extractor.

use std::collections::HashMap;
use parking_lot::Mutex;

use async_trait::async_trait;
use regex::Regex;

use serde_json::Value as JsonValue;
use tracing::{debug, warn};

use super::models::*;
use super::wbi;
use crate::extractor::error::ExtractorError;
use crate::extractor::http_client::HttpClient;
use crate::extractor::models::*;
use crate::extractor::{LiveExtractor, Result};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PLATFORM_ID: &str = "bilibili";
const PLATFORM_NAME: &str = "哔哩哔哩";

const BASE_URL: &str = "https://live.bilibili.com";

const CATEGORIES_URL: &str = "https://api.live.bilibili.com/room/v1/Area/getList";
#[allow(dead_code)]
const CATEGORY_ROOMS_URL: &str = "https://api.live.bilibili.com/xlive/web-interface/v1/second/getList?platform=web&parent_area_id={parent_id}&area_id={area_id}&page={page}";
const RECOMMEND_ROOMS_URL: &str =
    "https://api.live.bilibili.com/xlive/web-interface/v1/webMain/getList";
const ROOM_INFO_URL: &str =
    "https://api.live.bilibili.com/xlive/web-room/v1/index/getInfoByRoom?room_id={room_id}";
const ROOM_INIT_URL: &str = "https://api.live.bilibili.com/room/v1/Room/room_init?id={room_id}";
const PLAY_INFO_URL: &str = "https://api.live.bilibili.com/xlive/web-room/v2/index/getRoomPlayInfo?room_id={room_id}&qn={qn}&platform=h5&protocol=0,1&format=0,1,2&codec=0,1&dolby=5&panorama=1";
const SUPER_CHAT_URL: &str =
    "https://api.live.bilibili.com/av/v1/SuperChat/getMessageList?room_id={room_id}";

const SEARCH_ROOMS_BASE: &str = "https://api.bilibili.com/x/web-interface/search/type";
const SEARCH_ANCHORS_BASE: &str = "https://api.bilibili.com/x/web-interface/search/type";

/// Fallback quality list when the API does not return `g_qn_desc`.
const QUALITY_MAP: &[(&str, &str)] = &[
    ("原画", "10000"),
    ("蓝光8M", "400"),
    ("蓝光4M", "250"),
    ("超清", "150"),
    ("高清", "80"),
    ("流畅", "50"),
];

// ---------------------------------------------------------------------------
// Extractor
// ---------------------------------------------------------------------------

/// Bilibili (哔哩哔哩) live streaming platform extractor.
pub struct BilibiliExtractor {
    http: HttpClient,
    room_url_re: Regex,
    access_id: Mutex<String>,
    buvid3: Mutex<String>,
    buvid4: Mutex<String>,
}

impl BilibiliExtractor {
    pub fn new() -> Self {
        Self {
            http: HttpClient::builder()
                .default_header("Referer", "https://live.bilibili.com/")
                .build()
                .expect("failed to build HTTP client"),
            room_url_re: Regex::new(r"(?:https?://)?(?:www\.)?(?:live\.)?bilibili\.com/(\d+)")
                .unwrap(),
            access_id: Mutex::new(String::new()),
            buvid3: Mutex::new(String::new()),
            buvid4: Mutex::new(String::new()),
        }
    }

    /// Fetch a URL and deserialize the response as JSON.
    async fn fetch_json(&self, url: &str) -> Result<JsonValue> {
        self.ensure_buvid().await;
        self.http.get_json(url).await
    }

    /// Check the `code` field of a Bilibili API response.
    fn check_response(json: &JsonValue) -> Result<()> {
        let code = json.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);
        if code != 0 {
            let msg = json
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(ExtractorError::Other(format!(
                "Bilibili API error {}: {}",
                code, msg
            )));
        }
        Ok(())
    }

    /// Fetch and cache the `access_id` (w_webid) from `https://live.bilibili.com/lol`.
    ///
    /// Equivalent to Dart's `getAccessId()`.
    async fn get_access_id(&self) -> Result<String> {
        {
            let cached = self.access_id.lock();
            if !cached.is_empty() {
                return Ok(cached.clone());
            }
        }

        let resp = self
            .http
            .get("https://live.bilibili.com/lol")
            .send()
            .await?
            .text()
            .await?;

        let re = Regex::new(r#""access_id":"(.*?)""#).unwrap();
        let access_id = re
            .captures(&resp)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().replace('\\', ""))
            .unwrap_or_default();

        let mut cached = self.access_id.lock();
        *cached = access_id.clone();
        Ok(access_id)
    }

    /// Fetch `buvid3` and `buvid4` browser fingerprint cookies from Bilibili.
    ///
    /// Equivalent to Dart's `getBuvid()`. These cookies are required for
    /// all API requests to avoid -352 risk control errors.
    ///
    /// If the SPI endpoint fails (e.g. -352), the error is logged but not
    /// propagated — the extractor can still work without buvid when valid
    /// user cookies (SESSDATA) are present.
    async fn ensure_buvid(&self) {
        {
            let b3 = self.buvid3.lock();
            if !b3.is_empty() {
                return;
            }
        }

        let result: Result<JsonValue> = self
            .http
            .get_json("https://api.bilibili.com/x/frontend/finger/spi")
            .await;

        let json = match result {
            Ok(j) => j,
            Err(e) => {
                warn!("Failed to fetch buvid from SPI: {}", e);
                // Mark as attempted so we don't retry every request.
                let mut b3 = self.buvid3.lock();
                if b3.is_empty() {
                    *b3 = "_failed".to_string();
                }
                return;
            }
        };

        let data = match json.get("data") {
            Some(d) => d,
            None => {
                warn!("Missing data in buvid SPI response");
                let mut b3 = self.buvid3.lock();
                if b3.is_empty() {
                    *b3 = "_failed".to_string();
                }
                return;
            }
        };

        let b3 = data
            .get("b_3")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let b4 = data
            .get("b_4")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if b3.is_empty() {
            warn!("SPI returned empty buvid3");
            let mut cached = self.buvid3.lock();
            if cached.is_empty() {
                *cached = "_failed".to_string();
            }
            return;
        }

        {
            let mut cached3 = self.buvid3.lock();
            *cached3 = b3.clone();
        }
        {
            let mut cached4 = self.buvid4.lock();
            *cached4 = b4.clone();
        }

        // Update HTTP client cookies to include buvid.
        let existing = self.http.cookies();
        if existing.is_empty() {
            self.http
                .set_cookies(&format!("buvid3={};buvid4={};", b3, b4));
        } else if !existing.contains("buvid3") {
            self.http
                .set_cookies(&format!("{};buvid3={};buvid4={}", existing, b3, b4));
        }
    }

    // ------------------------------------------------------------------
    // Room resolution
    // ------------------------------------------------------------------

    /// Resolve a (possibly short) room_id to the real room_id via `room_init`.
    async fn resolve_room_id(&self, room_id: &str) -> Result<u64> {
        let url = ROOM_INIT_URL.replace("{room_id}", room_id);
        let json = self.fetch_json(&url).await?;
        Self::check_response(&json)?;
        json.pointer("/data/room_id")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| ExtractorError::Other("failed to resolve room_id".into()))
    }

    // ------------------------------------------------------------------
    // WBI-signed request helper
    // ------------------------------------------------------------------

    /// Perform a WBI-signed GET request to Bilibili API.
    ///
    /// `base_url` is the endpoint without query parameters.
    /// `params` are the query parameters to sign (decoded values).
    async fn wbi_get(
        &self,
        base_url: &str,
        params: Vec<(String, String)>,
    ) -> Result<JsonValue> {
        // 把 params Vec 转为 HashMap
        let params_map: HashMap<String, String> = params.into_iter().collect();

        // 直接用参数 map 签名，不经过 URL encode/decode round-trip
        let signed_params =
            wbi::sign_params(&self.http, Some(&self.http.cookies()), params_map).await?;

        // Build final URL from base_url + signed params.
        let final_query = wbi::encode_query_string(&signed_params);
        let url = format!("{}?{}", base_url, final_query);
        self.fetch_json(&url).await
    }

    // ------------------------------------------------------------------
    // Room-item parsing helpers
    // ------------------------------------------------------------------

    /// Parse a room listing item from a Bilibili API response object.
    fn parse_room_item(item: &JsonValue) -> Option<LiveRoomItem> {
        let room_id = item
            .get("roomid")
            .or_else(|| item.get("room_id"))
            .and_then(|v| v.as_u64())
            .map(|v| v.to_string())?;
        let title = str_field(item, "title");
        let cover = str_field_nonempty(item, "cover")
            .or_else(|| str_field_nonempty(item, "user_cover"))
            .unwrap_or_default();
        let online = item.get("online").and_then(|v| v.as_u64()).unwrap_or(0);
        let user_name = str_field(item, "uname");
        let user_avatar = str_field(item, "user_cover");

        Some(LiveRoomItem {
            room_id: room_id.clone(),
            title,
            cover,
            online,
            user_name,
            user_avatar,
            url: format!("{}/{}", BASE_URL, room_id),
            platform: PLATFORM_ID.to_string(),
        })
    }

    /// Parse a list of room items from a JSON array.
    fn parse_room_list(items: &[JsonValue]) -> Vec<LiveRoomItem> {
        items.iter().filter_map(Self::parse_room_item).collect()
    }

    // ------------------------------------------------------------------
    // Danmaku info
    // ------------------------------------------------------------------

    /// Fetch danmu token and host list from the `getDanmuInfo` API.
    ///
    /// Uses WBI signing — the plain endpoint returns -352/-412 without it.
    async fn fetch_danmu_info(&self, room_id: u64) -> Result<BilibiliDanmakuData> {
        let base = "https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo";
        let params = vec![("id".to_string(), room_id.to_string())];
        let json = self.wbi_get(base, params).await?;
        Self::check_response(&json)?;

        let data = json
            .get("data")
            .ok_or_else(|| ExtractorError::Other("missing data in danmu info".into()))?;

        let token = data
            .get("token")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mut host_list = Vec::new();
        if let Some(hosts) = data.get("host_list").and_then(|v| v.as_array()) {
            for host in hosts {
                host_list.push(BilibiliDanmuHost {
                    host: str_field(host, "host"),
                    port: host.get("port").and_then(|v| v.as_u64()).unwrap_or(2243) as u16,
                    wss_port: host.get("wss_port").and_then(|v| v.as_u64()).unwrap_or(443) as u16,
                    ws_port: host.get("ws_port").and_then(|v| v.as_u64()).unwrap_or(2244) as u16,
                });
            }
        }

        Ok(BilibiliDanmakuData {
            room_id,
            token,
            host_list,
        })
    }

    // ------------------------------------------------------------------
    // Fallback qualities
    // ------------------------------------------------------------------

    /// Build quality list from the static [`QUALITY_MAP`].
    fn fallback_qualities() -> Vec<LivePlayQuality> {
        QUALITY_MAP
            .iter()
            .map(|(name, qn)| LivePlayQuality {
                quality: name.to_string(),
                data: qn.to_string(),
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Free helper functions
// ---------------------------------------------------------------------------

/// Extract a string field from a JSON value, returning an empty string on miss.
///
/// If `key` starts with `/` it is treated as a JSON Pointer path
/// (e.g. `/data/info/title`), otherwise it is looked up as a direct
/// object key.
fn str_field(v: &JsonValue, key: &str) -> String {
    let val = if key.starts_with('/') {
        v.pointer(key)
    } else {
        v.get(key)
    };
    val.and_then(|v| v.as_str()).unwrap_or("").to_string()
}

/// Like `str_field` but returns `None` when the string is empty.
fn str_field_nonempty(v: &JsonValue, key: &str) -> Option<String> {
    let s = if key.starts_with('/') {
        v.pointer(key)?.as_str()?
    } else {
        v.get(key)?.as_str()?
    };
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// Extract a JSON field as a String, handling both string and integer values.
fn json_str_or_int(v: &JsonValue, key: &str) -> String {
    v.get(key)
        .and_then(|val| val.as_str().map(String::from).or_else(|| val.as_u64().map(|n| n.to_string())))
        .unwrap_or_default()
}

/// Percent-encode a string for URL use.
#[allow(dead_code)]
fn url_encode(s: &str) -> String {
    let mut encoded = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                encoded.push(c);
            }
            _ => {
                let mut buf = [0u8; 4];
                let s = c.encode_utf8(&mut buf);
                for b in s.bytes() {
                    encoded.push_str(&format!("%{:02X}", b));
                }
            }
        }
    }
    encoded
}

// ---------------------------------------------------------------------------
// LiveExtractor implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl LiveExtractor for BilibiliExtractor {
    fn id(&self) -> &str {
        PLATFORM_ID
    }

    fn name(&self) -> &str {
        PLATFORM_NAME
    }

    fn set_cookies(&self, cookies: &str) {
        if cookies.is_empty() {
            return;
        }
        if cookies.contains("buvid3") {
            self.http.set_cookies(cookies);
        } else {
            let buvid3 = self.buvid3.lock().clone();
            let buvid4 = self.buvid4.lock().clone();
            if buvid3.is_empty() || buvid3 == "_failed" {
                // buvid 还没获取或获取失败，先直接设置
                self.http.set_cookies(cookies);
            } else {
                self.http
                    .set_cookies(&format!("{};buvid3={};buvid4={}", cookies, buvid3, buvid4));
            }
        }
    }

    fn supports_url(&self, url: &str) -> bool {
        self.room_url_re.is_match(url) || url.contains("bilibili.com")
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
        let url = format!("{}?need_entrance=1&parent_id=0", CATEGORIES_URL);
        let json = self.fetch_json(&url).await?;
        Self::check_response(&json)?;

        let data = json
            .get("data")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ExtractorError::Other("missing data in categories".into()))?;

        let mut categories = Vec::with_capacity(data.len());
        for area in data {
            let id = json_str_or_int(area, "id");
            let name = str_field(area, "name");

            let mut sub_categories = Vec::new();
            if let Some(list) = area.get("list").and_then(|v| v.as_array()) {
                for item in list {
                    let sub_id = json_str_or_int(item, "id");
                    let sub_name = str_field(item, "name");
                    let parent_id = {
                        let s = json_str_or_int(item, "parent_id");
                        if s.is_empty() { None } else { Some(s) }
                    };
                    let pic_raw = item
                        .get("pic")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    let pic = if pic_raw.is_empty() {
                        None
                    } else {
                        Some(format!("{pic_raw}@100w.png"))
                    };
                    sub_categories.push(LiveSubCategory {
                        id: sub_id,
                        name: sub_name,
                        parent_id,
                        pic,
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
        let params = vec![
            ("context".to_string(), String::new()),
            ("search_type".to_string(), "live".to_string()),
            ("cover_type".to_string(), "user_cover".to_string()),
            ("order".to_string(), String::new()),
            ("keyword".to_string(), keyword.to_string()),
            ("category_id".to_string(), String::new()),
            ("__refresh__".to_string(), String::new()),
            ("_extra".to_string(), String::new()),
            ("highlight".to_string(), "0".to_string()),
            ("single_column".to_string(), "0".to_string()),
            ("page".to_string(), page.to_string()),
        ];

        let json = self.wbi_get(SEARCH_ROOMS_BASE, params).await?;
        Self::check_response(&json)?;

        let data = json
            .get("data")
            .ok_or_else(|| ExtractorError::Other("missing data in search result".into()))?;

        let result = data
            .get("result")
            .and_then(|v| v.get("live_room"))
            .and_then(|v| v.as_array())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        let em_re = Regex::new(r"<.*?em.*?>").unwrap();
        let items: Vec<LiveRoomItem> = result
            .iter()
            .filter_map(|item| {
                let room_id = item.get("roomid").and_then(|v| v.as_u64())?.to_string();
                let title_raw = str_field(item, "title");
                let title = em_re.replace_all(&title_raw, "").to_string();
                let cover_raw = str_field(item, "cover");
                let cover = if cover_raw.starts_with("//") {
                    format!("https:{}", cover_raw)
                } else {
                    cover_raw
                };
                let online = item.get("online").and_then(|v| v.as_u64()).unwrap_or(0);
                let user_name = str_field(item, "uname");

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
            })
            .collect();

        let has_more = items.len() >= 40;
        Ok(LiveSearchRoomResult { has_more, items })
    }

    async fn search_anchors(&self, keyword: &str, page: u32) -> Result<LiveSearchAnchorResult> {
        let params = vec![
            ("context".to_string(), String::new()),
            ("search_type".to_string(), "live_user".to_string()),
            ("cover_type".to_string(), "user_cover".to_string()),
            ("order".to_string(), String::new()),
            ("keyword".to_string(), keyword.to_string()),
            ("category_id".to_string(), String::new()),
            ("__refresh__".to_string(), String::new()),
            ("_extra".to_string(), String::new()),
            ("highlight".to_string(), "0".to_string()),
            ("single_column".to_string(), "0".to_string()),
            ("page".to_string(), page.to_string()),
        ];

        let json = self.wbi_get(SEARCH_ANCHORS_BASE, params).await?;
        Self::check_response(&json)?;

        let data = json
            .get("data")
            .ok_or_else(|| ExtractorError::Other("missing data in search result".into()))?;


        let result = data
            .get("result")
            .and_then(|v| v.as_array())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        let em_re = Regex::new(r"<.*?em.*?>").unwrap();
        let items: Vec<LiveAnchorItem> = result
            .iter()
            .filter_map(|item| {
                let user_id = item
                    .get("uid")
                    .or_else(|| item.get("mid"))
                    .and_then(|v| v.as_u64())
                    .filter(|&v| v > 0)
                    .map(|v| v.to_string())?;
                let uname_raw = str_field(item, "uname");
                let user_name = em_re.replace_all(&uname_raw, "").to_string();
                let uface = str_field(item, "uface");
                let user_avatar = if uface.is_empty() {
                    String::new()
                } else {
                    let base = if uface.starts_with("//") {
                        format!("https:{}", uface)
                    } else {
                        uface
                    };
                    format!("{}@400w.jpg", base)
                };
                let room_id = item
                    .get("roomid")
                    .or_else(|| item.get("room_id"))
                    .and_then(|v| v.as_u64())
                    .filter(|&v| v > 0)
                    .map(|v| v.to_string());
                let is_live = item
                    .get("is_live")
                    .map(|v| {
                        v.as_bool().unwrap_or_else(|| v.as_i64().unwrap_or(0) != 0)
                    })
                    .unwrap_or(false);

                Some(LiveAnchorItem {
                    user_id,
                    user_name,
                    user_avatar,
                    room_id: room_id.clone(),
                    is_live,
                    platform: PLATFORM_ID.to_string(),
                    url: room_id
                        .as_ref()
                        .map(|id| format!("{}/{}", BASE_URL, id))
                        .unwrap_or_default(),
                })
            })
            .collect();

        let has_more = items.len() >= 40;
        Ok(LiveSearchAnchorResult { has_more, items })
    }

    async fn get_category_rooms(
        &self,
        category: &LiveSubCategory,
        page: u32,
    ) -> Result<LiveCategoryResult> {
        let base = "https://api.live.bilibili.com/xlive/web-interface/v1/second/getList";
        let w_webid = self.get_access_id().await.unwrap_or_default();
        let params = vec![
            ("platform".to_string(), "web".to_string()),
            (
                "parent_area_id".to_string(),
                category.parent_id.clone().unwrap_or_else(|| "0".to_string()),
            ),
            ("area_id".to_string(), category.id.clone()),
            ("sort_type".to_string(), String::new()),
            ("page".to_string(), page.to_string()),
            ("w_webid".to_string(), w_webid),
        ];

        let json = self.wbi_get(base, params).await?;
        Self::check_response(&json)?;

        let data = json
            .get("data")
            .ok_or_else(|| ExtractorError::Other("missing data".into()))?;

        let list = data
            .get("list")
            .and_then(|v| v.as_array())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let has_more = data
            .get("has_more")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            == 1;

        let items: Vec<LiveRoomItem> = list
            .iter()
            .filter_map(|item| {
                let room_id = item
                    .get("roomid")
                    .or_else(|| item.get("room_id"))
                    .and_then(|v| v.as_u64())
                    .map(|v| v.to_string())?;
                let title = str_field(item, "title");
                let cover_raw = str_field(item, "cover");
                let cover = if cover_raw.is_empty() {
                    String::new()
                } else {
                    format!("{}@400w.jpg", cover_raw)
                };
                let online = item.get("online").and_then(|v| v.as_u64()).unwrap_or(0);
                let user_name = str_field(item, "uname");

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
            })
            .collect();

        Ok(LiveCategoryResult { has_more, items })
    }

    async fn get_recommend_rooms(&self, page: u32) -> Result<LiveCategoryResult> {
        let params = vec![
            ("platform".to_string(), "web".to_string()),
            ("page".to_string(), page.to_string()),
        ];

        let json = self.wbi_get(RECOMMEND_ROOMS_URL, params).await?;
        Self::check_response(&json)?;

        let data = json
            .get("data")
            .ok_or_else(|| ExtractorError::Other("missing data".into()))?;

        let list = data
            .get("recommend_room_list")
            .and_then(|v| v.as_array())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let has_more = data
            .get("has_more")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let items = Self::parse_room_list(list);

        Ok(LiveCategoryResult { has_more, items })
    }

    // ------------------------------------------------------------------
    // Room detail & playback
    // ------------------------------------------------------------------

    async fn get_room_detail(&self, room_id: &str) -> Result<LiveRoomDetail> {
        // 1. Resolve short room ID to real room ID.
        let real_room_id = self.resolve_room_id(room_id).await?;

        // 2. Fetch room info via getInfoByRoom.
        let info_url = ROOM_INFO_URL.replace("{room_id}", &real_room_id.to_string());
        let info_json = self.fetch_json(&info_url).await?;
        Self::check_response(&info_json)?;

        let room_info = info_json
            .pointer("/data/room_info")
            .ok_or_else(|| ExtractorError::Other("missing room_info".into()))?;

        let anchor_info = info_json.pointer("/data/anchor_info");

        let title = str_field(room_info, "title");
        let cover = str_field(room_info, "cover");
        let online = room_info
            .get("online")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let live_status = room_info
            .get("live_status")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let status = live_status == 1;

        let user_name = anchor_info
            .map(|a| str_field(a, "/base_info/uname"))
            .unwrap_or_default();
        let user_avatar = anchor_info
            .map(|a| str_field(a, "/base_info/face"))
            .unwrap_or_default();

        // 3. Fetch danmaku info.
        let danmaku_data = match self.fetch_danmu_info(real_room_id).await {
            Ok(data) => Some(serde_json::to_value(&data).unwrap_or_default()),
            Err(e) => {
                warn!("Failed to fetch danmaku info: {}", e);
                None
            }
        };

        // 4. Store play data for later use by get_play_qualities / get_play_urls.
        let play_data = BilibiliPlayData {
            room_id: real_room_id,
        };
        let data = serde_json::to_value(&play_data).ok();

        Ok(LiveRoomDetail {
            room_id: real_room_id.to_string(),
            title,
            cover,
            online,
            status,
            url: format!("{}/{}", BASE_URL, real_room_id),
            user_name,
            user_avatar,
            platform: PLATFORM_ID.to_string(),
            data,
            danmaku_data,
        })
    }

    async fn get_play_qualities(&self, detail: &LiveRoomDetail) -> Result<Vec<LivePlayQuality>> {
        // Call getRoomPlayInfo with qn=0 to fetch the g_qn_desc list.
        let url = PLAY_INFO_URL
            .replace("{room_id}", &detail.room_id)
            .replace("{qn}", "0");

        match self.fetch_json(&url).await {
            Ok(json) => {
                if let Some(qn_desc) = json
                    .pointer("/data/playurl_info/playurl/g_qn_desc")
                    .and_then(|v| v.as_array())
                {
                    let qualities: Vec<LivePlayQuality> = qn_desc
                        .iter()
                        .filter_map(|item| {
                            let qn = item.get("qn")?.as_u64()?;
                            let desc = item
                                .get("desc")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown");
                            Some(LivePlayQuality {
                                quality: desc.to_string(),
                                data: qn.to_string(),
                            })
                        })
                        .collect();

                    if !qualities.is_empty() {
                        return Ok(qualities);
                    }
                }
                debug!("g_qn_desc missing or empty, falling back to static quality map");
                Ok(Self::fallback_qualities())
            }
            Err(e) => {
                warn!(
                    "Failed to fetch play info for qualities: {}, using fallback",
                    e
                );
                Ok(Self::fallback_qualities())
            }
        }
    }

    async fn get_play_urls(
        &self,
        detail: &LiveRoomDetail,
        quality: &LivePlayQuality,
    ) -> Result<LivePlayUrl> {
        let url = PLAY_INFO_URL
            .replace("{room_id}", &detail.room_id)
            .replace("{qn}", &quality.data);

        let json = self.fetch_json(&url).await?;
        Self::check_response(&json)?;

        // Navigate: data -> playurl_info -> playurl -> stream
        let playurl = json
            .pointer("/data/playurl_info/playurl")
            .ok_or_else(|| ExtractorError::Other("missing playurl".into()))?;

        let streams = playurl
            .get("stream")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ExtractorError::Other("missing stream array".into()))?;

        let mut urls: Vec<String> = Vec::new();
        let mut url_type = UrlType::Flv;

        for stream in streams {
            let formats = stream
                .get("format")
                .and_then(|v| v.as_array())
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            for fmt in formats {
                let format_name = str_field(fmt, "format_name");
                let codecs = fmt
                    .get("codec")
                    .and_then(|v| v.as_array())
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);

                for codec in codecs {
                    let base_url = str_field(codec, "base_url");
                    if base_url.is_empty() {
                        continue;
                    }

                    let url_infos = codec
                        .get("url_info")
                        .and_then(|v| v.as_array())
                        .map(|v| v.as_slice())
                        .unwrap_or(&[]);

                    for url_info in url_infos {
                        let host = str_field(url_info, "host");
                        let extra = str_field(url_info, "extra");
                        let full_url = format!("{}{}{}", host, base_url, extra);
                        urls.push(full_url);
                    }

                    if !urls.is_empty() {
                        // Determine URL type from format name.
                        url_type = match format_name.as_str() {
                            "fmp4" => UrlType::M3u8,
                            _ => UrlType::Flv,
                        };
                    }
                }

                // Prefer the first format that yields URLs (FLV > fMP4).
                if !urls.is_empty() {
                    let headers = Some(vec![
                        ("referer".to_string(), "https://live.bilibili.com".to_string()),
                        ("user-agent".to_string(), "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.188".to_string()),
                    ]);
                    return Ok(LivePlayUrl { urls, url_type, headers });
                }
            }
        }

        if urls.is_empty() {
            return Err(ExtractorError::NoStreamsFound);
        }

        let headers = Some(vec![
            ("referer".to_string(), "https://live.bilibili.com".to_string()),
            ("user-agent".to_string(), "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.188".to_string()),
        ]);
        Ok(LivePlayUrl { urls, url_type, headers })
    }

    async fn get_live_status(&self, room_id: &str) -> Result<bool> {
        let info_url = ROOM_INFO_URL.replace("{room_id}", room_id);
        let json = self.fetch_json(&info_url).await?;
        Self::check_response(&json)?;

        let live_status = json
            .pointer("/data/room_info/live_status")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        Ok(live_status == 1)
    }

    async fn get_super_chat_messages(&self, room_id: &str) -> Result<Vec<LiveSuperChatMessage>> {
        let url = SUPER_CHAT_URL.replace("{room_id}", room_id);
        let json = self.fetch_json(&url).await?;

        let messages = json
            .pointer("/data/list")
            .and_then(|v| v.as_array());

        let Some(messages) = messages else {
            return Ok(vec![]);
        };

        let result: Vec<LiveSuperChatMessage> = messages
            .iter()
            .filter_map(|msg| {
                let uname = str_field(msg, "/user_info/uname");
                let face = str_field(msg, "/user_info/face");
                let message = str_field(msg, "message");
                let price = msg.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0) as i32;
                let keep_time = msg.get("time").and_then(|v| v.as_i64()).unwrap_or(0);
                let start_ts = msg.get("start_time").and_then(|v| v.as_i64()).unwrap_or(0) * 1000;
                let end_ts = msg.get("end_time").and_then(|v| v.as_i64()).unwrap_or(0) * 1000;
                let background_color = str_field(msg, "background_color");
                let background_bottom_color = str_field(msg, "background_bottom_color");

                let end_time = if end_ts > 0 {
                    end_ts
                } else {
                    start_ts + keep_time * 1000
                };

                Some(LiveSuperChatMessage {
                    user_name: uname,
                    face,
                    message,
                    price,
                    start_time: start_ts,
                    end_time,
                    background_color,
                    background_bottom_color,
                })
            })
            .collect();

        Ok(result)
    }
}
