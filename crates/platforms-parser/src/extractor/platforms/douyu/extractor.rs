//! Douyu (斗鱼) live streaming platform extractor.
//!
//! Fetches room details via the `betard` API, resolves play URLs through
//! `lapi/live/getH5Play` with an MD5 signing mechanism, and supports
//! category browsing and room/anchor search.

use async_trait::async_trait;
use md5::{Digest, Md5};
use regex::Regex;

use serde_json::Value as JsonValue;

use crate::extractor::error::ExtractorError;
use crate::extractor::http_client::HttpClient;
use crate::extractor::models::*;
use crate::extractor::platforms::douyu::models::*;
use crate::extractor::{LiveExtractor, Result};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PLATFORM_ID: &str = "douyu";
const PLATFORM_NAME: &str = "斗鱼";

const BASE_URL: &str = "https://www.douyu.com";

const CATEGORY_ROOMS_URL: &str =
    "https://www.douyu.com/gapi/rkc/directory/mixList/2_{category_id}/{page}";
const RECOMMEND_ROOMS_URL: &str = "https://www.douyu.com/japi/weblist/apinc/allpage/6/{page}";
const SEARCH_ROOMS_URL: &str =
    "https://www.douyu.com/japi/search/api/searchShow?kw={keyword}&page={page}&pageSize=20";
const SEARCH_ANCHORS_URL: &str = "https://www.douyu.com/japi/search/api/searchUser?kw={keyword}&page={page}&pageSize=20&filterType=1";
const ROOM_DETAIL_URL: &str = "https://www.douyu.com/betard/{room_id}";
const PLAY_URL_API: &str = "https://www.douyu.com/lapi/live/getH5PlayV1/{room_id}";
const ENCRYPTION_API: &str = "https://www.douyu.com/wgapi/livenc/liveweb/websec/getEncryption";

/// Fixed device ID for signing.
const DID: &str = "10000000000000000000000000001501";

// ---------------------------------------------------------------------------
// Extractor
// ---------------------------------------------------------------------------

/// Cached encryption keys from the Douyu API.
#[derive(Clone, Debug, Default)]
struct EncKeyCache {
    /// The `rand_str` field from the encryption API response.
    rand_str: String,
    /// Number of MD5 iterations.
    enc_time: i32,
    /// The `key` field.
    key: String,
    /// The `enc_data` field.
    enc_data: String,
    /// Whether this is a special signing mode (no salt).
    is_special: bool,
    /// Unix timestamp (seconds) when this cache expires.
    expire_at: i64,
}

/// Douyu (斗鱼) live streaming platform extractor.
pub struct DouyuExtractor {
    http: HttpClient,
    room_url_re: Regex,
    /// Cached encryption keys (protected by Mutex for async access).
    enc_key: parking_lot::Mutex<Option<EncKeyCache>>,
}

impl DouyuExtractor {
    pub fn new() -> Self {
        Self {
            http: HttpClient::builder()
                .build()
                .expect("failed to build HTTP client"),
            room_url_re: Regex::new(r"(?:https?://)?(?:www\.)?douyu\.com/(\d+)").unwrap(),
            enc_key: parking_lot::Mutex::new(None),
        }
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
    // Douyu signing (new encryption-based)
    // ------------------------------------------------------------------

    /// Fetch and cache encryption keys from the Douyu API.
    async fn update_enc_key(&self) -> Result<()> {
        // Check if cache is still valid.
        {
            let cache = self.enc_key.lock();
            if let Some(ref k) = *cache {
                if k.expire_at > chrono::Utc::now().timestamp() {
                    return Ok(());
                }
            }
        }

        let url = format!("{}?did={}", ENCRYPTION_API, DID);
        let resp: JsonValue = self.http.get_json(&url).await?;

        let data = resp
            .get("data")
            .ok_or_else(|| ExtractorError::Other("missing `data` in encryption response".into()))?;

        let rand_str = data
            .get("rand_str")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let enc_time = data.get("enc_time").and_then(|v| v.as_i64()).unwrap_or(1) as i32;
        let key = data
            .get("key")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let enc_data = data
            .get("enc_data")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let is_special = data.get("is_special").and_then(|v| v.as_i64()).unwrap_or(0) == 1;

        let cache = EncKeyCache {
            rand_str,
            enc_time,
            key,
            enc_data,
            is_special,
            expire_at: chrono::Utc::now().timestamp() + 86400,
        };

        let mut lock = self.enc_key.lock();
        *lock = Some(cache);
        Ok(())
    }

    /// Compute the Douyu signing parameters for the `getH5PlayV1` API.
    ///
    /// Returns the form-encoded POST body string.
    async fn compute_sign(&self, rid: &str, rate: i32, cdn: &str) -> Result<String> {
        self.update_enc_key().await?;

        let cache = self.enc_key.lock();
        let k = cache.as_ref().unwrap();

        let ts = chrono::Utc::now().timestamp();
        let salt = if k.is_special {
            String::new()
        } else {
            format!("{}{}", rid, ts)
        };

        // Compute secret: MD5 repeated enc_time times
        let mut secret = k.rand_str.clone();
        for _ in 0..k.enc_time {
            let input = format!("{}{}", secret, k.key);
            let mut hasher = Md5::new();
            hasher.update(input.as_bytes());
            secret = hex::encode(hasher.finalize());
        }

        // Compute auth: MD5(secret + key + salt)
        let auth_input = format!("{}{}{}", secret, k.key, salt);
        let mut hasher = Md5::new();
        hasher.update(auth_input.as_bytes());
        let auth = hex::encode(hasher.finalize());

        Ok(format!(
            "enc_data={}&tt={}&did={}&auth={}&cdn={}&rate={}&hevc=0&fa=0&ive=0&ver=Douyu_new&iar=0",
            k.enc_data, ts, DID, auth, cdn, rate
        ))
    }

    // ------------------------------------------------------------------
    // Room info helpers
    // ------------------------------------------------------------------

    /// Fetch room info from the `betard` API.
    async fn fetch_room_info(&self, room_id: &str) -> Result<JsonValue> {
        let url = ROOM_DETAIL_URL.replace("{room_id}", room_id);
        let json = self.fetch_json(&url).await?;
        Ok(json)
    }

    /// Extract room metadata from the betard API JSON response.
    fn extract_room_info(json: &JsonValue) -> DouyuRoomInfo {
        // The betard response has a nested `room` object.
        let room = json.get("room").unwrap_or(json);

        let rid = room
            .get("rid")
            .and_then(|v| v.as_i64())
            .or_else(|| json.get("rid").and_then(|v| v.as_i64()));

        let room_name = str_field(room, "roomName")
            .or_else(|| str_field(room, "room_name"))
            .unwrap_or_default();

        let nickname = str_field(room, "nickname")
            .or_else(|| str_field(json, "nickname"))
            .unwrap_or_default();

        let room_src = str_field(room, "roomSrc")
            .or_else(|| str_field(room, "room_src"))
            .unwrap_or_default();

        let avatar = str_field(room, "avatar")
            .or_else(|| str_field(room, "owner_avatar"))
            .unwrap_or_default();

        let online = room
            .get("online")
            .and_then(|v| v.as_u64())
            .or_else(|| room.get("ol").and_then(|v| v.as_u64()))
            .or_else(|| {
                // `hot` is in `room_biz_all.hot` as a string like "1361732".
                room.get("room_biz_all")
                    .and_then(|v| v.get("hot"))
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .or_else(|| {
                json.get("hot")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .unwrap_or(0);

        let is_live = room
            .get("isLive")
            .and_then(|v| v.as_i64())
            .or_else(|| room.get("room_status").and_then(|v| v.as_i64()))
            .or_else(|| room.get("show_status").and_then(|v| v.as_i64()))
            .unwrap_or(0) as i32;

        let cate_id = str_field(room, "cate_id")
            .or_else(|| {
                room.get("cateId")
                    .and_then(|v| v.as_i64())
                    .map(|v| v.to_string())
            })
            .unwrap_or_default();

        let cate_name = str_field(room, "cate_name")
            .or_else(|| str_field(room, "gameFullName"))
            .unwrap_or_default();

        let owner_uid = room
            .get("owner_uid")
            .and_then(|v| v.as_u64())
            .or_else(|| room.get("ownerUid").and_then(|v| v.as_u64()))
            .unwrap_or(0);

        DouyuRoomInfo {
            rid,
            room_name,
            nickname,
            room_src,
            avatar,
            online,
            is_live,
            cate_id,
            cate_name,
            owner_uid,
        }
    }

    /// Try to extract room info from the HTML page by parsing embedded JSON.
    // ------------------------------------------------------------------
    // Room item parsing helpers
    // ------------------------------------------------------------------

    /// Parse a room item from the category or recommend API response.
    fn parse_room_list_item(item: &JsonValue) -> Option<LiveRoomItem> {
        let room_id = item.get("rid")?.as_i64()?.to_string();

        let title = str_field(item, "roomName")
            .or_else(|| str_field(item, "room_name"))
            .or_else(|| str_field(item, "rn"))
            .unwrap_or_default();

        let cover = str_field(item, "roomSrc")
            .or_else(|| str_field(item, "room_src"))
            .or_else(|| str_field(item, "rs16"))
            .or_else(|| str_field(item, "verticalSrc"))
            .unwrap_or_default();

        let online = item
            .get("online")
            .and_then(|v| v.as_u64())
            .or_else(|| item.get("ol").and_then(|v| v.as_u64()))
            .unwrap_or(0);

        let user_name = str_field(item, "nickname")
            .or_else(|| str_field(item, "nick"))
            .or_else(|| str_field(item, "nn"))
            .unwrap_or_default();

        let user_avatar = str_field(item, "avatar").unwrap_or_default();

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

    /// Parse a search result room item from `searchShow` API.
    ///
    /// The `hot` field may be a formatted string like "1.2万" or a number.
    fn parse_search_room_item(item: &JsonValue) -> Option<LiveRoomItem> {
        let room_id = item.get("rid")?.as_i64()?.to_string();

        let title = str_field(item, "roomName")
            .or_else(|| str_field(item, "room_name"))
            .unwrap_or_default();

        let cover = str_field(item, "roomSrc")
            .or_else(|| str_field(item, "room_src"))
            .unwrap_or_default();

        // `hot` can be a number or a formatted string like "1.2万"
        let online = parse_hot_value(item.get("hot"));

        let user_name = str_field(item, "nickName")
            .or_else(|| str_field(item, "nickname"))
            .unwrap_or_default();

        let user_avatar = str_field(item, "avatar").unwrap_or_default();

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

    /// Fetch a single play URL for the given room, rate, and CDN.
    async fn get_play_url(&self, room_id: &str, rate: i32, cdn: &str) -> Result<String> {
        let sign_body = self.compute_sign(room_id, rate, cdn).await?;
        let url = PLAY_URL_API.replace("{room_id}", room_id);

        let resp: JsonValue = self
            .http
            .request(reqwest::Method::POST, &url)
            .header(
                "accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            )
            .header("accept-encoding", "gzip, deflate")
            .header("accept-language", "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3")
            .body(sign_body)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .send()
            .await?
            .json()
            .await?;

        let data = resp
            .get("data")
            .ok_or_else(|| ExtractorError::Other("missing `data` in play response".into()))?;

        let rtmp_url = data.get("rtmp_url").and_then(|v| v.as_str()).unwrap_or("");
        let rtmp_live = data.get("rtmp_live").and_then(|v| v.as_str()).unwrap_or("");

        if rtmp_url.is_empty() || rtmp_live.is_empty() {
            return Ok(String::new());
        }

        // Unescape HTML entities in rtmp_live.
        let rtmp_live = rtmp_live
            .replace("&amp;", "&")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&#39;", "'");

        Ok(format!("{}/{}", rtmp_url, rtmp_live))
    }
}

// ---------------------------------------------------------------------------
// Free helper functions
// ---------------------------------------------------------------------------

/// Extract a string field from a JSON value, returning `None` on miss.
fn str_field(v: &JsonValue, key: &str) -> Option<String> {
    v.get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// Generate a random 32-character hex string for Douyu's `dy_did` cookie.
fn generate_random_did() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    // Simple hex hash of timestamp + some randomness
    format!("{:032x}", now)
}

/// Parse Douyu's `hot` value which can be:
/// - A number (i64/u64)
/// - A formatted string like "1.2万" or "3.5亿"
/// Returns 0 if unparseable.
fn parse_hot_value(value: Option<&JsonValue>) -> u64 {
    let Some(v) = value else { return 0 };
    // Try as number first
    if let Some(n) = v.as_u64() {
        return n;
    }
    if let Some(n) = v.as_i64() {
        return n.max(0) as u64;
    }
    // Try as string with Chinese unit suffix
    if let Some(s) = v.as_str() {
        let s = s.trim();
        if s.is_empty() {
            return 0;
        }
        // Try pure number
        if let Ok(n) = s.parse::<u64>() {
            return n;
        }
        // Handle Chinese units: 万=10000, 亿=100000000
        let (num_part, multiplier) = if let Some(rest) = s.strip_suffix("亿") {
            (rest, 100_000_000u64)
        } else if let Some(rest) = s.strip_suffix("万") {
            (rest, 10_000u64)
        } else {
            (s, 1u64)
        };
        if let Ok(n) = num_part.parse::<f64>() {
            return (n * multiplier as f64) as u64;
        }
    }
    0
}

// ---------------------------------------------------------------------------
// LiveExtractor implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl LiveExtractor for DouyuExtractor {
    fn id(&self) -> &str {
        PLATFORM_ID
    }

    fn name(&self) -> &str {
        PLATFORM_NAME
    }

    fn supports_url(&self, url: &str) -> bool {
        self.room_url_re.is_match(url) || url.contains("douyu.com")
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
        // Use the mobile API which returns proper category hierarchy.
        let json = self.fetch_json("https://m.douyu.com/api/cate/list").await?;

        let data = json
            .get("data")
            .ok_or_else(|| ExtractorError::Other("missing `data` in cate response".into()))?;

        let cate1_list = data
            .get("cate1Info")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ExtractorError::Other("missing `cate1Info`".into()))?;

        let cate2_list = data
            .get("cate2Info")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut categories: Vec<LiveCategory> = Vec::with_capacity(cate1_list.len());

        for item in cate1_list {
            let cate1_id = item
                .get("cate1Id")
                .and_then(|v| v.as_i64())
                .map(|v| v.to_string())
                .unwrap_or_default();
            let cate1_name = str_field(item, "cate1Name").unwrap_or_default();

            let sub_categories: Vec<LiveSubCategory> = cate2_list
                .iter()
                .filter(|sub| {
                    sub.get("cate1Id")
                        .and_then(|v| v.as_i64())
                        .map(|v| v.to_string())
                        .as_deref()
                        == Some(&cate1_id)
                })
                .filter_map(|sub| {
                    let sub_id = sub
                        .get("cate2Id")
                        .and_then(|v| v.as_i64())
                        .map(|v| v.to_string())?;
                    let sub_name = str_field(sub, "cate2Name").unwrap_or_default();
                    if sub_name.is_empty() {
                        return None;
                    }
                    let icon = sub.get("icon").and_then(|v| v.as_str()).unwrap_or("");
                    let pic = if icon.is_empty() {
                        None
                    } else {
                        Some(icon.to_string())
                    };
                    Some(LiveSubCategory {
                        id: sub_id,
                        name: sub_name,
                        parent_id: Some(cate1_id.clone()),
                        pic,
                    })
                })
                .collect();

            categories.push(LiveCategory {
                id: cate1_id,
                name: cate1_name,
                sub_categories,
            });
        }

        categories.sort_by(|a, b| {
            a.id.parse::<i64>()
                .unwrap_or(0)
                .cmp(&b.id.parse::<i64>().unwrap_or(0))
        });

        Ok(categories)
    }

    async fn search_rooms(&self, keyword: &str, page: u32) -> Result<LiveSearchRoomResult> {
        let url = SEARCH_ROOMS_URL
            .replace("{keyword}", keyword)
            .replace("{page}", &page.to_string());

        // Douyu search requires specific headers and a cookie with dy_did.
        let did = generate_random_did();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::REFERER,
            "https://www.douyu.com/search/".parse().unwrap(),
        );
        headers.insert(
            reqwest::header::COOKIE,
            format!("dy_did={did};acf_did={did}").parse().unwrap(),
        );
        let text = self.http.get_text_with_headers(&url, &headers).await?;
        let json: JsonValue = serde_json::from_str(&text)?;

        // Check API error
        let error_code = json.get("error").and_then(|v| v.as_i64()).unwrap_or(-1);
        if error_code != 0 {
            let msg = json
                .get("msg")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(ExtractorError::Other(format!(
                "Douyu search API error: {msg}"
            )));
        }

        let data = json
            .get("data")
            .ok_or_else(|| ExtractorError::Other("missing `data` in search response".into()))?;

        let list = data
            .get("relateShow")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                ExtractorError::Other("missing `relateShow` in search response".into())
            })?;

        let mut items: Vec<LiveRoomItem> = Vec::with_capacity(list.len());
        for item in list {
            if let Some(room_item) = Self::parse_search_room_item(item) {
                items.push(room_item);
            }
        }

        let has_more = !list.is_empty();

        Ok(LiveSearchRoomResult { has_more, items })
    }

    async fn search_anchors(&self, keyword: &str, page: u32) -> Result<LiveSearchAnchorResult> {
        let url = SEARCH_ANCHORS_URL
            .replace("{keyword}", keyword)
            .replace("{page}", &page.to_string());

        // Douyu search requires specific headers and a cookie with dy_did.
        let did = generate_random_did();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::REFERER,
            "https://www.douyu.com/search/".parse().unwrap(),
        );
        headers.insert(
            reqwest::header::COOKIE,
            format!("dy_did={did};acf_did={did}").parse().unwrap(),
        );
        let text = self.http.get_text_with_headers(&url, &headers).await?;
        let json: JsonValue = serde_json::from_str(&text)?;

        let data = json
            .get("data")
            .ok_or_else(|| ExtractorError::Other("missing `data` in search response".into()))?;

        // The searchUser API returns "relateUser" with "anchorInfo" wrappers.
        let list = data
            .get("relateUser")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                ExtractorError::Other("missing `relateUser` in search response".into())
            })?;

        let mut items: Vec<LiveAnchorItem> = Vec::with_capacity(list.len());
        for entry in list {
            let info = match entry.get("anchorInfo") {
                Some(v) => v,
                None => continue,
            };
            let room_id = info
                .get("rid")
                .and_then(|v| v.as_i64())
                .map(|v| v.to_string());
            let user_name = str_field(info, "nickName")
                .or_else(|| str_field(info, "nickname"))
                .unwrap_or_default();
            let user_avatar = str_field(info, "avatar").unwrap_or_default();
            let is_live = info.get("isLive").and_then(|v| v.as_i64()).unwrap_or(0) != 0;

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

        let has_more = !items.is_empty();

        Ok(LiveSearchAnchorResult { has_more, items })
    }

    async fn get_category_rooms(
        &self,
        category: &LiveSubCategory,
        page: u32,
    ) -> Result<LiveCategoryResult> {
        let url = CATEGORY_ROOMS_URL
            .replace("{category_id}", &category.id)
            .replace("{page}", &page.to_string());

        let json = self.fetch_json(&url).await?;

        let data = json
            .get("data")
            .ok_or_else(|| ExtractorError::Other("missing `data` in response".into()))?;

        let list = data
            .get("rl")
            .and_then(|v| v.as_array())
            .ok_or_else(|| ExtractorError::Other("missing `rl` in response".into()))?;

        // Filter to live rooms only (type == 1), matching the Dart implementation.
        let items: Vec<LiveRoomItem> = list
            .iter()
            .filter(|item| item.get("type").and_then(|v| v.as_i64()).unwrap_or(0) == 1)
            .filter_map(Self::parse_room_list_item)
            .collect();

        let pgcnt = data.get("pgcnt").and_then(|v| v.as_u64()).unwrap_or(1);
        let has_more = (page as u64) < pgcnt;

        Ok(LiveCategoryResult { has_more, items })
    }

    async fn get_recommend_rooms(&self, page: u32) -> Result<LiveCategoryResult> {
        let url = RECOMMEND_ROOMS_URL.replace("{page}", &page.to_string());

        let json = self.fetch_json(&url).await?;

        let data = json
            .get("data")
            .ok_or_else(|| ExtractorError::Other("missing `data` in response".into()))?;

        let list = data
            .get("rl")
            .or_else(|| data.get("list"))
            .and_then(|v| v.as_array())
            .ok_or_else(|| ExtractorError::Other("missing room list in response".into()))?;

        // Filter to live rooms only (type == 1), matching the Dart implementation.
        let items: Vec<LiveRoomItem> = list
            .iter()
            .filter(|item| item.get("type").and_then(|v| v.as_i64()).unwrap_or(0) == 1)
            .filter_map(Self::parse_room_list_item)
            .collect();

        let pgcnt = data.get("pgcnt").and_then(|v| v.as_u64()).unwrap_or(1);
        let has_more = (page as u64) < pgcnt;

        Ok(LiveCategoryResult { has_more, items })
    }

    // ------------------------------------------------------------------
    // Room detail & playback
    // ------------------------------------------------------------------

    async fn get_room_detail(&self, room_id: &str) -> Result<LiveRoomDetail> {
        let json = self.fetch_room_info(room_id).await?;
        let room_info = Self::extract_room_info(&json);

        let rid_str = room_info
            .rid
            .map(|r| r.to_string())
            .unwrap_or_else(|| room_id.to_string());

        let status = room_info.is_live == 1;
        let title = if room_info.room_name.is_empty() {
            format!("斗鱼房间 {}", rid_str)
        } else {
            room_info.room_name
        };

        let url = Self::room_page_url(room_id);

        Ok(LiveRoomDetail {
            room_id: room_id.to_string(),
            title,
            cover: room_info.room_src,
            online: room_info.online,
            status,
            url,
            user_name: room_info.nickname,
            user_avatar: room_info.avatar,
            platform: PLATFORM_ID.to_string(),
            data: None, // Will be populated by get_play_qualities
            danmaku_data: None,
        })
    }

    async fn get_play_qualities(&self, detail: &LiveRoomDetail) -> Result<Vec<LivePlayQuality>> {
        let sign_body = self.compute_sign(&detail.room_id, 0, "").await?;
        let url = PLAY_URL_API.replace("{room_id}", &detail.room_id);

        let resp: JsonValue = self
            .http
            .request(reqwest::Method::POST, &url)
            .header(
                "accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            )
            .header("accept-encoding", "gzip, deflate")
            .header("accept-language", "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3")
            .body(sign_body)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .send()
            .await?
            .json()
            .await?;

        let data = resp
            .get("data")
            .ok_or_else(|| ExtractorError::Other("missing `data` in play response".into()))?;

        // Parse CDN list.
        let mut cdns: Vec<String> = Vec::new();
        if let Some(cdns_arr) = data.get("cdnsWithName").and_then(|v| v.as_array()) {
            for item in cdns_arr {
                if let Some(cdn) = item.get("cdn").and_then(|v| v.as_str()) {
                    cdns.push(cdn.to_string());
                }
            }
        }
        // Sort: scdn last.
        cdns.sort_by(|a, b| {
            let a_scdn = a.starts_with("scdn");
            let b_scdn = b.starts_with("scdn");
            a_scdn.cmp(&b_scdn)
        });

        // Parse multi-rate options.
        let mut qualities: Vec<LivePlayQuality> = Vec::new();
        if let Some(multirates) = data.get("multirates").and_then(|v| v.as_array()) {
            for mr in multirates {
                let rate = mr.get("rate").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
                let name = mr
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("原画")
                    .to_string();
                qualities.push(LivePlayQuality {
                    quality: name,
                    data: serde_json::to_string(&DouyuPlayData {
                        rate,
                        cdns: cdns.clone(),
                    })
                    .unwrap_or_default(),
                });
            }
        }

        if qualities.is_empty() {
            // Fallback: standard Douyu quality tiers.
            let fallback_rates: &[(&str, i32)] = &[
                ("原画", 0),
                ("蓝光4M", 400),
                ("蓝光2M", 250),
                ("超清", 150),
                ("高清", 80),
                ("流畅", 50),
            ];
            for (name, rate) in fallback_rates {
                qualities.push(LivePlayQuality {
                    quality: name.to_string(),
                    data: serde_json::to_string(&DouyuPlayData {
                        rate: *rate,
                        cdns: cdns.clone(),
                    })
                    .unwrap_or_default(),
                });
            }
        }

        Ok(qualities)
    }

    async fn get_play_urls(
        &self,
        detail: &LiveRoomDetail,
        quality: &LivePlayQuality,
    ) -> Result<LivePlayUrl> {
        let play_data: DouyuPlayData =
            serde_json::from_str(&quality.data).unwrap_or(DouyuPlayData {
                rate: 0,
                cdns: vec![],
            });

        let mut urls: Vec<String> = Vec::new();
        for cdn in &play_data.cdns {
            match self
                .get_play_url(&detail.room_id, play_data.rate, cdn)
                .await
            {
                Ok(url) if !url.is_empty() => urls.push(url),
                _ => {}
            }
        }

        if urls.is_empty() {
            return Err(ExtractorError::NoStreamsFound);
        }

        Ok(LivePlayUrl {
            urls,
            url_type: UrlType::Flv,
            headers: None,
        })
    }

    async fn get_live_status(&self, room_id: &str) -> Result<bool> {
        let json = self.fetch_room_info(room_id).await?;
        let room_info = Self::extract_room_info(&json);
        Ok(room_info.is_live == 1)
    }

    // ------------------------------------------------------------------
    // Super-chat (Douyu uses its own paid message system via danmaku)
    // ------------------------------------------------------------------

    async fn get_super_chat_messages(&self, _room_id: &str) -> Result<Vec<LiveSuperChatMessage>> {
        // Douyu super-chat messages (鱼丸, 佛跳墙) arrive through the danmaku
        // protocol, not through a separate REST API.
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_room_id_standard_url() {
        let extractor = DouyuExtractor::new();
        assert_eq!(
            extractor.extract_room_id("https://www.douyu.com/12345"),
            Some("12345".to_string())
        );
    }

    #[test]
    fn test_extract_room_id_no_www() {
        let extractor = DouyuExtractor::new();
        assert_eq!(
            extractor.extract_room_id("https://douyu.com/67890"),
            Some("67890".to_string())
        );
    }

    #[test]
    fn test_extract_room_id_no_protocol() {
        let extractor = DouyuExtractor::new();
        assert_eq!(
            extractor.extract_room_id("www.douyu.com/111"),
            Some("111".to_string())
        );
    }

    #[test]
    fn test_extract_room_id_invalid_url() {
        let extractor = DouyuExtractor::new();
        assert_eq!(extractor.extract_room_id("https://example.com/123"), None);
    }

    #[test]
    fn test_supports_url() {
        let extractor = DouyuExtractor::new();
        assert!(extractor.supports_url("https://www.douyu.com/12345"));
        assert!(extractor.supports_url("https://douyu.com/999"));
        assert!(!extractor.supports_url("https://www.huya.com/12345"));
    }

    #[test]
    fn test_str_field() {
        let json = serde_json::json!({"name": "test", "empty": ""});
        assert_eq!(str_field(&json, "name"), Some("test".to_string()));
        assert_eq!(str_field(&json, "empty"), None);
        assert_eq!(str_field(&json, "missing"), None);
    }
}
