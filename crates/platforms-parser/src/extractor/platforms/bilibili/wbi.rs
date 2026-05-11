//! WBI signature algorithm for Bilibili API requests.
//!
//! Strict port of the Dart `BiliBiliSite.getWbiSign` implementation.

use std::collections::HashMap;

use md5::Digest;

use crate::extractor::http_client::HttpClient;

/// Fixed reordering table used to derive the mixin key from
/// the concatenation of `img_key + sub_key`.
const MIXIN_KEY_ENC_TAB: [usize; 64] = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49, 33, 9, 42, 19,
    29, 28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
    22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
];

/// Fetch the current WBI img_key and sub_key from the Bilibili nav endpoint.
///
/// Returns `(img_key, sub_key)`.
pub async fn get_wbi_keys(
    http: &HttpClient,
    _cookies: Option<&str>,
) -> crate::extractor::Result<(String, String)> {
    let url = "https://api.bilibili.com/x/web-interface/nav";
    let json: serde_json::Value = http.get_json(url).await?;

    let wbi_img = json
        .pointer("/data/wbi_img")
        .ok_or_else(|| crate::extractor::error::ExtractorError::Other("missing wbi_img".into()))?;

    let img_url = wbi_img
        .get("img_url")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let sub_url = wbi_img
        .get("sub_url")
        .and_then(|v| v.as_str())
        .unwrap_or_default();

    let img_key = extract_key_from_url(img_url)
        .ok_or_else(|| crate::extractor::error::ExtractorError::Other("bad img_url".into()))?;
    let sub_key = extract_key_from_url(sub_url)
        .ok_or_else(|| crate::extractor::error::ExtractorError::Other("bad sub_url".into()))?;

    Ok((img_key, sub_key))
}

/// Derive the 32-byte mixin key from `img_key + sub_key`.
fn get_mixin_key(img_key: &str, sub_key: &str) -> String {
    let raw = format!("{}{}", img_key, sub_key);
    let bytes = raw.as_bytes();
    let mut key = String::with_capacity(32);
    for i in 0..32 {
        let idx = MIXIN_KEY_ENC_TAB[i];
        if idx < bytes.len() {
            key.push(bytes[idx] as char);
        }
    }
    key
}

/// Parse query parameters from a URL string into a `HashMap`.
///
/// Equivalent to Dart's `Uri.parse(url).queryParameters`.
#[allow(dead_code)]
fn parse_query_params(url: &str) -> HashMap<String, String> {
    let query = match url.find('?') {
        Some(pos) => &url[pos + 1..],
        None => return HashMap::new(),
    };
    // Strip fragment if present.
    let query = match query.find('#') {
        Some(pos) => &query[..pos],
        None => query,
    };
    let mut params = HashMap::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let mut parts = pair.splitn(2, '=');
        let key = parts.next().unwrap_or("").to_string();
        let value = parts.next().unwrap_or("").to_string();
        // Decode percent-encoded values (simple decode: %XX → byte).
        let value = percent_decode(&value);
        params.insert(key, value);
    }
    params
}

/// Simple percent-decode: replace `%XX` with the corresponding byte.
#[allow(dead_code)]
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut result = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if i + 2 < bytes.len() && bytes[i] == b'%' {
            if let Ok(byte) = u8::from_str_radix(
                std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""),
                16,
            ) {
                result.push(byte);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(result).unwrap_or_default()
}

/// URL-encode a string using `Uri.encodeQueryComponent` semantics.
///
/// - Unreserved chars (`a-zA-Z0-9-_.~`) are kept as-is.
/// - All other chars are percent-encoded (UTF-8 bytes, uppercase hex).
/// - Spaces become `%20` (NOT `+`).
fn uri_encode_query_component(s: &str) -> String {
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

/// Core WBI signing logic (testable, no I/O).
///
/// Given `img_key`, `sub_key`, pre-parsed params, and a fixed `wts` timestamp,
/// returns `(signed_params, query_string_before_hash)`.
fn sign_url_core(
    img_key: &str,
    sub_key: &str,
    params: HashMap<String, String>,
    wts: u64,
) -> (HashMap<String, String>, String) {
    let mixin_key = get_mixin_key(img_key, sub_key);

    let mut query_params = params;
    query_params.insert("wts".to_string(), wts.to_string());

    // Sort by key and filter values.
    let mut sorted_keys: Vec<&String> = query_params.keys().collect();
    sorted_keys.sort();

    let mut filtered: HashMap<String, String> = HashMap::new();
    for key in &sorted_keys {
        let value = query_params.get(*key).unwrap();
        let filtered_value: String = value.chars().filter(|c| !"'()*".contains(*c)).collect();
        filtered.insert((*key).clone(), filtered_value);
    }

    // Build query string (sorted, encoded).
    let mut sorted_filtered_keys: Vec<&String> = filtered.keys().collect();
    sorted_filtered_keys.sort();

    let query = sorted_filtered_keys
        .iter()
        .map(|key| {
            format!(
                "{}={}",
                uri_encode_query_component(key),
                uri_encode_query_component(filtered.get(*key).unwrap())
            )
        })
        .collect::<Vec<_>>()
        .join("&");

    // Compute w_rid.
    let to_hash = format!("{}{}", query, mixin_key);
    let digest = md5::Md5::digest(to_hash.as_bytes());
    let w_rid = hex::encode(digest);

    query_params.insert("w_rid".to_string(), w_rid);
    (query_params, query)
}

/// Sign query parameters using the WBI algorithm.
///
/// This is a strict port of Dart's `BiliBiliSite.getWbiSign(String url)`.
///
/// `params` should be pre-parsed query parameters (decoded values).
pub async fn sign_params(
    http: &HttpClient,
    cookies: Option<&str>,
    params: HashMap<String, String>,
) -> crate::extractor::Result<HashMap<String, String>> {
    let (img_key, sub_key) = get_wbi_keys(http, cookies).await?;

    let wts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let (signed, _) = sign_url_core(&img_key, &sub_key, params, wts);
    Ok(signed)
}

/// Encode params into a query string using `Uri.encodeQueryComponent` semantics,
/// for use in the actual HTTP request.
///
/// This matches how Dart's `HttpClient` sends the signed params.
pub fn encode_query_string(params: &HashMap<String, String>) -> String {
    let mut keys: Vec<&String> = params.keys().collect();
    keys.sort();
    keys.iter()
        .map(|k| {
            format!(
                "{}={}",
                uri_encode_query_component(k),
                uri_encode_query_component(params.get(*k).unwrap())
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Extract the key (filename without extension) from a Bilibili wbi_img URL.
fn extract_key_from_url(url: &str) -> Option<String> {
    let filename = url.rsplit('/').next()?;
    Some(filename.replace(".png", ""))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试 mixin key 生成
    /// 用固定 img_key + sub_key 验证 get_mixin_key 输出
    #[test]
    fn test_get_mixin_key() {
        let img_key = "345279632a42463b8a52796ee9167406";
        let sub_key = "02c18a0496498403616154987635620e";
        let mixin = get_mixin_key(img_key, sub_key);
        println!("img_key:  {}", img_key);
        println!("sub_key:  {}", sub_key);
        println!("mixin_key: {}", mixin);
        assert_eq!(mixin.len(), 32, "mixin key should be 32 chars");
    }

    /// 测试 uri_encode_query_component 编码
    #[test]
    fn test_uri_encode() {
        // 纯 ASCII 无需编码
        assert_eq!(uri_encode_query_component("platform"), "platform");
        assert_eq!(uri_encode_query_component("web"), "web");
        assert_eq!(uri_encode_query_component("123"), "123");
        // 空字符串
        assert_eq!(uri_encode_query_component(""), "");
        // 特殊字符应被编码
        assert_eq!(uri_encode_query_component("a b"), "a%20b");
        assert_eq!(uri_encode_query_component("a=b"), "a%3Db");
        assert_eq!(uri_encode_query_component("a&b"), "a%26b");
        // unreserved chars 不编码
        assert_eq!(uri_encode_query_component("-_.~"), "-_.~");
    }

    /// 测试 percent_decode
    #[test]
    fn test_percent_decode() {
        assert_eq!(percent_decode("hello"), "hello");
        assert_eq!(percent_decode("a%20b"), "a b");
        assert_eq!(percent_decode("a%3Db"), "a=b");
        assert_eq!(percent_decode(""), "");
    }

    /// 测试 parse_query_params
    #[test]
    fn test_parse_query_params() {
        let url = "https://example.com/api?a=1&b=2&c=hello";
        let params = parse_query_params(url);
        assert_eq!(params.get("a").unwrap(), "1");
        assert_eq!(params.get("b").unwrap(), "2");
        assert_eq!(params.get("c").unwrap(), "hello");
    }

    /// 测试 parse_query_params 处理空值
    #[test]
    fn test_parse_query_params_empty_value() {
        let url = "https://example.com/api?sort_type=&page=1";
        let params = parse_query_params(url);
        assert_eq!(params.get("sort_type").unwrap(), "");
        assert_eq!(params.get("page").unwrap(), "1");
    }

    /// 测试 parse_query_params 处理 percent-encoded 值
    #[test]
    fn test_parse_query_params_encoded() {
        let url = "https://example.com/api?keyword=hello%20world";
        let params = parse_query_params(url);
        assert_eq!(params.get("keyword").unwrap(), "hello world");
    }

    /// 核心签名测试 - 用固定参数验证
    ///
    /// 用这个测试和 Dart 输出对比:
    /// ```dart
    /// var imgKey = "345279632a42463b8a52796ee9167406";
    /// var subKey = "02c18a0496498403616154987635620e";
    /// var mixinKey = getMixinKey(imgKey + subKey);
    /// var url = "https://api.live.bilibili.com/xlive/web-interface/v1/second/getList?platform=web&parent_area_id=2&area_id=86&sort_type=&page=1&w_webid=test_token";
    /// var queryParams = Map<String, String>.from(Uri.parse(url).queryParameters);
    /// queryParams["wts"] = "1700000000";
    /// // ... 按 Dart 逻辑处理 ...
    /// ```
    #[test]
    fn test_sign_url_core() {
        let img_key = "345279632a42463b8a52796ee9167406";
        let sub_key = "02c18a0496498403616154987635620e";
        let wts: u64 = 1700000000;

        let mut params = HashMap::new();
        params.insert("platform".to_string(), "web".to_string());
        params.insert("parent_area_id".to_string(), "2".to_string());
        params.insert("area_id".to_string(), "86".to_string());
        params.insert("sort_type".to_string(), String::new());
        params.insert("page".to_string(), "1".to_string());
        params.insert("w_webid".to_string(), "test_token".to_string());

        let (params, query) = sign_url_core(img_key, sub_key, params, wts);

        println!("=== WBI 签名测试 ===");
        println!("mixin_key: {}", get_mixin_key(img_key, sub_key));
        println!("query (用于 MD5): {}", query);
        println!("w_rid: {}", params.get("w_rid").unwrap());
        println!("wts: {}", params.get("wts").unwrap());
        println!();
        println!("完整参数 (sorted):");
        let mut keys: Vec<&String> = params.keys().collect();
        keys.sort();
        for k in keys {
            println!("  {} = {}", k, params.get(k).unwrap());
        }

        // 验证 w_rid 存在且是 32 位 hex
        let w_rid = params.get("w_rid").unwrap();
        assert_eq!(w_rid.len(), 32, "w_rid should be 32 hex chars");
        assert!(w_rid.chars().all(|c| c.is_ascii_hexdigit()), "w_rid should be hex");

        // 验证 wts 存在
        assert_eq!(params.get("wts").unwrap(), "1700000000");

        // 验证其他参数保留
        assert_eq!(params.get("platform").unwrap(), "web");
        assert_eq!(params.get("parent_area_id").unwrap(), "2");
        assert_eq!(params.get("area_id").unwrap(), "86");
        assert_eq!(params.get("sort_type").unwrap(), "");
        assert_eq!(params.get("page").unwrap(), "1");
        assert_eq!(params.get("w_webid").unwrap(), "test_token");
    }

    /// 测试 encode_query_string 输出是否和 Dio 的 URL 编码一致
    ///
    /// Dio 会把 queryParameters 编码成 URL query string。
    /// 这里模拟 Rust 的 encode_query_string 输出，和 Dart 的 Dio 输出对比。
    #[test]
    fn test_encode_query_string_matches_dio() {
        let mut params = HashMap::new();
        params.insert("area_id".to_string(), "86".to_string());
        params.insert("page".to_string(), "1".to_string());
        params.insert("parent_area_id".to_string(), "2".to_string());
        params.insert("platform".to_string(), "web".to_string());
        params.insert("sort_type".to_string(), String::new());
        params.insert("w_rid".to_string(), "b452949c53cd957498cd3a2903217d54".to_string());
        params.insert("w_webid".to_string(), "test_token".to_string());
        params.insert("wts".to_string(), "1700000000".to_string());

        let qs = encode_query_string(&params);
        println!("Rust encode_query_string: {}", qs);
        println!();
        println!("Dart Dio 会生成:");
        println!("  area_id=86&page=1&parent_area_id=2&platform=web&sort_type=&w_rid=b452949c53cd957498cd3a2903217d54&w_webid=test_token&wts=1700000000");

        // 验证参数顺序是 sorted
        assert!(qs.starts_with("area_id=86&"));
        assert!(qs.contains("w_rid=b452949c53cd957498cd3a2903217d54"));
        assert!(qs.ends_with("&wts=1700000000"));
    }

    /// 测试 extract_key_from_url
    #[test]
    fn test_extract_key_from_url() {
        assert_eq!(
            extract_key_from_url("https://i0.hdslb.com/bfs/live/abc123.png"),
            Some("abc123".to_string())
        );
        assert_eq!(
            extract_key_from_url("https://i0.hdslb.com/bfs/live/xyz456.png"),
            Some("xyz456".to_string())
        );
        assert_eq!(extract_key_from_url(""), Some("".to_string()));
    }

    /// 测试 encode_query_string 输出格式
    #[test]
    fn test_encode_query_string() {
        let mut params = HashMap::new();
        params.insert("b".to_string(), "2".to_string());
        params.insert("a".to_string(), "1".to_string());
        let qs = encode_query_string(&params);
        assert_eq!(qs, "a=1&b=2", "should be sorted by key");
    }
}
