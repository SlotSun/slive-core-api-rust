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

/// Sign a URL's query parameters using the WBI algorithm.
///
/// This is a strict port of Dart's `BiliBiliSite.getWbiSign(String url)`:
///
/// 1. Parse query params from the URL.
/// 2. Add `wts` (current unix timestamp).
/// 3. Sort by key.
/// 4. Filter `!'()*` from values.
/// 5. Encode as query string using `Uri.encodeQueryComponent`.
/// 6. MD5 hash `query + mixinKey` → `w_rid`.
/// 7. Return signed params (including `wts` and `w_rid`).
pub async fn sign_url(
    http: &HttpClient,
    cookies: Option<&str>,
    url: &str,
) -> crate::extractor::Result<HashMap<String, String>> {
    let (img_key, sub_key) = get_wbi_keys(http, cookies).await?;
    let mixin_key = get_mixin_key(&img_key, &sub_key);

    let mut query_params = parse_query_params(url);

    // Add wts (current unix timestamp).
    let wts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
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
    Ok(query_params)
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
