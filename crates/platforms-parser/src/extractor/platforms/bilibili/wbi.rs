//! WBI signature algorithm for Bilibili API requests.
//!
//! Certain Bilibili APIs (e.g. search) require a `w_rid` + `wts` signature
//! computed via the WBI scheme.  The signing key is derived from two image
//! filenames published by the `/x/web-interface/nav` endpoint.

use md5::Digest;

use crate::extractor::http_client::HttpClient;

/// Fixed reordering table used to derive the mixin key from
/// the concatenation of `img_key + sub_key`.
const MIXIN_KEY_ENC_TAB: [usize; 64] = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49, 33, 9, 42, 19, 29,
    28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25,
    54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
];

/// Characters to strip from parameter values before hashing.
const FILTER_CHARS: &[char] = &['!', '\'', '(', ')', '*'];

/// Fetch the current WBI img_key and sub_key from the Bilibili nav endpoint.
///
/// Returns `(img_key, sub_key)`.
///
/// If `cookies` is `Some`, the request will include the `Cookie` header
/// (needed for authenticated WBI key retrieval).
pub async fn get_wbi_keys(
    http: &HttpClient,
    cookies: Option<&str>,
) -> crate::extractor::Result<(String, String)> {
    let url = "https://api.bilibili.com/x/web-interface/nav";
    let mut req = http.get(url);
    if let Some(c) = cookies {
        if !c.is_empty() {
            req = req.header("Cookie", c);
        }
    }
    let json: serde_json::Value = req.send().await?.json().await?;

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

/// Sign a set of query parameters in-place, adding `wts` and `w_rid`.
///
/// `params` should contain the base query parameters **without** `wts` or
/// `w_rid`. After this call the vector will also contain those two keys.
pub fn encode_wbi(params: &mut Vec<(String, String)>, img_key: &str, sub_key: &str) {
    let mixin_key = get_mixin_key(img_key, sub_key);

    // Remove stale wts/w_rid if present.
    params.retain(|(k, _)| k != "wts" && k != "w_rid");

    let wts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    params.push(("wts".to_string(), wts.to_string()));
    params.sort_by(|a, b| a.0.cmp(&b.0));

    // Filter out forbidden characters from values.
    let filtered: Vec<(String, String)> = params
        .iter()
        .map(|(k, v)| {
            let fv: String = v.chars().filter(|c| !FILTER_CHARS.contains(c)).collect();
            (k.clone(), fv)
        })
        .collect();

    // URL-encode each key/value pair using Bilibili's custom encoding
    // (percent-encode non-unreserved chars, spaces as %20, NOT as +).
    let query = filtered
        .iter()
        .map(|(k, v)| format!("{}={}", wbi_url_encode(k), wbi_url_encode(v)))
        .collect::<Vec<_>>()
        .join("&");

    let to_hash = format!("{}{}", query, mixin_key);
    let digest = md5::Md5::digest(to_hash.as_bytes());
    let w_rid = hex::encode(digest);

    params.push(("w_rid".to_string(), w_rid));
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Extract the key (filename without extension) from a Bilibili wbi_img URL.
///
/// e.g. `https://i0.hdslb.com/bfs/wbi/7cd084941338484aae1ad9425b84077c.png`
/// → `7cd084941338484aae1ad9425b84077c`
fn extract_key_from_url(url: &str) -> Option<String> {
    let filename = url.rsplit('/').next()?;
    Some(filename.replace(".png", ""))
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

/// URL-encode a string using Bilibili's WBI encoding rules.
///
/// - Unreserved chars (`a-zA-Z0-9-_.~`) are kept as-is.
/// - Forbidden chars (`!'()*`) are stripped entirely.
/// - All other chars are percent-encoded (UTF-8 bytes, uppercase hex).
/// - Spaces become `%20` (NOT `+`).
fn wbi_url_encode(s: &str) -> String {
    let mut encoded = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                encoded.push(c);
            }
            '!' | '\'' | '(' | ')' | '*' => {
                // Strip these characters entirely.
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
