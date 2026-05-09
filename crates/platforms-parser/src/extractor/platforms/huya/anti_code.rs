//! Huya stream anti-code construction.
//!
//! Ported from the Dart `HuyaSite.buildAntiCode` method.
//! The anti-code is a query-string that must be appended to the stream URL
//! so that Huya's CDN accepts the request.

use hex;
use md5::{Digest, Md5};
use url::form_urlencoded;

/// Rotate-left on the low 32 bits, preserving the high bits.
///
/// Equivalent to the Dart `rotl64` function.
fn rotl64(t: i64) -> i64 {
    let low = (t as u64) & 0xFFFF_FFFF;
    let rotated_low = ((low << 8) | (low >> 24)) & 0xFFFF_FFFF;
    let high = (t as u64) & !0xFFFF_FFFF;
    (high | rotated_low) as i64
}

/// Build the anti-code query string for a Huya stream.
///
/// # Arguments
/// * `stream`       – the `sStreamName` from `gameStreamInfoList`
/// * `presenter_uid` – the room owner's UID (`lChannelId`)
/// * `anti_code`    – the raw anti-code string from the stream info
///
/// # Returns
/// A query-string (without leading `?`) to append to the stream URL.
pub fn build_anti_code(stream: &str, presenter_uid: i64, anti_code: &str) -> String {
    // Parse the anti-code as query parameters
    let parsed: Vec<(String, String)> = form_urlencoded::parse(anti_code.as_bytes())
        .into_owned()
        .collect();

    let map: std::collections::HashMap<String, Vec<String>> =
        parsed
            .into_iter()
            .fold(std::collections::HashMap::new(), |mut acc, (k, v)| {
                acc.entry(k).or_default().push(v);
                acc
            });

    // If there's no `fm` parameter, return the original anti-code as-is
    let fm_values = match map.get("fm") {
        Some(v) if !v.is_empty() => v,
        _ => return anti_code.to_string(),
    };

    let ctype = map
        .get("ctype")
        .and_then(|v| v.first())
        .map(|s| s.as_str())
        .unwrap_or("huya_pc_exe");

    let platform_id: i64 = map
        .get("t")
        .and_then(|v| v.first())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let is_wap = platform_id == 103;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let seq_id = presenter_uid + now_ms;

    // MD5 hash: seqId|ctype|platformId
    let hash_input = format!("{}|{}|{}", seq_id, ctype, platform_id);
    let secret_hash = {
        let mut hasher = Md5::new();
        hasher.update(hash_input.as_bytes());
        hex::encode(hasher.finalize())
    };

    let converted_uid = rotl64(presenter_uid);
    let calc_uid = if is_wap { presenter_uid } else { converted_uid };

    // Decode the fm value (base64 -> utf8 -> take prefix before '_')
    let fm_encoded = fm_values.first().unwrap();
    let fm_decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, fm_encoded)
        .unwrap_or_default();
    let fm_str = String::from_utf8_lossy(&fm_decoded);
    let secret_prefix = fm_str.split('_').next().unwrap_or("");

    let ws_time = map
        .get("wsTime")
        .and_then(|v| v.first())
        .map(|s| s.as_str())
        .unwrap_or("0");

    // Build wsSecret = md5( prefix_calcUid_stream_secretHash_wsTime )
    let secret_str = format!(
        "{}_{}_{}_{}_{}",
        secret_prefix, calc_uid, stream, secret_hash, ws_time
    );
    let ws_secret = {
        let mut hasher = Md5::new();
        hasher.update(secret_str.as_bytes());
        hex::encode(hasher.finalize())
    };

    // Build the result query string
    let fs = map
        .get("fs")
        .and_then(|v| v.first())
        .map(|s| s.as_str())
        .unwrap_or("bgpd");

    let mut result = vec![
        format!("wsSecret={}", ws_secret),
        format!("wsTime={}", ws_time),
        format!("seqid={}", seq_id),
        format!("ctype={}", ctype),
        "ver=1".to_string(),
        format!("fs={}", fs),
        format!("fm={}", fm_encoded),
        format!("t={}", platform_id),
    ];

    if is_wap {
        let uuid = ((now_ms % 10_000_000_000) * 1000 % 0xFFFF_FFFF).to_string();
        result.push(format!("uid={}", presenter_uid));
        result.push(format!("uuid={}", uuid));
    } else {
        result.push(format!("u={}", converted_uid));
    }

    result.join("&")
}
