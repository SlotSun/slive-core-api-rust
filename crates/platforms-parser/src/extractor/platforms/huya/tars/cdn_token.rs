//! TARS request/response types for the Huya CDN token API (`getCdnTokenInfoEx`).
//!
//! Ported from the Dart `HuyaUserId`, `GetCdnTokenExReq`, `GetCdnTokenExResp`
//! and updated to match the working `rust-srec` implementation.

use bytes::Bytes;
use tars_codec::TarsError;
use tars_codec::de::TarsDeserializer;
use tars_codec::ser::TarsSerializer;
use tars_codec::types::{TarsMessage, TarsRequestHeader, TarsValue, next_request_id};

// ---------------------------------------------------------------------------
// HuyaUserId
// ---------------------------------------------------------------------------

/// TARS struct `HuyaUserId` – used as the `tId` field in CDN token requests.
#[derive(Debug, Clone, Default)]
pub struct HuyaUserId {
    pub l_uid: i64,
    pub s_guid: String,
    pub s_token: String,
    pub s_huya_ua: String,
    pub s_cookie: String,
    pub i_token_type: i32,
    pub s_device_info: String,
    pub s_qimei: String,
}

impl HuyaUserId {
    pub fn encode(&self) -> Result<Bytes, TarsError> {
        let mut s = TarsSerializer::new();
        s.write_i64(0, self.l_uid)?;
        s.write_string(1, &self.s_guid)?;
        s.write_string(2, &self.s_token)?;
        s.write_string(3, &self.s_huya_ua)?;
        s.write_string(4, &self.s_cookie)?;
        s.write_i32(5, self.i_token_type)?;
        s.write_string(6, &self.s_device_info)?;
        s.write_string(7, &self.s_qimei)?;
        Ok(s.into_bytes())
    }
}

// ---------------------------------------------------------------------------
// GetCdnTokenExReq
// ---------------------------------------------------------------------------

/// TARS struct `GetCdnTokenExReq` – request to `getCdnTokenInfoEx`.
#[derive(Debug)]
pub struct GetCdnTokenExReq {
    pub s_flv_url: String,
    pub s_stream_name: String,
    pub i_loop_time: i32,
    pub t_id: HuyaUserId,
    pub i_app_id: i32,
}

impl GetCdnTokenExReq {
    pub fn encode(&self) -> Result<Bytes, TarsError> {
        let mut s = TarsSerializer::new();
        s.write_string(0, &self.s_flv_url)?;
        s.write_string(1, &self.s_stream_name)?;
        s.write_i32(2, self.i_loop_time)?;
        let tid_bytes = self.t_id.encode()?;
        s.write_simple_list(3, &tid_bytes)?;
        s.write_i32(4, self.i_app_id)?;
        Ok(s.into_bytes())
    }
}

// ---------------------------------------------------------------------------
// GetCdnTokenExResp
// ---------------------------------------------------------------------------

/// TARS struct `GetCdnTokenExResp` – response from `getCdnTokenInfoEx`.
#[derive(Debug, Default)]
pub struct GetCdnTokenExResp {
    pub s_flv_token: String,
    pub i_expire_time: i32,
}

impl GetCdnTokenExResp {
    pub fn decode(raw: &[u8]) -> Result<Self, TarsError> {
        let mut de = TarsDeserializer::new(Bytes::copy_from_slice(raw));
        let mut resp = Self::default();

        while !de.is_empty() {
            let (tag, value) = de.read_value()?;
            match tag {
                0 => resp.s_flv_token = value.try_into_string().unwrap_or_default(),
                1 => resp.i_expire_time = value.try_into_i32().unwrap_or(0),
                _ => {}
            }
        }

        Ok(resp)
    }
}

// ---------------------------------------------------------------------------
// TUP HTTP helper
// ---------------------------------------------------------------------------

/// User-Agent used in Huya WUP API requests.
pub const HYSDK_UA: &str =
    "HYSDK(Windows,30000002)_APP(pc_exe&7090002&official)_SDK(trans&2.35.0.5996)";

/// Build a TUP-encoded `TarsMessage` for calling `getCdnTokenInfoEx`.
///
/// Uses `tars_codec::encode_tars_value_wrapped` to properly serialize the
/// request struct with StructBegin/StructEnd markers, matching the protocol
/// expected by Huya's WUP server.
fn build_cdn_token_tup_message(stream_name: &str) -> Result<TarsMessage, TarsError> {
    // Build HuyaUserId as TarsValue::Struct
    let mut tid_map = rustc_hash::FxHashMap::default();
    tid_map.insert(0u8, TarsValue::Long(0)); // lUid
    tid_map.insert(1, TarsValue::String(String::new())); // sGuid
    tid_map.insert(2, TarsValue::String(String::new())); // sToken
    tid_map.insert(3, TarsValue::String("pc_exe&7090002&official".to_string())); // sHuYaUA
    tid_map.insert(4, TarsValue::String(String::new())); // sCookie
    tid_map.insert(5, TarsValue::Int(0)); // iTokenType
    tid_map.insert(6, TarsValue::String(String::new())); // sDeviceInfo
    tid_map.insert(7, TarsValue::String(String::new())); // sQIMEI

    // Build GetCdnTokenExReq as TarsValue::Struct
    let mut req_map = rustc_hash::FxHashMap::default();
    req_map.insert(0u8, TarsValue::String(String::new())); // sFlvUrl
    req_map.insert(1, TarsValue::String(stream_name.to_string())); // sStreamName
    req_map.insert(2, TarsValue::Int(0)); // iLoopTime
    req_map.insert(3, TarsValue::Struct(tid_map)); // tId
    req_map.insert(4, TarsValue::Int(66)); // iAppId

    let req_value = TarsValue::Struct(req_map);

    // Serialize with StructBegin/StructEnd markers
    let req_bytes = tars_codec::encode_tars_value_wrapped(&req_value)?;

    // Build body map: "tReq" -> serialized request bytes
    let mut body = rustc_hash::FxHashMap::default();
    body.insert("tReq".to_string(), req_bytes.to_vec().into());

    Ok(TarsMessage {
        header: TarsRequestHeader {
            version: 3,
            packet_type: 0,
            message_type: 0,
            request_id: next_request_id(),
            servant_name: "liveui".to_string(),
            func_name: "getCdnTokenInfoEx".to_string(),
            timeout: 10000,
            context: Default::default(),
            status: Default::default(),
        },
        body,
    })
}

/// Call the Huya CDN token API over HTTPS (TUP protocol) and return the FLV token.
///
/// This is the Rust equivalent of the Dart `HuyaSite.getCndTokenInfoEx`.
///
/// Key differences from the broken version:
/// - Uses `https://wup.huya.com` (not `http://`)
/// - Uses `"tReq"` as the body key (not the function name)
/// - Uses `"tRsp"` to read the response (not the function name)
/// - Uses `encode_tars_value_wrapped` for proper struct serialization
pub async fn get_cdn_token_info_ex(
    client: &reqwest::Client,
    stream_name: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let message = build_cdn_token_tup_message(stream_name)?;

    let encoded = tars_codec::encode_request(&message)?;

    let resp = client
        .post("https://wup.huya.com")
        .header("Content-Type", "application/x-tars")
        .header("Origin", "https://www.huya.com")
        .header("Referer", "https://www.huya.com")
        .header("User-Agent", HYSDK_UA)
        .body(encoded.to_vec())
        .send()
        .await?;

    let resp_bytes = resp.bytes().await?;
    let resp_message = tars_codec::decode_response_from_bytes(resp_bytes)?;

    // Extract the response body — key is "tRsp", not the function name
    let resp_body = resp_message
        .body
        .get("tRsp")
        .ok_or("missing tRsp in response body")?;

    // Decode the TARS value from the response body bytes
    let tars_value = tars_codec::decode_tars_value(Bytes::from(resp_body.clone()))?;

    // Extract flv_token (tag 0)
    if let TarsValue::Struct(map) = tars_value {
        let flv_token = map
            .get(&0)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Ok(flv_token)
    } else {
        Err("unexpected response format".into())
    }
}
