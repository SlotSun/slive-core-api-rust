//! TARS message types for Huya danmaku WebSocket protocol.
//!
//! Ported from the Dart `HYPushMessage`, `HYMessage`, `HYSender`, `HYBulletFormat`.

use bytes::Bytes;
use tars_codec::TarsError;
use tars_codec::de::TarsDeserializer;
use tars_codec::ser::TarsSerializer;
use tars_codec::types::TarsValue;

// ---------------------------------------------------------------------------
// Join room encoding
// ---------------------------------------------------------------------------

/// Encode the TARS binary payload sent right after WebSocket connection to join a room.
///
/// The Dart code builds this in `HuyaDanmaku.getJoinData`:
///
/// ```ignore
/// inner: ayyuid(0), true(1), ""(2), ""(3), tid(4), sid(5), 0(6), 0(7)
/// outer: 1(0), inner_bytes(1)
/// ```
pub fn encode_join_room(ayyuid: i64, tid: i64, sid: i64) -> Result<Vec<u8>, TarsError> {
    // Inner struct
    let mut inner = TarsSerializer::new();
    inner.write_i64(0, ayyuid)?;
    inner.write_bool(1, true)?;
    inner.write_string(2, "")?;
    inner.write_string(3, "")?;
    inner.write_i64(4, tid)?;
    inner.write_i64(5, sid)?;
    inner.write_i32(6, 0)?;
    inner.write_i32(7, 0)?;
    let inner_bytes = inner.into_bytes();

    // Outer wrapper
    let mut outer = TarsSerializer::new();
    outer.write_i32(0, 1)?;
    outer.write_simple_list(1, &inner_bytes)?;

    Ok(outer.into_bytes().to_vec())
}

/// Pre-encoded heartbeat payload (`base64("ABQdAAwsNgBM")`).
pub const HEARTBEAT_DATA: &[u8] = &[0x00, 0x14, 0x1c, 0x00, 0x30, 0x36, 0x00, 0x4c];

// ---------------------------------------------------------------------------
// Outer message envelope
// ---------------------------------------------------------------------------

/// The outermost wrapper around every message from the Huya WebSocket.
///
/// When `msg_type == 7`, `data` is a nested TARS payload that should be decoded
/// as a `HYPushMessage`.
#[derive(Debug)]
pub struct HuyaWsMessage {
    pub msg_type: i32,
    pub data: Bytes,
}

impl HuyaWsMessage {
    /// Decode from raw WebSocket binary frame.
    pub fn decode(raw: &[u8]) -> Result<Self, TarsError> {
        let mut de = TarsDeserializer::new(Bytes::copy_from_slice(raw));
        let mut msg_type = 0i32;
        let mut data = Bytes::new();

        while !de.is_empty() {
            let (tag, value) = de.read_value()?;
            match tag {
                0 => msg_type = value.try_into_i32().unwrap_or(0),
                1 => data = value.try_into_simple_list().unwrap_or_default(),
                _ => {}
            }
        }

        Ok(Self { msg_type, data })
    }
}

// ---------------------------------------------------------------------------
// HYPushMessage (uri-based dispatch)
// ---------------------------------------------------------------------------

/// A push message envelope. The `uri` determines the payload kind.
///
/// - `uri == 1400` → chat message (decode `msg` as `HYMessage`)
/// - `uri == 8006` → online count (first i32 in `msg`)
#[derive(Debug)]
pub struct HYPushMessage {
    pub push_type: i32,
    pub uri: i32,
    pub msg: Bytes,
    pub protocol_type: i32,
}

impl HYPushMessage {
    pub fn decode(raw: &[u8]) -> Result<Self, TarsError> {
        let mut de = TarsDeserializer::new(Bytes::copy_from_slice(raw));
        let mut push_type = 0i32;
        let mut uri = 0i32;
        let mut msg = Bytes::new();
        let mut protocol_type = 0i32;

        while !de.is_empty() {
            let (tag, value) = de.read_value()?;
            match tag {
                0 => push_type = value.try_into_i32().unwrap_or(0),
                1 => uri = value.try_into_i32().unwrap_or(0),
                2 => msg = value.try_into_simple_list().unwrap_or_default(),
                3 => protocol_type = value.try_into_i32().unwrap_or(0),
                _ => {}
            }
        }

        Ok(Self {
            push_type,
            uri,
            msg,
            protocol_type,
        })
    }
}

// ---------------------------------------------------------------------------
// HYMessage (chat message, uri=1400)
// ---------------------------------------------------------------------------

/// Decoded chat message from the danmaku stream.
#[derive(Debug, Clone)]
pub struct HYMessage {
    pub sender: HYSender,
    pub content: String,
    pub font_color: i64,
}

/// Sender information attached to a chat message.
#[derive(Debug, Clone)]
pub struct HYSender {
    pub uid: i64,
    pub nick_name: String,
}

/// Bullet-format metadata (font colour, speed, etc.).
#[derive(Debug, Clone)]
pub struct HYBulletFormat {
    pub font_color: i64,
}

impl HYMessage {
    pub fn decode(raw: &[u8]) -> Result<Self, TarsError> {
        let mut de = TarsDeserializer::new(Bytes::copy_from_slice(raw));
        let mut sender = HYSender {
            uid: 0,
            nick_name: String::new(),
        };
        let mut content = String::new();
        let mut font_color = 0i64;

        while !de.is_empty() {
            let (tag, value) = de.read_value()?;
            match tag {
                0 => {
                    // sender is a nested struct — TARS may return it as a
                    // Struct (direct) or SimpleList (encoded bytes).
                    sender = decode_sender(value).unwrap_or(sender);
                }
                3 => content = value.try_into_string().unwrap_or_default(),
                6 => {
                    font_color = decode_bullet_format(value);
                }
                _ => {}
            }
        }

        Ok(Self {
            sender,
            content,
            font_color,
        })
    }
}

/// Decode sender info from a TarsValue that may be a Struct or SimpleList.
fn decode_sender(value: TarsValue) -> Result<HYSender, TarsError> {
    match value {
        TarsValue::Struct(map) => {
            // uid can be i32 (Int) or i64 (Long)
            let uid = tars_as_i64(map.get(&0));
            let nick_name = map
                .get(&2)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            Ok(HYSender { uid, nick_name })
        }
        TarsValue::SimpleList(bytes) => HYSender::decode(&bytes),
        _ => Err(TarsError::Unknown),
    }
}

/// Decode bullet format from a TarsValue that may be a Struct or SimpleList.
fn decode_bullet_format(value: TarsValue) -> i64 {
    match value {
        TarsValue::Struct(map) => tars_as_i64(map.get(&0)),
        TarsValue::SimpleList(bytes) => decode_bullet_font_color(&bytes),
        _ => 0,
    }
}

/// Extract an i64 from a TarsValue that may be Byte, Short, Int, or Long.
fn tars_as_i64(v: Option<&TarsValue>) -> i64 {
    match v {
        Some(TarsValue::Long(l)) => *l,
        Some(TarsValue::Int(i)) => *i as i64,
        Some(TarsValue::Short(s)) => *s as i64,
        Some(TarsValue::Byte(b)) => *b as i64,
        _ => 0,
    }
}

impl HYSender {
    fn decode(raw: &[u8]) -> Result<Self, TarsError> {
        let mut de = TarsDeserializer::new(Bytes::copy_from_slice(raw));
        let mut uid = 0i64;
        let mut nick_name = String::new();

        while !de.is_empty() {
            let (tag, value) = de.read_value()?;
            match tag {
                0 => uid = value.try_into_i64().unwrap_or(0),
                2 => nick_name = value.try_into_string().unwrap_or_default(),
                _ => {}
            }
        }

        Ok(Self { uid, nick_name })
    }
}

/// Extract just the `fontColor` (tag 0, i64) from a HYBulletFormat payload.
fn decode_bullet_font_color(raw: &[u8]) -> i64 {
    let mut de = TarsDeserializer::new(Bytes::copy_from_slice(raw));
    while !de.is_empty() {
        if let Ok((0, value)) = de.read_value() {
            return value.try_into_i64().unwrap_or(0);
        }
    }
    0
}
