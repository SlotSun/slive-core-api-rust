# slive-core-api-rust

> An AI-driven Rust library for multi-platform live streaming aggregation.

This project was entirely designed, implemented, and iterated by AI (Claude). Every line of code — from architecture design to platform-specific protocol reverse engineering — was generated through human-AI collaboration.

## Supported Platforms

| Platform | Stream Extraction | Danmaku (Chat) |
|----------|:-:|:-:|
| Bilibili (哔哩哔哩) | ✓ | ✓ |
| Douyin (抖音) | ✓ | ✓ |
| Douyu (斗鱼) | ✓ | ✓ |
| Huya (虎牙) | ✓ | ✓ |
| Twitch | ✓ | ✓ |

## Architecture

```
slive-core-api-rust/
├── crates/
│   ├── platforms-parser/   # Core library — extractors & danmaku providers
│   ├── tars-codec/         # Tars binary codec (for Huya protocol)
│   └── bytes-util/         # Bit-level read/write utilities
```

```
┌─────────────────────────────────────────────────────────────┐
│                    platforms-parser                          │
│                                                             │
│  ┌──────────────┐       ┌──────────────────────┐           │
│  │   extractor   │       │       danmaku         │           │
│  │               │       │                       │           │
│  │ LiveExtractor │       │   DanmakuProvider     │           │
│  │   (trait)     │       │   DanmakuProtocol     │           │
│  │               │       │   (trait)             │           │
│  │ ExtractorReg  │       │   ProviderRegistry    │           │
│  └───────┬───────┘       └──────────┬────────────┘           │
│          │                          │                        │
│  ┌───────┴──────────────────────────┴──────┐                │
│  │  Bilibili · Douyin · Douyu · Huya · Twitch │             │
│  └─────────────────────────────────────────┘                │
│                                                             │
│  ┌──────────────────┐  ┌──────────────────┐                 │
│  │   HttpClient      │  │   danmaku_mask    │                 │
│  │  (cookie+retry)   │  │                   │                 │
│  │                   │  │  DanmakuMask      │                 │
│  │                   │  │  FrequencyMask    │                 │
│  │                   │  │  WordBlacklist    │                 │
│  │                   │  │  (regex support)  │                 │
│  └──────────────────┘  └──────────────────┘                 │
│                                                             │
│  ┌──────────────────────────────────────────┐               │
│  │  statistics · sampler · writer            │               │
│  └──────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

- **HttpClient** — Shared HTTP client with timeout, retry, and cookie management
- **LiveExtractor** — Trait for stream URL extraction (room info, play URLs, quality selection)
- **DanmakuProvider** — Trait for real-time chat WebSocket connections
- **DanmakuMask** — Trait for danmaku filtering (frequency, blacklist, regex)
- **Platform-specific signing** — ABogus (Douyin), WBI (Bilibili), MD5 (Douyu)

## Usage

### Quick Start

```rust
use platforms_parser::extractor::{ExtractorRegistry, create_extractor};

#[tokio::main]
async fn main() {
    // By platform ID
    let ext = create_extractor("bilibili").unwrap();
    ext.set_cookies("SESSDATA=...");

    let detail = ext.get_room_detail("6").await.unwrap();
    println!("{}: {}", detail.user_name, detail.title);

    let qualities = ext.get_play_qualities(&detail).await.unwrap();
    let urls = ext.get_play_urls(&detail, &qualities[0]).await.unwrap();
    println!("Play URL: {}", urls.urls[0]);

    // By URL
    let registry = ExtractorRegistry::with_defaults();
    let ext = registry.get_by_url("https://www.huya.com/12345").unwrap();
    println!("Platform: {}", ext.name());
}
```

### Danmaku (Real-time Chat)

```rust
use platforms_parser::danmaku::{
    create_bilibili_danmaku_provider, ConnectionConfig,
    DanmakuItem, DanmakuType,
};

#[tokio::main]
async fn main() {
    let provider = create_bilibili_danmaku_provider();
    let config = ConnectionConfig::with_cookies(Some("SESSDATA=...".into()));
    let mut conn = provider.connect("12345", config).await.unwrap();

    while let Some(item) = provider.receive(&conn).await.unwrap() {
        match item {
            DanmakuItem::Message(msg) => match msg.message_type {
                DanmakuType::Chat => println!("{}: {}", msg.username, msg.content),
                DanmakuType::Gift => println!("{} sent a gift", msg.username),
                DanmakuType::SuperChat => println!("SC: {}", msg.content),
                _ => {}
            },
            DanmakuItem::Control(event) => { /* stream closed, room info changed, etc. */ }
        }
    }

    provider.disconnect(&mut conn).await.unwrap();
}
```

### Danmaku Mask (Filtering)

```rust
use platforms_parser::danmaku_mask::mask_config::{MaskConfig, FrequencyConfig};
use platforms_parser::danmaku_mask::mask_provider::MaskedDanmakuProvider;

// Create masked provider
let inner = create_huya_danmaku_provider();
let provider = MaskedDanmakuProvider::new(inner);

// Configure mask
let mask_config = MaskConfig {
    frequency: Some(FrequencyConfig {
        base_window_ms: 10000,  // 10 seconds window
        bucket_count: 5,
        use_normalization: true,
        max_frequency: 3,       // Max 3 times per window
    }),
    blacklist_words: Some(vec![
        "广告".to_string(),           // Plain text
        "代练".to_string(),           // Plain text
        "/加[微V]信/".to_string(),    // Regex: matches "加微信" or "加V信"
        "/(?i)spam/".to_string(),     // Regex: case-insensitive
    ]),
};

let config = ConnectionConfig::with_cookies(None)
    .with_mask(mask_config);

let conn = provider.connect("room_id", config).await?;

// Messages are automatically filtered
while let Some(item) = provider.receive(&conn).await? {
    // Only non-blocked messages reach here
}

// Runtime control
let stats = provider.stats(&conn.id).await;
println!("Passed: {}, Blocked: {}", stats.passed, stats.blocked);
```

### Statistics & Sampling

```rust
use platforms_parser::danmaku::{StatisticsAggregator, create_sampler, DanmakuSamplingConfig};

let mut stats = StatisticsAggregator::new();
// In your message loop:
stats.record_message(&msg.user_id, &msg.username, &msg.content, false, msg.timestamp);

let result = stats.finalize(chrono::Utc::now());
println!("Top talkers: {:?}", result.top_talkers);
println!("Word frequency: {:?}", result.word_frequency);
```

### XML Writer (Bilibili-compatible)

```rust
use platforms_parser::danmaku::XmlDanmakuWriter;

let mut writer = XmlDanmakuWriter::new(std::path::Path::new("output.xml")).await.unwrap();
writer.write_message(&msg).await.unwrap();
writer.finalize().await.unwrap();
```

---

## Public API Reference

### 1. LiveExtractor Trait

Core trait for extracting live stream information. All 5 platforms implement this.

```rust
#[async_trait]
pub trait LiveExtractor: Send + Sync {
    fn id(&self) -> &str;
    fn name(&self) -> &str;
    fn supports_url(&self, url: &str) -> bool;
    fn extract_room_id(&self, url: &str) -> Option<String>;
    fn set_cookies(&self, cookies: &str);

    // Discovery
    async fn get_categories(&self) -> Result<Vec<LiveCategory>>;
    async fn search_rooms(&self, keyword: &str, page: u32) -> Result<LiveSearchRoomResult>;
    async fn search_anchors(&self, keyword: &str, page: u32) -> Result<LiveSearchAnchorResult>;
    async fn get_category_rooms(&self, category: &LiveSubCategory, page: u32) -> Result<LiveCategoryResult>;
    async fn get_recommend_rooms(&self, page: u32) -> Result<LiveCategoryResult>;

    // Room detail
    async fn get_room_detail(&self, room_id: &str) -> Result<LiveRoomDetail>;
    async fn get_live_status(&self, room_id: &str) -> Result<bool>;

    // Playback
    async fn get_play_qualities(&self, detail: &LiveRoomDetail) -> Result<Vec<LivePlayQuality>>;
    async fn get_play_urls(&self, detail: &LiveRoomDetail, quality: &LivePlayQuality) -> Result<LivePlayUrl>;

    // Other
    async fn get_super_chat_messages(&self, room_id: &str) -> Result<Vec<LiveSuperChatMessage>>;
}
```

### 2. DanmakuProvider Trait

Core trait for real-time danmaku WebSocket connections.

```rust
#[async_trait]
pub trait DanmakuProvider: Send + Sync {
    fn platform(&self) -> &str;
    fn supports_url(&self, url: &str) -> bool;
    fn extract_room_id(&self, url: &str) -> Option<String>;

    async fn connect(&self, room_id: &str, config: ConnectionConfig) -> Result<DanmakuConnection>;
    async fn receive(&self, connection: &DanmakuConnection) -> Result<Option<DanmakuItem>>;
    async fn disconnect(&self, connection: &mut DanmakuConnection) -> Result<()>;
}
```

### 3. DanmakuProtocol Trait

Lower-level trait for implementing custom WebSocket-based danmaku providers.

```rust
pub trait DanmakuProtocol: Send + Sync + 'static {
    fn platform(&self) -> &str;
    fn supports_url(&self, url: &str) -> bool;
    fn extract_room_id(&self, url: &str) -> Option<String>;
    fn websocket_url(&self, room_id: &str) -> impl Future<Output = Result<String>> + Send;

    // All with default impls:
    fn headers(&self, room_id: &str) -> HeaderMap;
    fn cookies(&self) -> Option<String>;
    fn send_cookie_header(&self) -> bool;
    fn normalize_cookies(&self, cookies: &str) -> String;
    fn configure_connection(&mut self, cookies: Option<&str>, extras: Option<&HashMap<String, String>>);
    fn handshake_messages(&self, room_id: &str) -> impl Future<Output = Result<Vec<Message>>> + Send;
    fn heartbeat_message(&self) -> Option<Message>;
    fn heartbeat_interval(&self) -> Duration;
    fn decode_message(&self, message: &Message, room_id: &str, tx: &mpsc::Sender<Message>) -> impl Future<Output = Result<Vec<DanmakuItem>>> + Send;
}
```

### 4. DanmakuMask Trait

Trait for danmaku filtering strategies.

```rust
pub trait DanmakuMask: Send {
    fn should_block(&mut self, text: &str, now_ms: u64) -> bool;
    fn reset(&mut self);
}
```

**Built-in implementations:**

| Mask | Description |
|------|-------------|
| `FrequencyMask` | Sliding-window frequency limiter |
| `WordBlacklist` | Keyword blacklist with regex support |
| `CompositeMask` | Combines multiple masks (any blocks → blocked) |

---

## Data Models

### Extractor Models

| Struct | Description | Key Fields |
|--------|-------------|------------|
| `LiveRoomDetail` | Full room info | `room_id`, `title`, `cover`, `online`, `status`, `user_name`, `platform`, `data`, `danmaku_data` |
| `LiveRoomItem` | Room listing item | `room_id`, `title`, `cover`, `online`, `user_name`, `url`, `platform` |
| `LiveAnchorItem` | Anchor listing item | `user_id`, `user_name`, `room_id`, `is_live`, `url` |
| `LiveCategory` | Top-level category | `id`, `name`, `sub_categories` |
| `LiveSubCategory` | Sub-category | `id`, `name`, `parent_id` |
| `LivePlayQuality` | Quality level | `quality` (name), `data` (qn value) |
| `LivePlayUrl` | Play stream URLs | `urls` (list), `url_type` (Flv/M3u8) |
| `LiveSuperChatMessage` | Super chat message | `user_name`, `message`, `price`, `start_time`, `end_time` |
| `LiveCategoryResult` | Paginated room list | `has_more`, `items` |
| `LiveSearchRoomResult` | Search room results | `has_more`, `items` |
| `LiveSearchAnchorResult` | Search anchor results | `has_more`, `items` |

### Danmaku Models

| Struct/Enum | Description | Key Fields |
|-------------|-------------|------------|
| `DanmakuMessage` | A single chat message | `id`, `user_id`, `username`, `content`, `message_type`, `timestamp`, `metadata` |
| `DanmakuType` | Message type enum | `Chat`, `Gift`, `SuperChat`, `System`, `UserJoin`, `Follow`, `Subscription`, `Other` |
| `DanmakuItem` | Top-level danmaku event | `Message(DanmakuMessage)` or `Control(DanmakuControlEvent)` |
| `DanmakuControlEvent` | Control events | `StreamClosed`, `RoomInfoChanged`, `Other` |
| `DanmakuConnection` | Connection state | `id`, `platform`, `room_id`, `is_connected`, `connected_at`, `reconnect_count` |
| `ConnectionConfig` | Connection config | `cookies`, `websocket`, `extras`, `mask_config` |
| `DanmakuStatistics` | Aggregated stats | `total_count`, `top_talkers`, `word_frequency`, `rate_timeseries` |
| `TopTalker` | Top message sender | `user_id`, `username`, `message_count` |
| `WordFrequency` | Word count entry | `word`, `count` |
| `RateDataPoint` | Rate timeseries point | `timestamp`, `count` |

### Danmaku Mask Models

| Struct/Enum | Description | Key Fields |
|-------------|-------------|------------|
| `MaskConfig` | Mask configuration | `frequency`, `blacklist_words` |
| `FrequencyConfig` | Frequency mask config | `base_window_ms`, `bucket_count`, `use_normalization`, `max_frequency` |
| `MaskStats` | Filter statistics | `total_received`, `passed`, `blocked` |
| `FrequencyMask` | Sliding-window frequency limiter | - |
| `WordBlacklist` | Keyword blacklist (with regex) | - |
| `CompositeMask` | Combined masks | - |

### Enums

| Enum | Variants |
|------|----------|
| `UrlType` | `Flv`, `M3u8`, `Other(String)` |
| `ExtractorError` | `InvalidUrl`, `HttpError`, `UnsupportedExtractor`, `JsonError`, `NoStreamsFound`, `Other`, ... |
| `DanmakuError` | `Connection`, `Protocol`, `Io`, `Tars`, `ProtobufDecode`, `ProtobufEncode`, `Other` |
| `TarsError` | `Io`, `Utf8`, `InvalidTag`, `InvalidTypeId`, `TypeMismatch`, ... |

---

## Registries

### ExtractorRegistry

```rust
let registry = ExtractorRegistry::with_defaults(); // registers all 5 platforms
registry.get_by_id("bilibili");     // Some(Arc<dyn LiveExtractor>)
registry.get_by_url("https://live.bilibili.com/123"); // Some(Arc<dyn LiveExtractor>)
registry.platforms();               // ["huya", "bilibili", "douyin", "douyu", "twitch"]
registry.register(my_extractor);    // register custom extractor
```

### ProviderRegistry

```rust
let registry = ProviderRegistry::with_defaults(); // registers all 5 platforms
registry.get_by_platform("bilibili"); // Some(Arc<dyn DanmakuProvider>)
registry.get_by_url("https://live.douyin.com/xxx"); // Some(Arc<dyn DanmakuProvider>)
registry.platforms();                 // ["huya", "bilibili", "douyu", "douyin", "twitch"]
```

### Factory Functions

```rust
// Create extractor by platform ID
let ext = create_extractor("bilibili");       // Option<Arc<dyn LiveExtractor>>
let ext = create_extractor_from_url(url);     // Option<Arc<dyn LiveExtractor>>

// Create danmaku provider by platform
let p = create_bilibili_danmaku_provider();     // BilibiliDanmakuProvider
let p = create_douyin_danmaku_provider();       // DouyinDanmakuProvider
let p = create_douyu_danmaku_provider();        // DouyuDanmakuProvider
let p = create_huya_danmaku_provider();         // HuyaDanmakuProvider
let p = create_twitch_danmaku_provider();       // TwitchDanmakuProvider
```

---

## HttpClient

Shared HTTP client with cookie management, default headers, timeouts, and automatic retry with exponential backoff.

```rust
let http = HttpClient::builder()
    .user_agent("MyApp/1.0")
    .connect_timeout(Duration::from_secs(5))
    .read_timeout(Duration::from_secs(15))
    .default_header("Referer", "https://live.bilibili.com")
    .build()?;

http.set_cookies("SESSDATA=...");
let json: Value = http.get_json(url).await?;
let json: Value = http.get_json_with_headers(url, &headers).await?;
let text: String = http.post_form_text(url, &form_data).await?;
let json: Value = http.post_json_json(url, &body).await?;
let req: RequestBuilder = http.get(url); // low-level with cookies attached
```

---

## Danmaku Mask (Filtering)

### Overview

The danmaku mask system provides real-time message filtering with:

- **Frequency limiting** — Block repeated messages within a time window
- **Keyword blacklist** — Block messages containing blacklisted words
- **Regex support** — Use regex patterns for advanced matching
- **Composite filtering** — Combine multiple strategies

### WordBlacklist Regex Support

The `WordBlacklist` supports both plain text and regex patterns:

```rust
use platforms_parser::danmaku_mask::mask_word_blacklist::WordBlacklist;

let bl = WordBlacklist::new(vec![
    "广告".to_string(),           // Plain text (case-insensitive, normalized)
    "代练".to_string(),           // Plain text
    "/加[微V]信/".to_string(),    // Regex: matches "加微信" or "加V信"
    "/(?i)spam/".to_string(),     // Regex: case-insensitive
    "/\\d{5,}/".to_string(),      // Regex: matches 5+ digits
]);

// Plain text matching (normalized)
assert!(bl.contains_blacklisted("这里有广告"));      // true
assert!(bl.contains_blacklisted("广告！"));           // true (punctuation removed)

// Regex matching (original text)
assert!(bl.contains_blacklisted("加微信"));           // true
assert!(bl.contains_blacklisted("加V信"));            // true
assert!(bl.contains_blacklisted("this is SPAM"));    // true
assert!(bl.contains_blacklisted("加我12345"));        // true
```

**Regex format:**
- Wrapped in `/pattern/` (e.g., `/广告|代练/`)
- Uses Rust regex syntax
- Case-insensitive: `/(?i)pattern/`
- Invalid patterns are ignored (with warning)

### Runtime Control

```rust
use platforms_parser::danmaku_mask::mask_provider::MaskedDanmakuProvider;

let provider = MaskedDanmakuProvider::new(inner_provider);

// Dynamic mask management
provider.set_mask(&conn.id, Box::new(new_mask)).await;
provider.clear_mask(&conn.id).await;
provider.reset_mask(&conn.id).await;

// Statistics
let stats = provider.stats(&conn.id).await;
println!("Total: {}, Passed: {}, Blocked: {}", 
    stats.total_received, stats.passed, stats.blocked);
```

---

## Auxiliary Crates

### tars-codec

Tars binary protocol codec for Huya's proprietary communication protocol.

```rust
use tars_codec::{TarsCodec, TarsMessage, TarsValue, encode_request, decode_response};

// Encode
let encoded = encode_request(&message)?;

// Decode
let decoded = decode_response(&mut buffer)?;
```

| Item | Description |
|------|-------------|
| `TarsCodec` | tokio `Encoder`/`Decoder` implementation |
| `TarsMessage` | Message with header + body map |
| `TarsRequestHeader` | Request metadata (servant, func, timeout, etc.) |
| `TarsValue` | Recursive value type (Bool, Int, String, Struct, Map, List, ...) |
| `ValidatedBytes` | UTF-8 validated byte wrapper |

### bytes-util

Bit-level read/write utilities for binary protocol parsing.

| Item | Description |
|------|-------------|
| `BitReader<T>` | Read individual bits from any `io::Read` |
| `BitWriter<W>` | Write individual bits to any `io::Write` |
| `BytesCursor` / `BytesCursorExt` | Cursor over `Bytes` with extract helpers |

---

## Testing

```bash
# All tests
cargo test

# Unit tests only (no network)
cargo test --lib

# Platform integration tests
cargo test --test bilibili_integration -- --nocapture
cargo test --test bilibili_danmaku -- --nocapture
cargo test --test douyin_integration -- --nocapture
cargo test --test douyin_danmaku -- --nocapture
cargo test --test douyu_integration -- --nocapture
cargo test --test huya_integration -- --nocapture
cargo test --test twitch_integration -- --nocapture
```

Integration tests require a `test_cookies.json` in the project root:

```json
{
  "bilibili": "SESSDATA=...; bili_jct=...; DedeUserID=...; ...",
  "douyin": "__ac_nonce=...; sessionid=...; ..."
}
```

## AI-Driven Development

This library was built through an iterative AI development process (AI model: **Claude Opus 4.6**):

- **Protocol reverse engineering** — Douyin WebSocket handshake, X-Bogus signature, Bilibili WBI signing, Douyu STT encryption
- **Architecture design** — Shared HttpClient, trait-based extractor/provider pattern, generic WebSocket framework
- **Code quality** — Zero warnings, 150+ passing tests, clean separation of concerns
- **Refactoring** — Extracted shared HTTP client from 5 platform-specific implementations, unified error handling

## Acknowledgements

项目结构和部分代码参考了 [rust-srec](https://github.com/hua0512/rust-srec)

## Flutter / Dart Binding

A Flutter FFI plugin wrapping this library is available at:

**[slive_core](https://github.com/SlotSun/slive_core)** — Flutter plugin via `flutter_rust_bridge`

## License

[MIT](LICENSE)
