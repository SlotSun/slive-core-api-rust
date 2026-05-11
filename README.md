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
┌─────────────────────────────────────────────────┐
│              platforms-parser                    │
│                                                 │
│  ┌──────────────┐       ┌──────────────────┐    │
│  │   extractor   │       │     danmaku       │    │
│  │               │       │                   │    │
│  │ LiveExtractor │       │  DanmuProvider    │    │
│  │   (trait)     │       │  DanmuProtocol    │    │
│  │               │       │  (trait)          │    │
│  │ ExtractorReg  │       │  ProviderRegistry │    │
│  └───────┬───────┘       └────────┬──────────┘    │
│          │                        │               │
│  ┌───────┴────────────────────────┴──────┐       │
│  │  Bilibili · Douyin · Douyu · Huya · Twitch │  │
│  └───────────────────────────────────────┘       │
│                                                 │
│  ┌──────────────────┐  ┌──────────────────┐     │
│  │   HttpClient      │  │  statistics      │     │
│  │  (cookie+retry)   │  │  sampler         │     │
│  │                   │  │  writer          │     │
│  └──────────────────┘  └──────────────────┘     │
└─────────────────────────────────────────────────┘
```

### Key Components

- **HttpClient** — Shared HTTP client with timeout, retry, and cookie management
- **LiveExtractor** — Trait for stream URL extraction (room info, play URLs, quality selection)
- **DanmuProvider** — Trait for real-time chat WebSocket connections
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
    create_bilibili_danmu_provider, ConnectionConfig,
    DanmuItem, DanmuType,
};

#[tokio::main]
async fn main() {
    let provider = create_bilibili_danmu_provider();
    let config = ConnectionConfig::with_cookies(Some("SESSDATA=...".into()));
    let mut conn = provider.connect("12345", config).await.unwrap();

    while let Some(item) = provider.receive(&conn).await.unwrap() {
        match item {
            DanmuItem::Message(msg) => match msg.message_type {
                DanmuType::Chat => println!("{}: {}", msg.username, msg.content),
                DanmuType::Gift => println!("{} sent a gift", msg.username),
                DanmuType::SuperChat => println!("SC: {}", msg.content),
                _ => {}
            },
            DanmuItem::Control(event) => { /* stream closed, room info changed, etc. */ }
        }
    }

    provider.disconnect(&mut conn).await.unwrap();
}
```

### Statistics & Sampling

```rust
use platforms_parser::danmaku::{StatisticsAggregator, create_sampler, DanmuSamplingConfig};

let mut stats = StatisticsAggregator::new();
// In your message loop:
stats.record_message(&msg.user_id, &msg.username, &msg.content, false, msg.timestamp);

let result = stats.finalize(chrono::Utc::now());
println!("Top talkers: {:?}", result.top_talkers);
println!("Word frequency: {:?}", result.word_frequency);
```

### XML Writer (Bilibili-compatible)

```rust
use platforms_parser::danmaku::XmlDanmuWriter;

let mut writer = XmlDanmuWriter::new(std::path::Path::new("output.xml")).await.unwrap();
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

### 2. DanmuProvider Trait

Core trait for real-time danmaku WebSocket connections.

```rust
#[async_trait]
pub trait DanmuProvider: Send + Sync {
    fn platform(&self) -> &str;
    fn supports_url(&self, url: &str) -> bool;
    fn extract_room_id(&self, url: &str) -> Option<String>;

    async fn connect(&self, room_id: &str, config: ConnectionConfig) -> Result<DanmuConnection>;
    async fn receive(&self, connection: &DanmuConnection) -> Result<Option<DanmuItem>>;
    async fn disconnect(&self, connection: &mut DanmuConnection) -> Result<()>;
}
```

### 3. DanmuProtocol Trait

Lower-level trait for implementing custom WebSocket-based danmaku providers.

```rust
pub trait DanmuProtocol: Send + Sync + 'static {
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
    fn decode_message(&self, message: &Message, room_id: &str, tx: &mpsc::Sender<Message>) -> impl Future<Output = Result<Vec<DanmuItem>>> + Send;
}
```

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
| `LiveSuperChatMessage` | Super chat message | `id`, `user_name`, `content`, `price`, `currency` |
| `LiveCategoryResult` | Paginated room list | `has_more`, `items` |
| `LiveSearchRoomResult` | Search room results | `has_more`, `items` |
| `LiveSearchAnchorResult` | Search anchor results | `has_more`, `items` |

### Danmaku Models

| Struct/Enum | Description | Key Fields |
|-------------|-------------|------------|
| `DanmuMessage` | A single chat message | `id`, `user_id`, `username`, `content`, `message_type`, `timestamp`, `metadata` |
| `DanmuType` | Message type enum | `Chat`, `Gift`, `SuperChat`, `System`, `UserJoin`, `Follow`, `Subscription`, `Other` |
| `DanmuItem` | Top-level danmaku event | `Message(DanmuMessage)` or `Control(DanmuControlEvent)` |
| `DanmuControlEvent` | Control events | `StreamClosed`, `RoomInfoChanged`, `Other` |
| `DanmuConnection` | Connection state | `id`, `platform`, `room_id`, `is_connected`, `connected_at`, `reconnect_count` |
| `ConnectionConfig` | Connection config | `cookies`, `websocket` (WebSocketProviderConfig), `extras` |
| `DanmuStatistics` | Aggregated stats | `total_count`, `top_talkers`, `word_frequency`, `rate_timeseries` |
| `TopTalker` | Top message sender | `user_id`, `username`, `message_count` |
| `WordFrequency` | Word count entry | `word`, `count` |
| `RateDataPoint` | Rate timeseries point | `timestamp`, `count` |

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
registry.get_by_platform("bilibili"); // Some(Arc<dyn DanmuProvider>)
registry.get_by_url("https://live.douyin.com/xxx"); // Some(Arc<dyn DanmuProvider>)
registry.platforms();                 // ["huya", "bilibili", "douyu", "douyin", "twitch"]
```

### Factory Functions

```rust
// Create extractor by platform ID
let ext = create_extractor("bilibili");       // Option<Arc<dyn LiveExtractor>>
let ext = create_extractor_from_url(url);     // Option<Arc<dyn LiveExtractor>>

// Create danmaku provider by platform
let p = create_bilibili_danmu_provider();     // BilibiliDanmuProvider
let p = create_douyin_danmu_provider();       // DouyinDanmuProvider
let p = create_douyu_danmu_provider();        // DouyuDanmuProvider
let p = create_huya_danmu_provider();         // HuyaDanmuProvider
let p = create_twitch_danmu_provider();       // TwitchDanmuProvider
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
- **Code quality** — Zero warnings, 130+ passing tests, clean separation of concerns
- **Refactoring** — Extracted shared HTTP client from 5 platform-specific implementations, unified error handling

## Acknowledgements

项目结构和部分代码参考了 [rust-srec](https://github.com/hua0512/rust-srec)

## License

[MIT](LICENSE)
