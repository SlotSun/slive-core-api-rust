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

### Key Components

- **HttpClient** — Shared HTTP client with timeout, retry, and cookie management
- **LiveExtractor** — Trait for stream URL extraction (room info, play URLs, quality selection)
- **DanmuProvider** — Trait for real-time chat WebSocket connections
- **Platform-specific signing** — ABogus (Douyin), WBI (Bilibili), MD5 (Douyu)

## Usage

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

## AI-Driven Development

This library was built through an iterative AI development process:

- **Protocol reverse engineering** — Douyin WebSocket handshake, X-Bogus signature, Bilibili WBI signing, Douyu STT encryption
- **Architecture design** — Shared HttpClient, trait-based extractor/provider pattern, generic WebSocket framework
- **Code quality** — Zero warnings, 77+ passing unit tests, clean separation of concerns
- **Refactoring** — Extracted shared HTTP client from 5 platform-specific implementations, unified error handling

## License

[MIT](LICENSE)
