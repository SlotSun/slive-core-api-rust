//! Integration tests for Twitch live extractor.
//! Run with: `cargo test --test twitch_integration -- --nocapture`

use platforms_parser::extractor::LiveExtractor;
use platforms_parser::extractor::platforms::twitch::TwitchExtractor;

fn ensure_tls_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[test]
fn test_supports_url() {
    let ext = TwitchExtractor::new();
    assert!(ext.supports_url("https://www.twitch.tv/shroud"));
    assert!(ext.supports_url("https://twitch.tv/pokimane"));
    assert!(!ext.supports_url("https://www.huya.com/12345"));
}

#[test]
fn test_extract_room_id() {
    let ext = TwitchExtractor::new();
    assert_eq!(
        ext.extract_room_id("https://www.twitch.tv/shroud"),
        Some("shroud".to_string())
    );
    assert_eq!(
        ext.extract_room_id("https://twitch.tv/pokimane"),
        Some("pokimane".to_string())
    );
    // Should filter out non-channel paths
    assert_eq!(ext.extract_room_id("https://www.twitch.tv/directory"), None);
}

#[tokio::test]
async fn test_get_categories() {
    ensure_tls_provider();
    let ext = TwitchExtractor::new();
    match ext.get_categories().await {
        Ok(categories) => {
            println!("=== Twitch Categories ===");
            for cat in categories.iter().take(10) {
                println!(
                    "  [{}] {} ({} sub)",
                    cat.id,
                    cat.name,
                    cat.sub_categories.len()
                );
            }
            assert!(!categories.is_empty());
        }
        Err(e) => println!("  ⚠️ get_categories failed: {}", e),
    }
}

#[tokio::test]
async fn test_get_room_detail() {
    ensure_tls_provider();
    let ext = TwitchExtractor::new();
    // shroud is usually live, pokimane might not be
    let channel = "saineginjo";

    match ext.get_room_detail(channel).await {
        Ok(detail) => {
            println!("\n=== Twitch Room Detail ===");
            println!("  Room ID: {}", detail.room_id);
            println!("  Title:   {}", detail.title);
            println!("  User:    {}", detail.user_name);
            println!("  Online:  {}", detail.online);
            println!(
                "  Status:  {}",
                if detail.status { "LIVE" } else { "OFFLINE" }
            );

            if detail.status {
                let qualities = ext.get_play_qualities(&detail).await.unwrap_or_default();
                println!("  Qualities: {}", qualities.len());
                for q in &qualities {
                    println!("    - {} (data={})", q.quality, q.data);
                }
                if !qualities.is_empty() {
                    match ext.get_play_urls(&detail, &qualities[0]).await {
                        Ok(urls) => {
                            println!("  Play URLs: {}", urls.urls.len());
                            for (i, url) in urls.urls.iter().take(3).enumerate() {
                                let display = if url.len() > 120 {
                                    format!("{}...", &url[..120])
                                } else {
                                    url.clone()
                                };
                                println!("    [{}] {}", i, display);
                            }
                        }
                        Err(e) => println!("  ⚠️ get_play_urls failed: {}", e),
                    }
                }
            }
        }
        Err(e) => println!("  ⚠️ get_room_detail failed: {}", e),
    }
}

#[tokio::test]
async fn test_search_rooms() {
    ensure_tls_provider();
    let ext = TwitchExtractor::new();
    match ext.search_rooms("valorant", 1).await {
        Ok(result) => {
            println!("=== Twitch Search Rooms ===");
            println!(
                "  Results: {} (has_more={})",
                result.items.len(),
                result.has_more
            );
            for item in result.items.iter().take(5) {
                println!(
                    "  - [{}] {} ({} viewers)",
                    item.room_id, item.user_name, item.online
                );
            }
        }
        Err(e) => println!("  ⚠️ search_rooms failed: {}", e),
    }
}

#[tokio::test]
async fn test_search_anchors() {
    ensure_tls_provider();
    let ext = TwitchExtractor::new();
    match ext.search_anchors("shroud", 1).await {
        Ok(result) => {
            println!("=== Twitch Search Anchors ===");
            println!(
                "  Results: {} (has_more={})",
                result.items.len(),
                result.has_more
            );
            for item in result.items.iter().take(5) {
                println!(
                    "  - {} ({})",
                    item.user_name,
                    if item.is_live { "LIVE" } else { "OFFLINE" }
                );
            }
        }
        Err(e) => println!("  ⚠️ search_anchors failed: {}", e),
    }
}
