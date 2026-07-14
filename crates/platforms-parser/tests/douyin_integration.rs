//! Integration tests for Douyin live extractor.
//! Run with: `cargo test --test douyin_integration -- --nocapture`

mod common;

use platforms_parser::danmaku::provider::{ConnectionConfig, DanmakuProvider};
use platforms_parser::extractor::LiveExtractor;
use platforms_parser::extractor::platforms::douyin::DouyinExtractor;
use platforms_parser::extractor::platforms::douyin::danmaku::DouyinDanmakuProvider;
use platforms_parser::extractor::platforms::douyin::models::DouyinDanmakuData;

fn ensure_tls_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init();
    });
}

#[test]
fn test_supports_url() {
    let ext = DouyinExtractor::new();
    assert!(ext.supports_url("https://live.douyin.com/12345"));
    assert!(ext.supports_url("https://v.douyin.com/abc"));
    assert!(!ext.supports_url("https://www.huya.com/12345"));
}

#[test]
fn test_extract_room_id() {
    let ext = DouyinExtractor::new();
    assert_eq!(
        ext.extract_room_id("https://live.douyin.com/12345"),
        Some("12345".to_string())
    );
}

#[tokio::test]
async fn test_get_categories() {
    ensure_tls_provider();
    let ext = DouyinExtractor::new();
    match ext.get_categories().await {
        Ok(categories) => {
            println!("=== Douyin 分类 ===");
            for cat in &categories {
                println!(
                    "  [{}] {} ({} 子分类)",
                    cat.id,
                    cat.name,
                    cat.sub_categories.len()
                );
            }
        }
        Err(e) => println!("  ⚠️ 获取分类失败: {}", e),
    }
}

#[tokio::test]
async fn test_get_room_detail() {
    ensure_tls_provider();
    let ext = DouyinExtractor::new();
    let room_id = "447840496489";

    // First, fetch raw API response for debugging
    match ext.fetch_room_enter_debug(room_id).await {
        Ok(raw) => {
            let display = if raw.len() > 500 { &raw[..500] } else { &raw };
            println!(
                "\n=== Raw API response (first 500 chars) ===\n{}\n",
                display
            );
        }
        Err(e) => println!("  ⚠️ fetch_room_enter failed: {}", e),
    }

    match ext.get_room_detail(room_id).await {
        Ok(detail) => {
            println!("=== Douyin 房间详情 ===");
            println!("  房间ID: {}", detail.room_id);
            println!("  标题:   {}", detail.title);
            println!("  主播:   {}", detail.user_name);
            println!("  在线:   {}", detail.online);
            println!(
                "  状态:   {}",
                if detail.status {
                    "直播中"
                } else {
                    "未开播"
                }
            );

            if detail.status {
                let qualities = ext.get_play_qualities(&detail).await.unwrap_or_default();
                println!("  清晰度: {}", qualities.len());
                for q in &qualities {
                    println!("    - {} (data={})", q.quality, q.data);
                }
                if !qualities.is_empty() {
                    match ext.get_play_urls(&detail, &qualities[0]).await {
                        Ok(urls) => {
                            println!("  播放链接: {} 条", urls.urls.len());
                            for (i, url) in urls.urls.iter().take(3).enumerate() {
                                let display = if url.len() > 100 {
                                    format!("{}...", &url[..100])
                                } else {
                                    url.clone()
                                };
                                println!("    [{}] {}", i, display);
                            }
                        }
                        Err(e) => println!("  ⚠️ 获取播放链接失败: {}", e),
                    }
                }
            }
        }
        Err(e) => println!("  ⚠️ 获取房间详情失败: {}", e),
    }
}

#[tokio::test]
async fn test_search_rooms() {
    ensure_tls_provider();
    let ext = DouyinExtractor::new().with_auth_cookies(common::douyin_cookies());
    // Debug: fetch raw search response
    match ext.search_rooms_debug("三角洲").await {
        Ok(raw) => {
            let display = if raw.len() > 800 { &raw[..800] } else { &raw };
            println!(
                "\n=== Raw search response (first 800 chars) ===\n{}\n",
                display
            );
        }
        Err(e) => println!("  ⚠️ search_rooms_debug failed: {}", e),
    }
    match ext.search_rooms("三角洲", 1).await {
        Ok(result) => {
            println!("=== Douyin 搜索直播间 ===");
            println!(
                "  结果: {} (has_more={})",
                result.items.len(),
                result.has_more
            );
            for item in result.items.iter().take(5) {
                println!(
                    "  - [{}] {} ({} 在线)",
                    item.room_id, item.user_name, item.online
                );
            }
        }
        Err(e) => println!("  ⚠️ 搜索失败: {}", e),
    }
}

#[tokio::test]
async fn test_get_recommend_rooms() {
    ensure_tls_provider();
    let ext = DouyinExtractor::new();
    match ext.get_recommend_rooms(1).await {
        Ok(result) => {
            println!("=== Douyin 推荐直播间 ===");
            println!(
                "  结果: {} (has_more={})",
                result.items.len(),
                result.has_more
            );
            for item in result.items.iter().take(5) {
                println!(
                    "  - [{}] {} ({} 在线)",
                    item.room_id, item.user_name, item.online
                );
            }
        }
        Err(e) => println!("  ⚠️ 获取推荐失败: {}", e),
    }
}

#[tokio::test]
async fn test_danmaku_connect() {
    ensure_tls_provider();
    let ext = DouyinExtractor::new();
    let room_id = "447840496489";

    // Get room detail to obtain danmaku_data
    let detail = ext
        .get_room_detail(room_id)
        .await
        .expect("get_room_detail failed");
    println!("=== 弹幕连接测试 ===");
    println!("  房间: {} ({})", detail.room_id, detail.user_name);
    println!(
        "  状态: {}",
        if detail.status {
            "直播中"
        } else {
            "未开播"
        }
    );

    let danmaku_data = match &detail.danmaku_data {
        Some(d) => d.clone(),
        None => {
            println!("  ⚠️ 无 danmaku_data，跳过弹幕测试");
            return;
        }
    };

    // Parse the danmaku_data JSON to extract room_id
    let dm: DouyinDanmakuData = serde_json::from_value(danmaku_data).expect("parse danmaku_data");
    println!("  numeric room_id: {}", dm.room_id);
    println!("  web_rid: {}", dm.web_rid);

    // Connect to danmaku
    let provider = DouyinDanmakuProvider::new();
    let mut extras = std::collections::HashMap::new();
    extras.insert("room_id".to_string(), dm.room_id.clone());
    let config = ConnectionConfig::default().with_extras(extras);

    let mut conn = provider
        .connect(&dm.web_rid, config)
        .await
        .expect("danmaku connect failed");
    println!("  已连接: {} (id={})", conn.is_connected, conn.id);

    // Receive messages for ~15 seconds
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(15);
    let mut count = 0u32;
    while tokio::time::Instant::now() < deadline {
        match provider.receive(&conn).await {
            Ok(Some(item)) => {
                count += 1;
                match &item {
                    platforms_parser::danmaku::event::DanmakuItem::Message(msg) => {
                        println!(
                            "  [{}] {}: {}",
                            msg.message_type as u8, msg.username, msg.content
                        );
                    }
                    platforms_parser::danmaku::event::DanmakuItem::Control(evt) => {
                        println!("  [控制] {:?}", evt);
                    }
                }
                if count >= 20 {
                    break;
                }
            }
            Ok(None) => {
                // No message yet, small sleep
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
            Err(e) => {
                println!("  ⚠️ 接收错误: {}", e);
                break;
            }
        }
    }

    println!("  共收到 {} 条消息", count);
    provider
        .disconnect(&mut conn)
        .await
        .expect("disconnect failed");
    println!("  已断开连接");
}
