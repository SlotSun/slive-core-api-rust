//! Integration tests for Bilibili live extractor.
//! Run with: `cargo test --test bilibili_integration -- --nocapture`

mod common;

use platforms_parser::extractor::LiveExtractor;
use platforms_parser::extractor::platforms::bilibili::BilibiliExtractor;

fn ensure_tls_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[test]
fn test_supports_url() {
    let ext = BilibiliExtractor::new();
    assert!(ext.supports_url("https://live.bilibili.com/6"));
    assert!(ext.supports_url("https://www.bilibili.com/12345"));
    assert!(!ext.supports_url("https://www.huya.com/12345"));
}

#[test]
fn test_extract_room_id() {
    let ext = BilibiliExtractor::new();
    assert_eq!(
        ext.extract_room_id("https://live.bilibili.com/6"),
        Some("6".to_string())
    );
    assert_eq!(
        ext.extract_room_id("https://live.bilibili.com/21652717"),
        Some("21652717".to_string())
    );
}

#[tokio::test]
async fn test_get_categories() {
    ensure_tls_provider();
    let ext = BilibiliExtractor::new();
    let categories = ext.get_categories().await.expect("get_categories failed");
    println!("=== Bilibili 分类 ===");
    for cat in &categories {
        println!(
            "  [{}] {} ({} 子分类)",
            cat.id,
            cat.name,
            cat.sub_categories.len()
        );
    }
    assert!(!categories.is_empty());
}

#[tokio::test]
async fn test_get_sub_categories() {
    ensure_tls_provider();
    let ext = BilibiliExtractor::new();
    let categories = ext.get_categories().await.expect("get_categories failed");

    println!("=== Bilibili 子分类 ===");
    let mut total_subs = 0;
    let mut with_pic = 0;
    for cat in &categories {
        for sub in &cat.sub_categories {
            total_subs += 1;
            if sub.pic.is_some() {
                with_pic += 1;
            }
            if total_subs <= 10 {
                println!(
                    "  [{}] {} (parent={}, pic={})",
                    sub.id,
                    sub.name,
                    sub.parent_id.as_deref().unwrap_or("?"),
                    sub.pic.as_deref().unwrap_or("None"),
                );
            }
        }
    }
    println!("  总计: {} 子分类, {} 有图片", total_subs, with_pic);
    assert!(total_subs > 0, "should have sub-categories");

    // Pick the first real sub-category (skip id="0" "全部" entries) and fetch its rooms.
    let first_sub = categories
        .iter()
        .find_map(|c| {
            c.sub_categories
                .iter()
                .find(|s| s.id != "0")
        })
        .expect("should have at least one real sub-category");

    // getList 端点需要登录 cookie
    ext.set_cookies(common::bilibili_cookies());

    println!(
        "\n=== 获取子分类房间: {} (id={}) ===",
        first_sub.name, first_sub.id
    );
    match ext.get_category_rooms(first_sub, 1).await {
        Ok(result) => {
            println!("  房间数: {} (has_more={})", result.items.len(), result.has_more);
            for item in result.items.iter().take(5) {
                println!(
                    "  - [{}] {} ({} 在线)",
                    item.room_id, item.user_name, item.online
                );
            }
            assert!(!result.items.is_empty(), "sub-category should have rooms");
        }
        Err(e) => println!("  ⚠️ 获取子分类房间失败: {}", e),
    }
}

#[tokio::test]
async fn test_get_room_detail() {
    ensure_tls_provider();
    let ext = BilibiliExtractor::new();

    ext.set_cookies(common::bilibili_cookies());

    let room_id = "6";

    match ext.get_room_detail(room_id).await {
        Ok(detail) => {
            println!("\n=== Bilibili 房间详情 ===");
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
            println!("  平台:   {}", detail.platform);

            assert_eq!(detail.platform, "bilibili");
            assert!(!detail.room_id.is_empty());
            assert!(detail.data.is_some(), "data should be populated");

            if detail.status {
                let qualities = ext
                    .get_play_qualities(&detail)
                    .await
                    .expect("get_play_qualities failed");
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
    let ext = BilibiliExtractor::new();
    match ext.search_rooms("英雄联盟", 1).await {
        Ok(result) => {
            println!("=== 搜索直播间 ===");
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
            assert!(!result.items.is_empty());
        }
        Err(e) => println!("  ⚠️ 搜索失败: {}", e),
    }
}

#[tokio::test]
async fn test_search_anchors() {
    ensure_tls_provider();
    let ext = BilibiliExtractor::new();
    ext.set_cookies(common::bilibili_cookies());
    match ext.search_anchors("阳光男孩小丑熊", 1).await {
        Ok(result) => {
            println!("=== 搜索主播 ===");
            println!(
                "  结果: {} (has_more={})",
                result.items.len(),
                result.has_more
            );
            for item in result.items.iter().take(5) {
                println!(
                    "  - {} ({})",
                    item.user_name,
                    if item.is_live {
                        "直播中"
                    } else {
                        "未开播"
                    }
                );
            }
        }
        Err(e) => println!("  ⚠️ 搜索主播失败: {}", e),
    }
}
