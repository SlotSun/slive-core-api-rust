//! Integration tests for Douyu (斗鱼) live extractor.
//!
//! These tests make real HTTP requests to Douyu's API.
//! Run with: `cargo test --test douyu_integration -- --nocapture`

use platforms_parser::extractor::LiveExtractor;
use platforms_parser::extractor::platforms::douyu::DouyuExtractor;

/// Install the default rustls crypto provider once.
fn ensure_tls_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

// ============================================================================
// URL matching (no network)
// ============================================================================

#[test]
fn test_supports_url() {
    let ext = DouyuExtractor::new();
    assert!(ext.supports_url("https://www.douyu.com/9999"));
    assert!(ext.supports_url("https://douyu.com/12345"));
    assert!(ext.supports_url("http://www.douyu.com/12306"));
    assert!(!ext.supports_url("https://www.huya.com/12345"));
    assert!(!ext.supports_url("https://www.bilibili.com/12345"));
}

#[test]
fn test_extract_room_id() {
    let ext = DouyuExtractor::new();
    assert_eq!(
        ext.extract_room_id("https://www.douyu.com/9999"),
        Some("9999".to_string())
    );
    assert_eq!(
        ext.extract_room_id("https://www.douyu.com/12306"),
        Some("12306".to_string())
    );
    assert_eq!(
        ext.extract_room_id("https://douyu.com/12345"),
        Some("12345".to_string())
    );
    assert_eq!(ext.extract_room_id("https://www.huya.com/123"), None);
}

#[test]
fn test_id_and_name() {
    let ext = DouyuExtractor::new();
    assert_eq!(ext.id(), "douyu");
    assert_eq!(ext.name(), "斗鱼");
}

// ============================================================================
// Category tests
// ============================================================================

#[tokio::test]
async fn test_get_categories() {
    ensure_tls_provider();
    let ext = DouyuExtractor::new();
    let categories = ext.get_categories().await.expect("get_categories failed");

    assert!(!categories.is_empty(), "should have at least 1 category");
    println!("=== 斗鱼分类 ===");
    for cat in &categories {
        println!(
            "  [{}] {} ({} 个子分类)",
            cat.id,
            cat.name,
            cat.sub_categories.len()
        );
        for sub in cat.sub_categories.iter().take(3) {
            println!("    - [{}] {}", sub.id, sub.name);
        }
        if cat.sub_categories.len() > 3 {
            println!("    ... 共 {} 个", cat.sub_categories.len());
        }
    }
}

// ============================================================================
// Room detail tests
// ============================================================================

#[tokio::test]
async fn test_get_room_detail() {
    ensure_tls_provider();
    let ext = DouyuExtractor::new();
    let room_id = "12306";

    let detail = ext
        .get_room_detail(room_id)
        .await
        .expect("get_room_detail failed");

    println!("\n=== 斗鱼房间详情 (room_id={}) ===", room_id);
    println!("  房间ID:   {}", detail.room_id);
    println!("  标题:     {}", detail.title);
    println!("  主播:     {}", detail.user_name);
    println!("  封面:     {}", detail.cover);
    println!("  在线人数: {}", detail.online);
    println!(
        "  直播状态: {}",
        if detail.status {
            "直播中"
        } else {
            "未开播"
        }
    );
    println!("  链接:     {}", detail.url);
    println!("  平台:     {}", detail.platform);

    assert_eq!(detail.room_id, room_id);
    assert_eq!(detail.platform, "douyu");
}

// ============================================================================
// Play quality & URL tests
// ============================================================================

#[tokio::test]
async fn test_get_play_qualities() {
    ensure_tls_provider();
    let ext = DouyuExtractor::new();
    let detail = ext
        .get_room_detail("12306")
        .await
        .expect("get_room_detail failed");

    if !detail.status {
        println!("\n=== 房间未开播，跳过清晰度测试 ===");
        return;
    }

    let qualities = ext
        .get_play_qualities(&detail)
        .await
        .expect("get_play_qualities failed");

    println!("\n=== 清晰度列表 ===");
    for q in &qualities {
        println!("  - {} (data={})", q.quality, q.data);
    }

    assert!(!qualities.is_empty(), "should have at least 1 quality");
}

#[tokio::test]
async fn test_get_play_urls() {
    ensure_tls_provider();
    let ext = DouyuExtractor::new();
    let detail = ext
        .get_room_detail("12306")
        .await
        .expect("get_room_detail failed");

    if !detail.status {
        println!("\n=== 房间未开播，跳过播放链接测试 ===");
        return;
    }

    let qualities = ext
        .get_play_qualities(&detail)
        .await
        .expect("get_play_qualities failed");

    let quality = &qualities[0];
    println!("\n=== 播放链接 (清晰度: {}) ===", quality.quality);

    match ext.get_play_urls(&detail, quality).await {
        Ok(play_url) => {
            println!("  URL 数量: {}", play_url.urls.len());
            for (i, url) in play_url.urls.iter().enumerate() {
                let display = if url.len() > 120 {
                    format!("{}...", &url[..120])
                } else {
                    url.clone()
                };
                println!("  [{}] {}", i, display);
            }
            assert!(!play_url.urls.is_empty(), "should have at least 1 play URL");
        }
        Err(e) => {
            println!("  获取播放链接失败: {}", e);
        }
    }
}

// ============================================================================
// Search tests
// ============================================================================

#[tokio::test]
async fn test_search_rooms() {
    ensure_tls_provider();
    let ext = DouyuExtractor::new();

    let result = ext
        .search_rooms("英雄联盟", 1)
        .await
        .expect("search_rooms failed");

    println!("\n=== 搜索直播间: '英雄联盟' ===");
    println!(
        "  结果数: {} (has_more={})",
        result.items.len(),
        result.has_more
    );
    for item in result.items.iter().take(5) {
        println!(
            "  - [{}] {} ({} 在线)",
            item.room_id, item.user_name, item.online
        );
        println!("    标题: {}", item.title);
    }

    assert!(
        !result.items.is_empty(),
        "search should return at least 1 result"
    );
}

#[tokio::test]
async fn test_search_anchors() {
    ensure_tls_provider();
    let ext = DouyuExtractor::new();

    match ext.search_anchors("英雄联盟", 1).await {
        Ok(result) => {
            println!("\n=== 搜索主播: '英雄联盟' ===");
            println!(
                "  结果数: {} (has_more={})",
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

            assert!(
                !result.items.is_empty(),
                "search should return at least 1 result"
            );
        }
        Err(e) => println!("  搜索主播失败: {}", e),
    }
}

// ============================================================================
// Recommend rooms test
// ============================================================================

#[tokio::test]
async fn test_get_recommend_rooms() {
    ensure_tls_provider();
    let ext = DouyuExtractor::new();

    match ext.get_recommend_rooms(1).await {
        Ok(result) => {
            println!("\n=== 推荐房间 ===");
            println!(
                "  结果数: {} (has_more={})",
                result.items.len(),
                result.has_more
            );
            for item in result.items.iter().take(5) {
                println!(
                    "  - [{}] {} ({} 在线)",
                    item.room_id, item.user_name, item.online
                );
                println!("    标题: {}", item.title);
            }

            if result.items.is_empty() {
                println!("  推荐房间 API 返回空结果");
            }
        }
        Err(e) => println!("  获取推荐房间失败: {}", e),
    }
}

// ============================================================================
// Category rooms test
// ============================================================================

#[tokio::test]
async fn test_get_category_rooms() {
    ensure_tls_provider();
    let ext = DouyuExtractor::new();

    // First get categories
    let categories = ext.get_categories().await.expect("get_categories failed");
    assert!(!categories.is_empty());

    // Get rooms from the first sub-category
    let sub = categories.iter().find_map(|c| c.sub_categories.first());

    let sub = match sub {
        Some(s) => s,
        None => {
            println!("  没有子分类，跳过测试");
            return;
        }
    };

    println!("\n=== 分类房间 (分类: {}) ===", sub.name);
    let result = ext
        .get_category_rooms(sub, 1)
        .await
        .expect("get_category_rooms failed");

    println!(
        "  结果数: {} (has_more={})",
        result.items.len(),
        result.has_more
    );
    for item in result.items.iter().take(5) {
        println!(
            "  - [{}] {} ({} 在线)",
            item.room_id, item.user_name, item.online
        );
        println!("    标题: {}", item.title);
    }

    if result.items.is_empty() {
        println!("  分类房间 API 返回空结果");
    }
}

// ============================================================================
// Full flow test: detail -> qualities -> urls -> danmaku_data
// ============================================================================

#[tokio::test]
async fn test_full_flow() {
    ensure_tls_provider();
    let ext = DouyuExtractor::new();
    let room_id = "12306";

    println!("\n========================================");
    println!("  斗鱼直播间完整测试流程");
    println!("  房间: https://www.douyu.com/{}", room_id);
    println!("========================================\n");

    // Step 1: Get room detail
    println!(">>> Step 1: 获取房间详情...");
    let detail = ext
        .get_room_detail(room_id)
        .await
        .expect("get_room_detail failed");

    println!("  房间: {}", detail.title);
    println!("  主播: {}", detail.user_name);
    println!("  在线: {}", detail.online);
    println!(
        "  状态: {}",
        if detail.status {
            "直播中"
        } else {
            "未开播"
        }
    );

    // Step 2: Get qualities (works even when offline, returns cached)
    println!("\n>>> Step 2: 获取清晰度列表...");
    match ext.get_play_qualities(&detail).await {
        Ok(qualities) => {
            for q in &qualities {
                println!("  - {} (data={})", q.quality, q.data);
            }

            if detail.status && !qualities.is_empty() {
                // Step 3: Get play URLs for the first quality
                println!("\n>>> Step 3: 获取播放链接...");
                let quality = &qualities[0];
                match ext.get_play_urls(&detail, quality).await {
                    Ok(play_url) => {
                        println!("  URL 数量: {}", play_url.urls.len());
                        for (i, url) in play_url.urls.iter().enumerate() {
                            let display = if url.len() > 100 {
                                format!("{}...", &url[..100])
                            } else {
                                url.clone()
                            };
                            println!("  [{}] {}", i, display);
                        }
                    }
                    Err(e) => {
                        println!("  获取播放链接失败: {}", e);
                    }
                }
            }
        }
        Err(e) => {
            println!("  获取清晰度失败: {}", e);
        }
    }

    // Step 4: Check danmaku data
    println!("\n>>> Step 4: 检查弹幕参数...");
    if let Some(dm_data) = &detail.danmaku_data {
        println!("  danmaku_data: {}", dm_data);
    } else {
        println!("  danmaku_data: 空");
    }

    println!("\n========================================");
    println!("  完整流程测试结束");
    println!("========================================");
}
