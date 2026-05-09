//! Integration tests for the Huya extractor.
//!
//! These tests make real HTTP requests to Huya's API.
//! Run with: `cargo test --test huya_integration -- --nocapture`

use platforms_parser::extractor::LiveExtractor;
use platforms_parser::extractor::platforms::huya::HuyaExtractor;
use platforms_parser::extractor::platforms::huya::models::{
    HuyaDanmakuArgs, HuyaLineType, HuyaUrlDataModel,
};

/// Install the default rustls crypto provider once.
/// Required because `reqwest` is built with `rustls-tls-webpki-roots-no-provider`.
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
    let ext = HuyaExtractor::new();

    assert!(ext.supports_url("https://www.huya.com/10188"));
    assert!(ext.supports_url("https://huya.com/10188"));
    assert!(ext.supports_url("http://www.huya.com/12345"));
    assert!(ext.supports_url("www.huya.com/abc"));
    assert!(!ext.supports_url("https://www.bilibili.com/12345"));
    assert!(!ext.supports_url("https://www.douyu.com/12345"));
}

#[test]
fn test_extract_room_id() {
    let ext = HuyaExtractor::new();

    assert_eq!(
        ext.extract_room_id("https://www.huya.com/10188"),
        Some("10188".to_string())
    );
    assert_eq!(
        ext.extract_room_id("https://huya.com/999"),
        Some("999".to_string())
    );
    assert_eq!(ext.extract_room_id("https://www.bilibili.com/123"), None);
}

#[test]
fn test_id_and_name() {
    let ext = HuyaExtractor::new();
    assert_eq!(ext.id(), "huya");
    assert_eq!(ext.name(), "虎牙");
}

// ============================================================================
// Category tests
// ============================================================================

#[tokio::test]
async fn test_get_categories() {
    ensure_tls_provider();
    let ext = HuyaExtractor::new();
    let categories = ext.get_categories().await.expect("get_categories failed");

    // Huya has 4 top-level categories
    assert!(!categories.is_empty(), "should have at least 1 category");
    println!("=== 虎牙分类 ===");
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
    let ext = HuyaExtractor::new();
    let room_id = "10188";

    let detail = ext
        .get_room_detail(room_id)
        .await
        .expect("get_room_detail failed");

    println!("\n=== 房间详情 (room_id={}) ===", room_id);
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

    // Basic assertions
    assert_eq!(detail.room_id, room_id);
    assert_eq!(detail.platform, "huya");
    // Note: user_name may be empty for official event rooms

    // data field should contain HuyaUrlDataModel
    assert!(detail.data.is_some(), "data field should be populated");

    // danmaku_data field should contain HuyaDanmakuArgs
    assert!(
        detail.danmaku_data.is_some(),
        "danmaku_data should be populated"
    );

    // Print stream info if live
    if detail.status {
        let url_data: Option<HuyaUrlDataModel> = detail
            .data
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok());
        if let Some(ud) = url_data {
            println!("  CDN 线路数: {}", ud.lines.len());
            for line in &ud.lines {
                println!(
                    "    - [{}] {} ({})",
                    line.cdn_type,
                    line.line,
                    match line.line_type {
                        HuyaLineType::Flv => "FLV",
                        HuyaLineType::Hls => "HLS",
                    }
                );
            }
            println!("  清晰度选项: {}", ud.bit_rates.len());
            for br in &ud.bit_rates {
                println!("    - {} (bitrate={})", br.name, br.bit_rate);
            }
        }
    }
}

// ============================================================================
// Play quality & URL tests
// ============================================================================

#[tokio::test]
async fn test_get_play_qualities() {
    ensure_tls_provider();
    let ext = HuyaExtractor::new();
    let detail = ext
        .get_room_detail("10188")
        .await
        .expect("get_room_detail failed");

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
    let ext = HuyaExtractor::new();
    let detail = ext
        .get_room_detail("10188")
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

    // Test with the first quality
    let quality = &qualities[0];
    println!("\n=== 播放链接 (清晰度: {}) ===", quality.quality);

    match ext.get_play_urls(&detail, quality).await {
        Ok(play_url) => {
            println!("  类型: {:?}", play_url.url_type);
            println!("  URL 数量: {}", play_url.urls.len());
            for (i, url) in play_url.urls.iter().enumerate() {
                // Only print first 120 chars to avoid leaking full tokens
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
            println!("  获取播放链接失败 (可能是 CDN Token API 不可用): {}", e);
            // Don't panic - this depends on external API availability
        }
    }
}

// ============================================================================
// Search tests
// ============================================================================

#[tokio::test]
async fn test_search_rooms() {
    ensure_tls_provider();
    let ext = HuyaExtractor::new();

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
    let ext = HuyaExtractor::new();

    let result = ext
        .search_anchors("英雄联盟", 1)
        .await
        .expect("search_anchors failed");

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
}

// ============================================================================
// Recommend rooms test
// ============================================================================

#[tokio::test]
async fn test_get_recommend_rooms() {
    ensure_tls_provider();
    let ext = HuyaExtractor::new();

    let result = ext
        .get_recommend_rooms(1)
        .await
        .expect("get_recommend_rooms failed");

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

    // Note: recommend API may return empty results depending on Huya's current state
    if result.items.is_empty() {
        println!("  ⚠️ 推荐房间 API 返回空结果");
    }
}

// ============================================================================
// Category rooms test
// ============================================================================

#[tokio::test]
async fn test_get_category_rooms() {
    ensure_tls_provider();
    let ext = HuyaExtractor::new();

    // First get categories
    let categories = ext.get_categories().await.expect("get_categories failed");
    assert!(!categories.is_empty());

    // Get rooms from the first sub-category that has sub-categories
    let sub = categories.iter().find_map(|c| c.sub_categories.first());

    let sub = match sub {
        Some(s) => s,
        None => {
            println!("  ⚠️ 没有子分类，跳过测试");
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

    // Note: API may return empty results depending on Huya's current state
    if result.items.is_empty() {
        println!("  ⚠️ 分类房间 API 返回空结果");
    }
}

// ============================================================================
// Anti-code test (no network)
// ============================================================================

#[test]
fn test_anti_code() {
    use platforms_parser::extractor::platforms::huya::anti_code::build_anti_code;

    // Test with a mock anti-code string
    let anti_code = "fm=UHBsd19IWXlh&wsTime=63a1b2c3&ctype=huya_pc_exe&t=100&fs=bgpd&uid=12345";
    let result = build_anti_code("test_stream_12345", 12345, anti_code);

    println!("\n=== Anti-code 测试 ===");
    println!("  输入: {}", anti_code);
    println!("  输出: {}", result);

    // Should contain wsSecret and wsTime
    assert!(result.contains("wsSecret="), "should contain wsSecret");
    assert!(result.contains("wsTime="), "should contain wsTime");
    assert!(result.contains("seqid="), "should contain seqid");
    assert!(result.contains("ctype="), "should contain ctype");
}

// ============================================================================
// Full flow test: detail → qualities → urls
// ============================================================================

#[tokio::test]
async fn test_full_flow() {
    ensure_tls_provider();
    let ext = HuyaExtractor::new();
    let room_id = "10188";

    println!("\n========================================");
    println!("  虎牙直播间完整测试流程");
    println!("  房间: https://www.huya.com/{}", room_id);
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
            "✅ 直播中"
        } else {
            "❌ 未开播"
        }
    );

    if !detail.status {
        println!("\n  房间未开播，完整流程测试结束。");
        return;
    }

    // Step 2: Get qualities
    println!("\n>>> Step 2: 获取清晰度列表...");
    let qualities = ext
        .get_play_qualities(&detail)
        .await
        .expect("get_play_qualities failed");

    for q in &qualities {
        println!("  - {}", q.quality);
    }

    // Step 3: Get play URLs for the first quality
    println!("\n>>> Step 3: 获取播放链接...");
    let quality = &qualities[0];
    match ext.get_play_urls(&detail, quality).await {
        Ok(play_url) => {
            println!("  类型: {:?}", play_url.url_type);
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
            println!("  ⚠️ 获取播放链接失败: {}", e);
            println!("  (CDN Token API 可能需要网络访问)");
        }
    }

    // Step 4: Check danmaku args
    println!("\n>>> Step 4: 检查弹幕参数...");
    if let Some(dm_data) = &detail.danmaku_data {
        let args: Option<HuyaDanmakuArgs> = serde_json::from_value(dm_data.clone()).ok();
        if let Some(args) = args {
            println!("  ayyuid:   {}", args.ayyuid);
            println!("  top_sid:  {}", args.top_sid);
            println!("  sub_sid:  {}", args.sub_sid);
        } else {
            println!("  ⚠️ 无法解析弹幕参数");
        }
    } else {
        println!("  ⚠️ 弹幕参数为空");
    }

    println!("\n========================================");
    println!("  ✅ 完整流程测试结束");
    println!("========================================");
}
