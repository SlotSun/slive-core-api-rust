//! Integration test for MaskedDanmakuProvider with Douyin.
//!
//! Run with: `cargo test --test douyin_mask -- --nocapture`

use platforms_parser::danmaku::event::DanmakuItem;
use platforms_parser::danmaku::provider::{ConnectionConfig, DanmakuProvider};
use platforms_parser::danmaku_mask::mask_config::{FrequencyConfig, MaskConfig};
use platforms_parser::danmaku_mask::mask_provider::MaskedDanmakuProvider;
use platforms_parser::extractor::LiveExtractor;
use platforms_parser::extractor::platforms::douyin::DouyinExtractor;
use platforms_parser::extractor::platforms::douyin::danmaku::DouyinDanmakuProvider;
use platforms_parser::extractor::platforms::douyin::models::DouyinDanmakuData;

fn ensure_tls_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[tokio::test]
async fn test_douyin_mask() {
    ensure_tls_provider();

    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();

    let web_rid = "216792992689";
    let ext = DouyinExtractor::new();

    // Step 1: Get room detail
    println!(">>> Step 1: 获取房间详情...");
    let detail = ext
        .get_room_detail(web_rid)
        .await
        .expect("get_room_detail failed");

    println!("  房间: {}", detail.title);
    println!("  主播: {}", detail.user_name);
    println!("  状态: {}", if detail.status { "直播中" } else { "未开播" });

    // Step 2: Parse danmaku_data
    let danmaku_data = detail
        .danmaku_data
        .as_ref()
        .expect("no danmaku_data");

    let dm: DouyinDanmakuData =
        serde_json::from_value(danmaku_data.clone()).expect("parse danmaku_data");
    println!("  room_id: {}", dm.room_id);
    println!("  web_rid: {}", dm.web_rid);

    // Step 3: Create masked provider
    println!(">>> Step 2: 创建 MaskedDanmakuProvider...");
    let inner = DouyinDanmakuProvider::new();
    let provider = MaskedDanmakuProvider::new(inner);

    // Step 4: Connect with mask config
    let mask_config = MaskConfig {
        frequency: Some(FrequencyConfig {
            base_window_ms: 5000,
            bucket_count: 5,
            use_normalization: true,
            max_frequency: 2,
        }),
        blacklist_words: Some(vec![
            "广告".to_string(),
            "代练".to_string(),
        ]),
    };

    let mut extras = std::collections::HashMap::new();
    extras.insert("room_id".to_string(), dm.room_id.clone());

    let config = ConnectionConfig {
        cookies: None,
        websocket: None,
        extras: Some(extras),
        mask_config: Some(mask_config),
    };

    println!(">>> Step 3: 连接弹幕服务器 (带 mask)...");
    let mut connection = provider
        .connect(&dm.web_rid, config)
        .await
        .expect("connect failed");

    println!("  连接成功! id={}", connection.id);

    // Step 5: Receive messages for 30 seconds
    println!(">>> Step 4: 接收弹幕 30 秒...");
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);
    let mut msg_count = 0u32;
    let mut timeout_count = 0u32;

    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }

        match provider.receive(&connection).await {
            Ok(Some(DanmakuItem::Message(msg))) => {
                msg_count += 1;
                println!("  [MSG {}:{}] {}: {}", msg_count, msg_count, msg.username, msg.content);
            }
            Ok(Some(_)) => {}
            Ok(None) => {
                timeout_count += 1;
            }
            Err(e) => {
                println!("  [ERR] {}", e);
                break;
            }
        }
    }

    println!(">>> 结果:");
    println!("  消息: {}", msg_count);
    println!("  超时: {}", timeout_count);

    // Step 6: Disconnect
    provider
        .disconnect(&mut connection)
        .await
        .expect("disconnect failed");

    println!(">>> 断开连接完成");
}
