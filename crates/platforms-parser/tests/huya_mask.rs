//! Integration test for MaskedDanmakuProvider with Huya.
//!
//! Connects to a Huya room with mask enabled, verifies filtering works.
//!
//! Run with: `cargo test --test huya_mask -- --nocapture`

use std::time::Duration;

use platforms_parser::danmaku::event::DanmakuItem;
use platforms_parser::danmaku::provider::{ConnectionConfig, DanmakuProvider};
use platforms_parser::danmaku_mask::mask_config::{FrequencyConfig, MaskConfig};
use platforms_parser::danmaku_mask::mask_provider::MaskedDanmakuProvider;
use platforms_parser::extractor::LiveExtractor;
use platforms_parser::extractor::platforms::huya::danmaku::create_huya_danmu_provider;
use platforms_parser::extractor::platforms::huya::HuyaExtractor;
use platforms_parser::extractor::platforms::huya::models::HuyaDanmakuArgs;

fn ensure_tls_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[tokio::test]
async fn test_huya_mask_basic() {
    ensure_tls_provider();

    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();

    let room_id = "546540";
    let ext = HuyaExtractor::new();

    // Step 1: Get room detail
    println!(">>> Step 1: 获取房间详情...");
    let detail = ext
        .get_room_detail(room_id)
        .await
        .expect("get_room_detail failed");

    println!("  房间: {}", detail.title);
    println!("  主播: {}", detail.user_name);

    // Step 2: Parse danmaku args
    let args: HuyaDanmakuArgs = detail
        .danmaku_data
        .as_ref()
        .and_then(|d| serde_json::from_value(d.clone()).ok())
        .expect("failed to parse danmaku_data");

    let mut extras = std::collections::HashMap::new();
    extras.insert("ayyuid".to_string(), args.ayyuid.to_string());
    extras.insert("top_sid".to_string(), args.top_sid.to_string());
    extras.insert("sub_sid".to_string(), args.sub_sid.to_string());

    // Step 3: Create masked provider with frequency mask
    println!(">>> Step 2: 创建 MaskedDanmakuProvider...");
    let inner = create_huya_danmu_provider();
    let provider = MaskedDanmakuProvider::new(inner);

    // Step 4: Connect with mask config
    let mask_config = MaskConfig {
        frequency: Some(FrequencyConfig {
            base_window_ms: 5000,
            bucket_count: 5,
            use_normalization: true,
            max_frequency: 2, // 同一条弹幕 5s 内最多出现 2 次
        }),
        blacklist_words: None,
    };

    let config = ConnectionConfig {
        cookies: None,
        websocket: None,
        extras: Some(extras),
        mask_config: Some(mask_config),
    };

    println!(">>> Step 3: 连接弹幕服务器 (带 mask)...");
    let mut connection = provider
        .connect(room_id, config)
        .await
        .expect("connect failed");

    println!("  连接成功! id={}", connection.id);

    // Step 5: Receive messages for 30 seconds, count blocked vs passed
    println!(">>> Step 4: 接收弹幕 30 秒...");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut msg_count = 0u32;
    let mut ctrl_count = 0u32;
    let mut timeout_count = 0u32;

    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }

        match provider.receive(&connection).await {
            Ok(Some(item)) => match item {
                DanmakuItem::Message(msg) => {
                    msg_count += 1;
                    println!("  [MSG {}:{}] {}: {}", msg_count, msg_count, msg.username, msg.content);
                }
                DanmakuItem::Control(ctrl) => {
                    ctrl_count += 1;
                    println!("  [CTRL] {:?}", ctrl);
                }
            },
            Ok(None) => {
                timeout_count += 1;
                if timeout_count % 30 == 0 {
                    println!("  ... {} timeouts", timeout_count);
                }
            }
            Err(e) => {
                println!("  [ERR] {}", e);
                break;
            }
        }
    }

    println!(">>> 结果:");
    println!("  消息: {}", msg_count);
    println!("  控制事件: {}", ctrl_count);
    println!("  超时: {}", timeout_count);

    // Step 6: Disconnect
    provider
        .disconnect(&mut connection)
        .await
        .expect("disconnect failed");

    println!(">>> 断开连接完成");
}

#[tokio::test]
async fn test_huya_mask_blacklist() {
    ensure_tls_provider();

    let room_id = "546540";
    let ext = HuyaExtractor::new();

    let detail = ext
        .get_room_detail(room_id)
        .await
        .expect("get_room_detail failed");

    let args: HuyaDanmakuArgs = detail
        .danmaku_data
        .as_ref()
        .and_then(|d| serde_json::from_value(d.clone()).ok())
        .expect("failed to parse danmaku_data");

    let mut extras = std::collections::HashMap::new();
    extras.insert("ayyuid".to_string(), args.ayyuid.to_string());
    extras.insert("top_sid".to_string(), args.top_sid.to_string());
    extras.insert("sub_sid".to_string(), args.sub_sid.to_string());

    let inner = create_huya_danmu_provider();
    let provider = MaskedDanmakuProvider::new(inner);

    // 带屏蔽词的 mask
    let mask_config = MaskConfig {
        frequency: None,
        blacklist_words: Some(vec![
            "广告".to_string(),
            "代练".to_string(),
            "加微信".to_string(),
        ]),
    };

    let config = ConnectionConfig {
        cookies: None,
        websocket: None,
        extras: Some(extras),
        mask_config: Some(mask_config),
    };

    let mut connection = provider
        .connect(room_id, config)
        .await
        .expect("connect failed");

    println!(">>> 屏蔽词测试: 接收弹幕 10 秒...");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut passed = 0u32;

    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }

        match provider.receive(&connection).await {
            Ok(Some(DanmakuItem::Message(msg))) => {
                passed += 1;
                if passed <= 5 {
                    println!("  [PASS] {}: {}", msg.username, msg.content);
                }
            }
            Ok(Some(_)) => {}
            Ok(None) => {}
            Err(_) => break,
        }
    }

    println!(">>> 通过的消息数: {}", passed);

    provider
        .disconnect(&mut connection)
        .await
        .expect("disconnect failed");
}
