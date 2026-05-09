//! Integration test for Huya danmaku (chat) WebSocket connection.
//!
//! Connects to room chuhe and prints live chat messages for 30 seconds.
//!
//! Run with: `cargo test --test huya_danmaku -- --nocapture`

use std::collections::HashMap;
use std::time::Duration;

use platforms_parser::danmaku::event::DanmuItem;
use platforms_parser::danmaku::message::DanmuType;
use platforms_parser::danmaku::provider::{ConnectionConfig, DanmuProvider};
use platforms_parser::extractor::LiveExtractor;
use platforms_parser::extractor::platforms::huya::HuyaExtractor;
use platforms_parser::extractor::platforms::huya::danmaku::create_huya_danmu_provider;
use platforms_parser::extractor::platforms::huya::models::HuyaDanmakuArgs;

/// Install the default rustls crypto provider once.
fn ensure_tls_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[tokio::test]
async fn test_huya_danmaku() {
    ensure_tls_provider();

    // Initialize tracing to see background task error logs
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();

    let room_id = "chuhe";
    let ext = HuyaExtractor::new();

    // Step 1: Get room detail to extract danmaku args
    println!(">>> Step 1: 获取房间详情...");
    let detail = ext
        .get_room_detail(room_id)
        .await
        .expect("get_room_detail failed");

    println!("  房间: {}", detail.title);
    println!("  主播: {}", detail.user_name);
    println!(
        "  状态: {}",
        if detail.status {
            "直播中"
        } else {
            "未开播"
        }
    );

    // Step 2: Extract danmaku args
    println!("\n>>> Step 2: 提取弹幕参数...");
    let dm_data = detail
        .danmaku_data
        .as_ref()
        .expect("danmaku_data should be present");

    let args: HuyaDanmakuArgs =
        serde_json::from_value(dm_data.clone()).expect("failed to parse HuyaDanmakuArgs");

    println!("  ayyuid:  {}", args.ayyuid);
    println!("  top_sid: {}", args.top_sid);
    println!("  sub_sid: {}", args.sub_sid);

    assert!(args.ayyuid > 0, "ayyuid should be non-zero for a live room");
    assert!(
        args.top_sid > 0,
        "top_sid should be non-zero for a live room"
    );

    // Step 3: Connect to danmaku
    println!("\n>>> Step 3: 连接弹幕服务器...");
    let provider = create_huya_danmu_provider();

    let mut extras = HashMap::new();
    extras.insert("ayyuid".to_string(), args.ayyuid.to_string());
    extras.insert("top_sid".to_string(), args.top_sid.to_string());
    extras.insert("sub_sid".to_string(), args.sub_sid.to_string());

    let config = ConnectionConfig {
        cookies: None,
        websocket: None,
        extras: Some(extras),
    };

    let mut connection = provider
        .connect(room_id, config)
        .await
        .expect("failed to connect to danmaku");

    println!("  连接成功! ID: {}", connection.id);

    // Step 4: Receive messages for 30 seconds
    println!("\n>>> Step 4: 接收弹幕 (30 秒)...\n");

    let timeout = Duration::from_secs(30);
    let start = std::time::Instant::now();
    let mut msg_count = 0u32;
    let mut chat_count = 0u32;
    let mut online_count = 0u32;
    let mut warned = false;

    while start.elapsed() < timeout {
        match provider.receive(&connection).await {
            Ok(Some(item)) => {
                msg_count += 1;
                match item {
                    DanmuItem::Message(msg) => match msg.message_type {
                        DanmuType::Chat => {
                            chat_count += 1;
                            println!("  💬 [{}] {}", msg.username, msg.content);
                        }
                        DanmuType::Gift => {
                            println!("  🎁 [{}] {}", msg.username, msg.content);
                        }
                        DanmuType::SuperChat => {
                            println!("  💰 [{}] {}", msg.username, msg.content);
                        }
                        _ => {
                            println!("  📢 [{}] {}", msg.username, msg.content);
                        }
                    },
                    DanmuItem::Control(event) => {
                        println!("  ⚙️ 控制事件: {:?}", event);
                    }
                }
            }
            Ok(None) => {
                // Timeout — no message available yet. Continue waiting.
                if !warned && start.elapsed() > Duration::from_secs(5) {
                    warned = true;
                    println!("  ⚠️ 5秒内未收到消息，继续等待...");
                }
            }
            Err(e) => {
                // Channel closed — background task exited.
                println!("  ❌ 弹幕连接断开: {}", e);
                break;
            }
        }
    }

    // Step 5: Disconnect
    println!("\n>>> Step 5: 断开连接...");
    provider
        .disconnect(&mut connection)
        .await
        .expect("disconnect failed");

    println!("\n========================================");
    println!("  弹幕测试结果");
    println!("========================================");
    println!("  总消息数:   {}", msg_count);
    println!("  聊天消息:   {}", chat_count);
    println!("  在线人数:   {}", online_count);
    println!("  连接时长:   {:.1}秒", start.elapsed().as_secs_f64());
    println!("========================================");

    // At minimum, we should have connected successfully
    // (messages depend on room activity)
    assert!(
        msg_count > 0 || !detail.status,
        "should receive at least 1 message if room is live"
    );
}
