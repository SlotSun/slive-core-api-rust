//! Integration test for Douyu (斗鱼) danmaku (chat) WebSocket connection.
//!
//! Connects to room 12306 and prints live chat messages for 30 seconds.
//!
//! Run with: `cargo test --test douyu_danmaku -- --nocapture`

use std::time::Duration;

use platforms_parser::danmaku::event::DanmakuItem;
use platforms_parser::danmaku::message::DanmakuType;
use platforms_parser::danmaku::provider::{ConnectionConfig, DanmakuProvider};
use platforms_parser::extractor::platforms::douyu::DouyuExtractor;
use platforms_parser::extractor::LiveExtractor;
use platforms_parser::extractor::platforms::douyu::danmaku::create_douyu_danmu_provider;

/// Install the default rustls crypto provider once.
fn ensure_tls_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[tokio::test]
async fn test_douyu_danmaku() {
    ensure_tls_provider();

    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();

    let room_id = "3168536";
    let ext = DouyuExtractor::new();

    // Step 1: Get room detail
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

    // Step 2: Connect to danmaku
    println!("\n>>> Step 2: 连接弹幕服务器...");
    let provider = create_douyu_danmu_provider();

    let config = ConnectionConfig::default();

    let mut connection = provider
        .connect(room_id, config)
        .await
        .expect("failed to connect to danmaku");

    println!("  连接成功! ID: {}", connection.id);

    // Step 3: Receive messages for 30 seconds
    println!("\n>>> Step 3: 接收弹幕 (30 秒)...\n");

    let timeout = Duration::from_secs(30);
    let start = std::time::Instant::now();
    let mut msg_count = 0u32;
    let mut chat_count = 0u32;
    let mut gift_count = 0u32;
    let mut warned = false;

    while start.elapsed() < timeout {
        match provider.receive(&connection).await {
            Ok(Some(item)) => {
                msg_count += 1;
                match item {
                    DanmakuItem::Message(msg) => match msg.message_type {
                        DanmakuType::Chat => {
                            chat_count += 1;
                            println!("  [聊天] [{}] {}", msg.username, msg.content);
                        }
                        DanmakuType::Gift => {
                            gift_count += 1;
                            println!("  [礼物] [{}] {}", msg.username, msg.content);
                        }
                        DanmakuType::SuperChat => {
                            println!("  [醒目留言] [{}] {}", msg.username, msg.content);
                        }
                        _ => {
                            println!("  [其他] [{}] {}", msg.username, msg.content);
                        }
                    },
                    DanmakuItem::Control(event) => {
                        println!("  [控制] {:?}", event);
                    }
                }
            }
            Ok(None) => {
                if !warned && start.elapsed() > Duration::from_secs(5) {
                    warned = true;
                    println!("  5秒内未收到消息，继续等待...");
                }
            }
            Err(e) => {
                println!("  弹幕连接断开: {}", e);
                break;
            }
        }
    }

    // Step 4: Disconnect
    println!("\n>>> Step 4: 断开连接...");
    provider
        .disconnect(&mut connection)
        .await
        .expect("disconnect failed");

    println!("\n========================================");
    println!("  弹幕测试结果");
    println!("========================================");
    println!("  总消息数:   {}", msg_count);
    println!("  聊天消息:   {}", chat_count);
    println!("  礼物消息:   {}", gift_count);
    println!("  连接时长:   {:.1}秒", start.elapsed().as_secs_f64());
    println!("========================================");

    // At minimum, we should have connected successfully
    assert!(
        msg_count > 0 || !detail.status,
        "should receive at least 1 message if room is live"
    );
}
