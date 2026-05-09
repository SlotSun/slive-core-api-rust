//! Integration test for Bilibili danmaku (chat) WebSocket connection.
//!
//! Connects to a live Bilibili room and prints chat messages for 30 seconds.
//!
//! Run with: `cargo test --test bilibili_danmaku -- --nocapture`

mod common;

use std::time::Duration;

use platforms_parser::danmaku::event::DanmuItem;
use platforms_parser::danmaku::message::DanmuType;
use platforms_parser::danmaku::provider::{ConnectionConfig, DanmuProvider};
use platforms_parser::extractor::LiveExtractor;
use platforms_parser::extractor::platforms::bilibili::BilibiliExtractor;
use platforms_parser::extractor::platforms::bilibili::danmaku::create_bilibili_danmu_provider;

/// Install the default rustls crypto provider once.
fn ensure_tls_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

#[tokio::test]
async fn test_bilibili_danmaku() {
    ensure_tls_provider();

    // Initialize tracing to see background task logs
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();

    let cookies = common::bilibili_cookies();
    let room_id = "23197314";
    let ext = BilibiliExtractor::new();
    ext.set_cookies(cookies);

    // Step 1: Get room detail to find the real room ID and check live status
    println!(">>> Step 1: 获取房间详情...");
    let detail = ext
        .get_room_detail(room_id)
        .await
        .expect("get_room_detail failed");

    println!("  房间ID: {}", detail.room_id);
    println!("  标题:   {}", detail.title);
    println!("  主播:   {}", detail.user_name);
    println!(
        "  状态:   {}",
        if detail.status {
            "直播中"
        } else {
            "未开播"
        }
    );

    if !detail.status {
        println!("\n  房间未开播，跳过弹幕测试。");
        return;
    }

    // Step 2: Connect to danmaku
    println!("\n>>> Step 2: 连接弹幕服务器...");
    let provider = create_bilibili_danmu_provider();

    let config = ConnectionConfig {
        cookies: Some(cookies.to_string()),
        websocket: None,
        extras: None,
    };

    let mut connection = provider
        .connect(&detail.room_id, config)
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
                    DanmuItem::Message(msg) => match msg.message_type {
                        DanmuType::Chat => {
                            chat_count += 1;
                            println!("  💬 [{}] {}", msg.username, msg.content);
                        }
                        DanmuType::Gift => {
                            gift_count += 1;
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
                // Timeout — continue waiting.
                if !warned && start.elapsed() > Duration::from_secs(5) {
                    warned = true;
                    println!("  ⚠️ 5秒内未收到消息，继续等待...");
                }
            }
            Err(e) => {
                println!("  ❌ 弹幕连接断开: {}", e);
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
    println!("  Bilibili 弹幕测试结果");
    println!("========================================");
    println!("  房间:     {}", detail.title);
    println!("  主播:     {}", detail.user_name);
    println!("  总消息数:   {}", msg_count);
    println!("  聊天消息:   {}", chat_count);
    println!("  礼物消息:   {}", gift_count);
    println!("  连接时长:   {:.1}秒", start.elapsed().as_secs_f64());
    println!("========================================");

    assert!(
        msg_count > 0,
        "should receive at least 1 message if room is live"
    );
}
