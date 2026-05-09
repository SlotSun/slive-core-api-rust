//! Twitch danmaku (chat) provider.
//!
//! Connects to Twitch chat via IRC (WebSocket) protocol at
//! `wss://irc-ws.chat.twitch.tv:443`.
//!
//! Anonymous login uses `justinfan*` which allows read-only access.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use crate::danmaku::error::{DanmakuError, Result};
use crate::danmaku::event::{DanmuControlEvent, DanmuItem};
use crate::danmaku::message::{DanmuMessage, DanmuType};
use crate::danmaku::provider::{ConnectionConfig, DanmuConnection, DanmuProvider};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const IRC_WS_URL: &str = "wss://irc-ws.chat.twitch.tv:443";

/// Interval between PING heartbeats (Twitch requires a PING every ~5 minutes).
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);

/// Maximum time `receive` will wait for a message before returning `Ok(None)`.
const RECEIVE_TIMEOUT: Duration = Duration::from_millis(100);

/// Maximum time to wait for the initial WebSocket handshake.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Channel buffer size for decoded danmu items.
const MESSAGE_CHANNEL_SIZE: usize = 256;

// ---------------------------------------------------------------------------
// IRC tag parsing
// ---------------------------------------------------------------------------

/// Parse IRC tags string into a key-value map.
///
/// IRC tags format: `@key1=value1;key2=value2;key3`
/// Values can be empty, and some characters are escaped:
///   `\:` → `;`, `\s` → ` `, `\\` → `\`, `\r` → `\r`, `\n` → `\n`
fn parse_irc_tags(tags_str: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for part in tags_str.trim_start_matches('@').split(';') {
        if let Some((key, value)) = part.split_once('=') {
            map.insert(key.to_string(), unescape_irc_tag(value));
        } else if !part.is_empty() {
            map.insert(part.to_string(), String::new());
        }
    }
    map
}

/// Unescape IRC tag values.
fn unescape_irc_tag(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some(':') => result.push(';'),
                Some('s') => result.push(' '),
                Some('\\') => result.push('\\'),
                Some('r') => result.push('\r'),
                Some('n') => result.push('\n'),
                Some(other) => {
                    result.push('\\');
                    result.push(other);
                }
                None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }
    result
}

/// Parse a single IRC message line into components.
///
/// Format: `@tags :source COMMAND #channel :message`
/// Returns: (tags, source, command, channel, trailing)
fn parse_irc_message(line: &str) -> (HashMap<String, String>, &str, &str, &str, &str) {
    let line = line.trim();
    let mut remaining = line;

    // 1. Parse tags (if present)
    let tags = if remaining.starts_with('@') {
        let end = remaining.find(' ').unwrap_or(remaining.len());
        let tags_str = &remaining[..end];
        remaining = remaining[end..].trim_start();
        parse_irc_tags(tags_str)
    } else {
        HashMap::new()
    };

    // 2. Parse source (if present, starts with ':')
    let source = if remaining.starts_with(':') {
        let end = remaining.find(' ').unwrap_or(remaining.len());
        let src = &remaining[1..end];
        remaining = remaining[end..].trim_start();
        src
    } else {
        ""
    };

    // 3. Parse command
    let command_end = remaining.find(' ').unwrap_or(remaining.len());
    let command = &remaining[..command_end];
    remaining = remaining[command_end..].trim_start();

    // 4. Parse trailing params
    let (channel, trailing) = if remaining.starts_with(':') {
        ("", &remaining[1..])
    } else {
        // First param is channel (or numeric), trailing starts after ' :'
        let colon_pos = remaining.find(" :");
        if let Some(pos) = colon_pos {
            let params = remaining[..pos].trim();
            let trail = &remaining[pos + 2..];
            (params, trail)
        } else {
            (remaining.trim(), "")
        }
    };

    (tags, source, command, channel, trailing)
}

/// Extract the username from the IRC source field.
/// Source format: `user!user@user.tmi.twitch.tv`
fn extract_username_from_source(source: &str) -> &str {
    source.find('!').map(|i| &source[..i]).unwrap_or(source)
}

// ---------------------------------------------------------------------------
// Connection state
// ---------------------------------------------------------------------------

/// Per-connection bookkeeping kept inside [`TwitchDanmuProvider`].
struct TwitchConnectionState {
    /// Receiver end of the decoded-message channel.
    message_rx: Arc<Mutex<mpsc::Receiver<DanmuItem>>>,
    /// Sender used to signal the background task to shut down.
    shutdown_tx: Option<mpsc::Sender<()>>,
    /// Background task handles.
    tasks: Vec<JoinHandle<()>>,
}

impl TwitchConnectionState {
    fn abort_tasks(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }
}

impl Drop for TwitchConnectionState {
    fn drop(&mut self) {
        self.abort_tasks();
    }
}

// ---------------------------------------------------------------------------
// Background IRC WebSocket task
// ---------------------------------------------------------------------------

/// Long-running task that:
/// 1. Connects to the Twitch IRC WebSocket.
/// 2. Sends CAP REQ, PASS, NICK, JOIN messages.
/// 3. Sends periodic PING heartbeats.
/// 4. Parses incoming IRC messages and forwards [`DanmuItem`]s through `message_tx`.
async fn run_twitch_irc_task(
    room_id: String,
    channel: String,
    message_tx: mpsc::Sender<DanmuItem>,
    mut shutdown_rx: mpsc::Receiver<()>,
) {
    // --- Connect ----------------------------------------------------------
    let ws_stream = match tokio::time::timeout(CONNECT_TIMEOUT, connect_async(IRC_WS_URL)).await {
        Ok(Ok((stream, _response))) => stream,
        Ok(Err(e)) => {
            error!("Failed to connect to Twitch IRC WebSocket: {e}");
            return;
        }
        Err(_) => {
            error!("Timeout connecting to Twitch IRC WebSocket");
            return;
        }
    };

    let (mut ws_sink, mut ws_source) = ws_stream.split();
    info!(room_id = %room_id, channel = %channel, "Connected to Twitch IRC WebSocket");

    // --- IRC handshake ----------------------------------------------------
    // CAP REQ: request membership, tags, and commands capabilities
    let cap_req = "CAP REQ :twitch.tv/membership twitch.tv/tags twitch.tv/commands";
    if let Err(e) = ws_sink.send(Message::Text(cap_req.into())).await {
        error!("Failed to send CAP REQ: {e}");
        return;
    }

    // PASS: anonymous login
    if let Err(e) = ws_sink.send(Message::Text("PASS SCHMOOPIIE".into())).await {
        error!("Failed to send PASS: {e}");
        return;
    }

    // NICK: anonymous username
    let nick = format!("justinfan{}", rand::random::<u32>() % 100000);
    if let Err(e) = ws_sink
        .send(Message::Text(format!("NICK {}", nick).into()))
        .await
    {
        error!("Failed to send NICK: {e}");
        return;
    }

    // JOIN channel
    if let Err(e) = ws_sink
        .send(Message::Text(format!("#{}", channel.to_lowercase()).into()))
        .await
    {
        error!("Failed to send JOIN: {e}");
        return;
    }

    debug!(room_id = %room_id, nick = %nick, "Sent IRC handshake");

    // --- Main loop --------------------------------------------------------
    let mut heartbeat_timer = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate first tick.
    heartbeat_timer.tick().await;

    loop {
        tokio::select! {
            // PING keepalive
            _ = heartbeat_timer.tick() => {
                let ping = "PING :tmi.twitch.tv";
                if let Err(e) = ws_sink.send(Message::Text(ping.into())).await {
                    error!("Failed to send PING: {e}");
                    break;
                }
                trace!(room_id = %room_id, "Sent PING");
            }

            // Incoming WebSocket frame
            msg_opt = ws_source.next() => {
                match msg_opt {
                    Some(Ok(Message::Text(text))) => {
                        let text_str: &str = &text;
                        for line in text_str.lines() {
                            let line = line.trim();
                            if line.is_empty() {
                                continue;
                            }
                            match process_irc_line(line, &channel, &mut ws_sink).await {
                                Ok(items) => {
                                    for item in items {
                                        if message_tx.send(item).await.is_err() {
                                            debug!(room_id = %room_id, "Message channel closed");
                                            return;
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(room_id = %room_id, error = %e, "Failed to process IRC line");
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!(room_id = %room_id, "IRC WebSocket closed by server");
                        let _ = message_tx
                            .send(DanmuItem::Control(DanmuControlEvent::StreamClosed {
                                message: Some("IRC WebSocket closed by server".to_string()),
                                action: None,
                            }))
                            .await;
                        break;
                    }
                    Some(Err(e)) => {
                        error!(room_id = %room_id, "IRC WebSocket error: {e}");
                        break;
                    }
                    None => {
                        info!(room_id = %room_id, "IRC WebSocket stream ended");
                        break;
                    }
                    _ => {
                        // Ignore binary / ping / pong frames
                    }
                }
            }

            // Shutdown signal from `disconnect()`
            _ = shutdown_rx.recv() => {
                debug!(room_id = %room_id, "Shutdown signal received");
                // Send PART before closing
                let _ = ws_sink
                    .send(Message::Text(format!("PART #{}", channel.to_lowercase()).into()))
                    .await;
                let _ = ws_sink.close().await;
                return;
            }
        }
    }

    // Connection lost – close the socket.
    let _ = ws_sink.close().await;
}

/// Process a single IRC line and return zero or more [`DanmuItem`]s.
///
/// Also handles PING→PONG keepalive by sending PONG through `ws_sink`.
async fn process_irc_line(
    line: &str,
    _channel: &str,
    ws_sink: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
) -> std::result::Result<Vec<DanmuItem>, String> {
    let (tags, source, command, irc_channel, trailing) = parse_irc_message(line);

    match command {
        // ---- PING → PONG -------------------------------------------------
        "PING" => {
            let pong = if trailing.is_empty() {
                "PONG :tmi.twitch.tv".to_string()
            } else {
                format!("PONG :{}", trailing)
            };
            if let Err(e) = ws_sink.send(Message::Text(pong.into())).await {
                return Err(format!("Failed to send PONG: {e}"));
            }
            trace!("Responded to PING with PONG");
            Ok(vec![])
        }

        // ---- PONG (ignore) -----------------------------------------------
        "PONG" => Ok(vec![]),

        // ---- 001 (welcome) -----------------------------------------------
        "001" => {
            debug!("Received IRC welcome (001)");
            Ok(vec![])
        }

        // ---- 353 (names list) – ignore ------------------------------------
        "353" | "366" | "372" | "375" | "376" => Ok(vec![]),

        // ---- CAP (capability acknowledgement) – ignore --------------------
        "CAP" => Ok(vec![]),

        // ---- RECONNECT ----------------------------------------------------
        "RECONNECT" => {
            warn!("Twitch IRC server requested RECONNECT");
            Ok(vec![DanmuItem::Control(DanmuControlEvent::Other {
                kind: "reconnect".to_string(),
                message: Some("Server requested reconnect".to_string()),
                metadata: None,
            })])
        }

        // ---- ROOMSTATE ----------------------------------------------------
        "ROOMSTATE" => {
            debug!(channel = %irc_channel, tags = ?tags, "ROOMSTATE");
            Ok(vec![])
        }

        // ---- NOTICE -------------------------------------------------------
        "NOTICE" => {
            let msg_id = tags.get("msg-id").map(|s| s.as_str()).unwrap_or("");
            debug!(channel = %irc_channel, msg_id = %msg_id, message = %trailing, "NOTICE");
            Ok(vec![])
        }

        // ---- USERSTATE ----------------------------------------------------
        "USERSTATE" => {
            trace!(channel = %irc_channel, "USERSTATE");
            Ok(vec![])
        }

        // ---- CLEARCHAT (timeout / ban) ------------------------------------
        "CLEARCHAT" => {
            let target_user = trailing;
            if target_user.is_empty() {
                // Entire chat was cleared
                Ok(vec![DanmuItem::Control(DanmuControlEvent::Other {
                    kind: "clearchat".to_string(),
                    message: Some("Chat was cleared by a moderator".to_string()),
                    metadata: None,
                })])
            } else {
                // A specific user was timed out or banned
                let duration = tags.get("ban-duration").cloned();
                let ban_reason = format!(
                    "{}{}",
                    if duration.is_some() {
                        "timed out"
                    } else {
                        "banned"
                    },
                    duration
                        .as_ref()
                        .map(|d| format!(" for {}s", d))
                        .unwrap_or_default()
                );

                Ok(vec![DanmuItem::Control(DanmuControlEvent::Other {
                    kind: "clearchat_user".to_string(),
                    message: Some(format!("{}: {}", target_user, ban_reason)),
                    metadata: {
                        let mut m = HashMap::new();
                        m.insert("target_user".to_string(), serde_json::json!(target_user));
                        if let Some(d) = duration {
                            m.insert("duration".to_string(), serde_json::json!(d));
                        }
                        Some(m)
                    },
                })])
            }
        }

        // ---- CLEARMSG (single message deleted) ----------------------------
        "CLEARMSG" => {
            let target_msg_id = tags.get("target-msg-id").cloned();
            let login = tags.get("login").cloned();
            debug!(
                target_msg_id = ?target_msg_id,
                login = ?login,
                message = %trailing,
                "CLEARMSG"
            );
            Ok(vec![])
        }

        // ---- USERNOTICE (sub, gift, hype chat, etc.) ----------------------
        "USERNOTICE" => {
            let msg_id = tags.get("msg-id").map(|s| s.as_str()).unwrap_or("");
            let display_name = tags
                .get("display-name")
                .cloned()
                .unwrap_or_else(|| source.to_string());

            match msg_id {
                "sub" | "resub" => {
                    let months = tags
                        .get("msg-param-cumulative-months")
                        .cloned()
                        .unwrap_or_default();
                    let sub_plan = tags
                        .get("msg-param-sub-plan-name")
                        .cloned()
                        .unwrap_or_default();
                    let content = if trailing.is_empty() {
                        format!("subscribed ({})", sub_plan)
                    } else {
                        trailing.to_string()
                    };

                    let mut metadata = HashMap::new();
                    metadata.insert("event_type".to_string(), serde_json::json!("sub"));
                    metadata.insert("months".to_string(), serde_json::json!(months));
                    metadata.insert("sub_plan".to_string(), serde_json::json!(sub_plan));

                    let user_id = tags.get("user-id").cloned().unwrap_or_default();
                    let msg_uuid = tags
                        .get("id")
                        .cloned()
                        .unwrap_or_else(|| Uuid::new_v4().to_string());

                    Ok(vec![DanmuItem::Message(DanmuMessage {
                        id: msg_uuid,
                        user_id,
                        username: display_name,
                        content,
                        color: None,
                        timestamp: chrono::Utc::now(),
                        message_type: DanmuType::Subscription,
                        metadata: Some(metadata),
                    })])
                }

                "subgift" | "anonsubgift" => {
                    let recipient = tags
                        .get("msg-param-recipient-display-name")
                        .cloned()
                        .unwrap_or_default();
                    let gift_months = tags
                        .get("msg-param-gift-months")
                        .cloned()
                        .unwrap_or_default();
                    let sub_plan = tags
                        .get("msg-param-sub-plan-name")
                        .cloned()
                        .unwrap_or_default();
                    let content = format!("gifted a sub to {} ({})", recipient, sub_plan);

                    let mut metadata = HashMap::new();
                    metadata.insert("event_type".to_string(), serde_json::json!("subgift"));
                    metadata.insert("recipient".to_string(), serde_json::json!(recipient));
                    metadata.insert("gift_months".to_string(), serde_json::json!(gift_months));

                    let user_id = tags.get("user-id").cloned().unwrap_or_default();
                    let msg_uuid = tags
                        .get("id")
                        .cloned()
                        .unwrap_or_else(|| Uuid::new_v4().to_string());

                    Ok(vec![DanmuItem::Message(DanmuMessage {
                        id: msg_uuid,
                        user_id,
                        username: display_name,
                        content,
                        color: None,
                        timestamp: chrono::Utc::now(),
                        message_type: DanmuType::Gift,
                        metadata: Some(metadata),
                    })])
                }

                "submysterygift" => {
                    let mass_gift_count = tags
                        .get("msg-param-mass-gift-count")
                        .cloned()
                        .unwrap_or_default();
                    let sub_plan = tags
                        .get("msg-param-sub-plan-name")
                        .cloned()
                        .unwrap_or_default();
                    let content = format!("gifted {} {} subs", mass_gift_count, sub_plan);

                    let mut metadata = HashMap::new();
                    metadata.insert(
                        "event_type".to_string(),
                        serde_json::json!("submysterygift"),
                    );
                    metadata.insert(
                        "mass_gift_count".to_string(),
                        serde_json::json!(mass_gift_count),
                    );

                    let user_id = tags.get("user-id").cloned().unwrap_or_default();
                    let msg_uuid = tags
                        .get("id")
                        .cloned()
                        .unwrap_or_else(|| Uuid::new_v4().to_string());

                    Ok(vec![DanmuItem::Message(DanmuMessage {
                        id: msg_uuid,
                        user_id,
                        username: display_name,
                        content,
                        color: None,
                        timestamp: chrono::Utc::now(),
                        message_type: DanmuType::Gift,
                        metadata: Some(metadata),
                    })])
                }

                "raid" => {
                    let raider_display = tags
                        .get("msg-param-displayName")
                        .cloned()
                        .unwrap_or_default();
                    let viewer_count = tags
                        .get("msg-param-viewerCount")
                        .cloned()
                        .unwrap_or_default();
                    let content = format!(
                        "{} is raiding with {} viewers",
                        raider_display, viewer_count
                    );

                    let mut metadata = HashMap::new();
                    metadata.insert("event_type".to_string(), serde_json::json!("raid"));
                    metadata.insert("viewer_count".to_string(), serde_json::json!(viewer_count));

                    let msg_uuid = tags
                        .get("id")
                        .cloned()
                        .unwrap_or_else(|| Uuid::new_v4().to_string());

                    Ok(vec![DanmuItem::Message(DanmuMessage {
                        id: msg_uuid,
                        user_id: tags.get("user-id").cloned().unwrap_or_default(),
                        username: display_name,
                        content,
                        color: None,
                        timestamp: chrono::Utc::now(),
                        message_type: DanmuType::System,
                        metadata: Some(metadata),
                    })])
                }

                "ritual" => {
                    let ritual_name = tags
                        .get("msg-param-ritual-name")
                        .cloned()
                        .unwrap_or_default();
                    if ritual_name == "new_chatter" {
                        let msg_uuid = tags
                            .get("id")
                            .cloned()
                            .unwrap_or_else(|| Uuid::new_v4().to_string());
                        Ok(vec![DanmuItem::Message(DanmuMessage {
                            id: msg_uuid,
                            user_id: tags.get("user-id").cloned().unwrap_or_default(),
                            username: display_name.clone(),
                            content: format!("{} is new here!", display_name),
                            color: None,
                            timestamp: chrono::Utc::now(),
                            message_type: DanmuType::UserJoin,
                            metadata: Some({
                                let mut m = HashMap::new();
                                m.insert(
                                    "event_type".to_string(),
                                    serde_json::json!("new_chatter"),
                                );
                                m
                            }),
                        })])
                    } else {
                        Ok(vec![])
                    }
                }

                _ => {
                    debug!(msg_id = %msg_id, trailing = %trailing, "Unhandled USERNOTICE");
                    Ok(vec![])
                }
            }
        }

        // ---- PRIVMSG (chat message) ---------------------------------------
        "PRIVMSG" => {
            let display_name = tags
                .get("display-name")
                .cloned()
                .unwrap_or_else(|| extract_username_from_source(source).to_string());

            let user_id = tags.get("user-id").cloned().unwrap_or_default();
            let color = tags.get("color").cloned();
            let is_subscriber = tags.get("subscriber").map(|v| v == "1").unwrap_or(false);
            let is_turbo = tags.get("turbo").map(|v| v == "1").unwrap_or(false);
            let is_mod = tags.get("mod").map(|v| v == "1").unwrap_or(false);
            let badges = tags.get("badges").cloned().unwrap_or_default();
            let msg_id = tags
                .get("id")
                .cloned()
                .unwrap_or_else(|| Uuid::new_v4().to_string());

            // Check for bits (cheers)
            let bits = tags.get("bits").cloned();

            let content = trailing.to_string();

            let mut metadata = HashMap::new();
            if is_subscriber {
                metadata.insert("subscriber".to_string(), serde_json::json!(true));
            }
            if is_turbo {
                metadata.insert("turbo".to_string(), serde_json::json!(true));
            }
            if is_mod {
                metadata.insert("mod".to_string(), serde_json::json!(true));
            }
            if !badges.is_empty() {
                metadata.insert("badges".to_string(), serde_json::json!(badges));
            }

            // Determine message type
            let (message_type, final_content) = if let Some(bits_count) = &bits {
                metadata.insert("bits".to_string(), serde_json::json!(bits_count));
                metadata.insert("event_type".to_string(), serde_json::json!("cheer"));
                (DanmuType::SuperChat, content.clone())
            } else {
                (DanmuType::Chat, content.clone())
            };

            let danmu = DanmuMessage {
                id: msg_id,
                user_id,
                username: display_name,
                content: final_content,
                color: color.clone(),
                timestamp: chrono::Utc::now(),
                message_type,
                metadata: if metadata.is_empty() {
                    None
                } else {
                    Some(metadata)
                },
            };

            Ok(vec![DanmuItem::Message(danmu)])
        }

        // ---- Unhandled commands -------------------------------------------
        _ => {
            trace!(command = %command, "Unhandled IRC command");
            Ok(vec![])
        }
    }
}

// ---------------------------------------------------------------------------
// TwitchDanmuProvider
// ---------------------------------------------------------------------------

/// Platform-specific danmaku provider for Twitch.
pub struct TwitchDanmuProvider {
    connections: tokio::sync::RwLock<HashMap<String, Arc<Mutex<TwitchConnectionState>>>>,
}

impl TwitchDanmuProvider {
    pub fn new() -> Self {
        Self {
            connections: tokio::sync::RwLock::new(HashMap::new()),
        }
    }
}

/// Convenience factory used by the `ProviderRegistry`.
pub fn create_twitch_danmu_provider() -> TwitchDanmuProvider {
    TwitchDanmuProvider::new()
}

#[async_trait]
impl DanmuProvider for TwitchDanmuProvider {
    fn platform(&self) -> &str {
        "twitch"
    }

    fn supports_url(&self, url: &str) -> bool {
        url.contains("twitch.tv")
    }

    fn extract_room_id(&self, url: &str) -> Option<String> {
        let re = regex::Regex::new(r"(?:https?://)?(?:www\.)?twitch\.tv/(\w+)").ok()?;
        re.captures(url)
            .and_then(|c| c.get(1).map(|m| m.as_str().to_string()))
            .filter(|s| {
                ![
                    "directory",
                    "settings",
                    "subscriptions",
                    "inventory",
                    "wallet",
                    "downloads",
                    "store",
                    "turbo",
                    "prime",
                    "jobs",
                    "p",
                    "videos",
                    "clip",
                    "events",
                ]
                .contains(&s.as_str())
            })
    }

    async fn connect(&self, room_id: &str, _config: ConnectionConfig) -> Result<DanmuConnection> {
        let channel = room_id.to_string();

        let connection_id = format!("twitch-{}-{}", room_id, Uuid::new_v4());
        let (message_tx, message_rx) = mpsc::channel(MESSAGE_CHANNEL_SIZE);
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let room_id_owned = room_id.to_string();
        let channel_owned = channel.clone();
        let handle = tokio::spawn(run_twitch_irc_task(
            room_id_owned,
            channel_owned,
            message_tx,
            shutdown_rx,
        ));

        let state = TwitchConnectionState {
            message_rx: Arc::new(Mutex::new(message_rx)),
            shutdown_tx: Some(shutdown_tx),
            tasks: vec![handle],
        };

        self.connections
            .write()
            .await
            .insert(connection_id.clone(), Arc::new(Mutex::new(state)));

        let mut conn = DanmuConnection::new(connection_id, "twitch", room_id);
        conn.set_connected();

        Ok(conn)
    }

    async fn disconnect(&self, connection: &mut DanmuConnection) -> Result<()> {
        if let Some(state_arc) = self.connections.write().await.remove(&connection.id) {
            let mut state = state_arc.lock().await;
            if let Some(tx) = state.shutdown_tx.take() {
                let _ = tx.try_send(());
            }
            state.abort_tasks();
        }
        connection.set_disconnected();
        Ok(())
    }

    async fn receive(&self, connection: &DanmuConnection) -> Result<Option<DanmuItem>> {
        let state_arc = {
            let map = self.connections.read().await;
            map.get(&connection.id).cloned()
        };

        let Some(state_arc) = state_arc else {
            return Err(DanmakuError::connection("Connection not found"));
        };

        let message_rx = {
            let state = state_arc.lock().await;
            state.message_rx.clone()
        };

        let next = tokio::time::timeout(RECEIVE_TIMEOUT, async move {
            let mut rx = message_rx.lock().await;
            rx.recv().await
        })
        .await;

        match next {
            Ok(Some(msg)) => Ok(Some(msg)),
            Ok(None) => {
                // Channel closed – the background task has exited.
                let _ = self.connections.write().await.remove(&connection.id);
                Err(DanmakuError::connection("Message channel closed"))
            }
            Err(_) => Ok(None), // Timeout – no message available right now.
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_irc_tags_basic() {
        let tags = parse_irc_tags(
            "@badge-info=;badges=broadcaster/1;color=#1E90FF;display-name=TestUser;subscriber=0;turbo=0",
        );
        assert_eq!(tags.get("display-name").unwrap(), "TestUser");
        assert_eq!(tags.get("color").unwrap(), "#1E90FF");
        assert_eq!(tags.get("subscriber").unwrap(), "0");
        assert_eq!(tags.get("badges").unwrap(), "broadcaster/1");
    }

    #[test]
    fn test_parse_irc_tags_escaped() {
        let tags = parse_irc_tags(r"@display-name=Test\:User\sName;msg=hello\s\world");
        assert_eq!(tags.get("display-name").unwrap(), "Test;User Name");
        assert_eq!(tags.get("msg").unwrap(), "hello \\world");
    }

    #[test]
    fn test_unescape_irc_tag() {
        assert_eq!(unescape_irc_tag(r"hello\s\sworld"), "hello  world");
        assert_eq!(unescape_irc_tag(r"test\:value"), "test;value");
        assert_eq!(unescape_irc_tag(r"back\\slash"), "back\\slash");
    }

    #[test]
    fn test_parse_irc_message_privmsg() {
        let line = r"@badge-info=;badges=broadcaster/1;color=#1E90FF;display-name=TestUser;user-id=12345 :testuser!testuser@testuser.tmi.twitch.tv PRIVMSG #testchannel :Hello World!";
        let (tags, source, command, _channel, trailing) = parse_irc_message(line);
        assert_eq!(command, "PRIVMSG");
        assert_eq!(source, "testuser!testuser@testuser.tmi.twitch.tv");
        assert_eq!(trailing, "Hello World!");
        assert_eq!(tags.get("display-name").unwrap(), "TestUser");
    }

    #[test]
    fn test_parse_irc_message_ping() {
        let line = "PING :tmi.twitch.tv";
        let (tags, source, command, _channel, trailing) = parse_irc_message(line);
        assert!(tags.is_empty());
        assert_eq!(source, "");
        assert_eq!(command, "PING");
        assert_eq!(trailing, "tmi.twitch.tv");
    }

    #[test]
    fn test_extract_username_from_source() {
        assert_eq!(
            extract_username_from_source("testuser!testuser@testuser.tmi.twitch.tv"),
            "testuser"
        );
        assert_eq!(
            extract_username_from_source("justinfan12345"),
            "justinfan12345"
        );
    }

    #[test]
    fn test_extract_room_id_standard_url() {
        let provider = TwitchDanmuProvider::new();
        assert_eq!(
            provider.extract_room_id("https://www.twitch.tv/shroud"),
            Some("shroud".to_string())
        );
    }

    #[test]
    fn test_extract_room_id_no_www() {
        let provider = TwitchDanmuProvider::new();
        assert_eq!(
            provider.extract_room_id("https://twitch.tv/pokimanelol"),
            Some("pokimanelol".to_string())
        );
    }

    #[test]
    fn test_extract_room_id_no_protocol() {
        let provider = TwitchDanmuProvider::new();
        assert_eq!(
            provider.extract_room_id("twitch.tv/xqc"),
            Some("xqc".to_string())
        );
    }

    #[test]
    fn test_extract_room_id_invalid_url() {
        let provider = TwitchDanmuProvider::new();
        assert_eq!(provider.extract_room_id("https://example.com"), None);
    }

    #[test]
    fn test_extract_room_id_reserved_path() {
        let provider = TwitchDanmuProvider::new();
        assert_eq!(
            provider.extract_room_id("https://www.twitch.tv/directory"),
            None
        );
        assert_eq!(
            provider.extract_room_id("https://www.twitch.tv/settings"),
            None
        );
    }

    #[test]
    fn test_supports_url() {
        let provider = TwitchDanmuProvider::new();
        assert!(provider.supports_url("https://www.twitch.tv/shroud"));
        assert!(provider.supports_url("twitch.tv/xqc"));
        assert!(!provider.supports_url("https://www.youtube.com/watch?v=abc"));
    }

    #[test]
    fn test_parse_irc_message_usernotice_sub() {
        let line = r"@badge-info=subscriber/6;badges=subscriber/6;color=#FF4500;display-name=SubUser;msg-id=sub;msg-param-cumulative-months=6;msg-param-sub-plan-name=Channel\sSubscription\s(testchannel);user-id=67890 :subuser!subuser@subuser.tmi.twitch.tv USERNOTICE #testchannel";
        let (tags, _source, command, _channel, trailing) = parse_irc_message(line);
        assert_eq!(command, "USERNOTICE");
        assert_eq!(tags.get("msg-id").unwrap(), "sub");
        assert_eq!(tags.get("msg-param-cumulative-months").unwrap(), "6");
        assert_eq!(
            tags.get("msg-param-sub-plan-name").unwrap(),
            "Channel Subscription (testchannel)"
        );
        assert!(trailing.is_empty());
    }

    #[test]
    fn test_parse_irc_message_clearchat() {
        let line = r"@room-id=123456;target-user-id=7890;ban-duration=600 :tmi.twitch.tv CLEARCHAT #testchannel :baduser";
        let (tags, _source, command, _irc_channel, trailing) = parse_irc_message(line);
        assert_eq!(command, "CLEARCHAT");
        assert_eq!(trailing, "baduser");
        assert_eq!(tags.get("ban-duration").unwrap(), "600");
    }
}
