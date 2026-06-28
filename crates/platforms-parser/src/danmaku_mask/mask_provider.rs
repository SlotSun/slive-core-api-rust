use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::danmaku::error::Result;
use crate::danmaku::event::DanmakuItem;
use crate::danmaku::provider::{ConnectionConfig, DanmakuConnection, DanmakuProvider};

use super::mask_trait::DanmakuMask;

/// Statistics for mask filtering.
#[derive(Debug, Clone, Default)]
pub struct MaskStats {
    /// Total messages received from the inner provider.
    pub total_received: u64,
    /// Messages that passed the mask.
    pub passed: u64,
    /// Messages blocked by the mask.
    pub blocked: u64,
}

/// Wrapper that applies [`DanmakuMask`] to any [`DanmakuProvider`].
///
/// Messages are masked transparently in `receive()`.
/// Control events always pass through.
pub struct MaskedDanmakuProvider<P: DanmakuProvider> {
    inner: P,
    /// Per-connection mask instances.
    masks: Arc<Mutex<HashMap<String, Box<dyn DanmakuMask>>>>,
    /// Per-connection statistics.
    stats: Arc<Mutex<HashMap<String, MaskStats>>>,
}

impl<P: DanmakuProvider> MaskedDanmakuProvider<P> {
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            masks: Arc::new(Mutex::new(HashMap::new())),
            stats: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get a reference to the inner provider.
    pub fn inner(&self) -> &P {
        &self.inner
    }

    /// Set or replace the mask for an existing connection.
    pub async fn set_mask(&self, connection_id: &str, mask: Box<dyn DanmakuMask>) {
        let mut map = self.masks.lock().await;
        map.insert(connection_id.to_string(), mask);
    }

    /// Remove the mask for a connection.
    pub async fn clear_mask(&self, connection_id: &str) {
        let mut map = self.masks.lock().await;
        map.remove(connection_id);
    }

    /// Reset the mask state for a connection.
    pub async fn reset_mask(&self, connection_id: &str) {
        let mut map = self.masks.lock().await;
        if let Some(m) = map.get_mut(connection_id) {
            m.reset();
        }
    }

    /// Get statistics for a connection.
    pub async fn stats(&self, connection_id: &str) -> MaskStats {
        let map = self.stats.lock().await;
        map.get(connection_id).cloned().unwrap_or_default()
    }

    /// Reset statistics for a connection.
    pub async fn reset_stats(&self, connection_id: &str) {
        let mut map = self.stats.lock().await;
        map.insert(connection_id.to_string(), MaskStats::default());
    }
}

#[async_trait::async_trait]
impl<P: DanmakuProvider> DanmakuProvider for MaskedDanmakuProvider<P> {
    fn platform(&self) -> &str {
        self.inner.platform()
    }

    async fn connect(&self, room_id: &str, config: ConnectionConfig) -> Result<DanmakuConnection> {
        // Build mask from config before connecting.
        let mask = config
            .mask_config
            .as_ref()
            .and_then(|mc| mc.clone().build());

        let conn = self.inner.connect(room_id, config).await?;

        // Store mask for this connection.
        if let Some(m) = mask {
            let mut map = self.masks.lock().await;
            map.insert(conn.id.clone(), m);
        }

        // Initialize stats.
        let mut stats = self.stats.lock().await;
        stats.insert(conn.id.clone(), MaskStats::default());

        Ok(conn)
    }

    async fn disconnect(&self, connection: &mut DanmakuConnection) -> Result<()> {
        // Clean up mask and stats.
        let mut map = self.masks.lock().await;
        map.remove(&connection.id);
        let mut stats = self.stats.lock().await;
        stats.remove(&connection.id);

        self.inner.disconnect(connection).await
    }

    async fn receive(&self, connection: &DanmakuConnection) -> Result<Option<DanmakuItem>> {
        loop {
            let item = self.inner.receive(connection).await?;

            let Some(item) = item else {
                return Ok(None);
            };

            // Control events always pass through.
            if matches!(item, DanmakuItem::Control(_)) {
                return Ok(Some(item));
            }

            // Check if message should be blocked by mask.
            let should_block = if let DanmakuItem::Message(ref msg) = item {
                let now_ms = chrono::Utc::now().timestamp_millis() as u64;
                let mut map = self.masks.lock().await;
                if let Some(mask) = map.get_mut(&connection.id) {
                    mask.should_block(&msg.content, now_ms)
                } else {
                    false
                }
            } else {
                false
            };

            // Update stats in a single lock acquisition.
            {
                let mut stats = self.stats.lock().await;
                if let Some(s) = stats.get_mut(&connection.id) {
                    s.total_received += 1;
                    if should_block {
                        s.blocked += 1;
                    } else {
                        s.passed += 1;
                    }
                }
            }

            if should_block {
                continue; // Blocked → try next message
            }

            return Ok(Some(item));
        }
    }

    fn supports_url(&self, url: &str) -> bool {
        self.inner.supports_url(url)
    }

    fn extract_room_id(&self, url: &str) -> Option<String> {
        self.inner.extract_room_id(url)
    }
}
