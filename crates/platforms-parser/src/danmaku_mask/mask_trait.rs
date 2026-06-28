/// Trait for danmaku masking strategies.
///
/// Implement this trait to create custom masks.
/// Multiple masks can be combined via [`CompositeMask`].
pub trait DanmakuMask: Send {
    /// Returns `true` if the message should be blocked.
    fn should_block(&mut self, text: &str, now_ms: u64) -> bool;

    /// Reset internal state (e.g., frequency counters).
    fn reset(&mut self);
}
