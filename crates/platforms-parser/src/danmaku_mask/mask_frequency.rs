use std::collections::HashMap;
use xxhash_rust::xxh3::xxh3_64;

use super::mask_trait::DanmakuMask;

/// Sliding-window frequency mask.
///
/// Tracks how many times each message hash appears within a time window.
/// Messages exceeding `max_frequency` within the window are blocked.
pub struct FrequencyMask {
    max_frequency: u16,
    bucket_count: usize,
    bucket_size_ms: u64,

    current_bucket: usize,
    last_shift_ms: u64,

    /// Per-bucket hit counts (hash → count).
    buckets: Vec<HashMap<u64, u16>>,
    /// Global frequency map across the entire window.
    freq_map: HashMap<u64, u16>,

    use_normalization: bool,
}

impl FrequencyMask {
    pub fn new(
        base_window_ms: u32,
        bucket_count: u16,
        use_normalization: bool,
        max_frequency: u16,
    ) -> Self {
        // 最小窗口 10 秒，防止除零和过于激进的过滤
        let base_window_ms = base_window_ms.max(10_000);
        let bucket_count = bucket_count.max(1) as usize;
        let bucket_size_ms = base_window_ms as u64 / bucket_count as u64;

        Self {
            max_frequency,
            bucket_count,
            bucket_size_ms,
            current_bucket: 0,
            last_shift_ms: 0,
            buckets: (0..bucket_count)
                .map(|_| HashMap::with_capacity(128))
                .collect(),
            freq_map: HashMap::with_capacity(1024),
            use_normalization,
        }
    }

    fn normalize(&self, text: &str) -> String {
        if !self.use_normalization {
            return text.to_owned();
        }
        text.trim()
            .to_lowercase()
            .replace(char::is_whitespace, "")
            .replace(|c: char| "~!！?？,.，。".contains(c), "")
    }

    fn shift_if_needed(&mut self, now_ms: u64) {
        if self.last_shift_ms == 0 {
            // Initialize: align to the start of the current bucket so the
            // while-loop can still expire stale buckets on the first call.
            self.last_shift_ms = now_ms.saturating_sub(self.bucket_size_ms);
        }

        // 限制循环次数，最多执行 bucket_count 次，避免长时间未收到消息时的性能问题
        let max_iterations = self.bucket_count;
        let mut iterations = 0;

        while now_ms.saturating_sub(self.last_shift_ms) >= self.bucket_size_ms
            && iterations < max_iterations
        {
            self.last_shift_ms += self.bucket_size_ms;

            // Expire the current bucket before moving to the next one.
            let old_idx = self.current_bucket;
            for (&hash, &count) in self.buckets[old_idx].iter() {
                if let Some(v) = self.freq_map.get_mut(&hash) {
                    if *v <= count {
                        self.freq_map.remove(&hash);
                    } else {
                        *v -= count;
                    }
                }
            }
            self.buckets[old_idx].clear();

            // Move to the next bucket.
            self.current_bucket = (self.current_bucket + 1) % self.bucket_count;
            iterations += 1;
        }

        // 如果循环被限制，重置 last_shift_ms 到当前时间
        if iterations >= max_iterations {
            self.last_shift_ms = now_ms;
        }
    }

    /// Check a single text, update internal state, return whether it should be blocked.
    pub fn check(&mut self, text: &str, now_ms: u64) -> bool {
        self.shift_if_needed(now_ms);

        let normalized = self.normalize(text);
        let hash = xxh3_64(normalized.as_bytes());

        let freq = *self.freq_map.get(&hash).unwrap_or(&0);
        let allowed = freq < self.max_frequency;

        if allowed {
            let bucket = &mut self.buckets[self.current_bucket];
            *bucket.entry(hash).or_insert(0) += 1;
            *self.freq_map.entry(hash).or_insert(0) += 1;
        }

        !allowed
    }
}

impl DanmakuMask for FrequencyMask {
    fn should_block(&mut self, text: &str, now_ms: u64) -> bool {
        self.check(text, now_ms)
    }

    fn reset(&mut self) {
        for bucket in &mut self.buckets {
            bucket.clear();
        }
        self.freq_map.clear();
        self.current_bucket = 0;
        self.last_shift_ms = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blocks_duplicates() {
        let mut m = FrequencyMask::new(5000, 5, true, 3);

        assert!(!m.should_block("哈哈", 1000)); // 1st
        assert!(!m.should_block("哈哈", 1000)); // 2nd
        assert!(!m.should_block("哈哈", 1000)); // 3rd
        assert!(m.should_block("哈哈", 1000));  // 4th → blocked
    }

    #[test]
    fn test_normalization() {
        let mut m = FrequencyMask::new(5000, 5, true, 2);

        // 空格、标点都会被归一化
        assert!(!m.should_block("  哈哈  ", 1000));
        assert!(!m.should_block("哈哈！", 1000));
        assert!(m.should_block("哈哈", 1000)); // 归一化后相同 → blocked
    }

    #[test]
    fn test_no_normalization() {
        let mut m = FrequencyMask::new(5000, 5, false, 2);

        // 不归一化，不同写法算不同消息
        assert!(!m.should_block("哈哈", 1000));
        assert!(!m.should_block("哈哈！", 1000)); // 带标点，不同
        assert!(!m.should_block("哈哈", 1000));   // 第2次 "哈哈"
        assert!(m.should_block("哈哈", 1000));    // 第3次 → blocked
    }

    #[test]
    fn test_different_messages_independent() {
        let mut m = FrequencyMask::new(5000, 5, true, 2);

        assert!(!m.should_block("aaa", 1000));
        assert!(!m.should_block("bbb", 1000));
        assert!(!m.should_block("aaa", 1000)); // aaa 第2次
        assert!(!m.should_block("bbb", 1000)); // bbb 第2次
        assert!(m.should_block("aaa", 1000));  // aaa 第3次 → blocked
        assert!(m.should_block("bbb", 1000));  // bbb 第3次 → blocked
    }

    #[test]
    fn test_window_expiration() {
        // 窗口 10000ms，2 个桶，每桶 5000ms
        let mut m = FrequencyMask::new(10000, 2, true, 2);

        assert!(!m.should_block("test", 0));
        assert!(!m.should_block("test", 0));
        assert!(m.should_block("test", 0)); // 第3次 blocked

        // 超过整个窗口后，旧数据过期
        assert!(!m.should_block("test", 11000)); // 应该放行
    }

    #[test]
    fn test_reset() {
        let mut m = FrequencyMask::new(5000, 5, true, 2);

        m.should_block("test", 1000);
        m.should_block("test", 1000);
        assert!(m.should_block("test", 1000));

        m.reset();
        assert!(!m.should_block("test", 1000)); // reset 后放行
    }

    #[test]
    fn test_max_frequency_1() {
        // max_frequency=1 表示同一条消息只能出现1次
        let mut m = FrequencyMask::new(5000, 5, true, 1);

        assert!(!m.should_block("666", 1000)); // 第1次放行
        assert!(m.should_block("666", 1000));  // 第2次 blocked
        assert!(m.should_block("666", 1000));  // 第3次 blocked
    }

    #[test]
    fn test_chinese_punctuation() {
        let mut m = FrequencyMask::new(5000, 5, true, 2);

        assert!(!m.should_block("牛逼！", 1000));
        assert!(!m.should_block("牛逼！", 1000));
        assert!(m.should_block("牛逼", 1000)); // 归一化后相同 → blocked
    }
}
