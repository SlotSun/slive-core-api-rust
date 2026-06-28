use crate::danmaku_mask::mask_trait::DanmakuMask;
use crate::danmaku_mask::mask_frequency::FrequencyMask;
use crate::danmaku_mask::mask_word_blacklist::WordBlacklist;
use crate::danmaku_mask::mask_composite::CompositeMask;

/// Configuration for building a [`DanmakuMask`] at runtime.
#[derive(Debug, Clone, Default)]
pub struct MaskConfig {
    pub frequency: Option<FrequencyConfig>,
    pub blacklist_words: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct FrequencyConfig {
    pub base_window_ms: u32,
    pub bucket_count: u16,
    pub use_normalization: bool,
    pub max_frequency: u16,
}

impl MaskConfig {
    /// Build a mask from this config. Returns `None` if no masks are configured.
    pub fn build(self) -> Option<Box<dyn DanmakuMask>> {
        let mut masks: Vec<Box<dyn DanmakuMask>> = Vec::new();

        if let Some(fc) = self.frequency {
            masks.push(Box::new(FrequencyMask::new(
                fc.base_window_ms,
                fc.bucket_count,
                fc.use_normalization,
                fc.max_frequency,
            )));
        }

        if let Some(words) = self.blacklist_words {
            if !words.is_empty() {
                masks.push(Box::new(WordBlacklist::new(words)));
            }
        }

        match masks.len() {
            0 => None,
            1 => masks.into_iter().next(),
            _ => Some(Box::new(CompositeMask::new(masks))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_empty() {
        let cfg = MaskConfig::default();
        assert!(cfg.build().is_none());
    }

    #[test]
    fn test_build_frequency_only() {
        let cfg = MaskConfig {
            frequency: Some(FrequencyConfig {
                base_window_ms: 5000,
                bucket_count: 5,
                use_normalization: true,
                max_frequency: 3,
            }),
            blacklist_words: None,
        };
        let mask = cfg.build();
        assert!(mask.is_some());
    }

    #[test]
    fn test_build_blacklist_only() {
        let cfg = MaskConfig {
            frequency: None,
            blacklist_words: Some(vec!["bad".to_string()]),
        };
        let mask = cfg.build();
        assert!(mask.is_some());
    }

    #[test]
    fn test_build_empty_blacklist_returns_none() {
        let cfg = MaskConfig {
            frequency: None,
            blacklist_words: Some(vec![]),
        };
        assert!(cfg.build().is_none());
    }

    #[test]
    fn test_build_both_creates_composite() {
        let cfg = MaskConfig {
            frequency: Some(FrequencyConfig {
                base_window_ms: 5000,
                bucket_count: 5,
                use_normalization: true,
                max_frequency: 3,
            }),
            blacklist_words: Some(vec!["bad".to_string()]),
        };
        let mask = cfg.build();
        assert!(mask.is_some());
    }

    #[test]
    fn test_built_frequency_mask_works() {
        let cfg = MaskConfig {
            frequency: Some(FrequencyConfig {
                base_window_ms: 5000,
                bucket_count: 5,
                use_normalization: true,
                max_frequency: 2,
            }),
            blacklist_words: None,
        };
        let mut mask = cfg.build().unwrap();

        assert!(!mask.should_block("test", 1000));
        assert!(!mask.should_block("test", 1000));
        assert!(mask.should_block("test", 1000));
    }

    #[test]
    fn test_built_blacklist_mask_works() {
        let cfg = MaskConfig {
            frequency: None,
            blacklist_words: Some(vec!["spam".to_string()]),
        };
        let mut mask = cfg.build().unwrap();

        assert!(mask.should_block("this is spam", 0));
        assert!(!mask.should_block("hello", 0));
    }

    #[test]
    fn test_built_composite_mask_works() {
        let cfg = MaskConfig {
            frequency: Some(FrequencyConfig {
                base_window_ms: 5000,
                bucket_count: 5,
                use_normalization: true,
                max_frequency: 1,
            }),
            blacklist_words: Some(vec!["bad".to_string()]),
        };
        let mut mask = cfg.build().unwrap();

        // 频控
        assert!(!mask.should_block("hello", 1000));
        assert!(mask.should_block("hello", 1000));

        // 屏蔽词
        assert!(mask.should_block("bad word", 1000));
    }

    #[test]
    fn test_clone_works() {
        let cfg = MaskConfig {
            frequency: Some(FrequencyConfig {
                base_window_ms: 5000,
                bucket_count: 5,
                use_normalization: true,
                max_frequency: 3,
            }),
            blacklist_words: Some(vec!["bad".to_string()]),
        };
        let cfg2 = cfg.clone();
        assert!(cfg2.build().is_some());
    }
}
