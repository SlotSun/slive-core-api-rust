use crate::danmaku_mask::mask_trait::DanmakuMask;

/// Combines multiple masks. A message is blocked if **any** mask blocks it.
pub struct CompositeMask {
    masks: Vec<Box<dyn DanmakuMask>>,
}

impl CompositeMask {
    pub fn new(masks: Vec<Box<dyn DanmakuMask>>) -> Self {
        Self { masks }
    }
}

impl DanmakuMask for CompositeMask {
    fn should_block(&mut self, text: &str, now_ms: u64) -> bool {
        // 遍历所有 mask，确保每个 mask 的状态都被更新
        let mut blocked = false;
        for m in self.masks.iter_mut() {
            if m.should_block(text, now_ms) {
                blocked = true;
            }
        }
        blocked
    }

    fn reset(&mut self) {
        self.masks.iter_mut().for_each(|m| m.reset());
    }
}

#[cfg(test)]
mod tests {
    use crate::danmaku_mask::mask_word_blacklist::WordBlacklist;
    use crate::danmaku_mask::mask_frequency::FrequencyMask;
    use super::*;

    #[test]
    fn test_empty_masks() {
        let mut c = CompositeMask::new(vec![]);
        assert!(!c.should_block("anything", 0));
    }

    #[test]
    fn test_single_mask() {
        let mut c = CompositeMask::new(vec![
            Box::new(WordBlacklist::new(vec!["bad".to_string()])),
        ]);

        assert!(!c.should_block("good", 0));
        assert!(c.should_block("bad word", 0));
    }

    #[test]
    fn test_combined() {
        let mut c = CompositeMask::new(vec![
            Box::new(FrequencyMask::new(5000, 5, true, 2)),
            Box::new(WordBlacklist::new(vec!["spam".to_string()])),
        ]);

        // 频控生效
        assert!(!c.should_block("hello", 1000));
        assert!(!c.should_block("hello", 1000));
        assert!(c.should_block("hello", 1000));

        // 屏蔽词生效
        assert!(c.should_block("this is spam", 1000));
    }

    #[test]
    fn test_any_mask_blocks() {
        // 只要有一个 mask 拦截，就blocked
        let mut c = CompositeMask::new(vec![
            Box::new(WordBlacklist::new(vec!["aaa".to_string()])),
            Box::new(WordBlacklist::new(vec!["bbb".to_string()])),
        ]);

        assert!(c.should_block("aaa", 0));
        assert!(c.should_block("bbb", 0));
        assert!(!c.should_block("ccc", 0));
    }

    #[test]
    fn test_reset_all() {
        let mut c = CompositeMask::new(vec![
            Box::new(FrequencyMask::new(5000, 5, true, 1)),
        ]);

        c.should_block("test", 1000);
        assert!(c.should_block("test", 1000)); // blocked

        c.reset();
        assert!(!c.should_block("test", 1000)); // reset 后放行
    }
}
