use std::collections::HashSet;

use regex::Regex;

use super::mask_trait::DanmakuMask;

/// 检查关键词是否为正则格式（以 / 开头和结尾）。
///
/// 例如: `/广告|代练/` 是正则格式，`广告` 不是。
pub fn is_regex_format(keyword: &str) -> bool {
    let trimmed = keyword.trim();
    trimmed.starts_with('/') && trimmed.ends_with('/') && trimmed.len() > 2
}

/// 移除正则格式的前后斜杠。
///
/// 例如: `/广告|代练/` -> `广告|代练`
pub fn remove_regex_format(keyword: &str) -> &str {
    let trimmed = keyword.trim();
    &trimmed[1..trimmed.len() - 1]
}

/// Word blacklist mask with regex support.
///
/// Blocks messages that contain any of the blacklisted words.
/// Supports both plain text and regex patterns:
/// - Plain text: case-insensitive, ignores whitespace/punctuation
/// - Regex: wrapped in `/pattern/` (e.g., `/广告|代练/`)
pub struct WordBlacklist {
    /// Original words (for reference).
    words: Vec<String>,
    /// Normalized plain words for matching.
    normalized: HashSet<String>,
    /// Compiled regex patterns.
    regex_patterns: Vec<Regex>,
}

impl WordBlacklist {
    pub fn new(words: Vec<String>) -> Self {
        let mut normalized = HashSet::new();
        let mut regex_patterns = Vec::new();

        for word in &words {
            if is_regex_format(word) {
                // 正则表达式
                let pattern = remove_regex_format(word);
                match Regex::new(pattern) {
                    Ok(re) => regex_patterns.push(re),
                    Err(e) => {
                        tracing::warn!("正则表达式格式错误: {} - {}", word, e);
                    }
                }
            } else {
                // 普通文本
                normalized.insert(Self::normalize(word));
            }
        }

        Self {
            words,
            normalized,
            regex_patterns,
        }
    }

    fn normalize(text: &str) -> String {
        text.trim()
            .to_lowercase()
            .replace(char::is_whitespace, "")
            .replace(|c: char| "~!！?？,.，。".contains(c), "")
    }

    /// Returns `true` if the text contains any blacklisted word.
    pub fn contains_blacklisted(&self, text: &str) -> bool {
        let normalized = Self::normalize(text);

        // 检查普通文本
        if self.normalized.iter().any(|w| normalized.contains(w.as_str())) {
            return true;
        }

        // 检查正则表达式（对原始文本匹配，保留原始格式）
        if self.regex_patterns.iter().any(|re| re.is_match(text)) {
            return true;
        }

        false
    }

    /// Get the list of blacklisted words.
    pub fn words(&self) -> &[String] {
        &self.words
    }

    /// Get the number of compiled regex patterns.
    pub fn regex_count(&self) -> usize {
        self.regex_patterns.len()
    }

    /// Get the number of plain text words.
    pub fn plain_count(&self) -> usize {
        self.normalized.len()
    }
}

impl DanmakuMask for WordBlacklist {
    fn should_block(&mut self, text: &str, _now_ms: u64) -> bool {
        self.contains_blacklisted(text)
    }

    fn reset(&mut self) {
        // Word blacklist has no state to reset.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_match() {
        let bl = WordBlacklist::new(vec!["spam".to_string()]);
        assert!(bl.contains_blacklisted("this is spam"));
        assert!(!bl.contains_blacklisted("this is fine"));
    }

    #[test]
    fn test_case_insensitive() {
        let bl = WordBlacklist::new(vec!["SPAM".to_string()]);
        assert!(bl.contains_blacklisted("spam"));
        assert!(bl.contains_blacklisted("Spam"));
        assert!(bl.contains_blacklisted("SPAM"));
    }

    #[test]
    fn test_chinese_words() {
        let bl = WordBlacklist::new(vec!["广告".to_string(), "代练".to_string()]);
        assert!(bl.contains_blacklisted("这里有广告"));
        assert!(bl.contains_blacklisted("代练加我"));
        assert!(!bl.contains_blacklisted("正常弹幕"));
    }

    #[test]
    fn test_normalization() {
        let bl = WordBlacklist::new(vec!["加微信".to_string()]);
        assert!(bl.contains_blacklisted("加 微 信"));
        assert!(bl.contains_blacklisted("加微信！"));
        assert!(bl.contains_blacklisted("加,微信"));
    }

    #[test]
    fn test_empty_blacklist() {
        let bl = WordBlacklist::new(vec![]);
        assert!(!bl.contains_blacklisted("任何消息"));
        assert!(!bl.contains_blacklisted(""));
    }

    #[test]
    fn test_multiple_words_any_match() {
        let bl = WordBlacklist::new(vec![
            "广告".to_string(),
            "代练".to_string(),
            "加微信".to_string(),
        ]);

        assert!(bl.contains_blacklisted("来看广告"));
        assert!(bl.contains_blacklisted("代练上分"));
        assert!(bl.contains_blacklisted("加微信好友"));
        assert!(!bl.contains_blacklisted("正常聊天"));
    }

    #[test]
    fn test_substring_match() {
        let bl = WordBlacklist::new(vec!["代练".to_string()]);

        // 子串匹配
        assert!(bl.contains_blacklisted("代练"));
        assert!(bl.contains_blacklisted("找代练上分"));
        assert!(bl.contains_blacklisted("代练加我"));
        assert!(!bl.contains_blacklisted("代替练习"));
    }

    #[test]
    fn test_should_block_trait() {
        let mut bl = WordBlacklist::new(vec!["spam".to_string()]);

        assert!(bl.should_block("this is spam", 0));
        assert!(!bl.should_block("hello", 0));
    }

    #[test]
    fn test_words_accessor() {
        let words = vec!["广告".to_string(), "代练".to_string()];
        let bl = WordBlacklist::new(words.clone());
        assert_eq!(bl.words(), &words);
    }

    // =========================================================================
    // 正则表达式测试
    // =========================================================================

    #[test]
    fn test_is_regex_format() {
        assert!(is_regex_format("/广告/"));
        assert!(is_regex_format("/广告|代练/"));
        assert!(is_regex_format("/\\d+/"));
        assert!(!is_regex_format("广告"));
        assert!(!is_regex_format("/"));
        assert!(!is_regex_format("//"));
        assert!(!is_regex_format("广告/"));
    }

    #[test]
    fn test_remove_regex_format() {
        assert_eq!(remove_regex_format("/广告/"), "广告");
        assert_eq!(remove_regex_format("/广告|代练/"), "广告|代练");
        assert_eq!(remove_regex_format("/\\d+/"), "\\d+");
    }

    #[test]
    fn test_regex_basic() {
        let bl = WordBlacklist::new(vec!["/广告|代练/".to_string()]);

        assert!(bl.contains_blacklisted("这里有广告"));
        assert!(bl.contains_blacklisted("代练加我"));
        assert!(!bl.contains_blacklisted("正常弹幕"));
    }

    #[test]
    fn test_regex_complex() {
        // 匹配包含数字的消息
        let bl = WordBlacklist::new(vec!["/\\d{5,}/".to_string()]);

        assert!(bl.contains_blacklisted("加我微信12345"));
        assert!(!bl.contains_blacklisted("加我微信123"));
        assert!(!bl.contains_blacklisted("没有数字"));
    }

    #[test]
    fn test_regex_mixed_with_plain() {
        let bl = WordBlacklist::new(vec![
            "广告".to_string(),           // 普通文本
            "/代练|代打/".to_string(),     // 正则
            "/加[微V]信/".to_string(),    // 正则
        ]);

        assert!(bl.contains_blacklisted("这里有广告"));
        assert!(bl.contains_blacklisted("代练上分"));
        assert!(bl.contains_blacklisted("代打上分"));
        assert!(bl.contains_blacklisted("加微信"));
        assert!(bl.contains_blacklisted("加V信"));
        assert!(!bl.contains_blacklisted("正常聊天"));
    }

    #[test]
    fn test_regex_invalid_pattern() {
        // 无效正则应该被忽略（不 panic）
        let bl = WordBlacklist::new(vec!["/[invalid/".to_string()]);

        // 无效正则不会匹配任何内容
        assert!(!bl.contains_blacklisted("any text"));
        assert_eq!(bl.regex_count(), 0);
    }

    #[test]
    fn test_regex_count() {
        let bl = WordBlacklist::new(vec![
            "广告".to_string(),
            "/代练/".to_string(),
            "/\\d+/".to_string(),
        ]);

        assert_eq!(bl.regex_count(), 2);
        assert_eq!(bl.plain_count(), 1);
    }

    #[test]
    fn test_regex_case_insensitive() {
        // Rust 使用 (?i) 开启大小写不敏感
        let bl = WordBlacklist::new(vec!["/(?i)spam/".to_string()]);

        assert!(bl.contains_blacklisted("this is spam"));
        assert!(bl.contains_blacklisted("this is SPAM"));
        assert!(bl.contains_blacklisted("this is Spam"));
        assert!(!bl.contains_blacklisted("this is ham"));
    }

    #[test]
    fn test_regex_with_whitespace() {
        // 正则匹配原始文本（不归一化）
        let bl = WordBlacklist::new(vec!["/加\\s*微\\s*信/".to_string()]);

        assert!(bl.contains_blacklisted("加微信"));
        assert!(bl.contains_blacklisted("加 微 信"));
        assert!(bl.contains_blacklisted("加  微  信"));
        assert!(!bl.contains_blacklisted("加微"));
    }
}
