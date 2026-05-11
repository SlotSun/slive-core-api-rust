//! Registry of available platform extractors.

use crate::extractor::LiveExtractor;
use crate::extractor::platforms::bilibili::BilibiliExtractor;
use crate::extractor::platforms::douyin::DouyinExtractor;
use crate::extractor::platforms::douyu::DouyuExtractor;
use crate::extractor::platforms::huya::HuyaExtractor;
use crate::extractor::platforms::twitch::TwitchExtractor;
use std::sync::Arc;

/// Registry of available platform extractors.
#[derive(Default)]
pub struct ExtractorRegistry {
    extractors: Vec<Arc<dyn LiveExtractor>>,
}

impl ExtractorRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            extractors: Vec::new(),
        }
    }

    /// Create a registry with all default platform extractors.
    pub fn with_defaults() -> Self {
        let mut registry = Self::new();
        registry.register(Arc::new(HuyaExtractor::new()));
        registry.register(Arc::new(BilibiliExtractor::new()));
        registry.register(Arc::new(DouyinExtractor::new()));
        registry.register(Arc::new(DouyuExtractor::new()));
        registry.register(Arc::new(TwitchExtractor::new()));
        registry
    }

    /// Register an extractor.
    pub fn register(&mut self, extractor: Arc<dyn LiveExtractor>) {
        self.extractors.push(extractor);
    }

    /// Get an extractor by platform ID (e.g. "bilibili", "douyin", "huya", "douyu", "twitch").
    pub fn get_by_id(&self, id: &str) -> Option<Arc<dyn LiveExtractor>> {
        self.extractors
            .iter()
            .find(|e| e.id().eq_ignore_ascii_case(id))
            .cloned()
    }

    /// Get an extractor that supports the given URL.
    pub fn get_by_url(&self, url: &str) -> Option<Arc<dyn LiveExtractor>> {
        self.extractors
            .iter()
            .find(|e| e.supports_url(url))
            .cloned()
    }

    /// List all registered platform IDs.
    pub fn platforms(&self) -> Vec<&str> {
        self.extractors.iter().map(|e| e.id()).collect()
    }
}

/// Create an extractor by platform ID.
pub fn create_extractor(site: &str) -> Option<Arc<dyn LiveExtractor>> {
    ExtractorRegistry::with_defaults().get_by_id(site)
}

/// Create an extractor that supports the given URL.
pub fn create_extractor_from_url(url: &str) -> Option<Arc<dyn LiveExtractor>> {
    ExtractorRegistry::with_defaults().get_by_url(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_with_defaults() {
        let registry = ExtractorRegistry::with_defaults();
        let platforms = registry.platforms();
        assert_eq!(platforms.len(), 5);
        assert!(platforms.contains(&"huya"));
        assert!(platforms.contains(&"bilibili"));
        assert!(platforms.contains(&"douyu"));
        assert!(platforms.contains(&"douyin"));
        assert!(platforms.contains(&"twitch"));
    }

    #[test]
    fn test_get_by_id() {
        let registry = ExtractorRegistry::with_defaults();

        let bilibili = registry.get_by_id("bilibili");
        assert!(bilibili.is_some());
        assert_eq!(bilibili.unwrap().id(), "bilibili");

        let huya = registry.get_by_id("HUYA");
        assert!(huya.is_some());
        assert_eq!(huya.unwrap().id(), "huya");
    }

    #[test]
    fn test_get_by_url() {
        let registry = ExtractorRegistry::with_defaults();

        let huya = registry.get_by_url("https://www.huya.com/12345");
        assert!(huya.is_some());
        assert_eq!(huya.unwrap().id(), "huya");

        let bilibili = registry.get_by_url("https://live.bilibili.com/12345");
        assert!(bilibili.is_some());
        assert_eq!(bilibili.unwrap().id(), "bilibili");
    }

    #[test]
    fn test_get_by_id_unknown() {
        let registry = ExtractorRegistry::with_defaults();
        assert!(registry.get_by_id("unknown").is_none());
    }

    #[test]
    fn test_create_extractor() {
        let ext = create_extractor("douyin");
        assert!(ext.is_some());
        assert_eq!(ext.unwrap().id(), "douyin");
    }

    #[test]
    fn test_create_extractor_from_url() {
        let ext = create_extractor_from_url("https://www.douyu.com/12345");
        assert!(ext.is_some());
        assert_eq!(ext.unwrap().id(), "douyu");
    }
}
