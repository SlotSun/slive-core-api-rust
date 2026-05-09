pub mod bilibili;
pub mod douyin;
pub mod douyu;
pub mod huya;
pub mod twitch;

use crate::extractor::LiveExtractor;
use std::sync::Arc;

/// Create all default platform extractors.
pub fn default_extractors() -> Vec<Arc<dyn LiveExtractor>> {
    vec![
        Arc::new(huya::HuyaExtractor::new()),
        Arc::new(bilibili::BilibiliExtractor::new()),
        Arc::new(douyin::DouyinExtractor::new()),
        Arc::new(douyu::DouyuExtractor::new()),
        Arc::new(twitch::TwitchExtractor::new()),
    ]
}

/// Find the extractor that supports a given URL.
pub fn find_extractor(url: &str) -> Option<Arc<dyn LiveExtractor>> {
    default_extractors()
        .into_iter()
        .find(|e| e.supports_url(url))
}
