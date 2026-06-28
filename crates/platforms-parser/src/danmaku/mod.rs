pub mod error;
pub mod event;
pub mod message;
pub mod provider;
pub mod registry;
pub mod sampler;
pub mod statistics;
pub mod websocket;
pub mod writer;

pub use error::{DanmakuError, Result};
pub use event::{DanmakuControlEvent, DanmakuItem};
pub use message::{DanmakuMessage, DanmakuType};
pub use provider::{ConnectionConfig, DanmakuConnection, DanmakuProvider};
pub use registry::ProviderRegistry;
pub use sampler::{
    DanmakuSampler, DanmakuSamplingConfig, FixedIntervalSampler, VelocitySampler, create_sampler,
};
pub use statistics::{
    DanmakuStatistics, RateDataPoint, StatisticsAggregator, TopTalker, WordFrequency,
};
pub use websocket::{DanmakuProtocol, WebSocketDanmakuProvider};
pub use writer::{XmlDanmakuWriter, escape_xml, message_type_to_int};

pub use crate::extractor::platforms::huya::danmaku::HuyaDanmakuProvider;
pub use crate::extractor::platforms::twitch::danmaku::TwitchDanmakuProvider;
