//! Memory optimization - buffer pooling and allocation management

pub mod buffer_pool;
pub mod config;
pub mod zero_copy;

pub use buffer_pool::{global_buffer_pool, BufferPool, EnhancedPoolStats, PoolStats};
pub use config::{
    AutoTuningEngine, AutoTuningStats, MemoryPoolConfig, MemoryUsageTracker, TuningDecision,
};
pub use zero_copy::ZeroCopyOps;
