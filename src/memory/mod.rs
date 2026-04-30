//! Memory optimization - buffer pooling and allocation management

pub mod buffer_pool;

pub use buffer_pool::{BufferPool, global_buffer_pool, PoolStats};
