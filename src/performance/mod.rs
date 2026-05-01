//! Performance counters and monitoring

pub mod counters;

pub use counters::{global_counter_registry, Counter, CounterRegistry, CounterType, Timer};
