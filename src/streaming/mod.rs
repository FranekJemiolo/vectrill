//! Streaming semantics - watermarks, windows, and state management

pub mod state;
pub mod watermark;
pub mod window;

pub use state::{AggregateState, WindowState, WindowStateStore};
pub use watermark::{Watermark, WatermarkTracker};
pub use window::{WindowKey, WindowSpec, WindowType};
