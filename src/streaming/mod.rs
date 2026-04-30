//! Streaming semantics - watermarks, windows, and state management

pub mod watermark;
pub mod window;

pub use watermark::{Watermark, WatermarkTracker};
pub use window::{WindowSpec, WindowKey, WindowType};
