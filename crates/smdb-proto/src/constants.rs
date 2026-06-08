pub const PROTOCOL_VERSION: u32 = 1;
pub const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024;
pub const DEFAULT_PORT: u16 = 7632;
pub const DEFAULT_METRICS_PORT: u16 = 7633;
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");
