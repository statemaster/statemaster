use std::path::PathBuf;

/// Controls how aggressively fsync is called after writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncMode {
    /// Call fsync after every write transaction. Strongest durability
    /// guarantee; appropriate for production.
    Synchronous,
    /// Skip fsync. Much faster; data may be lost on OS crash. Suitable
    /// for tests and development environments where speed matters more
    /// than durability.
    Relaxed,
}

impl Default for FsyncMode {
    fn default() -> Self {
        FsyncMode::Synchronous
    }
}

/// Runtime configuration for the `Engine`.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Directory on disk where the embedded database file is stored.
    pub data_dir: PathBuf,

    /// How to handle fsync on writes.
    pub fsync_mode: FsyncMode,

    /// How often the background dispatcher polls the outbox (in milliseconds).
    pub dispatcher_interval_ms: u64,

    /// Maximum number of concurrent subscribers. Attempts to register beyond
    /// this limit will silently drop the oldest subscriber (FIFO eviction).
    pub max_subscribers: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            fsync_mode: FsyncMode::default(),
            dispatcher_interval_ms: 100,
            max_subscribers: 1024,
        }
    }
}
