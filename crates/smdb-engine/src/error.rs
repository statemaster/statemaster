use thiserror::Error;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("core error: {0}")]
    Core(#[from] smdb_core::error::CoreError),

    #[error("storage error: {0}")]
    Storage(#[from] smdb_storage::StorageError),

    #[error("machine not found: '{0}'")]
    MachineNotFound(String),

    #[error("entity locked: '{0}'")]
    EntityLocked(String),

    #[error("engine is shutting down")]
    ShuttingDown,
}

pub type Result<T> = std::result::Result<T, EngineError>;
