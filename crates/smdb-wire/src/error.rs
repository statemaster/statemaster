use thiserror::Error;

#[derive(Debug, Error)]
pub enum WireError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Proto(#[from] smdb_proto::ProtoError),

    #[error("engine error: {0}")]
    Engine(#[from] smdb_engine::EngineError),

    #[error("tls error: {0}")]
    Tls(String),

    #[error("auth error: {0}")]
    Auth(String),

    #[error("session not found: {0}")]
    SessionNotFound(String),

    #[error("connection closed")]
    ConnectionClosed,
}

pub type Result<T> = std::result::Result<T, WireError>;
