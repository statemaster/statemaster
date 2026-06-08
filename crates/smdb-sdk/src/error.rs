use thiserror::Error;

#[derive(Debug, Error)]
pub enum SdkError {
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("authentication failed: {0}")]
    AuthFailed(String),

    #[error("request timed out")]
    Timeout,

    #[error("transition rejected: [{code}] {message}")]
    Rejected {
        code: String,
        message: String,
        current_state: Option<String>,
        version: Option<u64>,
    },

    #[error("proto error: {0}")]
    Proto(#[from] smdb_proto::ProtoError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("disconnected from server")]
    Disconnected,

    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, SdkError>;
