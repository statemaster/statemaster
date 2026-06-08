use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtoError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid frame type: 0x{0:02X}")]
    InvalidFrameType(u8),

    #[error("frame too large: {size} bytes exceeds maximum {max} bytes")]
    FrameTooLarge { size: u32, max: u32 },

    #[error("deserialization error: {0}")]
    DeserializationError(String),

    #[error("serialization error: {0}")]
    SerializationError(String),

    #[error("incomplete frame")]
    IncompleteFrame,

    #[error("invalid handshake: {0}")]
    InvalidHandshake(String),
}

pub type Result<T> = std::result::Result<T, ProtoError>;
