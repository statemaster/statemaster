use thiserror::Error;

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("version conflict: entity '{entity_id}' expected version {expected} but found {actual}")]
    VersionConflict {
        entity_id: String,
        expected: u64,
        actual: u64,
    },

    #[error("transaction failed: {0}")]
    TransactionFailed(String),

    #[error("corrupted data: {0}")]
    Corrupted(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

impl From<redb::Error> for StorageError {
    fn from(e: redb::Error) -> Self {
        StorageError::Internal(e.to_string())
    }
}

impl From<redb::DatabaseError> for StorageError {
    fn from(e: redb::DatabaseError) -> Self {
        StorageError::Internal(e.to_string())
    }
}

impl From<redb::TransactionError> for StorageError {
    fn from(e: redb::TransactionError) -> Self {
        StorageError::TransactionFailed(e.to_string())
    }
}

impl From<redb::CommitError> for StorageError {
    fn from(e: redb::CommitError) -> Self {
        StorageError::TransactionFailed(e.to_string())
    }
}

impl From<redb::TableError> for StorageError {
    fn from(e: redb::TableError) -> Self {
        StorageError::Internal(e.to_string())
    }
}

impl From<redb::StorageError> for StorageError {
    fn from(e: redb::StorageError) -> Self {
        StorageError::Internal(e.to_string())
    }
}

impl From<rmp_serde::encode::Error> for StorageError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        StorageError::Corrupted(format!("serialization error: {e}"))
    }
}

impl From<rmp_serde::decode::Error> for StorageError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        StorageError::Corrupted(format!("deserialization error: {e}"))
    }
}
