//! `smdb-sdk` — Reference Rust client SDK for StateMaster.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use smdb_sdk::{ClientBuilder, SdkError};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), SdkError> {
//!     let client = ClientBuilder::new("localhost:7632")
//!         .token("my-token")
//!         .pool_size(8)
//!         .connect_timeout(Duration::from_secs(5))
//!         .request_timeout(Duration::from_secs(30))
//!         .max_retries(3)
//!         .build()
//!         .await?;
//!
//!     let response = client
//!         .transition("order_1", "fulfillment", "ship")
//!         .expected_version(3)
//!         .ctx(serde_json::json!({"carrier": "ups"}))
//!         .actor("svc:billing")
//!         .idempotency_key("key-123")
//!         .send()
//!         .await?;
//!
//!     println!("moved to: {}", response.to_state);
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod config;
pub mod connection;
pub mod error;
pub mod response;
pub mod subscription;

pub use client::{Client, HistoryBuilder, TransitionBuilder};
pub use config::{ClientBuilder, ClientConfig};
pub use error::{Result, SdkError};
pub use response::TransitionResponse;
pub use subscription::Subscription;

// Re-export core types that consumers will frequently need.
pub use smdb_core::prelude::{
    ChangeRecord, EntityState, MachineDefinition, TransitionRecord,
};
