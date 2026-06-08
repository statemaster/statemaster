use std::time::Duration;

use crate::client::Client;
use crate::error::Result;

/// Configuration for a StateMaster client connection.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Server address, e.g. "localhost:7632".
    pub addr: String,
    /// Bearer token for authentication.
    pub token: String,
    /// Whether to use TLS.
    pub tls: bool,
    /// Number of connections in the pool.
    pub pool_size: usize,
    /// Timeout for establishing a connection.
    pub connect_timeout: Duration,
    /// Timeout for individual requests.
    pub request_timeout: Duration,
    /// Maximum number of retries on transient failures.
    pub max_retries: u32,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            addr: "localhost:7632".into(),
            token: String::new(),
            tls: false,
            pool_size: 8,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
        }
    }
}

/// Fluent builder for [`ClientConfig`] that opens the connection on `build()`.
pub struct ClientBuilder {
    config: ClientConfig,
}

impl ClientBuilder {
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            config: ClientConfig {
                addr: addr.into(),
                ..ClientConfig::default()
            },
        }
    }

    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.config.token = token.into();
        self
    }

    pub fn tls(mut self, tls: bool) -> Self {
        self.config.tls = tls;
        self
    }

    pub fn pool_size(mut self, pool_size: usize) -> Self {
        self.config.pool_size = pool_size;
        self
    }

    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.config.max_retries = max_retries;
        self
    }

    /// Connect to the server and return a ready [`Client`].
    pub async fn build(self) -> Result<Client> {
        Client::connect(self.config).await
    }
}
