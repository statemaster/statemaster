/// Configuration for the StateMaster wire server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// The address to listen on, e.g. "0.0.0.0:7632".
    pub listen_addr: String,

    /// Path to the TLS certificate file (PEM). If None, plain TCP is used.
    pub tls_cert_path: Option<String>,

    /// Path to the TLS private key file (PEM). If None, plain TCP is used.
    pub tls_key_path: Option<String>,

    /// Valid bearer tokens for authentication. An empty list means no client can authenticate.
    pub auth_tokens: Vec<String>,

    /// Maximum number of concurrent client connections.
    pub max_connections: usize,

    /// Maximum allowed frame body size in bytes.
    pub max_frame_size: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:7632".to_string(),
            tls_cert_path: None,
            tls_key_path: None,
            auth_tokens: Vec::new(),
            max_connections: 1024,
            max_frame_size: smdb_proto::MAX_FRAME_SIZE,
        }
    }
}
