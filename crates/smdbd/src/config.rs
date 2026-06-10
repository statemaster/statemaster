use std::net::SocketAddr;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

// ---------------------------------------------------------------------------
// File format (TOML sections, all optional)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    #[serde(default)]
    pub server: ServerSection,
    #[serde(default)]
    pub storage: StorageSection,
    #[serde(default)]
    pub tls: TlsSection,
    #[serde(default)]
    pub logging: LoggingSection,
    #[serde(default)]
    pub dispatcher: DispatcherSection,
    #[serde(default)]
    pub auth: AuthSection,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerSection {
    pub listen_addr: String,
    pub metrics_addr: String,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:7632".to_string(),
            metrics_addr: "0.0.0.0:7633".to_string(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageSection {
    pub data_dir: String,
}

impl Default for StorageSection {
    fn default() -> Self {
        Self {
            data_dir: "data".to_string(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsSection {
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingSection {
    pub level: String,
    pub format: LogFormat,
}

impl Default for LoggingSection {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: LogFormat::Json,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Json,
    Text,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DispatcherSection {
    pub interval_ms: u64,
}

impl Default for DispatcherSection {
    fn default() -> Self {
        Self { interval_ms: 100 }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthSection {
    pub tokens: Vec<String>,
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

pub enum ConfigSource {
    File(String),
    Defaults,
}

/// Load the config file at `path`. A missing file is only an error when the
/// path was given explicitly (`--config`); otherwise defaults apply.
pub fn load(path: &str, explicit: bool) -> Result<(FileConfig, ConfigSource)> {
    match std::fs::read_to_string(path) {
        Ok(raw) => {
            let cfg: FileConfig =
                toml::from_str(&raw).with_context(|| format!("parsing config file '{path}'"))?;
            Ok((cfg, ConfigSource::File(path.to_string())))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && !explicit => {
            Ok((FileConfig::default(), ConfigSource::Defaults))
        }
        Err(e) => Err(e).with_context(|| format!("reading config file '{path}'")),
    }
}

// ---------------------------------------------------------------------------
// Resolved config (defaults < file < env < CLI flags)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: String,
    pub metrics_addr: String,
    pub data_dir: String,
    pub log_level: String,
    pub log_format: LogFormat,
    pub dispatcher_interval_ms: u64,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
    pub auth_tokens: Vec<String>,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        self.listen_addr
            .parse::<SocketAddr>()
            .with_context(|| format!("invalid listen_addr '{}'", self.listen_addr))?;
        self.metrics_addr
            .parse::<SocketAddr>()
            .with_context(|| format!("invalid metrics_addr '{}'", self.metrics_addr))?;

        const LEVELS: [&str; 5] = ["trace", "debug", "info", "warn", "error"];
        if !LEVELS.contains(&self.log_level.as_str()) {
            anyhow::bail!(
                "invalid log level '{}' (expected one of {LEVELS:?})",
                self.log_level
            );
        }

        if self.dispatcher_interval_ms == 0 {
            anyhow::bail!("dispatcher interval_ms must be greater than 0");
        }

        match (&self.tls_cert_path, &self.tls_key_path) {
            (Some(_), None) | (None, Some(_)) => {
                anyhow::bail!("tls cert_path and key_path must be set together")
            }
            (Some(cert), Some(key)) => {
                for (name, p) in [("cert_path", cert), ("key_path", key)] {
                    if !Path::new(p).is_file() {
                        anyhow::bail!("tls {name} '{p}' does not exist or is not a file");
                    }
                }
            }
            (None, None) => {}
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Example config (kept in sync with deploy/statemaster.toml)
// ---------------------------------------------------------------------------

#[cfg(test)]
pub const EXAMPLE_CONFIG: &str = r#"
[server]
listen_addr = "0.0.0.0:7632"
metrics_addr = "0.0.0.0:7633"

[storage]
data_dir = "/var/lib/statemaster/data"

[logging]
level = "info"
format = "json"

[dispatcher]
interval_ms = 100

[tls]
cert_path = "/etc/statemaster/cert.pem"
key_path = "/etc/statemaster/key.pem"

[auth]
tokens = ["changeme"]
"#;

#[cfg(test)]
mod tests {
    use super::*;

    fn resolved(file: FileConfig) -> Config {
        Config {
            listen_addr: file.server.listen_addr,
            metrics_addr: file.server.metrics_addr,
            data_dir: file.storage.data_dir,
            log_level: file.logging.level,
            log_format: file.logging.format,
            dispatcher_interval_ms: file.dispatcher.interval_ms,
            tls_cert_path: None,
            tls_key_path: None,
            auth_tokens: file.auth.tokens,
        }
    }

    #[test]
    fn default_config_validates() {
        let cfg = resolved(FileConfig::default());
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.listen_addr, "0.0.0.0:7632");
        assert_eq!(cfg.dispatcher_interval_ms, 100);
        assert_eq!(cfg.log_format, LogFormat::Json);
    }

    #[test]
    fn example_config_parses_and_validates() {
        let file: FileConfig = toml::from_str(EXAMPLE_CONFIG).unwrap();
        assert_eq!(file.storage.data_dir, "/var/lib/statemaster/data");
        assert_eq!(file.auth.tokens, vec!["changeme"]);
        assert_eq!(
            file.tls.cert_path.as_deref(),
            Some("/etc/statemaster/cert.pem")
        );
        // TLS paths don't exist on the test machine, so validate without them.
        let cfg = resolved(file);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn empty_file_uses_defaults() {
        let file: FileConfig = toml::from_str("").unwrap();
        assert_eq!(file.server.listen_addr, "0.0.0.0:7632");
        assert_eq!(file.logging.level, "info");
        assert!(file.auth.tokens.is_empty());
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let res: Result<FileConfig, _> = toml::from_str("[server]\nlisten = \"0.0.0.0:1\"\n");
        assert!(res.is_err());
    }

    #[test]
    fn invalid_addr_fails_validation() {
        let mut cfg = resolved(FileConfig::default());
        cfg.listen_addr = "not-an-addr".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn invalid_log_level_fails_validation() {
        let mut cfg = resolved(FileConfig::default());
        cfg.log_level = "verbose".to_string();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn zero_dispatcher_interval_fails_validation() {
        let mut cfg = resolved(FileConfig::default());
        cfg.dispatcher_interval_ms = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn tls_paths_must_be_paired() {
        let mut cfg = resolved(FileConfig::default());
        cfg.tls_cert_path = Some("/tmp/cert.pem".to_string());
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn log_format_text_parses() {
        let file: FileConfig = toml::from_str("[logging]\nformat = \"text\"\n").unwrap();
        assert_eq!(file.logging.format, LogFormat::Text);
    }
}
