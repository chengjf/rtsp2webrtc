use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub server: ServerConfig,
    pub streams: Vec<StreamConfig>,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub cors: CorsConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    pub tls: Option<TlsConfig>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind_addr: SocketAddr,
    /// Optional API key. When set, all mutating endpoints and WebSocket
    /// connections require `Authorization: Bearer <key>` or `?key=<key>`.
    #[serde(default)]
    pub api_key: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct StreamConfig {
    pub id: String,
    #[serde(default)]
    pub name: String,
    pub url: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct LimitsConfig {
    #[serde(default = "default_max_peers")]
    pub max_peers: usize,
    #[serde(default = "default_max_per_stream")]
    pub max_per_stream: usize,
    /// Max stream-create requests per minute (0 = unlimited).
    #[serde(default)]
    pub create_per_min: u32,
}

#[derive(Deserialize, Clone, Debug)]
pub struct CorsConfig {
    /// Allowed origins. Empty = restrictive (same-origin only).
    /// Use `["*"]` to allow all origins.
    #[serde(default)]
    pub allowed_origins: Vec<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct LoggingConfig {
    /// `"text"` or `"json"`.
    #[serde(default = "default_log_format")]
    pub format: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct TlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
}

// ── Defaults ──

fn default_bind() -> SocketAddr {
    "0.0.0.0:3000".parse().unwrap()
}

fn default_max_peers() -> usize {
    50
}

fn default_max_per_stream() -> usize {
    20
}

fn default_log_format() -> String {
    "text".into()
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_peers: default_max_peers(),
            max_per_stream: default_max_per_stream(),
            create_per_min: 0,
        }
    }
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self {
            allowed_origins: vec![],
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            format: default_log_format(),
        }
    }
}

impl Config {
    /// Load from `config.toml` in the current directory, or path from
    /// `CONFIG_PATH` environment variable.
    pub fn load() -> Self {
        let path = std::env::var("CONFIG_PATH")
            .unwrap_or_else(|_| "config.toml".into());

        let content = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read config file {path}: {e}"));

        toml::from_str(&content)
            .unwrap_or_else(|e| panic!("invalid config file {path}: {e}"))
    }

    /// Look up a stream config by ID.
    pub fn find_stream(&self, id: &str) -> Option<&StreamConfig> {
        self.streams.iter().find(|s| s.id == id)
    }

    /// Default stream ID (first configured stream).
    pub fn default_stream_id(&self) -> &str {
        &self.streams.first().expect("no streams in config").id
    }
}
