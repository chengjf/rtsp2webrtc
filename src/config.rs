use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub server: ServerConfig,
    pub streams: Vec<StreamConfig>,
    #[serde(default)]
    pub limits: LimitsConfig,
    pub tls: Option<TlsConfig>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind_addr: SocketAddr,
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

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_peers: default_max_peers(),
            max_per_stream: default_max_per_stream(),
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
