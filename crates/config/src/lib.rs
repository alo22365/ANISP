//! Environment-backed application configuration.

use std::{
    env::{self, VarError},
    error::Error,
    fmt,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
};

const DEFAULT_SERVER_HOST: &str = "0.0.0.0";
const DEFAULT_SERVER_PORT: u16 = 3000;
const DEFAULT_LOG_FILTER: &str = "info";
const DEFAULT_CLICKHOUSE_URL: &str = "http://127.0.0.1:8123";
const DEFAULT_CLICKHOUSE_DATABASE: &str = "anisp";
const DEFAULT_CLICKHOUSE_USER: &str = "anisp";
const DEFAULT_CLICKHOUSE_PASSWORD: &str = "anisp";
const DEFAULT_CLICKHOUSE_REQUEST_TIMEOUT_MS: u64 = 3000;
const DEFAULT_POSTGRES_URL: &str = "postgres://anisp:anisp@127.0.0.1:5432/anisp";
const DEFAULT_POSTGRES_MAX_CONNECTIONS: u32 = 10;
const DEFAULT_POSTGRES_REQUEST_TIMEOUT_MS: u64 = 3000;
const DEFAULT_SUPPORT_OUTBOX_POLL_INTERVAL_MS: u64 = 1000;
const DEFAULT_SUPPORT_OUTBOX_BATCH_SIZE: u32 = 100;
const DEFAULT_SUPPORT_OUTBOX_CLAIM_LEASE_MS: u64 = 30_000;
const DEFAULT_SUPPORT_OUTBOX_MAX_ATTEMPTS: u32 = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub observability: ObservabilityConfig,
    pub clickhouse: ClickHouseConfig,
    pub postgres: PostgresConfig,
    pub support_outbox: SupportOutboxConfig,
    pub git_outbox: GitOutboxConfig,
    pub context_git: ContextGitConfig,
}

impl AppConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut config = Self::from_values(
            read_env("ANISP_SERVER_HOST")?,
            read_env("ANISP_SERVER_PORT")?,
            read_env("RUST_LOG")?,
            read_env("ANISP_CLICKHOUSE_URL")?,
            read_env("ANISP_CLICKHOUSE_DATABASE")?,
            read_env("ANISP_CLICKHOUSE_USER")?,
            read_env("ANISP_CLICKHOUSE_PASSWORD")?,
            read_env("ANISP_CLICKHOUSE_REQUEST_TIMEOUT_MS")?,
            read_env("ANISP_POSTGRES_URL")?,
            read_env("ANISP_POSTGRES_MAX_CONNECTIONS")?,
            read_env("ANISP_POSTGRES_REQUEST_TIMEOUT_MS")?,
            read_env("ANISP_SUPPORT_OUTBOX_POLL_INTERVAL_MS")?,
            read_env("ANISP_SUPPORT_OUTBOX_BATCH_SIZE")?,
            read_env("ANISP_SUPPORT_OUTBOX_CLAIM_LEASE_MS")?,
            read_env("ANISP_SUPPORT_OUTBOX_MAX_ATTEMPTS")?,
        )?;
        config.git_outbox = GitOutboxConfig::from_values(
            read_env("ANISP_GIT_OUTBOX_POLL_INTERVAL_MS")?,
            read_env("ANISP_GIT_OUTBOX_BATCH_SIZE")?,
            read_env("ANISP_GIT_OUTBOX_CLAIM_LEASE_MS")?,
            read_env("ANISP_GIT_OUTBOX_MAX_ATTEMPTS")?,
        )?;
        config.context_git = ContextGitConfig::from_values(
            read_env("ANISP_CONTEXT_GIT_LOOKBACK_HOURS")?,
            read_env("ANISP_CONTEXT_GIT_MAX_COMMITS")?,
        )?;
        Ok(config)
    }

    fn from_values(
        host: Option<String>,
        port: Option<String>,
        log_filter: Option<String>,
        clickhouse_url: Option<String>,
        clickhouse_database: Option<String>,
        clickhouse_user: Option<String>,
        clickhouse_password: Option<String>,
        clickhouse_request_timeout_ms: Option<String>,
        postgres_url: Option<String>,
        postgres_max_connections: Option<String>,
        postgres_request_timeout_ms: Option<String>,
        support_outbox_poll_interval_ms: Option<String>,
        support_outbox_batch_size: Option<String>,
        support_outbox_claim_lease_ms: Option<String>,
        support_outbox_max_attempts: Option<String>,
    ) -> Result<Self, ConfigError> {
        let host = host.unwrap_or_else(|| DEFAULT_SERVER_HOST.to_owned());
        let host = IpAddr::from_str(&host).map_err(|_| ConfigError::InvalidHost(host))?;

        let port = match port {
            Some(value) => value
                .parse::<u16>()
                .map_err(|_| ConfigError::InvalidPort(value))?,
            None => DEFAULT_SERVER_PORT,
        };

        let log_filter = log_filter.unwrap_or_else(|| DEFAULT_LOG_FILTER.to_owned());
        if log_filter.trim().is_empty() {
            return Err(ConfigError::EmptyLogFilter);
        }

        let clickhouse_url = value_or_default(
            "ANISP_CLICKHOUSE_URL",
            clickhouse_url,
            DEFAULT_CLICKHOUSE_URL,
        )?;
        let clickhouse_database = value_or_default(
            "ANISP_CLICKHOUSE_DATABASE",
            clickhouse_database,
            DEFAULT_CLICKHOUSE_DATABASE,
        )?;
        let clickhouse_user = value_or_default(
            "ANISP_CLICKHOUSE_USER",
            clickhouse_user,
            DEFAULT_CLICKHOUSE_USER,
        )?;
        let clickhouse_password =
            clickhouse_password.unwrap_or_else(|| DEFAULT_CLICKHOUSE_PASSWORD.to_owned());
        let clickhouse_request_timeout_ms = match clickhouse_request_timeout_ms {
            Some(value) => value
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or(ConfigError::InvalidClickHouseRequestTimeout(value))?,
            None => DEFAULT_CLICKHOUSE_REQUEST_TIMEOUT_MS,
        };
        let postgres_url =
            value_or_default("ANISP_POSTGRES_URL", postgres_url, DEFAULT_POSTGRES_URL)?;
        let postgres_max_connections = match postgres_max_connections {
            Some(value) => value
                .parse::<u32>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or(ConfigError::InvalidPostgresMaxConnections(value))?,
            None => DEFAULT_POSTGRES_MAX_CONNECTIONS,
        };
        let postgres_request_timeout_ms = match postgres_request_timeout_ms {
            Some(value) => value
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or(ConfigError::InvalidPostgresRequestTimeout(value))?,
            None => DEFAULT_POSTGRES_REQUEST_TIMEOUT_MS,
        };
        let support_outbox_poll_interval_ms = match support_outbox_poll_interval_ms {
            Some(value) => value
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or(ConfigError::InvalidSupportOutboxPollInterval(value))?,
            None => DEFAULT_SUPPORT_OUTBOX_POLL_INTERVAL_MS,
        };
        let support_outbox_batch_size = match support_outbox_batch_size {
            Some(value) => value
                .parse::<u32>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or(ConfigError::InvalidSupportOutboxBatchSize(value))?,
            None => DEFAULT_SUPPORT_OUTBOX_BATCH_SIZE,
        };
        let support_outbox_claim_lease_ms = match support_outbox_claim_lease_ms {
            Some(value) => value
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or(ConfigError::InvalidSupportOutboxClaimLease(value))?,
            None => DEFAULT_SUPPORT_OUTBOX_CLAIM_LEASE_MS,
        };
        let support_outbox_max_attempts = match support_outbox_max_attempts {
            Some(value) => value
                .parse::<u32>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or(ConfigError::InvalidSupportOutboxMaxAttempts(value))?,
            None => DEFAULT_SUPPORT_OUTBOX_MAX_ATTEMPTS,
        };

        Ok(Self {
            server: ServerConfig { host, port },
            observability: ObservabilityConfig { log_filter },
            clickhouse: ClickHouseConfig {
                url: clickhouse_url,
                database: clickhouse_database,
                user: clickhouse_user,
                password: clickhouse_password,
                request_timeout_ms: clickhouse_request_timeout_ms,
            },
            postgres: PostgresConfig {
                url: postgres_url,
                max_connections: postgres_max_connections,
                request_timeout_ms: postgres_request_timeout_ms,
            },
            support_outbox: SupportOutboxConfig {
                poll_interval_ms: support_outbox_poll_interval_ms,
                batch_size: support_outbox_batch_size,
                claim_lease_ms: support_outbox_claim_lease_ms,
                max_attempts: support_outbox_max_attempts,
            },
            git_outbox: GitOutboxConfig::default(),
            context_git: ContextGitConfig::default(),
        })
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                port: DEFAULT_SERVER_PORT,
            },
            observability: ObservabilityConfig {
                log_filter: DEFAULT_LOG_FILTER.to_owned(),
            },
            clickhouse: ClickHouseConfig::default(),
            postgres: PostgresConfig::default(),
            support_outbox: SupportOutboxConfig::default(),
            git_outbox: GitOutboxConfig::default(),
            context_git: ContextGitConfig::default(),
        }
    }
}

/// Validated limits for fixed, case-creation-anchored Git enrichment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextGitConfig {
    lookback_hours: u32,
    max_commits: u32,
}
impl ContextGitConfig {
    pub fn new(lookback_hours: u32, max_commits: u32) -> Result<Self, ConfigError> {
        if !(1..=720).contains(&lookback_hours) {
            return Err(ConfigError::InvalidContextGitSetting(
                "ANISP_CONTEXT_GIT_LOOKBACK_HOURS",
            ));
        }
        if !(1..=500).contains(&max_commits) {
            return Err(ConfigError::InvalidContextGitSetting(
                "ANISP_CONTEXT_GIT_MAX_COMMITS",
            ));
        }
        Ok(Self {
            lookback_hours,
            max_commits,
        })
    }
    fn from_values(lookback: Option<String>, commits: Option<String>) -> Result<Self, ConfigError> {
        let defaults = Self::default();
        let parse = |value: Option<String>, default, name| {
            value.map_or(Ok(default), |v| {
                v.parse::<u32>()
                    .map_err(|_| ConfigError::InvalidContextGitSetting(name))
            })
        };
        Self::new(
            parse(
                lookback,
                defaults.lookback_hours,
                "ANISP_CONTEXT_GIT_LOOKBACK_HOURS",
            )?,
            parse(
                commits,
                defaults.max_commits,
                "ANISP_CONTEXT_GIT_MAX_COMMITS",
            )?,
        )
    }
    pub fn lookback_hours(&self) -> u32 {
        self.lookback_hours
    }
    pub fn max_commits(&self) -> u32 {
        self.max_commits
    }
}
impl Default for ContextGitConfig {
    fn default() -> Self {
        Self {
            lookback_hours: 24,
            max_commits: 50,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitOutboxConfig {
    poll_interval_ms: u64,
    batch_size: u32,
    claim_lease_ms: u64,
    max_attempts: u32,
}
impl GitOutboxConfig {
    fn from_values(
        poll: Option<String>,
        batch: Option<String>,
        lease: Option<String>,
        attempts: Option<String>,
    ) -> Result<Self, ConfigError> {
        fn positive(
            name: &'static str,
            value: Option<String>,
            default: u64,
            maximum: u64,
        ) -> Result<u64, ConfigError> {
            match value {
                None => Ok(default),
                Some(value) => value
                    .parse::<u64>()
                    .ok()
                    .filter(|v| *v > 0 && *v <= maximum)
                    .ok_or(ConfigError::InvalidGitOutboxSetting(name)),
            }
        }
        Ok(Self {
            poll_interval_ms: positive(
                "ANISP_GIT_OUTBOX_POLL_INTERVAL_MS",
                poll,
                1000,
                86_400_000,
            )?,
            batch_size: positive("ANISP_GIT_OUTBOX_BATCH_SIZE", batch, 100, 500)? as u32,
            claim_lease_ms: positive("ANISP_GIT_OUTBOX_CLAIM_LEASE_MS", lease, 30_000, 86_400_000)?,
            max_attempts: positive(
                "ANISP_GIT_OUTBOX_MAX_ATTEMPTS",
                attempts,
                10,
                i32::MAX as u64,
            )? as u32,
        })
    }
    pub fn poll_interval_ms(&self) -> u64 {
        self.poll_interval_ms
    }
    pub fn batch_size(&self) -> u32 {
        self.batch_size
    }
    pub fn claim_lease_ms(&self) -> u64 {
        self.claim_lease_ms
    }
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }
}
impl Default for GitOutboxConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 1000,
            batch_size: 100,
            claim_lease_ms: 30_000,
            max_attempts: 10,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportOutboxConfig {
    poll_interval_ms: u64,
    batch_size: u32,
    claim_lease_ms: u64,
    max_attempts: u32,
}

impl SupportOutboxConfig {
    pub fn poll_interval_ms(&self) -> u64 {
        self.poll_interval_ms
    }

    pub fn batch_size(&self) -> u32 {
        self.batch_size
    }

    pub fn claim_lease_ms(&self) -> u64 {
        self.claim_lease_ms
    }

    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }
}

impl Default for SupportOutboxConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: DEFAULT_SUPPORT_OUTBOX_POLL_INTERVAL_MS,
            batch_size: DEFAULT_SUPPORT_OUTBOX_BATCH_SIZE,
            claim_lease_ms: DEFAULT_SUPPORT_OUTBOX_CLAIM_LEASE_MS,
            max_attempts: DEFAULT_SUPPORT_OUTBOX_MAX_ATTEMPTS,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct PostgresConfig {
    url: String,
    max_connections: u32,
    request_timeout_ms: u64,
}

impl PostgresConfig {
    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn max_connections(&self) -> u32 {
        self.max_connections
    }

    pub fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            url: DEFAULT_POSTGRES_URL.to_owned(),
            max_connections: DEFAULT_POSTGRES_MAX_CONNECTIONS,
            request_timeout_ms: DEFAULT_POSTGRES_REQUEST_TIMEOUT_MS,
        }
    }
}

impl fmt::Debug for PostgresConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresConfig")
            .field("url", &"[redacted]")
            .field("max_connections", &self.max_connections)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct ClickHouseConfig {
    url: String,
    database: String,
    user: String,
    password: String,
    request_timeout_ms: u64,
}

impl ClickHouseConfig {
    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub fn password(&self) -> &str {
        &self.password
    }

    pub fn request_timeout_ms(&self) -> u64 {
        self.request_timeout_ms
    }
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: DEFAULT_CLICKHOUSE_URL.to_owned(),
            database: DEFAULT_CLICKHOUSE_DATABASE.to_owned(),
            user: DEFAULT_CLICKHOUSE_USER.to_owned(),
            password: DEFAULT_CLICKHOUSE_PASSWORD.to_owned(),
            request_timeout_ms: DEFAULT_CLICKHOUSE_REQUEST_TIMEOUT_MS,
        }
    }
}

impl fmt::Debug for ClickHouseConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClickHouseConfig")
            .field("url", &self.url)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &"[redacted]")
            .field("request_timeout_ms", &self.request_timeout_ms)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    host: IpAddr,
    port: u16,
}

impl ServerConfig {
    pub fn host(&self) -> IpAddr {
        self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservabilityConfig {
    log_filter: String,
}

impl ObservabilityConfig {
    pub fn log_filter(&self) -> &str {
        &self.log_filter
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    NonUnicodeEnvironmentVariable(&'static str),
    InvalidHost(String),
    InvalidPort(String),
    EmptyLogFilter,
    EmptyEnvironmentVariable(&'static str),
    InvalidClickHouseRequestTimeout(String),
    InvalidPostgresMaxConnections(String),
    InvalidPostgresRequestTimeout(String),
    InvalidSupportOutboxPollInterval(String),
    InvalidSupportOutboxBatchSize(String),
    InvalidSupportOutboxClaimLease(String),
    InvalidSupportOutboxMaxAttempts(String),
    InvalidGitOutboxSetting(&'static str),
    InvalidContextGitSetting(&'static str),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidGitOutboxSetting(name) => write!(formatter, "invalid {name}"),
            Self::InvalidContextGitSetting(name) => write!(formatter, "invalid {name}"),
            Self::NonUnicodeEnvironmentVariable(name) => {
                write!(
                    formatter,
                    "environment variable {name} is not valid Unicode"
                )
            }
            Self::InvalidHost(value) => write!(formatter, "invalid ANISP_SERVER_HOST: {value}"),
            Self::InvalidPort(value) => write!(formatter, "invalid ANISP_SERVER_PORT: {value}"),
            Self::EmptyLogFilter => formatter.write_str("RUST_LOG cannot be empty"),
            Self::EmptyEnvironmentVariable(name) => {
                write!(formatter, "environment variable {name} cannot be empty")
            }
            Self::InvalidClickHouseRequestTimeout(value) => {
                write!(
                    formatter,
                    "invalid ANISP_CLICKHOUSE_REQUEST_TIMEOUT_MS: {value}"
                )
            }
            Self::InvalidPostgresMaxConnections(value) => {
                write!(formatter, "invalid ANISP_POSTGRES_MAX_CONNECTIONS: {value}")
            }
            Self::InvalidPostgresRequestTimeout(value) => {
                write!(
                    formatter,
                    "invalid ANISP_POSTGRES_REQUEST_TIMEOUT_MS: {value}"
                )
            }
            Self::InvalidSupportOutboxPollInterval(value) => {
                write!(
                    formatter,
                    "invalid ANISP_SUPPORT_OUTBOX_POLL_INTERVAL_MS: {value}"
                )
            }
            Self::InvalidSupportOutboxBatchSize(value) => {
                write!(
                    formatter,
                    "invalid ANISP_SUPPORT_OUTBOX_BATCH_SIZE: {value}"
                )
            }
            Self::InvalidSupportOutboxClaimLease(value) => {
                write!(
                    formatter,
                    "invalid ANISP_SUPPORT_OUTBOX_CLAIM_LEASE_MS: {value}"
                )
            }
            Self::InvalidSupportOutboxMaxAttempts(value) => {
                write!(
                    formatter,
                    "invalid ANISP_SUPPORT_OUTBOX_MAX_ATTEMPTS: {value}"
                )
            }
        }
    }
}

impl Error for ConfigError {}

fn read_env(name: &'static str) -> Result<Option<String>, ConfigError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => Err(ConfigError::NonUnicodeEnvironmentVariable(name)),
    }
}

fn value_or_default(
    name: &'static str,
    value: Option<String>,
    default: &str,
) -> Result<String, ConfigError> {
    let value = value.unwrap_or_else(|| default.to_owned());
    if value.trim().is_empty() {
        return Err(ConfigError::EmptyEnvironmentVariable(name));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_git_defaults_custom_maximum_and_invalid_settings() {
        assert_eq!(
            ContextGitConfig::from_values(None, None).unwrap(),
            ContextGitConfig::default()
        );
        assert_eq!(ContextGitConfig::default().lookback_hours(), 24);
        assert_eq!(ContextGitConfig::default().max_commits(), 50);
        let custom = ContextGitConfig::from_values(Some("12".into()), Some("25".into())).unwrap();
        assert_eq!((custom.lookback_hours(), custom.max_commits()), (12, 25));
        assert!(ContextGitConfig::new(720, 500).is_ok());
        for value in ["0", "-1", "721", "invalid", "", "4294967296"] {
            assert!(ContextGitConfig::from_values(Some(value.into()), None).is_err());
        }
        for value in ["0", "-1", "501", "invalid", "", "4294967296"] {
            assert!(ContextGitConfig::from_values(None, Some(value.into())).is_err());
        }
    }

    #[test]
    fn git_outbox_settings_load_and_validate() {
        assert_eq!(
            GitOutboxConfig::from_values(None, None, None, None).unwrap(),
            GitOutboxConfig::default()
        );
        let configured = GitOutboxConfig::from_values(
            Some("250".into()),
            Some("500".into()),
            Some("60000".into()),
            Some("5".into()),
        )
        .unwrap();
        assert_eq!(
            (
                configured.poll_interval_ms(),
                configured.batch_size(),
                configured.claim_lease_ms(),
                configured.max_attempts()
            ),
            (250, 500, 60000, 5)
        );
        for value in ["0", "501", "invalid"] {
            assert!(GitOutboxConfig::from_values(None, Some(value.into()), None, None).is_err());
        }
        assert!(GitOutboxConfig::from_values(Some("0".into()), None, None, None).is_err());
        assert!(GitOutboxConfig::from_values(None, None, Some("0".into()), None).is_err());
        assert!(GitOutboxConfig::from_values(None, None, None, Some("2147483648".into())).is_err());
    }

    #[test]
    fn uses_defaults_when_values_are_absent() {
        let config = AppConfig::from_values(
            None, None, None, None, None, None, None, None, None, None, None, None, None, None,
            None,
        )
        .unwrap();

        assert_eq!(config, AppConfig::default());
        assert_eq!(
            config.server.socket_addr(),
            SocketAddr::from(([0, 0, 0, 0], 3000))
        );
        assert_eq!(config.clickhouse, ClickHouseConfig::default());
        assert_eq!(config.clickhouse.request_timeout_ms(), 3000);
        assert_eq!(config.postgres, PostgresConfig::default());
        assert_eq!(config.support_outbox, SupportOutboxConfig::default());
    }

    #[test]
    fn loads_supplied_values() {
        let config = AppConfig::from_values(
            Some("127.0.0.1".to_owned()),
            Some("8080".to_owned()),
            Some("debug,anisp_server=trace".to_owned()),
            Some("http://clickhouse:8123".to_owned()),
            Some("custom_database".to_owned()),
            Some("custom_user".to_owned()),
            Some("custom_password".to_owned()),
            Some("1500".to_owned()),
            Some("postgres://db-user:secret@postgres:5432/custom".to_owned()),
            Some("20".to_owned()),
            Some("2500".to_owned()),
            Some("750".to_owned()),
            Some("50".to_owned()),
            Some("5000".to_owned()),
            Some("7".to_owned()),
        )
        .unwrap();

        assert_eq!(config.server.host(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(config.server.port(), 8080);
        assert_eq!(
            config.observability.log_filter(),
            "debug,anisp_server=trace"
        );
        assert_eq!(config.clickhouse.url(), "http://clickhouse:8123");
        assert_eq!(config.clickhouse.database(), "custom_database");
        assert_eq!(config.clickhouse.user(), "custom_user");
        assert_eq!(config.clickhouse.password(), "custom_password");
        assert_eq!(config.clickhouse.request_timeout_ms(), 1500);
        assert_eq!(
            config.postgres.url(),
            "postgres://db-user:secret@postgres:5432/custom"
        );
        assert_eq!(config.postgres.max_connections(), 20);
        assert_eq!(config.postgres.request_timeout_ms(), 2500);
        assert_eq!(config.support_outbox.poll_interval_ms(), 750);
        assert_eq!(config.support_outbox.batch_size(), 50);
        assert_eq!(config.support_outbox.claim_lease_ms(), 5000);
        assert_eq!(config.support_outbox.max_attempts(), 7);
    }

    #[test]
    fn rejects_invalid_port() {
        let error = AppConfig::from_values(
            None,
            Some("not-a-port".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap_err();

        assert_eq!(error, ConfigError::InvalidPort("not-a-port".to_owned()));
    }

    #[test]
    fn rejects_empty_clickhouse_database() {
        let error = AppConfig::from_values(
            None,
            None,
            None,
            None,
            Some("  ".to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap_err();

        assert_eq!(
            error,
            ConfigError::EmptyEnvironmentVariable("ANISP_CLICKHOUSE_DATABASE")
        );
    }

    #[test]
    fn rejects_invalid_clickhouse_timeout() {
        for value in ["0", "abc"] {
            let error = AppConfig::from_values(
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(value.to_owned()),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap_err();
            assert_eq!(
                error,
                ConfigError::InvalidClickHouseRequestTimeout(value.to_owned())
            );
        }
    }

    #[test]
    fn rejects_invalid_postgres_pool_size() {
        for value in ["0", "abc"] {
            let error = AppConfig::from_values(
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(value.to_owned()),
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap_err();
            assert_eq!(
                error,
                ConfigError::InvalidPostgresMaxConnections(value.to_owned())
            );
        }
    }

    #[test]
    fn rejects_invalid_postgres_timeout() {
        for value in ["0", "abc"] {
            let error = AppConfig::from_values(
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(value.to_owned()),
                None,
                None,
                None,
                None,
            )
            .unwrap_err();
            assert_eq!(
                error,
                ConfigError::InvalidPostgresRequestTimeout(value.to_owned())
            );
        }
    }

    #[test]
    fn rejects_invalid_support_outbox_settings() {
        for value in ["0", "abc"] {
            let poll_error = AppConfig::from_values(
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(value.to_owned()),
                None,
                None,
                None,
            )
            .unwrap_err();
            assert_eq!(
                poll_error,
                ConfigError::InvalidSupportOutboxPollInterval(value.to_owned())
            );

            let batch_error = AppConfig::from_values(
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(value.to_owned()),
                None,
                None,
            )
            .unwrap_err();
            assert_eq!(
                batch_error,
                ConfigError::InvalidSupportOutboxBatchSize(value.to_owned())
            );

            let lease_error = AppConfig::from_values(
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(value.to_owned()),
                None,
            )
            .unwrap_err();
            assert_eq!(
                lease_error,
                ConfigError::InvalidSupportOutboxClaimLease(value.to_owned())
            );

            let attempts_error = AppConfig::from_values(
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(value.to_owned()),
            )
            .unwrap_err();
            assert_eq!(
                attempts_error,
                ConfigError::InvalidSupportOutboxMaxAttempts(value.to_owned())
            );
        }
    }
}
