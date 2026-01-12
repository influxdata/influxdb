use clap::Parser;
use secrecy::{ExposeSecret, Secret};
use std::{error::Error, path::PathBuf};
use url::Url;

use crate::commands::common::Format;

mod system;
use system::SystemConfig;

#[derive(Debug, Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Debug, Parser)]
pub enum SubCommand {
    /// List databases
    Databases(DatabaseConfig),

    /// List tokens
    Tokens(ShowTokensConfig),

    /// List plugins
    Plugins(PluginsConfig),

    /// Display system table data.
    System(SystemConfig),

    /// Show retention policies with effective retention for each table
    Retention(RetentionConfig),
}

#[derive(Debug, Parser)]
pub struct ShowTokensConfig {
    /// The host URL of the running InfluxDB 3 Enterprise server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    host_url: Url,

    /// The token for authentication with the InfluxDB 3 Enterprise server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN", hide_env_values = true)]
    auth_token: Option<Secret<String>>,

    /// The format in which to output the list of databases
    #[clap(value_enum, long = "format", default_value = "pretty")]
    output_format: Format,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

#[derive(Debug, Parser)]
pub struct PluginsConfig {
    /// The host URL of the running InfluxDB 3 Core server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    host_url: Url,

    /// The token for authentication with the InfluxDB 3 Core server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN", hide_env_values = true)]
    auth_token: Option<Secret<String>>,

    /// The format in which to output the list of plugins
    #[clap(value_enum, long = "format", default_value = "pretty")]
    output_format: Format,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

#[derive(Debug, Parser)]
pub struct RetentionConfig {
    /// The host URL of the running InfluxDB 3 server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    host_url: Url,

    /// The token for authentication with the InfluxDB 3 server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN", hide_env_values = true)]
    auth_token: Option<Secret<String>>,

    /// Optional database name to filter results
    #[clap(long = "database")]
    database: Option<String>,

    /// The format in which to output the retention policies
    #[clap(value_enum, long = "format", default_value = "pretty")]
    output_format: Format,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

#[derive(Debug, Parser)]
pub struct DatabaseConfig {
    /// The host URL of the running InfluxDB 3 Core server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    host_url: Url,

    /// The token for authentication with the InfluxDB 3 Core server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN", hide_env_values = true)]
    auth_token: Option<Secret<String>>,

    /// Include databases that were marked as deleted in the output
    #[clap(long = "show-deleted", default_value = "false")]
    show_deleted: bool,

    /// The format in which to output the list of databases
    #[clap(value_enum, long = "format", default_value = "pretty")]
    output_format: Format,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

pub(crate) async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    match config.cmd {
        SubCommand::Databases(DatabaseConfig {
            host_url,
            auth_token,
            show_deleted,
            output_format,
            ca_cert,
            tls_no_verify,
        }) => {
            let mut client = influxdb3_client::Client::new(host_url, ca_cert, tls_no_verify)?;

            if let Some(t) = auth_token {
                client = client.with_auth_token(t.expose_secret());
            }

            let resp_bytes = client
                .api_v3_configure_db_show()
                .with_format(output_format.into())
                .with_show_deleted(show_deleted)
                .send()
                .await?;

            println!("{}", std::str::from_utf8(&resp_bytes)?);
        }
        SubCommand::Plugins(PluginsConfig {
            host_url,
            auth_token,
            output_format,
            ca_cert,
            tls_no_verify,
        }) => {
            let mut client = influxdb3_client::Client::new(host_url, ca_cert, tls_no_verify)?;

            if let Some(t) = auth_token {
                client = client.with_auth_token(t.expose_secret());
            }

            let resp_bytes = client
                .api_v3_query_sql("_internal", "SELECT * FROM system.plugin_files")
                .format(output_format.into())
                .send()
                .await?;

            println!("{}", std::str::from_utf8(&resp_bytes)?);
        }
        SubCommand::System(cfg) => system::command(cfg).await?,
        SubCommand::Tokens(show_tokens_config) => {
            let mut client = influxdb3_client::Client::new(
                show_tokens_config.host_url.clone(),
                show_tokens_config.ca_cert,
                show_tokens_config.tls_no_verify,
            )?;

            if let Some(t) = show_tokens_config.auth_token {
                client = client.with_auth_token(t.expose_secret());
            }
            let resp_bytes = client
                .api_v3_query_sql("_internal", "select * from system.tokens")
                .format(show_tokens_config.output_format.into())
                .send()
                .await?;
            println!("{}", std::str::from_utf8(&resp_bytes)?);
        }
        SubCommand::Retention(retention_config) => {
            show_retention_policies(retention_config).await?;
        }
    }

    Ok(())
}

/// Show retention policies for databases.
/// This queries system.databases to show database-level retention periods.
async fn show_retention_policies(config: RetentionConfig) -> Result<(), Box<dyn Error>> {
    let mut client = influxdb3_client::Client::new(
        config.host_url.clone(),
        config.ca_cert,
        config.tls_no_verify,
    )?;

    if let Some(t) = config.auth_token {
        client = client.with_auth_token(t.expose_secret());
    }

    // Build the SQL query to show database retention policies
    let query = if let Some(db) = &config.database {
        format!(
            "SELECT \
                database_name, \
                CASE \
                    WHEN retention_period_ns IS NULL THEN 'infinite' \
                    WHEN retention_period_ns % 86400000000000 = 0 THEN \
                        CAST(retention_period_ns / 86400000000000 AS VARCHAR) || 'd' \
                    WHEN retention_period_ns % 3600000000000 = 0 THEN \
                        CAST(retention_period_ns / 3600000000000 AS VARCHAR) || 'h' \
                    WHEN retention_period_ns % 60000000000 = 0 THEN \
                        CAST(retention_period_ns / 60000000000 AS VARCHAR) || 'm' \
                    ELSE CAST(retention_period_ns / 1000000000 AS VARCHAR) || 's' \
                END as retention_period \
            FROM system.databases \
            WHERE database_name = '{}' \
            ORDER BY database_name",
            db.replace('\'', "''") // Escape single quotes
        )
    } else {
        "SELECT \
            database_name, \
            CASE \
                WHEN retention_period_ns IS NULL THEN 'infinite' \
                WHEN retention_period_ns % 86400000000000 = 0 THEN \
                    CAST(retention_period_ns / 86400000000000 AS VARCHAR) || 'd' \
                WHEN retention_period_ns % 3600000000000 = 0 THEN \
                    CAST(retention_period_ns / 3600000000000 AS VARCHAR) || 'h' \
                WHEN retention_period_ns % 60000000000 = 0 THEN \
                    CAST(retention_period_ns / 60000000000 AS VARCHAR) || 'm' \
                ELSE CAST(retention_period_ns / 1000000000 AS VARCHAR) || 's' \
            END as retention_period \
        FROM system.databases \
        ORDER BY database_name"
            .to_string()
    };

    let resp_bytes = client
        .api_v3_query_sql("_internal", query)
        .format(config.output_format.into())
        .send()
        .await?;

    println!("{}", std::str::from_utf8(&resp_bytes)?);
    Ok(())
}
