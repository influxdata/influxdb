pub mod token;

use crate::commands::common::{DataType, InfluxDb3Config, SeparatedKeyValue, parse_key_val};
use hashbrown::HashMap;
use humantime::Duration;
use influxdb3_catalog::log::ErrorBehavior;
use influxdb3_catalog::log::TriggerSettings;
use influxdb3_catalog::log::TriggerSpecificationDefinition;
use influxdb3_client::Client;
use influxdb3_types::http::LastCacheSize;
use influxdb3_types::http::LastCacheTtl;
use owo_colors::OwoColorize;
use secrecy::ExposeSecret;
use secrecy::Secret;
use serde_json::json;
use std::error::Error;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str;
use token::CreateTokenConfig;
use token::handle_token_creation_with_config;
use tokio::fs;
use url::Url;
use walkdir::WalkDir;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

impl Config {
    fn get_client(&self) -> Result<Client, Box<dyn Error>> {
        let (host_url, auth_token, ca_cert, tls_no_verify) = match &self.cmd {
            SubCommand::Database(DatabaseConfig {
                host_url,
                auth_token,
                ca_cert,
                tls_no_verify,
                ..
            })
            | SubCommand::LastCache(LastCacheConfig {
                ca_cert,
                tls_no_verify,
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            })
            | SubCommand::DistinctCache(DistinctCacheConfig {
                ca_cert,
                tls_no_verify,
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            })
            | SubCommand::Table(TableConfig {
                ca_cert,
                tls_no_verify,
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            })
            | SubCommand::Trigger(TriggerConfig {
                ca_cert,
                tls_no_verify,
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            }) => (host_url, auth_token, ca_cert, tls_no_verify),
            SubCommand::Token(create_token_config) => {
                let host_settings = create_token_config.get_connection_settings()?;
                // We need to return references, so we'll handle this differently
                return Ok({
                    let mut client = Client::new(
                        host_settings.host_url.clone(),
                        host_settings.ca_cert.clone(),
                        host_settings.tls_no_verify,
                    )?;
                    if let Some(token) = &host_settings.auth_token {
                        client = client.with_auth_token(token.expose_secret());
                    }
                    client
                });
            }
        };

        let mut client = Client::new(host_url.clone(), ca_cert.clone(), *tls_no_verify)?;
        if let Some(token) = &auth_token {
            client = client.with_auth_token(token.expose_secret());
        }
        Ok(client)
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCommand {
    /// Create a new database
    Database(DatabaseConfig),
    /// Create a new last value cache
    #[clap(name = "last_cache")]
    LastCache(LastCacheConfig),
    /// Create a new distinct value cache
    #[clap(name = "distinct_cache")]
    DistinctCache(DistinctCacheConfig),
    /// Create a new table in a database
    Table(TableConfig),
    /// Create a new auth token
    Token(CreateTokenConfig),
    /// Create a new trigger for the processing engine that executes a plugin on either WAL rows, scheduled tasks, or requests to the serve at `/api/v3/engine/<path>`
    Trigger(TriggerConfig),
}

#[derive(Debug, clap::Args)]
pub struct DatabaseConfig {
    /// The host URL of the running InfluxDB 3 Core server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    pub host_url: Url,

    /// The token for authentication with the InfluxDB 3 Core server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN", hide_env_values = true)]
    pub auth_token: Option<Secret<String>>,

    /// The name of the database to create. Valid database names are
    /// alphanumeric with - and _ allowed and starts with a letter or number
    #[clap(env = "INFLUXDB3_DATABASE_NAME", required = true)]
    pub database_name: String,

    #[clap(long = "retention-period")]
    /// The retention period for the database as a human-readable duration, e.g., "30d", "24h"
    pub retention_period: Option<Duration>,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

#[derive(Debug, clap::Args)]
pub struct LastCacheConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The table name for which the cache is being created
    #[clap(short = 't', long = "table")]
    table: String,

    /// Which columns in the table to use as keys in the cache. This is a comma separated list
    ///
    /// Example: --key-columns "foo,bar,baz"
    #[clap(long = "key-columns", value_delimiter = ',')]
    key_columns: Option<Vec<String>>,

    /// Which columns in the table to store as values in the cache. This is a comma separated list
    ///
    /// Example: --value-columns "foo,bar,baz"
    #[clap(long = "value-columns", value_delimiter = ',')]
    value_columns: Option<Vec<String>>,

    /// The number of entries per unique key column combination the cache will store
    ///
    /// Higher values can increase memory usage significantly
    #[clap(long = "count", default_value = "1")]
    count: Option<LastCacheSize>,

    /// The time-to-live (TTL) for entries in a cache. This uses a humantime form: "10s", "1min 30sec", "3 hours"
    ///
    /// See the parse_duration docs for more details about acceptable forms:
    /// <https://docs.rs/humantime/2.1.0/humantime/fn.parse_duration.html>
    #[clap(long = "ttl", default_value = "4 hours")]
    ttl: Option<Duration>,

    /// Give a name for the cache.
    #[clap(required = false)]
    cache_name: Option<String>,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

#[derive(Debug, clap::Args)]
pub struct DistinctCacheConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The table name for which the cache is being created
    #[clap(short = 't', long = "table")]
    table: String,

    /// Which columns in the table to cache distinct values for, as a comma-separated list of the
    /// column names.
    ///
    /// The cache is a hieararchical structure, with a level for each column specified; the order
    /// specified here will determine the order of the levels from top-to-bottom of the cache
    /// hierarchy.
    #[clap(long = "columns", value_delimiter = ',')]
    columns: Vec<String>,

    /// The maximum number of distinct value combinations to hold in the cache
    #[clap(long = "max-cardinality", default_value = "100000")]
    max_cardinality: Option<NonZeroUsize>,

    /// The maximum age of an entry in the cache entered as a human-readable duration, e.g., "30d", "24h"
    #[clap(long = "max-age", default_value = "1d")]
    max_age: Option<humantime::Duration>,

    /// Give the name of the cache.
    ///
    /// This will be automatically generated if not provided
    #[clap(required = false)]
    cache_name: Option<String>,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

#[derive(Debug, clap::Args)]
pub struct TableConfig {
    #[clap(long = "tags", value_delimiter = ',', num_args = 1..)]
    /// The list of tag names to be created for the table. Tags are alphanumeric, can contain - and _, and start with a letter or number
    tags: Option<Vec<String>>,

    #[clap(short = 'f', long = "fields", value_parser = parse_key_val::<String, DataType>, value_delimiter = ',')]
    /// The list of field names and their data type to be created for the table. Fields are alphanumeric, can contain - and _, and start with a letter or number
    /// The expected format is a list like so: 'field_name:data_type'. Valid data types are: int64, uint64, float64, utf8, and bool
    fields: Vec<(String, DataType)>,

    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    #[clap(required = true)]
    /// The name of the table to be created
    table_name: String,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

#[derive(Debug, clap::Parser)]
pub struct TriggerConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,
    /// Path to plugin file or directory. For single-file plugins, provide the .py file path.
    /// For multi-file plugins, provide the directory path (must contain __init__.py).
    /// When not using --upload, this should be the filename/path on the server's plugin-dir.
    /// Supports gh: prefix for [influxdb3_plugins](https://github.com/influxdata/influxdb3_plugins) repo.
    #[clap(long = "path")]
    path: Option<PathBuf>,
    /// Deprecated: Use --path instead. Python file name of the file on the server's plugin-dir.
    #[clap(long = "plugin-filename", conflicts_with = "path")]
    plugin_filename: Option<String>,
    /// When the trigger should fire
    #[clap(long = "trigger-spec",
          value_parser = TriggerSpecificationDefinition::from_string_rep,
          help = "The plugin file must be for the given trigger type of wal, schedule, or request. Trigger specification format:\nFor wal_rows use: 'table:<TABLE_NAME>' or 'all_tables'\nFor scheduled use: 'cron:<CRON_EXPRESSION>' or 'every:<duration e.g. 10m>'\nFor request use: 'path:<PATH>' e.g. path:foo will be at /api/v3/engine/foo")]
    trigger_specification: TriggerSpecificationDefinition,
    /// Comma separated list of key/value pairs to use as trigger arguments. Example: key1=val1,key2=val2
    #[clap(long = "trigger-arguments", value_delimiter = ',')]
    trigger_arguments: Option<Vec<SeparatedKeyValue<String, String>>>,
    /// Create trigger in disabled state
    #[clap(long)]
    disabled: bool,
    /// Run each instance of the trigger asynchronously, allowing multiple triggers to run simultaneously.
    #[clap(long)]
    run_asynchronous: bool,
    /// How you wish the system to respond in the event of an error from the plugin
    #[clap(long, value_enum, default_value_t = ErrorBehavior::Log)]
    error_behavior: ErrorBehavior,
    /// Upload plugin file from local filesystem instead of using server's plugin-dir
    #[clap(long)]
    upload: bool,
    /// Name for the new trigger
    trigger_name: String,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

pub async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    let client = config.get_client()?;
    match config.cmd {
        SubCommand::Database(DatabaseConfig {
            database_name,
            retention_period,
            ..
        }) => {
            client
                .api_v3_configure_db_create(&database_name, retention_period.map(Into::into))
                .await?;

            println!("Database {:?} created successfully", &database_name);
        }
        SubCommand::LastCache(LastCacheConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            table,
            cache_name,
            key_columns,
            value_columns,
            count,
            ttl,
            ..
        }) => {
            let mut b = client.api_v3_configure_last_cache_create(database_name, table);

            // Add optional parameters:
            if let Some(name) = cache_name {
                b = b.name(name);
            }
            if let Some(keys) = key_columns {
                b = b.key_columns(keys);
            }
            if let Some(vals) = value_columns {
                b = b.value_columns(vals);
            }
            if let Some(count) = count {
                b = b.count(count);
            }
            if let Some(ttl) = ttl {
                b = b.ttl(LastCacheTtl::from_secs(ttl.as_secs()));
            }

            // Make the request:
            match b.send().await? {
                Some(def) => println!(
                    "new cache created: {}",
                    serde_json::to_string_pretty(&def)
                        .expect("serialize last cache definition as JSON")
                ),
                None => println!("a cache already exists for the provided parameters"),
            }
        }
        SubCommand::DistinctCache(DistinctCacheConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            table,
            cache_name,
            columns,
            max_cardinality,
            max_age,
            ..
        }) => {
            let mut b =
                client.api_v3_configure_distinct_cache_create(database_name, table, columns);

            // Add the optional stuff:
            if let Some(name) = cache_name {
                b = b.name(name);
            }
            if let Some(max_cardinality) = max_cardinality {
                b = b.max_cardinality(max_cardinality);
            }
            if let Some(max_age) = max_age {
                b = b.max_age(max_age.into());
            }

            match b.send().await? {
                Some(def) => println!(
                    "new cache created: {}",
                    serde_json::to_string_pretty(&def)
                        .expect("serialize distinct cache definition as JSON")
                ),
                None => println!("a cache already exists for the provided parameters"),
            }
        }
        SubCommand::Table(TableConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            table_name,
            tags,
            fields,
            ..
        }) => {
            client
                .api_v3_configure_table_create(
                    &database_name,
                    &table_name,
                    tags.unwrap_or_default(),
                    fields,
                )
                .await?;

            println!(
                "Table {:?}.{:?} created successfully",
                &database_name, &table_name
            );
        }
        SubCommand::Token(token_creation_config) => {
            let output_format = token_creation_config
                .get_output_format()
                .cloned()
                .unwrap_or(token::TokenOutputFormat::Text);
            match handle_token_creation_with_config(client, token_creation_config).await {
                Ok(response) => match output_format {
                    token::TokenOutputFormat::Json => {
                        let help_msg = format!(
                            "Store this token securely, as it will not be shown again. \
                            HTTP requests require the following header: \"Authorization: Bearer {}\"",
                            response.token
                        );
                        let json = json!({"token": response.token, "help_msg": help_msg});
                        let stringified = serde_json::to_string_pretty(&json)
                            .expect("token details to be parseable");
                        println!("{stringified}");
                    }
                    token::TokenOutputFormat::Text => {
                        let token = response.token;
                        let title = format!("{}", "New token created successfully!".underline());
                        let token_label = format!("{}", "Token:".bold());
                        let header_label = format!("{}", "HTTP Requests Header:".bold());
                        let important_label = format!("{}", "IMPORTANT:".red().bold());
                        let important_message =
                            "Store this token securely, as it will not be shown again.";
                        println!(
                            "\n{title}\n\n\
                            {token_label} {token}\n\
                            {header_label} Authorization: Bearer {token}\n\n\
                            {important_label} {important_message}\n"
                        );
                    }
                },
                Err(err) => {
                    println!("Failed to create token, error: {err:?}");
                }
            }
        }
        SubCommand::Trigger(TriggerConfig {
            influxdb3_config: InfluxDb3Config { database_name, .. },
            trigger_name,
            path,
            plugin_filename,
            trigger_specification,
            trigger_arguments,
            disabled,
            run_asynchronous,
            error_behavior,
            upload,
            ..
        }) => {
            // Determine which path/filename to use: --path supersedes --plugin-filename
            let plugin_path_source = match (path, plugin_filename) {
                (Some(p), _) => p,
                (None, Some(f)) => PathBuf::from(f),
                (None, None) => {
                    return Err("Either --path or --plugin-filename must be provided".into());
                }
            };

            let final_plugin_filename = if upload {
                let path = plugin_path_source;

                if !path.exists() {
                    return Err(format!("File not found: {}", path.display()).into());
                }

                if path.is_file() {
                    // Single file upload (existing behavior)
                    let content = fs::read_to_string(&path).await?;

                    let filename = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .ok_or("Invalid filename")?
                        .to_string();

                    client
                        .api_v3_create_plugin_file(&database_name, &filename, &content)
                        .await?;

                    filename
                } else if path.is_dir() {
                    // Multi-file plugin upload (new behavior)
                    let plugin_name = path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .ok_or("Invalid directory name")?
                        .to_string();

                    // Validate __init__.py exists
                    let init_file = path.join("__init__.py");
                    if !init_file.exists() {
                        return Err(format!(
                            "Multi-file plugin directory must contain __init__.py: {}",
                            path.display()
                        )
                        .into());
                    }

                    // Recursively collect all .py files
                    for entry in WalkDir::new(&path)
                        .follow_links(false)
                        .into_iter()
                        .filter_entry(|e| {
                            // Skip __pycache__ directories
                            e.file_name()
                                .to_str()
                                .map(|s| s != "__pycache__")
                                .unwrap_or(true)
                        })
                        .filter_map(Result::ok)
                    {
                        if entry.file_type().is_file()
                            && entry.path().extension().and_then(|s| s.to_str()) == Some("py")
                        {
                            let content = fs::read_to_string(entry.path()).await?;

                            // Get relative path from plugin directory
                            let relative_path = entry
                                .path()
                                .strip_prefix(&path)
                                .map_err(|e| format!("Failed to get relative path: {}", e))?;

                            // Combine plugin name with relative path
                            let file_path = Path::new(&plugin_name)
                                .join(relative_path)
                                .to_str()
                                .ok_or("Invalid file path")?
                                .to_string();

                            client
                                .api_v3_create_plugin_file(&database_name, &file_path, &content)
                                .await?;
                        }
                    }

                    plugin_name
                } else {
                    return Err(format!("Invalid path: {}", path.display()).into());
                }
            } else {
                // When not uploading, extract the filename/path as a string for server reference
                plugin_path_source
                    .to_str()
                    .ok_or("Invalid path encoding")?
                    .to_string()
            };

            let trigger_arguments: Option<HashMap<String, String>> = trigger_arguments.map(|a| {
                a.into_iter()
                    .map(|SeparatedKeyValue((k, v))| (k, v))
                    .collect::<HashMap<String, String>>()
            });

            let trigger_settings = TriggerSettings {
                run_async: run_asynchronous,
                error_behavior,
            };

            match client
                .api_v3_configure_processing_engine_trigger_create(
                    database_name,
                    &trigger_name,
                    final_plugin_filename,
                    trigger_specification.string_rep(),
                    trigger_arguments,
                    disabled,
                    trigger_settings,
                )
                .await
            {
                Err(e) => {
                    eprintln!("Failed to create trigger: {e}");
                    return Err(e.into());
                }
                Ok(_) => println!("Trigger {trigger_name} created successfully"),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use std::path::PathBuf;
    use std::time::Duration;

    use clap::Parser;
    use influxdb3_catalog::log::{ErrorBehavior, TriggerSpecificationDefinition};

    #[test]
    fn parse_args_create_last_cache() {
        let args = super::Config::parse_from([
            "create",
            "last_cache",
            "--database",
            "bar",
            "--table",
            "foo",
            "--key-columns",
            "tag1,tag2,tag3",
            "--value-columns",
            "field1,field2,field3",
            "--ttl",
            "1 hour",
            "--count",
            "15",
            "bar",
        ]);
        let super::SubCommand::LastCache(super::LastCacheConfig {
            table,
            cache_name,
            key_columns,
            value_columns,
            count,
            ttl,
            influxdb3_config: crate::commands::common::InfluxDb3Config { database_name, .. },
            ..
        }) = args.cmd
        else {
            panic!("Did not parse args correctly: {args:#?}")
        };
        assert_eq!("bar", database_name);
        assert_eq!("foo", table);
        assert!(cache_name.is_some_and(|n| n == "bar"));
        assert!(key_columns.is_some_and(|keys| keys == ["tag1", "tag2", "tag3"]));
        assert!(value_columns.is_some_and(|vals| vals == ["field1", "field2", "field3"]));
        assert!(count.is_some_and(|c| c == 15));
        assert!(ttl.is_some_and(|t| t.as_secs() == 3600));
    }

    #[test]
    fn parse_args_create_trigger_arguments() {
        let args = super::Config::parse_from([
            "create",
            "trigger",
            "--trigger-spec",
            "every:10s",
            "--path",
            "plugin.py",
            "--database",
            "test",
            "--trigger-arguments",
            "query_path=/metrics?format=json,whatever=hello",
            "test-trigger",
        ]);
        let super::SubCommand::Trigger(super::TriggerConfig {
            trigger_name,
            trigger_arguments,
            trigger_specification,
            path,
            disabled,
            run_asynchronous,
            error_behavior,
            influxdb3_config: crate::commands::common::InfluxDb3Config { database_name, .. },
            ..
        }) = args.cmd
        else {
            panic!("Did not parse args correctly: {args:#?}")
        };
        assert_eq!("test", database_name);
        assert_eq!("test-trigger", trigger_name);
        assert_eq!(Some(PathBuf::from("plugin.py")), path);
        assert_eq!(
            TriggerSpecificationDefinition::Every {
                duration: Duration::from_secs(10)
            },
            trigger_specification
        );
        assert!(!disabled);
        assert!(!run_asynchronous);
        assert_eq!(ErrorBehavior::Log, error_behavior);

        let trigger_arguments = trigger_arguments.expect("args must include trigger arguments");

        assert_eq!(2, trigger_arguments.len());

        let query_path = trigger_arguments
            .into_iter()
            .find(|v| v.0.0 == "query_path")
            .expect("must include query_path trigger argument");

        assert_eq!("/metrics?format=json", query_path.0.1);
    }
}
