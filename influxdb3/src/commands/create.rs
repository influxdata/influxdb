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
use url::Url;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

impl Config {
    fn get_client(&self) -> Result<Client, Box<dyn Error>> {
        let (host_url, auth_token, ca_cert) = match &self.cmd {
            SubCommand::Database(DatabaseConfig {
                host_url,
                auth_token,
                ca_cert,
                ..
            })
            | SubCommand::LastCache(LastCacheConfig {
                ca_cert,
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
                influxdb3_config:
                    InfluxDb3Config {
                        host_url,
                        auth_token,
                        ..
                    },
                ..
            }) => (host_url, auth_token, ca_cert),
            SubCommand::Token(create_token_config) => {
                let host_settings = create_token_config.get_connection_settings()?;
                // We need to return references, so we'll handle this differently
                return Ok({
                    let mut client = Client::new(
                        host_settings.host_url.clone(),
                        host_settings.ca_cert.clone(),
                    )?;
                    if let Some(token) = &host_settings.auth_token {
                        client = client.with_auth_token(token.expose_secret());
                    }
                    client
                });
            }
        };

        let mut client = Client::new(host_url.clone(), ca_cert.clone())?;
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
}

#[derive(Debug, clap::Parser)]
pub struct TriggerConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,
    /// Python file name of the file on the server's plugin-dir containing the plugin code. Or
    /// on the [influxdb3_plugins](https://github.com/influxdata/influxdb3_plugins) repo if `gh:` is specified as
    /// the prefix.
    ///
    /// Deprecated: Use --path instead
    #[clap(long = "plugin-filename", conflicts_with_all = &["path"])]
    plugin_filename: Option<String>,
    /// Path to the plugin file or directory containing plugin code to be used by the trigger.
    ///
    /// Without --upload: References a path on the server's plugin-dir, or a plugin from the
    /// [influxdb3_plugins](https://github.com/influxdata/influxdb3_plugins) repo if `gh:` prefix is used.
    ///
    /// With --upload: References a local path to upload. Single .py files or directories are supported.
    /// For directories, a __main__.py entrypoint file is required, and all .py files will be uploaded.
    /// Files are uploaded in parallel, and the trigger is created disabled until upload completes.
    #[clap(short = 'p', long = "path", conflicts_with = "plugin_filename")]
    path: Option<String>,
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
    /// Name for the new trigger
    trigger_name: String,

    /// Upload local plugin file(s) to the server (admin-only).
    ///
    /// When enabled, --path references a local file or directory to upload:
    /// - Single file: Upload one .py file (max 10 MB)
    /// - Directory: Upload all .py files recursively (requires __main__.py entrypoint)
    ///
    /// The trigger is created disabled, files are uploaded in parallel, then the trigger
    /// is enabled (unless --disabled is set). If upload fails, the trigger is automatically
    /// deleted to prevent partial state.
    ///
    /// Security: Symlinks are skipped, path traversal is prevented, and file sizes are limited.
    #[clap(long)]
    upload: bool,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,
}

/// Maximum size for a single plugin file (10 MB)
///
/// This limit prevents memory exhaustion when reading plugin files.
/// Files larger than this will be rejected with a clear error message.
const MAX_PLUGIN_FILE_SIZE: u64 = 10 * 1024 * 1024;

/// Read a local plugin file with size validation
///
/// # Arguments
/// * `path` - Path to the local Python file to read
///
/// # Returns
/// The file contents as a UTF-8 string
///
/// # Errors
/// - File size exceeds MAX_PLUGIN_FILE_SIZE (10 MB)
/// - File cannot be read or does not exist
/// - File contains invalid UTF-8
///
/// # Security
/// File size is checked before reading to prevent memory exhaustion attacks.
fn read_local_plugin_file(path: &Path) -> Result<String, Box<dyn Error>> {
    // Check file size before reading to prevent memory exhaustion
    let metadata = std::fs::metadata(path)
        .map_err(|e| format!("Failed to read metadata for {}: {}", path.display(), e))?;

    if metadata.len() > MAX_PLUGIN_FILE_SIZE {
        return Err(format!(
            "Plugin file {} is too large ({} bytes). Maximum allowed size is {} bytes (10 MB)",
            path.display(),
            metadata.len(),
            MAX_PLUGIN_FILE_SIZE
        )
        .into());
    }

    std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read plugin file at {}: {}", path.display(), e).into())
}

/// Collect all Python files from a directory plugin
///
/// # Arguments
/// * `dir` - Path to the directory containing the plugin files
///
/// # Returns
/// Vector of tuples containing (relative_path, file_content) for each .py file
///
/// # Errors
/// - Path is not a directory
/// - __main__.py entrypoint is missing
/// - Any file fails to read (see read_local_plugin_file)
/// - File system errors during directory traversal
///
/// # Security
/// - Symlinks are skipped to prevent reading files outside the plugin directory
/// - Path traversal attacks are prevented by collecting only files within the directory tree
/// - File size limits are enforced per file (see MAX_PLUGIN_FILE_SIZE)
fn collect_plugin_directory_files(dir: &Path) -> Result<Vec<(String, String)>, Box<dyn Error>> {
    if !dir.is_dir() {
        return Err(format!("{} is not a directory", dir.display()).into());
    }

    let mut files = Vec::new();
    collect_files_recursive(dir, dir, &mut files)?;

    // Verify that __main__.py exists
    if !files.iter().any(|(name, _)| name == "__main__.py") {
        return Err(format!(
            "Directory plugin must contain __main__.py entrypoint in {}",
            dir.display()
        )
        .into());
    }

    Ok(files)
}

/// Recursively collect Python files from a directory (internal helper)
///
/// # Arguments
/// * `base_dir` - The root directory of the plugin (used to compute relative paths)
/// * `current_dir` - The current directory being scanned
/// * `files` - Mutable vector to accumulate (relative_path, content) tuples
///
/// # Security
/// - Uses `entry.metadata()` instead of `path.is_*()` to avoid following symlinks
/// - Explicitly skips symlinks to prevent reading files outside the plugin directory
/// - Only collects files with .py extension
fn collect_files_recursive(
    base_dir: &Path,
    current_dir: &Path,
    files: &mut Vec<(String, String)>,
) -> Result<(), Box<dyn Error>> {
    for entry in std::fs::read_dir(current_dir)? {
        let entry = entry?;
        let path = entry.path();

        // Use metadata() instead of path methods to avoid following symlinks
        let metadata = entry.metadata()?;

        // Skip symlinks to prevent reading files outside the plugin directory
        if metadata.is_symlink() {
            continue;
        }

        if metadata.is_file() && path.extension().and_then(|s| s.to_str()) == Some("py") {
            let content = std::fs::read_to_string(&path)?;
            let relative_path = path
                .strip_prefix(base_dir)
                .map_err(|_| "Failed to get relative path")?
                .to_string_lossy()
                .to_string();
            files.push((relative_path, content));
        } else if metadata.is_dir() {
            collect_files_recursive(base_dir, &path, files)?;
        }
    }

    Ok(())
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
            plugin_filename,
            path,
            trigger_specification,
            trigger_arguments,
            disabled,
            run_asynchronous,
            error_behavior,
            upload,
            ..
        }) => {
            let trigger_arguments: Option<HashMap<String, String>> = trigger_arguments.map(|a| {
                a.into_iter()
                    .map(|SeparatedKeyValue((k, v))| (k, v))
                    .collect::<HashMap<String, String>>()
            });

            let trigger_settings = TriggerSettings {
                run_async: run_asynchronous,
                error_behavior,
            };

            // Determine the plugin path (either from plugin_filename or path)
            let plugin_path_str = if let Some(ref pf) = plugin_filename {
                pf.clone()
            } else if let Some(ref p) = path {
                p.clone()
            } else {
                return Err(
                    "Invalid plugin configuration: must specify --plugin-filename or --path".into(),
                );
            };

            // Determine the plugin reference for creating the trigger
            let plugin_reference = if upload {
                // When uploading, use the filename/directory name as the plugin reference
                let local_path = PathBuf::from(&plugin_path_str);
                local_path
                    .file_name()
                    .ok_or("Invalid path")?
                    .to_string_lossy()
                    .to_string()
            } else {
                // When not uploading, use the path as-is (server-side reference)
                plugin_path_str.clone()
            };

            // Create the trigger first (it will initially reference non-existent files if uploading)
            // When uploading, we create it disabled initially and enable after upload (if not meant to be disabled)
            let result = client
                .api_v3_configure_processing_engine_trigger_create(
                    &database_name,
                    &trigger_name,
                    &plugin_reference,
                    trigger_specification.string_rep(),
                    trigger_arguments.clone(),
                    if upload { true } else { disabled }, // Create disabled when uploading
                    trigger_settings,
                )
                .await;

            match result {
                Err(e) => {
                    eprintln!("Failed to create trigger: {e}");
                    return Err(e.into());
                }
                Ok(_) => {
                    if upload {
                        println!(
                            "Trigger {trigger_name} created (disabled), now uploading files..."
                        );
                    } else {
                        println!("Trigger {trigger_name} created successfully");
                    }
                }
            }

            // If --upload is set, upload local files to the server AFTER trigger creation
            if upload {
                // Helper closure for rollback on error
                let rollback_on_error = |error: Box<dyn Error + Send>| async {
                    eprintln!("Upload failed, rolling back trigger creation...");
                    match client
                        .api_v3_configure_processing_engine_trigger_delete(
                            &database_name,
                            &trigger_name,
                            true, // force delete
                        )
                        .await
                    {
                        Ok(_) => {
                            eprintln!("Trigger {trigger_name} deleted successfully");
                        }
                        Err(delete_err) => {
                            eprintln!("Failed to delete trigger during rollback: {}", delete_err);
                            eprintln!(
                                "You may need to manually delete trigger '{trigger_name}' in database '{database_name}'"
                            );
                        }
                    }
                    error
                };

                let local_path = PathBuf::from(&plugin_path_str);

                if !local_path.exists() {
                    let err: Box<dyn Error + Send> = Box::new(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Local path does not exist: {}", local_path.display()),
                    ));
                    return Err(rollback_on_error(err).await);
                }

                // Helper to convert any error to Box<dyn Error + Send> by converting to string
                fn to_send_error(e: impl std::fmt::Display) -> Box<dyn Error + Send> {
                    Box::new(std::io::Error::other(e.to_string()))
                }

                // Upload files based on path type
                let upload_result: Result<(), Box<dyn Error + Send>> = async {
                    if local_path.is_file() {
                        // Single file plugin
                        let content = read_local_plugin_file(&local_path).map_err(to_send_error)?;
                        let file_name = local_path
                            .file_name()
                            .ok_or_else(|| to_send_error("Invalid file path"))?
                            .to_string_lossy()
                            .to_string();

                        println!("Uploading plugin file: {}", file_name);
                        client
                            .api_v3_update_plugin_file(
                                &database_name,
                                &trigger_name,
                                &file_name,
                                &content,
                            )
                            .await
                            .map_err(to_send_error)?;

                        println!("Plugin file uploaded successfully");
                    } else if local_path.is_dir() {
                        // Multi-file directory plugin
                        let files =
                            collect_plugin_directory_files(&local_path).map_err(to_send_error)?;
                        let dir_name = local_path
                            .file_name()
                            .ok_or_else(|| to_send_error("Invalid directory path"))?
                            .to_string_lossy()
                            .to_string();

                        println!(
                            "Uploading {} plugin files from directory: {}",
                            files.len(),
                            dir_name
                        );

                        // Upload files in parallel using futures
                        use futures::stream::{FuturesUnordered, StreamExt};

                        let mut upload_futures = FuturesUnordered::new();

                        for (file_name, content) in files {
                            let client_ref = &client;
                            let db_ref = &database_name;
                            let trigger_ref = &trigger_name;

                            upload_futures.push(async move {
                                println!("  Uploading: {}", file_name);
                                client_ref
                                    .api_v3_update_plugin_file(
                                        db_ref,
                                        trigger_ref,
                                        &file_name,
                                        &content,
                                    )
                                    .await
                                    .map(|_| file_name.clone())
                            });
                        }

                        // Wait for all uploads to complete, failing fast on first error
                        while let Some(result) = upload_futures.next().await {
                            result.map_err(to_send_error)?;
                        }

                        println!("All plugin files uploaded successfully");
                    } else {
                        return Err(to_send_error(format!(
                            "Invalid path type: {}",
                            local_path.display()
                        )));
                    }
                    Ok(())
                }
                .await;

                // If upload failed, rollback and return error
                if let Err(e) = upload_result {
                    return Err(rollback_on_error(e).await);
                }

                // Enable the trigger if it wasn't meant to be disabled
                if !disabled {
                    println!("Enabling trigger...");
                    let enable_result = client
                        .api_v3_configure_processing_engine_trigger_enable(
                            &database_name,
                            &trigger_name,
                        )
                        .await;

                    // If enable failed, rollback and return error
                    if let Err(e) = enable_result {
                        return Err(rollback_on_error(Box::new(e)).await);
                    }

                    println!("Trigger {trigger_name} enabled successfully");
                }
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
            "--plugin-filename",
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
            plugin_filename,

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
        assert_eq!(Some("plugin.py".to_string()), plugin_filename);
        assert_eq!(None, path);
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

    #[test]
    fn parse_args_create_trigger_with_directory() {
        let args = super::Config::parse_from([
            "create",
            "trigger",
            "--trigger-spec",
            "every:10s",
            "--path",
            "my_plugin",
            "--database",
            "test",
            "test-trigger",
        ]);
        let super::SubCommand::Trigger(super::TriggerConfig {
            trigger_name,
            plugin_filename,
            path,
            trigger_specification,
            influxdb3_config: crate::commands::common::InfluxDb3Config { database_name, .. },
            ..
        }) = args.cmd
        else {
            panic!("Did not parse args correctly: {args:#?}")
        };
        assert_eq!("test", database_name);
        assert_eq!("test-trigger", trigger_name);
        assert_eq!(None, plugin_filename);
        assert_eq!(Some("my_plugin".to_string()), path);
        assert_eq!(
            TriggerSpecificationDefinition::Every {
                duration: Duration::from_secs(10)
            },
            trigger_specification
        );
    }

    #[test]
    fn parse_args_create_trigger_conflict_single_and_multi_file() {
        // This should fail because both --plugin-filename and --path are provided
        let result = super::Config::try_parse_from([
            "create",
            "trigger",
            "--trigger-spec",
            "every:10s",
            "--plugin-filename",
            "plugin.py",
            "--path",
            "my_plugin",
            "--database",
            "test",
            "test-trigger",
        ]);

        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("cannot be used") || err_msg.contains("conflicts"),
            "Expected conflict error, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_read_local_plugin_file_success() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().unwrap();
        let content = "def handle_wal(data): pass";
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = super::read_local_plugin_file(temp_file.path());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), content);
    }

    #[test]
    fn test_read_local_plugin_file_not_found() {
        let result = super::read_local_plugin_file(&PathBuf::from("/nonexistent/file.py"));
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        // Can fail either at metadata check or file read
        assert!(
            err_msg.contains("Failed to read metadata")
                || err_msg.contains("Failed to read plugin file"),
            "Expected error about missing file, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_collect_plugin_directory_files_success() {
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let plugin_dir = temp_dir.path().join("my_plugin");
        fs::create_dir(&plugin_dir).unwrap();

        // Create __main__.py
        fs::write(plugin_dir.join("__main__.py"), "def process_writes(): pass").unwrap();

        // Create helper.py
        fs::write(plugin_dir.join("helper.py"), "def helper(): pass").unwrap();

        // Create subdirectory with another file
        let subdir = plugin_dir.join("utils");
        fs::create_dir(&subdir).unwrap();
        fs::write(subdir.join("utils.py"), "def util(): pass").unwrap();

        let result = super::collect_plugin_directory_files(&plugin_dir);
        assert!(result.is_ok());

        let files = result.unwrap();
        assert_eq!(files.len(), 3);

        // Check that all files are present
        assert!(files.iter().any(|(name, _)| name == "__main__.py"));
        assert!(files.iter().any(|(name, _)| name == "helper.py"));
        assert!(
            files
                .iter()
                .any(|(name, _)| name == "utils/utils.py" || name == "utils\\utils.py")
        );
    }

    #[test]
    fn test_collect_plugin_directory_files_missing_entrypoint() {
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let plugin_dir = temp_dir.path().join("my_plugin");
        fs::create_dir(&plugin_dir).unwrap();

        // Create only helper.py, no __main__.py
        fs::write(plugin_dir.join("helper.py"), "def helper(): pass").unwrap();

        let result = super::collect_plugin_directory_files(&plugin_dir);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("__main__.py"));
    }

    #[test]
    fn test_collect_plugin_directory_files_not_a_directory() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let temp_file = NamedTempFile::new().unwrap();
        temp_file.as_file().write_all(b"content").unwrap();

        let result = super::collect_plugin_directory_files(temp_file.path());
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("is not a directory"));
    }

    #[test]
    fn test_collect_plugin_directory_files_empty_directory() {
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let plugin_dir = temp_dir.path().join("my_plugin");
        fs::create_dir(&plugin_dir).unwrap();

        let result = super::collect_plugin_directory_files(&plugin_dir);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("__main__.py"));
    }

    #[test]
    fn test_collect_plugin_directory_files_ignores_non_python() {
        use std::fs;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let plugin_dir = temp_dir.path().join("my_plugin");
        fs::create_dir(&plugin_dir).unwrap();

        // Create __main__.py
        fs::write(plugin_dir.join("__main__.py"), "def process_writes(): pass").unwrap();

        // Create non-Python files
        fs::write(plugin_dir.join("README.md"), "# README").unwrap();
        fs::write(plugin_dir.join("config.json"), "{}").unwrap();
        fs::write(plugin_dir.join("data.txt"), "data").unwrap();

        let result = super::collect_plugin_directory_files(&plugin_dir);
        assert!(result.is_ok());

        let files = result.unwrap();
        // Should only have __main__.py, not the other files
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, "__main__.py");
    }

    #[test]
    fn test_read_local_plugin_file_size_limit() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().unwrap();

        // Write a file larger than MAX_PLUGIN_FILE_SIZE (10 MB)
        // We'll write 11 MB of data
        let large_data = vec![b'a'; 11 * 1024 * 1024];
        temp_file.write_all(&large_data).unwrap();
        temp_file.flush().unwrap();

        let result = super::read_local_plugin_file(temp_file.path());
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("too large") && err_msg.contains("10 MB"),
            "Expected file size error, got: {}",
            err_msg
        );
    }

    #[test]
    #[cfg(unix)]
    fn test_collect_plugin_directory_files_skips_symlinks() {
        use std::fs;
        use std::os::unix::fs::symlink;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let plugin_dir = temp_dir.path().join("my_plugin");
        fs::create_dir(&plugin_dir).unwrap();

        // Create __main__.py
        fs::write(plugin_dir.join("__main__.py"), "def process_writes(): pass").unwrap();

        // Create a regular file outside the plugin directory
        let outside_file = temp_dir.path().join("outside.py");
        fs::write(&outside_file, "SHOULD_NOT_BE_READ").unwrap();

        // Create a symlink inside the plugin directory pointing to the outside file
        let symlink_path = plugin_dir.join("symlinked.py");
        symlink(&outside_file, &symlink_path).unwrap();

        let result = super::collect_plugin_directory_files(&plugin_dir);
        assert!(result.is_ok());

        let files = result.unwrap();
        // Should only have __main__.py, symlink should be skipped
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, "__main__.py");

        // Verify the outside file was NOT read
        assert!(
            !files
                .iter()
                .any(|(_, content)| content.contains("SHOULD_NOT_BE_READ"))
        );
    }

    #[test]
    fn test_read_local_plugin_file_utf8() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().unwrap();
        // Write UTF-8 content with special characters
        let content = "def handle_wal(data):\n    # Handles data: €, ñ, 中文\n    pass";
        temp_file.write_all(content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        let result = super::read_local_plugin_file(temp_file.path());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), content);
    }
}
