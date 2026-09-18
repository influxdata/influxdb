use super::common::{DataType, InfluxDb3Config, parse_key_val};
use humantime::Duration;
use influxdb3_client::Client;
use secrecy::ExposeSecret;
use std::error::Error;
use std::path::PathBuf;
use tokio::fs;
use walkdir::WalkDir;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCommand {
    /// Update a database
    Database(UpdateDatabase),
    /// Add tag and field columns to a table
    Table(UpdateTable),
    /// Update a trigger's plugin file
    Trigger(UpdateTrigger),
}

#[derive(Debug, clap::Args)]
pub struct UpdateTable {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The list of tag names to add to the table. Tags are alphanumeric, can contain - and _, and start with a letter or number
    /// This flag takes one or more values, so put the table name before it: `update table -d mydb mytable --tags a,b`
    #[clap(long = "tags", value_delimiter = ',', num_args = 1..)]
    tags: Option<Vec<String>>,

    /// The list of field names and their data type to add to the table. Fields are alphanumeric, can contain - and _, and start with a letter or number
    /// The expected format is a list like so: 'field_name:data_type'. Valid data types are: int64, uint64, float64, utf8, and bool
    #[clap(short = 'f', long = "fields", value_parser = parse_key_val::<String, DataType>, value_delimiter = ',')]
    fields: Vec<(String, DataType)>,

    /// The name of the table to update
    table_name: String,

    /// An optional arg to use a custom CA, useful for testing with self-signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

#[derive(Debug, clap::Args)]
pub struct UpdateDatabase {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The retention period as a human-readable duration (e.g., "30d", "24h") or "none" to clear
    #[clap(long, short = 'r')]
    retention_period: Option<String>,

    /// An optional arg to use a custom CA, useful for testing with self-signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

#[derive(Debug, clap::Args)]
pub struct UpdateTrigger {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// The name of the trigger to update
    #[clap(long = "trigger-name", short = 't')]
    trigger_name: String,

    /// Path to file containing plugin code to update
    #[clap(long, short = 'p')]
    path: PathBuf,

    /// An optional arg to use a custom CA, useful for testing with self-signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

pub async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    match config.cmd {
        SubCommand::Database(UpdateDatabase {
            influxdb3_config:
                InfluxDb3Config {
                    host_url,
                    auth_token,
                    database_name,
                    ..
                },
            retention_period,
            ca_cert,
            tls_no_verify,
        }) => {
            let mut client = Client::new(host_url, ca_cert, tls_no_verify)?;
            if let Some(token) = &auth_token {
                client = client.with_auth_token(token.expose_secret());
            }

            if let Some(retention_str) = retention_period {
                let retention = if retention_str.to_lowercase() == "none" {
                    None
                } else {
                    Some(retention_str.parse::<Duration>()?.into())
                };
                client
                    .api_v3_configure_db_update(&database_name, retention)
                    .await?;

                println!("Database \"{database_name}\" updated successfully");
            } else {
                return Err("--retention-period is required for update database".into());
            }
        }
        SubCommand::Table(UpdateTable {
            influxdb3_config:
                InfluxDb3Config {
                    host_url,
                    auth_token,
                    database_name,
                    ..
                },
            tags,
            fields,
            table_name,
            ca_cert,
            tls_no_verify,
        }) => {
            let mut client = Client::new(host_url, ca_cert, tls_no_verify)?;
            if let Some(token) = &auth_token {
                client = client.with_auth_token(token.expose_secret());
            }

            let tags = tags.unwrap_or_default();
            if tags.is_empty() && fields.is_empty() {
                return Err("one of --tags or --fields is required for update table".into());
            }

            let n_tags = tags.len();
            let n_fields = fields.len();
            client
                .api_v3_configure_table_add_columns(&database_name, &table_name, tags, fields)
                .await?;

            // Adding a column that already exists at its declared type is a
            // no-op, so report what was requested rather than what changed.
            println!(
                "Table \"{database_name}\".\"{table_name}\" updated with {n_tags} tag(s) and {n_fields} field(s)"
            );
        }
        SubCommand::Trigger(UpdateTrigger {
            influxdb3_config:
                InfluxDb3Config {
                    host_url,
                    auth_token,
                    database_name,
                    ..
                },
            trigger_name,
            path,
            ca_cert,
            tls_no_verify,
        }) => {
            let mut client = Client::new(host_url, ca_cert, tls_no_verify)?;
            if let Some(token) = &auth_token {
                client = client.with_auth_token(token.expose_secret());
            }

            if !path.exists() {
                return Err(format!("Path does not exist: {}", path.display()).into());
            }

            if path.is_file() {
                let content = fs::read_to_string(&path).await?;

                client
                    .api_v3_update_plugin_file(&database_name, &trigger_name, &content)
                    .await?;

                println!("Trigger '{}' updated successfully", trigger_name);
            } else if path.is_dir() {
                let init_file = path.join("__init__.py");
                if !init_file.exists() {
                    return Err(format!(
                        "Multi-file plugin directory must contain __init__.py: {}",
                        path.display()
                    )
                    .into());
                }

                // Collect all Python files from the directory
                let mut files = Vec::new();
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
                            .map_err(|e| format!("Failed to get relative path: {}", e))?
                            .to_str()
                            .ok_or("Invalid file path encoding")?
                            .to_string();

                        files.push((relative_path, content));
                    }
                }

                if files.is_empty() {
                    return Err("No Python files found in directory".into());
                }

                client
                    .api_v3_replace_plugin_directory(&database_name, &trigger_name, files)
                    .await?;

                println!(
                    "Trigger '{}' updated successfully (atomic directory replacement)",
                    trigger_name
                );
            } else {
                return Err(format!("Invalid path: {}", path.display()).into());
            }
        }
    }
    Ok(())
}
