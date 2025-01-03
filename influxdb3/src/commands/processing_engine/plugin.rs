use crate::commands::common::InfluxDb3Config;
use secrecy::ExposeSecret;
use std::error::Error;
use std::fs::File;
use std::io::Read;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    action: PluginAction,
}

#[derive(Debug, clap::Subcommand)]
enum PluginAction {
    /// Create a new processing engine plugin
    Create(CreateConfig),
    /// Delete an existing processing engine plugin
    Delete(DeleteConfig),
}

impl PluginAction {
    pub fn get_influxdb3_config(&self) -> &InfluxDb3Config {
        match self {
            Self::Create(create) => &create.influxdb3_config,
            Self::Delete(delete) => &delete.influxdb3_config,
        }
    }
}

#[derive(Debug, clap::Parser)]
struct CreateConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// Name of the plugin to create
    #[clap(long = "plugin-name")]
    plugin_name: String,
    /// Python file containing the plugin code
    #[clap(long = "code-filename")]
    code_file: String,
    /// Entry point function for the plugin
    #[clap(long = "entry-point")]
    function_name: String,
    /// Type of trigger the plugin processes
    #[clap(long = "plugin-type", default_value = "wal_rows")]
    plugin_type: String,
}

#[derive(Debug, clap::Parser)]
struct DeleteConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// Name of the plugin to delete
    #[clap(long = "plugin-name")]
    plugin_name: String,
}

pub(super) async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    let influxdb3_config = config.action.get_influxdb3_config();
    let mut client = influxdb3_client::Client::new(influxdb3_config.host_url.clone())?;
    if let Some(token) = &influxdb3_config.auth_token {
        client = client.with_auth_token(token.expose_secret());
    }
    match config.action {
        PluginAction::Create(create_config) => {
            let code = open_file(&create_config.code_file)?;
            client
                .api_v3_configure_processing_engine_plugin_create(
                    create_config.influxdb3_config.database_name,
                    &create_config.plugin_name,
                    code,
                    create_config.function_name,
                    create_config.plugin_type,
                )
                .await?;
            println!("Plugin {} created successfully", create_config.plugin_name);
        }
        PluginAction::Delete(delete_config) => {
            client
                .api_v3_configure_processing_engine_plugin_delete(
                    delete_config.influxdb3_config.database_name,
                    &delete_config.plugin_name,
                )
                .await?;
            println!("Plugin {} deleted successfully", delete_config.plugin_name);
        }
    }
    Ok(())
}

fn open_file(file: &str) -> Result<String, Box<dyn Error>> {
    let mut file = File::open(file)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    Ok(contents)
}
