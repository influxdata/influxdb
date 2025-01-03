use crate::commands::common::InfluxDb3Config;
use influxdb3_client::Client;
use influxdb3_wal::TriggerSpecificationDefinition;
use secrecy::ExposeSecret;
use std::error::Error;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    action: TriggerAction,
}

impl Config {
    fn get_client(&self) -> Result<Client, Box<dyn Error>> {
        let influxdb3_config = match &self.action {
            TriggerAction::Create(create) => &create.influxdb3_config,
            TriggerAction::Activate(manage) | TriggerAction::Deactivate(manage) => {
                &manage.influxdb3_config
            }
            TriggerAction::Delete(delete) => &delete.influxdb3_config,
        };
        let mut client = Client::new(influxdb3_config.host_url.clone())?;
        if let Some(token) = &influxdb3_config.auth_token {
            client = client.with_auth_token(token.expose_secret());
        }
        Ok(client)
    }
}

#[derive(Debug, clap::Subcommand)]
enum TriggerAction {
    /// Create a new trigger using an existing plugin
    Create(CreateConfig),
    /// Activate a trigger to enable plugin execution
    Activate(ManageConfig),
    /// Deactivate a trigger to disable plugin execution
    Deactivate(ManageConfig),
    /// Delete a trigger
    Delete(DeleteConfig),
}

#[derive(Debug, clap::Parser)]
struct CreateConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// Name for the new trigger
    #[clap(long = "trigger-name")]
    trigger_name: String,
    /// Plugin to execute when trigger fires
    #[clap(long = "plugin-name")]
    plugin_name: String,
    /// When the trigger should fire
    #[clap(long = "trigger-spec",
          value_parser = TriggerSpecificationDefinition::from_string_rep,
          help = "Trigger specification format: 'table:<TABLE_NAME>' or 'all_tables'")]
    trigger_specification: TriggerSpecificationDefinition,
    /// Create trigger in disabled state
    #[clap(long)]
    disabled: bool,
}

#[derive(Debug, clap::Parser)]
struct ManageConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// Name of trigger to manage
    #[clap(long = "trigger-name")]
    trigger_name: String,
}

#[derive(Debug, clap::Parser)]
struct DeleteConfig {
    #[clap(flatten)]
    influxdb3_config: InfluxDb3Config,

    /// Name of trigger to delete
    #[clap(long = "trigger-name")]
    trigger_name: String,

    /// Force deletion even if trigger is active
    #[clap(long)]
    force: bool,
}

// [Previous CreateConfig and ManageConfig structs remain unchanged]

pub(super) async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    let client = config.get_client()?;
    match config.action {
        TriggerAction::Create(create_config) => {
            client
                .api_v3_configure_processing_engine_trigger_create(
                    create_config.influxdb3_config.database_name,
                    &create_config.trigger_name,
                    &create_config.plugin_name,
                    create_config.trigger_specification.string_rep(),
                    create_config.disabled,
                )
                .await?;
            println!(
                "Trigger {} created successfully",
                create_config.trigger_name
            );
        }
        TriggerAction::Activate(manage_config) => {
            client
                .api_v3_configure_processing_engine_trigger_activate(
                    manage_config.influxdb3_config.database_name,
                    &manage_config.trigger_name,
                )
                .await?;
            println!(
                "Trigger {} activated successfully",
                manage_config.trigger_name
            );
        }
        TriggerAction::Deactivate(manage_config) => {
            client
                .api_v3_configure_processing_engine_trigger_deactivate(
                    manage_config.influxdb3_config.database_name,
                    &manage_config.trigger_name,
                )
                .await?;
            println!(
                "Trigger {} deactivated successfully",
                manage_config.trigger_name
            );
        }
        TriggerAction::Delete(delete_config) => {
            client
                .api_v3_configure_processing_engine_trigger_delete(
                    delete_config.influxdb3_config.database_name,
                    &delete_config.trigger_name,
                    delete_config.force,
                )
                .await?;
            println!(
                "Trigger {} deleted successfully",
                delete_config.trigger_name
            );
        }
    }
    Ok(())
}
