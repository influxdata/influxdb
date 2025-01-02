mod plugin;
mod trigger;

use std::error::Error;

#[derive(Debug, clap::Parser)]
pub(crate) struct Config {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Parser)]
enum Command {
    /// Manage plugins (create, delete, update, etc.)
    Plugin(plugin::Config),
    /// Manage triggers (create, delete, activate, deactivate, etc.)
    Trigger(trigger::Config),
}

pub(crate) async fn command(config: Config) -> Result<(), Box<dyn Error>> {
    match config.command {
        Command::Plugin(plugin_config) => {
            plugin::command(plugin_config).await?;
        }
        Command::Trigger(trigger_config) => {
            trigger::command(trigger_config).await?;
        }
    }
    Ok(())
}
