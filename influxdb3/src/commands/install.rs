use crate::commands::serve::setup_processing_engine_env_manager;
use anyhow::bail;
use influxdb3_clap_blocks::plugins::ProcessingEngineConfig;
use influxdb3_client::Client;
#[cfg(feature = "system-py")]
use influxdb3_processing_engine::virtualenv;
use secrecy::{ExposeSecret, Secret};
use url::Url;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCommand {
    /// Install packages within the plugin environment
    Package(PackageConfig),
}

pub async fn command(config: Config) -> Result<(), anyhow::Error> {
    match config.cmd {
        SubCommand::Package(package_config) => {
            package_config.run_command().await?;
        }
    }
    Ok(())
}

#[derive(Debug, clap::Args)]
pub struct PackageConfig {
    /// Install packages from local directory
    #[arg(long, conflicts_with = "remote")]
    local: bool,

    /// Have the remote influxdb install packages
    #[arg(long)]
    remote: bool,
    /// The host URL of the running InfluxDB 3 Core server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    host_url: Url,
    /// The token for authentication with the InfluxDB 3 Core server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    auth_token: Option<Secret<String>>,

    /// The processing engine config.
    #[clap(flatten)]
    pub processing_engine_config: ProcessingEngineConfig,

    /// Path to requirements.txt file
    #[arg(short = 'r', long = "requirements")]
    requirements: Option<String>,

    /// Package names to install
    #[arg(required_unless_present = "requirements")]
    packages: Vec<String>,
}

impl PackageConfig {
    async fn run_command(&self) -> Result<(), anyhow::Error> {
        if self.local {
            self.run_command_local()
        } else if self.remote {
            self.run_command_remote().await
        } else {
            bail!("Please specify either --local or --remote")
        }
    }

    #[cfg(feature = "system-py")]
    fn run_command_local(&self) -> Result<(), anyhow::Error> {
        // we're running locally, so need to make sure we're in the environment before running commands.
        let environment_manager =
            setup_processing_engine_env_manager(&self.processing_engine_config);
        if environment_manager.plugin_dir.is_none() {
            bail!("need plugin dir to install local packages")
        };
        // Initialize Python, so that we're operating against the virtual env if required.
        virtualenv::init_pyo3(&environment_manager.virtual_env_location);

        if let Some(requirements_path) = &self.requirements {
            environment_manager
                .package_manager
                .install_requirements(requirements_path.to_string())?;
        } else {
            environment_manager
                .package_manager
                .install_packages(self.packages.clone())?;
        }
        Ok(())
    }

    #[cfg(not(feature = "system-py"))]
    fn run_command_local(&self) -> Result<(), anyhow::Error> {
        bail!("can't run local commands without system-py enabled")
    }

    async fn run_command_remote(&self) -> Result<(), anyhow::Error> {
        let mut client = Client::new(self.host_url.clone())?;
        if let Some(token) = &self.auth_token {
            client = client.with_auth_token(token.expose_secret());
        }
        if let Some(requirements_path) = &self.requirements {
            client
                .api_v3_configure_processing_engine_trigger_install_requirements(requirements_path)
                .await?;
        } else {
            client
                .api_v3_configure_plugin_environment_install_packages(self.packages.clone())
                .await?;
        }
        Ok(())
    }
}
