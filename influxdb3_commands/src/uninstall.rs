use std::path::PathBuf;

use influxdb3_client::Client;
use secrecy::Secret;
use url::Url;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum SubCommand {
    /// Uninstall packages from the plugin environment
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
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    host_url: Url,

    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN", hide_env_values = true)]
    auth_token: Option<Secret<String>>,

    /// Package names to uninstall, versions are not accepted.
    #[arg(required = true)]
    packages: Vec<String>,

    /// An optional arg to use a custom CA, useful for testing with self-signed certs
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    ca_cert: Option<PathBuf>,

    /// Disable TLS certificate verification
    #[clap(long = "tls-no-verify", env = "INFLUXDB3_TLS_NO_VERIFY")]
    tls_no_verify: bool,
}

impl PackageConfig {
    async fn run_command(&self) -> Result<(), anyhow::Error> {
        let client = Client::new(
            self.host_url.clone(),
            self.ca_cert.clone(),
            self.tls_no_verify,
        )?
        .with_resolved_auth_token(self.auth_token.as_ref())
        .await?;
        client
            .api_v3_configure_plugin_environment_uninstall_packages(self.packages.clone())
            .await?;
        Ok(())
    }
}
