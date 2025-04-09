use std::{error::Error, path::PathBuf};

use clap::Parser;
use influxdb3_client::Client;
use influxdb3_types::http::CreateTokenWithPermissionsResponse;
use secrecy::Secret;
use url::Url;

#[derive(Debug, clap::Parser)]
pub struct TokenCommands {
    #[clap(subcommand)]
    pub commands: TokenSubCommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum TokenSubCommand {
    #[clap(name = "--admin")]
    Admin(AdminTokenConfig),
}

#[derive(Debug, Parser)]
pub struct AdminTokenConfig {
    /// The host URL of the running InfluxDB 3 Enterprise server
    #[clap(
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    pub host_url: Url,

    /// Admin token will be regenerated when this is set
    #[clap(long, default_value = "false")]
    pub regenerate: bool,

    /// The token for authentication with the InfluxDB 3 Enterprise server
    #[clap(long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    pub auth_token: Option<Secret<String>>,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(long = "tls-ca")]
    pub ca_cert: Option<PathBuf>,
}

pub(crate) async fn handle_token_creation(
    client: Client,
    config: TokenCommands,
) -> Result<CreateTokenWithPermissionsResponse, Box<dyn Error>> {
    match config.commands {
        TokenSubCommand::Admin(admin_token_config) => {
            handle_admin_token_creation(client, admin_token_config).await
        }
    }
}

pub(crate) async fn handle_admin_token_creation(
    client: Client,
    config: AdminTokenConfig,
) -> Result<CreateTokenWithPermissionsResponse, Box<dyn Error>> {
    let json_body = if config.regenerate {
        client
            .api_v3_configure_regenerate_admin_token()
            .await?
            .expect("token creation to return full token info")
    } else {
        client
            .api_v3_configure_create_admin_token()
            .await?
            .expect("token creation to return full token info")
    };
    Ok(json_body)
}
