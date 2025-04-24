use std::{error::Error, io, path::PathBuf};

use clap::{Parser, ValueEnum};
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
    #[clap(long = "tls-ca", env = "INFLUXDB3_TLS_CA")]
    pub ca_cert: Option<PathBuf>,

    /// Output format for token, supports just json or text
    #[clap(long)]
    pub format: Option<TokenOutputFormat>,
}

#[derive(Debug, ValueEnum, Clone)]
pub enum TokenOutputFormat {
    Json,
    Text,
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
        println!("Are you sure you want to regenerate admin token? Enter 'yes' to confirm",);
        let mut confirmation = String::new();
        let _ = io::stdin().read_line(&mut confirmation);
        if confirmation.trim() == "yes" {
            client
                .api_v3_configure_regenerate_admin_token()
                .await?
                .expect("token creation to return full token info")
        } else {
            return Err("Cannot regenerate token without confirmation".into());
        }
    } else {
        client
            .api_v3_configure_create_admin_token()
            .await?
            .expect("token creation to return full token info")
    };
    Ok(json_body)
}
