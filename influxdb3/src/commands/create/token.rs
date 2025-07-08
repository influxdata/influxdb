use std::{error::Error, io, path::PathBuf};

use clap::{
    Arg, Args, Command as ClapCommand, CommandFactory, Error as ClapError, FromArgMatches, Parser,
    ValueEnum, error::ErrorKind,
};
use influxdb3_client::Client;
use influxdb3_types::http::CreateTokenWithPermissionsResponse;
use owo_colors::OwoColorize;
use secrecy::Secret;
use url::Url;

pub(crate) async fn handle_token_creation_with_config(
    client: Client,
    config: CreateTokenConfig,
) -> Result<CreateTokenWithPermissionsResponse, Box<dyn Error>> {
    match config.admin_config {
        Some(admin_config) => {
            if admin_config.name.is_some() {
                handle_named_admin_token_creation(client, admin_config).await
            } else {
                handle_admin_token_creation(client, admin_config).await
            }
        }
        _ => Err(
            "cannot create token, error with parameters run `influxdb3 create token --help`".into(),
        ),
    }
}

pub(crate) async fn handle_admin_token_creation(
    client: Client,
    config: CreateAdminTokenConfig,
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

pub(crate) async fn handle_named_admin_token_creation(
    client: Client,
    config: CreateAdminTokenConfig,
) -> Result<CreateTokenWithPermissionsResponse, Box<dyn Error>> {
    let json_body = client
        .api_v3_configure_create_named_admin_token(
            config.name.expect("token name to be present"),
            config.expiry.map(|expiry| expiry.as_secs()),
        )
        .await?
        .expect("token creation to return full token info");
    Ok(json_body)
}

#[derive(Debug, ValueEnum, Clone, Copy)]
pub enum TokenOutputFormat {
    Json,
    Text,
}

#[derive(Parser, Clone, Debug)]
pub struct InfluxDb3ServerConfig {
    /// The host URL of the running InfluxDB 3 Core server.
    ///
    /// Note: When using --regenerate, the effective default changes to http://127.0.0.1:8182
    /// (admin token recovery endpoint) unless a custom host is specified.
    #[clap(
        name = "host",
        long = "host",
        default_value = "http://127.0.0.1:8181",
        env = "INFLUXDB3_HOST_URL",
        help = "The host URL of the running InfluxDB 3 Core server (default: http://127.0.0.1:8181, or :8182 with --regenerate)"
    )]
    pub host_url: Url,

    /// The token for authentication with the InfluxDB 3 Core server to create permissions.
    /// This will be the admin token to create tokens with permissions
    #[clap(name = "token", long = "token", env = "INFLUXDB3_AUTH_TOKEN")]
    pub auth_token: Option<Secret<String>>,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(name = "tls-ca", long = "tls-ca")]
    pub ca_cert: Option<PathBuf>,
}

#[derive(Parser, Debug)]
pub struct CreateAdminTokenConfig {
    /// Operator token will be regenerated when this is set.
    ///
    /// When used without --host, connects to the admin token recovery endpoint (port 8182)
    /// instead of the default server endpoint (port 8181).
    #[clap(
        name = "regenerate",
        long = "regenerate",
        help = "Regenerate the operator token (uses port 8182 by default instead of 8181)"
    )]
    pub regenerate: bool,

    // for named admin and permission tokens this is mandatory but not for admin tokens
    /// Name of the token
    #[clap(long)]
    pub name: Option<String>,

    /// Expires in `duration`,
    ///   e.g 10d for 10 days
    ///       1y for 1 year
    #[clap(long)]
    pub expiry: Option<humantime::Duration>,

    #[clap(flatten)]
    pub host: InfluxDb3ServerConfig,

    /// Output format for token, supports just json or text
    #[clap(long)]
    pub format: Option<TokenOutputFormat>,
}

impl CreateAdminTokenConfig {
    pub fn as_args() -> Vec<Arg> {
        let admin_config = Self::command();
        let args = admin_config.get_arguments();
        args.into_iter().map(|arg| arg.to_owned()).collect()
    }
}

// There are few traits manually implemented for CreateTokenConfig. The reason is,
//   `influxdb3 create token --permission` was implemented as subcommands. With clap it is not
//   possible to have multiple `--permission` when it is implemented as a subcommand. In order to
//   maintain backwards compatibility `CreateTokenConfig` is implemented with roughly the shape of
//   an enum. But it is wired manually into clap's lifecycle by implementing the traits,
//     - `CommandFactory`, this allows us to dynamically switch the "expected" command.
//         For example,
//           - when triggering `--help` the command sent back is exactly the same as what was
//           before (using subcommands). The help messages are overridden so that redundant
//           switches are removed in global "usage" section.
//           - when triggered without `--help` switch then it has `--admin` as a subcommand and the
//           non admin config is included directly on the CreateTokenConfig. This is key, as this
//           enables `--permission` to be set multiple times.
//     - `FromArgMatches`, this allows us to check if it's for `--admin` and populate the right
//       variant. This is a handle for getting all the matches (based on command generated) that we
//       could use to initialise `CreateTokenConfig`
#[derive(Debug)]
pub struct CreateTokenConfig {
    pub admin_config: Option<CreateAdminTokenConfig>,
}

impl CreateTokenConfig {
    pub fn get_connection_settings(&self) -> Result<&InfluxDb3ServerConfig, &'static str> {
        match &self.admin_config {
            Some(admin_config) => Ok(&admin_config.host),
            None => Err("cannot find server config"),
        }
    }

    /// Get the effective host URL for the operation.
    ///
    /// When `--regenerate` is used and no custom host is provided, this will return
    /// the admin token recovery endpoint (port 8182) instead of the default (port 8181).
    ///
    /// # Examples
    /// - `influxdb3 create token --admin` → uses http://127.0.0.1:8181
    /// - `influxdb3 create token --admin --regenerate` → uses http://127.0.0.1:8182
    /// - `influxdb3 create token --admin --regenerate --host http://custom:9999` → uses http://custom:9999
    pub fn get_effective_host_url(&self, default_url: &Url) -> Url {
        match &self.admin_config {
            Some(admin_config) if admin_config.regenerate => {
                // Check if the host URL is the default value (normalize by removing trailing slash)
                if default_url.as_str().trim_end_matches('/') == "http://127.0.0.1:8181" {
                    // Use the admin token recovery endpoint
                    Url::parse("http://127.0.0.1:8182").expect("hardcoded URL should be valid")
                } else {
                    // User provided a custom URL, use it as-is
                    default_url.clone()
                }
            }
            _ => default_url.clone(),
        }
    }

    pub fn get_output_format(&self) -> Option<&TokenOutputFormat> {
        match &self.admin_config {
            Some(admin_config) => admin_config.format.as_ref(),
            None => None,
        }
    }
}

impl FromArgMatches for CreateTokenConfig {
    fn from_arg_matches(matches: &clap::ArgMatches) -> Result<Self, clap::Error> {
        let admin_subcmd_matches = matches
            .subcommand_matches("--admin")
            .expect("--admin must be present");
        let name = admin_subcmd_matches.get_one::<String>("name");
        let regenerate = admin_subcmd_matches
            .get_one::<bool>("regenerate")
            .cloned()
            .unwrap_or_default();

        if name.is_some() && regenerate {
            return Err(ClapError::raw(
                ErrorKind::ArgumentConflict,
                "--regenerate cannot be used with --name, --regenerate only applies for operator token".yellow(),
            ));
        }

        Ok(Self {
            admin_config: Some(CreateAdminTokenConfig::from_arg_matches(
                admin_subcmd_matches,
            )?),
        })
    }

    fn update_from_arg_matches(&mut self, matches: &clap::ArgMatches) -> Result<(), clap::Error> {
        *self = Self::from_arg_matches(matches)?;
        Ok(())
    }
}

impl Args for CreateTokenConfig {
    // we're not flattening so these can just return command()
    fn augment_args(_cmd: clap::Command) -> clap::Command {
        Self::command()
    }

    fn augment_args_for_update(_cmd: clap::Command) -> clap::Command {
        Self::command()
    }
}

impl CommandFactory for CreateTokenConfig {
    fn command() -> clap::Command {
        let admin_sub_cmd = ClapCommand::new("--admin")
            .override_usage("influxdb3 create token --admin [OPTIONS]")
            .about("Create or regenerate an admin token")
            .long_about(
                "Create or regenerate an admin token.\n\n\
                            When using --regenerate without specifying --host, the command will \
                            connect to the admin token recovery endpoint (http://127.0.0.1:8182) \
                            instead of the default server endpoint (http://127.0.0.1:8181).",
            );
        let all_args = CreateAdminTokenConfig::as_args();
        let admin_sub_cmd = admin_sub_cmd.args(all_args);

        // NB: Because of using enum variants `--admin` and `--permission`, we require this
        //     elaborate wiring of `--permission` to allow `--permission` to be specified
        //     multiple times. See `CreateTokenConfig` for full explanation
        ClapCommand::new("token").subcommand(admin_sub_cmd)
    }

    fn command_for_update() -> clap::Command {
        Self::command()
    }
}
