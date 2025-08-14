use std::{error::Error, io, path::PathBuf, sync::Arc};

use clap::{
    Arg, Args, Command as ClapCommand, CommandFactory, Error as ClapError, FromArgMatches, Parser,
    ValueEnum, error::ErrorKind,
};
use influxdb3_authz::TokenInfo;
use influxdb3_catalog::catalog::{compute_token_hash, create_token_and_hash};
use influxdb3_client::Client;
use influxdb3_types::http::CreateTokenWithPermissionsResponse;
use owo_colors::OwoColorize;
use secrecy::Secret;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct AdminTokenFile {
    /// The raw token string
    pub token: String,
    /// The token name
    pub name: String,
    /// Optional expiry timestamp in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiry_millis: Option<i64>,
}
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
    if config.offline {
        // Generate token without server
        let token = generate_offline_token();

        let output_file = config
            .output_file
            .ok_or("--output-file is required with --offline")?;

        // Create admin token file with metadata
        let token_file = AdminTokenFile {
            token: token.clone(),
            name: "_admin".to_string(),
            expiry_millis: None,
        };

        let json = serde_json::to_string_pretty(&token_file)?;

        // Write token atomically with correct permissions
        write_file_atomically(&output_file, &json)?;

        println!("Token saved to: {}", output_file.display());

        // For offline mode, we return a mock success response
        let hash = compute_token_hash(&token);
        let token_info = Arc::new(TokenInfo {
            id: 0.into(),
            name: Arc::from("_admin"),
            hash,
            description: None,
            created_by: None,
            created_at: chrono::Utc::now().timestamp_millis(),
            updated_at: None,
            updated_by: None,
            expiry_millis: i64::MAX,
            permissions: vec![], // Admin tokens don't need explicit permissions
        });

        CreateTokenWithPermissionsResponse::from_token_info(token_info, token)
            .ok_or_else(|| "Failed to create token response".into())
    } else {
        let json_body = if config.regenerate {
            println!("Are you sure you want to regenerate admin token? Enter 'yes' to confirm",);
            let mut confirmation = String::new();
            io::stdin().read_line(&mut confirmation)?;
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
}

fn generate_offline_token() -> String {
    create_token_and_hash().0
}

pub(crate) async fn handle_named_admin_token_creation(
    client: Client,
    config: CreateAdminTokenConfig,
) -> Result<CreateTokenWithPermissionsResponse, Box<dyn Error>> {
    if config.offline {
        // Generate token without server
        let token = generate_offline_token();

        let output_file = config
            .output_file
            .ok_or("--output-file is required with --offline")?;

        let token_name = config.name.expect("token name to be present");

        let expiry_millis = config
            .expiry
            .map(|expiry| chrono::Utc::now().timestamp_millis() + (expiry.as_secs() as i64 * 1000));
        let token_file = AdminTokenFile {
            token: token.clone(),
            name: token_name.clone(),
            expiry_millis,
        };

        let json = serde_json::to_string_pretty(&token_file)?;

        // Write token atomically with correct permissions
        write_file_atomically(&output_file, &json)?;

        println!("Token saved to: {}", output_file.display());

        // For offline mode, we return a mock success response
        let hash = compute_token_hash(&token);
        let token_info = Arc::new(TokenInfo {
            id: 0.into(),
            name: Arc::from(token_name.as_str()),
            hash,
            description: None,
            created_by: None,
            created_at: chrono::Utc::now().timestamp_millis(),
            updated_at: None,
            updated_by: None,
            expiry_millis: expiry_millis.unwrap_or(i64::MAX),
            permissions: vec![], // Admin tokens don't need explicit permissions
        });

        CreateTokenWithPermissionsResponse::from_token_info(token_info, token)
            .ok_or_else(|| "Failed to create token response".into())
    } else {
        let json_body = client
            .api_v3_configure_create_named_admin_token(
                config.name.expect("token name to be present"),
                config.expiry.map(|expiry| expiry.as_secs()),
            )
            .await?
            .expect("token creation to return full token info");
        Ok(json_body)
    }
}

#[derive(Debug, ValueEnum, Clone, Copy)]
pub enum TokenOutputFormat {
    Json,
    Text,
}

#[derive(Parser, Clone, Debug)]
pub struct InfluxDb3ServerConfig {
    /// The host URL of the running InfluxDB 3 Core server.
    #[clap(
        name = "host",
        short = 'H',
        long = "host",
        env = "INFLUXDB3_HOST_URL",
        default_value = "http://127.0.0.1:8181"
    )]
    pub host_url: Url,

    /// The token for authentication with the InfluxDB 3 Core server to create permissions.
    /// This will be the admin token to create tokens with permissions
    #[clap(
        name = "token",
        long = "token",
        env = "INFLUXDB3_AUTH_TOKEN",
        hide_env_values = true
    )]
    pub auth_token: Option<Secret<String>>,

    /// An optional arg to use a custom ca for useful for testing with self signed certs
    #[clap(name = "tls-ca", long = "tls-ca")]
    pub ca_cert: Option<PathBuf>,
}

#[derive(Parser, Debug)]
pub struct CreateAdminTokenConfig {
    /// Operator token will be regenerated when this is set.
    ///
    /// When used without --host, connects to the default server endpoint (port 8181).
    /// To use the admin token recovery endpoint, specify --host with the recovery endpoint address.
    #[clap(
        name = "regenerate",
        long = "regenerate",
        help = "Regenerate the operator token. By default connects to the main server (http://127.0.0.1:8181).
                To use the admin token recovery endpoint, specify --host with the recovery endpoint address"
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

    /// Generate token without connecting to server (enterprise feature)
    #[clap(long, requires = "output_file")]
    pub offline: bool,

    /// File path to save the token (required with --offline)
    #[clap(long, value_name = "FILE")]
    pub output_file: Option<PathBuf>,
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
                            When using --regenerate, the command connects to the default server \
                            endpoint (http://127.0.0.1:8181) unless you specify a different --host. \
                            To use the admin token recovery endpoint, specify --host with the \
                            recovery endpoint address configured via --admin-token-recovery-http-bind.",
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

/// Write a file atomically by writing to a temporary file and moving it into place
/// This ensures the file either exists with correct permissions or doesn't exist at all
pub(crate) fn write_file_atomically(path: &PathBuf, content: &str) -> Result<(), Box<dyn Error>> {
    use std::io::Write;

    // Ensure content ends with a newline
    let content_with_newline = if content.ends_with('\n') {
        content.to_string()
    } else {
        format!("{content}\n")
    };

    // Create a temporary file in the same directory as the target
    let parent = path.parent().ok_or("Invalid file path")?;
    let file_name = path.file_name().ok_or("Invalid file name")?;
    let temp_path = parent.join(format!(".{}.tmp", file_name.to_string_lossy()));

    // Write to temporary file with proper error handling
    if let Err(e) = || -> Result<(), Box<dyn Error>> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600) // Set permissions atomically during creation
                .open(&temp_path)?;

            file.write_all(content_with_newline.as_bytes())?;
            file.sync_all()?; // Ensure data is written to disk
        }

        #[cfg(not(unix))]
        {
            use std::fs::File;
            let mut file = File::create(&temp_path)?;
            file.write_all(content_with_newline.as_bytes())?;
            file.sync_all()?;
        }

        Ok(())
    }() {
        // Clean up temp file on error
        let _ = std::fs::remove_file(&temp_path);
        return Err(e);
    }

    std::fs::rename(&temp_path, path).map_err(|e| -> Box<dyn Error> {
        // Clean up temp file on rename error
        let _ = std::fs::remove_file(&temp_path);
        format!("Failed to atomically rename temporary file: {e}").into()
    })?;

    Ok(())
}
