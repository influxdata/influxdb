use sha2::Digest;
use sha2::Sha256;
use std::error::Error;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Debug, clap::Parser)]
pub enum SubCommand {
    Token { token: String },
}

pub fn command(config: Config) -> Result<(), Box<dyn Error>> {
    match config.cmd {
        SubCommand::Token { token } => {
            println!(
                "\
                Token Input: {token}\n\
                Hashed Output: {hashed}\n\n\
                Start the server with `influxdb3 serve --bearer-token {hashed}`\n\n\
                HTTP requests require the following header: \"Authorization: Bearer {token}\"\n\
                This will grant you access to every HTTP endpoint or deny it otherwise
            ",
                hashed = hex::encode(&Sha256::digest(&token)[..])
            );
        }
    }
    Ok(())
}
