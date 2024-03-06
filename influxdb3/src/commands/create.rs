use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine as _;
use rand::rngs::OsRng;
use rand::RngCore;
use sha2::Digest;
use sha2::Sha512;
use std::error::Error;

#[derive(Debug, clap::Parser)]
pub struct Config {
    #[clap(subcommand)]
    cmd: SubCommand,
}

#[derive(Debug, clap::Parser)]
pub enum SubCommand {
    Token,
}

pub fn command(config: Config) -> Result<(), Box<dyn Error>> {
    match config.cmd {
        SubCommand::Token => {
            let token = {
                let mut token = String::from("apiv3_");
                let mut key = [0u8; 64];
                OsRng.fill_bytes(&mut key);
                token.push_str(&B64.encode(key));
                token
            };
            println!(
                "\
                Token: {token}\n\
                Hashed Token: {hashed}\n\n\
                Start the server with `influxdb3 serve --bearer-token {hashed}`\n\n\
                HTTP requests require the following header: \"Authorization: Bearer {token}\"\n\
                This will grant you access to every HTTP endpoint or deny it otherwise
            ",
                hashed = B64.encode(&Sha512::digest(&token)[..])
            );
        }
    }
    Ok(())
}
