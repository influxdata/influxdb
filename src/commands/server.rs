//! Implementation of command line option for manipulating and showing server
//! config

use crate::commands::server_remote;
use structopt::StructOpt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Remote: {0}")]
    RemoteError(#[from] server_remote::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, StructOpt)]
#[structopt(name = "server", about = "IOx server commands")]
pub enum Config {
    Remote(crate::commands::server_remote::Config),
}

pub async fn command(url: String, config: Config) -> Result<()> {
    match config {
        Config::Remote(config) => Ok(server_remote::command(url, config).await?),
    }
}
