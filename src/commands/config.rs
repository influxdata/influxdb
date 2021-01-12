//! Implementation of command line option for manipulating and showing server
//! config

use std::{io::ErrorKind, net::SocketAddr, path::PathBuf};

use dotenv::dotenv;
use lazy_static::lazy_static;
use structopt::StructOpt;

/// The default bind address for the HTTP API.
pub const DEFAULT_API_BIND_ADDR: &str = "127.0.0.1:8080";

/// The default bind address for the gRPC.
pub const DEFAULT_GRPC_BIND_ADDR: &str = "127.0.0.1:8082";

lazy_static! {
    static ref DEFAULT_DATA_DIR: String = dirs::home_dir()
        .map(|mut path| {
            path.push(".influxdb_iox");
            path
        })
        .and_then(|dir| dir.to_str().map(|s| s.to_string()))
        .unwrap();
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "server",
    about = "Runs in server mode (default)",
    long_about = "Run the IOx server.\n\nThe configuration options below can be \
    set either with the command line flags or with the specified environment \
    variable. If there is a file named '.env' in the current working directory, \
    it is sourced before loading the configuration.
    
Configuration is loaded from the following sources (highest precedence first):
        - command line arguments
        - user set environment variables
        - .env file contents
        - pre-configured default values"
)]
pub struct Config {
    /// This controls the IOx server logging level, as described in
    /// https://crates.io/crates/env_logger.
    ///
    /// Levels for different modules can be specified as well. For example
    /// `debug,hyper::proto::h1=info` specifies debug logging for all modules
    /// except for the `hyper::proto::h1' module which will only display info
    /// level logging.
    #[structopt(long = "--log", env = "RUST_LOG")]
    pub rust_log: Option<String>,

    /// The identifier for the server.
    ///
    /// Used for writing to object storage and as an identifier that is added to
    /// replicated writes, WAL segments and Chunks. Must be unique in a group of
    /// connected or semi-connected IOx servers. Must be a number that can be
    /// represented by a 32-bit unsigned integer.
    #[structopt(long = "--writer-id", env = "INFLUXDB_IOX_ID")]
    pub writer_id: Option<u32>,

    /// The address on which IOx will serve HTTP API requests.
    #[structopt(
        long = "--api-bind", 
        env = "INFLUXDB_IOX_BIND_ADDR", 
        default_value = DEFAULT_API_BIND_ADDR,
    )]
    pub http_bind_address: SocketAddr,

    /// The address on which IOx will serve Storage gRPC API requests.
    #[structopt(
        long = "--grpc-bind", 
        env = "INFLUXDB_IOX_GRPC_BIND_ADDR", 
        default_value = DEFAULT_GRPC_BIND_ADDR,
    )]
    pub grpc_bind_address: SocketAddr,

    /// The location InfluxDB IOx will use to store files locally.
    #[structopt(long = "--data-dir", env = "INFLUXDB_IOX_DB_DIR", default_value = &DEFAULT_DATA_DIR)]
    pub database_directory: PathBuf,

    /// If using Google Cloud Storage for the object store, this item, as well
    /// as SERVICE_ACCOUNT must be set.
    #[structopt(long = "--gcp-bucket", env = "INFLUXDB_IOX_GCP_BUCKET")]
    pub gcp_bucket: Option<String>,
}

/// Load the config.
///
/// This pulls in config from the following sources, in order of precedence:
///
///     - command line arguments
///     - user set environment variables
///     - .env file contents
///     - pre-configured default values
pub fn load_config() -> Config {
    // Source the .env file before initialising the Config struct - this sets
    // any envs in the file, which the Config struct then uses.
    //
    // Precedence is given to existing env variables.
    match dotenv() {
        Ok(_) => {}
        Err(dotenv::Error::Io(err)) if err.kind() == ErrorKind::NotFound => {
            // Ignore this - a missing env file is not an error, defaults will
            // be applied when initialising the Config struct.
        }
        Err(e) => {
            eprintln!("FATAL Error loading config: {}", e);
            eprintln!("Aborting");
            std::process::exit(1);
        }
    };

    // Load the Config struct - this pulls in any envs set by the user or
    // sourced above, and applies any defaults.
    //
    // Strip the "server" portion of the args so the generated Clap instance
    // plays nicely with the subcommand bits in main.
    let args = std::env::args().skip(1);

    Config::from_iter(args)
}
