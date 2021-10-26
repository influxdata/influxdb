use data_types::server_id::ServerId;
use structopt::StructOpt;

/// CLI config for server ID.
#[derive(Debug, StructOpt, Clone)]
pub struct ServerIdConfig {
    /// The identifier for the server.
    ///
    /// Used for writing to object storage and as an identifier that is added to
    /// replicated writes, write buffer segments, and Chunks. Must be unique in
    /// a group of connected or semi-connected IOx servers. Must be a nonzero
    /// number that can be represented by a 32-bit unsigned integer.
    #[structopt(long = "--server-id", env = "INFLUXDB_IOX_ID")]
    pub server_id: Option<ServerId>,
}
