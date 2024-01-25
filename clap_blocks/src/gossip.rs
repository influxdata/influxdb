//! CLI config for cluster gossip communication.

use crate::socket_addr::SocketAddr;
use std::str::FromStr;

/// Configuration parameters for the cluster gossip communication mechanism.
#[derive(Debug, Clone, clap::Parser)]
#[allow(missing_copy_implementations)]
pub struct GossipConfig {
    /// A comma-delimited set of seed gossip peer addresses.
    ///
    /// Example: "10.0.0.1:4242,10.0.0.2:4242"
    ///
    /// These seeds will be used to discover all other peers that talk to the
    /// same seeds. Typically all nodes in the cluster should use the same set
    /// of seeds.
    #[clap(
        long = "gossip-seed-list",
        env = "INFLUXDB_IOX_GOSSIP_SEED_LIST",
        required = false,
        num_args=1..,
        value_delimiter = ',',
        requires = "gossip_bind_address", // Field name, not flag
    )]
    pub seed_list: Vec<String>,

    /// The UDP socket address IOx will use for gossip communication between
    /// peers.
    ///
    /// Example: "0.0.0.0:4242"
    ///
    /// If not provided, the gossip sub-system is disabled.
    #[clap(
        long = "gossip-bind-address",
        env = "INFLUXDB_IOX_GOSSIP_BIND_ADDR",
        default_value = "0.0.0.0:4242",
        required = false,
        action
    )]
    pub gossip_bind_address: SocketAddr,
}

impl GossipConfig {
    /// constructor for GossipConfig
    ///
    pub fn new(bind_address: &str, seed_list: Vec<String>) -> Self {
        Self {
            seed_list,
            gossip_bind_address: SocketAddr::from_str(bind_address).unwrap(),
        }
    }
}
