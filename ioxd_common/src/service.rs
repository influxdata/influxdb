use std::sync::Arc;

use clap_blocks::{run_config::RunConfig, socket_addr::SocketAddr};

use crate::server_type::ServerType;

/// A service that will start on the specified addresses
#[derive(Debug)]
pub struct Service {
    pub http_bind_address: Option<SocketAddr>,
    pub grpc_bind_address: SocketAddr,
    pub server_type: Arc<dyn ServerType>,
}

impl Service {
    pub fn create(server_type: Arc<dyn ServerType>, run_config: &RunConfig) -> Self {
        Self {
            http_bind_address: Some(run_config.http_bind_address),
            grpc_bind_address: run_config.grpc_bind_address,
            server_type,
        }
    }

    pub fn create_grpc_only(server_type: Arc<dyn ServerType>, run_config: &RunConfig) -> Self {
        Self {
            http_bind_address: None,
            grpc_bind_address: run_config.grpc_bind_address,
            server_type,
        }
    }
}
