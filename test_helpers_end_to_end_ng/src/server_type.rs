use std::process::Command;

use super::addrs::BindAddresses;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServerType {
    AllInOne,
    Ingester,
    Router,
    Querier,
}

impl ServerType {
    /// returns the name of the 'run' subcommand of the `influxdb_iox` binary
    pub fn run_command(&self) -> &'static str {
        match self {
            Self::AllInOne => "all-in-one",
            Self::Ingester => "ingester",
            Self::Router => "router",
            Self::Querier => "querier",
        }
    }
}

pub trait AddAddrEnv {
    /// add the relevant bind addreses for the server type
    fn add_addr_env(&mut self, server_type: ServerType, addrs: &BindAddresses) -> &mut Self;
}

impl AddAddrEnv for Command {
    fn add_addr_env(&mut self, server_type: ServerType, addrs: &BindAddresses) -> &mut Self {
        match server_type {
            ServerType::AllInOne => self
                .env(
                    "INFLUXDB_IOX_ROUTER_HTTP_BIND_ADDR",
                    addrs.router_http_api().bind_addr().as_ref(),
                )
                .env(
                    "INFLUXDB_IOX_ROUTER_GRPC_BIND_ADDR",
                    addrs.router_grpc_api().bind_addr().as_ref(),
                )
                .env(
                    "INFLUXDB_IOX_QUERIER_GRPC_BIND_ADDR",
                    addrs.querier_grpc_api().bind_addr().as_ref(),
                )
                .env(
                    "INFLUXDB_IOX_INGESTER_GRPC_BIND_ADDR",
                    addrs.ingester_grpc_api().bind_addr().as_ref(),
                )
                .env(
                    "INFLUXDB_IOX_COMPACTOR_GRPC_BIND_ADDR",
                    addrs.compactor_grpc_api().bind_addr().as_ref(),
                ),
            ServerType::Ingester => self
                .env(
                    "INFLUXDB_IOX_BIND_ADDR",
                    addrs.router_http_api().bind_addr().as_ref(),
                )
                .env(
                    "INFLUXDB_IOX_GRPC_BIND_ADDR",
                    addrs.ingester_grpc_api().bind_addr().as_ref(),
                ),
            ServerType::Router => self
                .env(
                    "INFLUXDB_IOX_BIND_ADDR",
                    addrs.router_http_api().bind_addr().as_ref(),
                )
                .env(
                    "INFLUXDB_IOX_GRPC_BIND_ADDR",
                    addrs.router_grpc_api().bind_addr().as_ref(),
                ),
            ServerType::Querier => self
                .env(
                    "INFLUXDB_IOX_BIND_ADDR",
                    addrs.router_http_api().bind_addr().as_ref(),
                )
                .env(
                    "INFLUXDB_IOX_GRPC_BIND_ADDR",
                    addrs.querier_grpc_api().bind_addr().as_ref(),
                ),
        }
    }
}
