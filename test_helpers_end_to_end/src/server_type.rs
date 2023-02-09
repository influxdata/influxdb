use super::addrs::BindAddresses;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServerType {
    AllInOne,
    Ingester2,
    Router2,
    Querier2,
    Compactor2,
}

impl ServerType {
    /// returns the name of the 'run' subcommand of the `influxdb_iox` binary
    pub fn run_command(&self) -> &'static str {
        match self {
            Self::AllInOne => "all-in-one",
            Self::Ingester2 => "ingester2",
            Self::Router2 => "router2",
            Self::Querier2 => "querier",
            Self::Compactor2 => "compactor2",
        }
    }
}

pub trait AddAddrEnv {
    /// add the relevant bind addreses for the server type
    fn add_addr_env(&mut self, server_type: ServerType, addrs: &BindAddresses) -> &mut Self;
}

impl AddAddrEnv for std::process::Command {
    fn add_addr_env(&mut self, server_type: ServerType, addrs: &BindAddresses) -> &mut Self {
        self.envs(addr_envs(server_type, addrs))
    }
}

impl AddAddrEnv for assert_cmd::Command {
    fn add_addr_env(&mut self, server_type: ServerType, addrs: &BindAddresses) -> &mut Self {
        self.envs(addr_envs(server_type, addrs))
    }
}

fn addr_envs(server_type: ServerType, addrs: &BindAddresses) -> Vec<(&'static str, String)> {
    match server_type {
        ServerType::AllInOne => vec![
            (
                "INFLUXDB_IOX_ROUTER_HTTP_BIND_ADDR",
                addrs.router_http_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_ROUTER_GRPC_BIND_ADDR",
                addrs.router_grpc_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_QUERIER_GRPC_BIND_ADDR",
                addrs.querier_grpc_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_INGESTER_GRPC_BIND_ADDR",
                addrs.ingester_grpc_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_COMPACTOR_GRPC_BIND_ADDR",
                addrs.compactor_grpc_api().bind_addr().to_string(),
            ),
        ],
        ServerType::Ingester2 => vec![
            (
                "INFLUXDB_IOX_BIND_ADDR",
                addrs.router_http_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_GRPC_BIND_ADDR",
                addrs.ingester_grpc_api().bind_addr().to_string(),
            ),
            ("INFLUXDB_IOX_RPC_MODE", "2".to_string()),
        ],
        ServerType::Router2 => vec![
            (
                "INFLUXDB_IOX_BIND_ADDR",
                addrs.router_http_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_GRPC_BIND_ADDR",
                addrs.router_grpc_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_INGESTER_ADDRESSES",
                addrs.ingester_grpc_api().bind_addr().to_string(),
            ),
            ("INFLUXDB_IOX_RPC_MODE", "2".to_string()),
        ],
        ServerType::Querier2 => vec![
            (
                "INFLUXDB_IOX_BIND_ADDR",
                addrs.router_http_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_GRPC_BIND_ADDR",
                addrs.querier_grpc_api().bind_addr().to_string(),
            ),
            ("INFLUXDB_IOX_RPC_MODE", "2".to_string()),
        ],
        ServerType::Compactor2 => vec![
            (
                "INFLUXDB_IOX_BIND_ADDR",
                addrs.router_http_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_GRPC_BIND_ADDR",
                addrs.compactor_grpc_api().bind_addr().to_string(),
            ),
            ("INFLUXDB_IOX_RPC_MODE", "2".to_string()),
        ],
    }
}
