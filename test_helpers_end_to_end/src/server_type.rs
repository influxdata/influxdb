use super::addrs::BindAddresses;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServerType {
    AllInOne,
    Ingester,
    Router,
    Querier,
    Compactor,
    Catalog,
    ParquetCache,
}

impl ServerType {
    /// returns the name of the 'run' subcommand of the `influxdb_iox` binary
    pub fn run_command(&self) -> &'static str {
        match self {
            Self::AllInOne => "all-in-one",
            Self::Ingester => "ingester",
            Self::Router => "router",
            Self::Querier => "querier",
            Self::Compactor => "compactor",
            Self::Catalog => "catalog",
            Self::ParquetCache => "parquet-cache",
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
        ServerType::Ingester => vec![
            (
                "INFLUXDB_IOX_BIND_ADDR",
                addrs.ingester_http_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_GRPC_BIND_ADDR",
                addrs.ingester_grpc_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_GOSSIP_BIND_ADDR",
                addrs.ingester_gossip_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_GOSSIP_SEED_LIST",
                addrs
                    .all_gossip_apis()
                    .into_iter()
                    .map(|a| a.bind_addr().to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
        ],
        ServerType::Router => vec![
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
            (
                "INFLUXDB_IOX_GOSSIP_BIND_ADDR",
                addrs.router_gossip_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_GOSSIP_SEED_LIST",
                addrs
                    .all_gossip_apis()
                    .into_iter()
                    .map(|a| a.bind_addr().to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
        ],
        ServerType::Querier => vec![
            (
                "INFLUXDB_IOX_BIND_ADDR",
                addrs.querier_http_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_GRPC_BIND_ADDR",
                addrs.querier_grpc_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_GOSSIP_BIND_ADDR",
                addrs.querier_gossip_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_GOSSIP_SEED_LIST",
                addrs
                    .all_gossip_apis()
                    .into_iter()
                    .map(|a| a.bind_addr().to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
        ],
        ServerType::Compactor => vec![
            (
                "INFLUXDB_IOX_BIND_ADDR",
                addrs.compactor_http_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_GRPC_BIND_ADDR",
                addrs.compactor_grpc_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_GOSSIP_BIND_ADDR",
                addrs.compactor_gossip_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_GOSSIP_SEED_LIST",
                addrs
                    .all_gossip_apis()
                    .into_iter()
                    .map(|a| a.bind_addr().to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
        ],
        ServerType::Catalog => vec![
            (
                "INFLUXDB_IOX_BIND_ADDR",
                addrs.catalog_http_api().bind_addr().to_string(),
            ),
            (
                "INFLUXDB_IOX_GRPC_BIND_ADDR",
                addrs.catalog_grpc_api().bind_addr().to_string(),
            ),
        ],
        ServerType::ParquetCache => vec![(
            "INFLUXDB_IOX_BIND_ADDR",
            addrs.parquet_cache_http_api().bind_addr().to_string(),
        )],
    }
}
