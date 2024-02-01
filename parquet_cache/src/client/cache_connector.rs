use std::fmt::Debug;

use tower::{Layer, ServiceBuilder};

use super::{http::HttpService, keyspace::HostKeyspaceService};

pub type ClientCacheConnector = HostKeyspaceService<HttpService>;

/// Data cache errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Failure getting data from the cache.
    #[error("Fetch error: {0}")]
    FetchData(#[from] super::keyspace::Error),

    /// Failure reading the (already fetched) data from cache.
    #[error("Data error: {0}")]
    ReadData(String),
}

/// Builder for the cache connector service.
pub fn build_cache_connector(ns_service_addr: impl ToString) -> ClientCacheConnector {
    ServiceBuilder::new()
        .layer(MapToHost(ns_service_addr.to_string()))
        .service(HttpService::new())
}

#[derive(Debug)]
struct MapToHost(pub String);

impl<S> Layer<S> for MapToHost {
    type Service = HostKeyspaceService<S>;

    fn layer(&self, service: S) -> Self::Service {
        HostKeyspaceService::new(service, self.0.clone())
    }
}
