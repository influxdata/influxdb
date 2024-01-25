use std::{collections::HashMap, sync::Arc, task::Poll};

use arc_swap::ArcSwap;
use backoff::{Backoff, BackoffConfig};
use bytes::Buf;
use http::uri::Authority;
use hyper::{Body, Method, Response, StatusCode, Uri};
use mpchash::HashRing;
use observability_deps::tracing::warn;
use tokio::sync::OnceCell;
use tower::{Service, ServiceExt};

use super::request::{PinnedFuture, RawRequest};
use crate::data_types::{KeyspaceResponseBody, ServiceNode, ServiceNodeId};

/// Errors associated fetching data from the cache.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Generic connection failure to remote data cache service.
    #[error("Connection error: {0}")]
    Connection(#[from] hyper::Error),

    /// Error in constructing request.
    #[error("Request error: {0}")]
    Request(String),

    /// Error with hashring keyspace
    #[error("Keyspace error: {0}")]
    Keyspace(String),

    /// Invalid addr
    #[error("Invalid addr: {0}")]
    InvalidAddr(#[from] http::uri::InvalidUri),

    /// Failure reading data from cache.
    #[error("Data error: {0}")]
    ReadData(String),
}

#[derive(Debug, Clone)]
pub struct HostKeyspaceService<S> {
    /// Inner service
    service: S,
    /// Namespace service addr (for requests to any cache server).
    dst: String,
    /// Inner state
    inner: Arc<HostKeyspace>,
}

impl<S> HostKeyspaceService<S> {
    /// Create keyspace middleware [`HostKeyspaceService`]
    pub fn new(service: S, dst: String) -> Self {
        Self {
            service,
            dst,
            inner: Default::default(),
        }
    }
}

impl<S> HostKeyspaceService<S>
where
    S: Clone + Send + Sync + Service<RawRequest, Response = Response<Body>, Error = hyper::Error>,
    for<'b> <S as Service<RawRequest>>::Future: std::marker::Send + 'b,
{
    /// Primary goal of [`HostKeyspaceService`] is to add the host to the [`RawRequest`].
    async fn add_host_to_request(&mut self, mut req: RawRequest) -> Result<RawRequest, Error> {
        let host = match &req.key {
            Some(obj_key) => self.hostname(obj_key).await?,
            None => self.dst.clone(), // k8s namespace service addr
        };

        req.uri_parts.authority =
            Some(Authority::from_maybe_shared(host).map_err(Error::InvalidAddr)?);

        Ok(req)
    }

    /// Hostname provided based upon hashed keyspace.
    /// Lookup, if missing the re-query service for the latest keyspace.
    async fn hostname(&mut self, key: &String) -> Result<String, Error> {
        let node = self.inner.key_to_node(key);

        match self.inner.hostname_table.load().get(&node) {
            Some(hostname) => Ok(hostname.to_owned()),
            None => {
                let keyspace = self.get_service_nodes().await?;
                let inner = &mut self.inner;
                inner.build_keyspace(keyspace);
                let node = inner.key_to_node(key);

                let hostname = inner.hostname_table
                    .load()
                    .get(&node)
                    .ok_or(Error::Keyspace(format!("key {} was assigned to node {}, but node was not found in latest keyspace hosts", key, node)))?
                    .to_owned();
                Ok(hostname)
            }
        }
    }

    /// Get list of [`ServiceNode`]s from cache service.
    async fn get_service_nodes(&mut self) -> Result<Vec<ServiceNode>, Error> {
        // use the Namespace service addr (self.dst), and not an individual server, to fetch the keyspace.
        let uri_parts = format!("{}/keyspace", &self.dst)
            .parse::<Uri>()
            .map(http::uri::Parts::from)
            .map_err(Error::InvalidAddr)?;

        let req = RawRequest {
            uri_parts,
            method: Method::GET,
            ..Default::default()
        };

        let service = self.service.ready().await?;
        let resp = service.call(req).await.map_err(Error::Connection)?;

        match resp.status() {
            StatusCode::OK => {
                let reader = hyper::body::aggregate(resp.into_body())
                    .await
                    .map_err(|e| Error::Keyspace(e.to_string()))?
                    .reader();

                let keyspace_nodes: KeyspaceResponseBody =
                    serde_json::from_reader(reader).map_err(|e| Error::Keyspace(e.to_string()))?;

                Ok(keyspace_nodes.nodes)
            }
            _ => Err(Error::Keyspace(String::from("keyspace request failure"))),
        }
    }

    /// Initialize the keyspace on service start.
    /// Has backoff-and-retry; intended to be called once.
    async fn initialized(&mut self) {
        Backoff::new(&BackoffConfig::default())
            .retry_all_errors("probe data cache service for keyspace", || {
                let mut this = self.clone();
                async move {
                    let probe = this
                        .get_service_nodes()
                        .await
                        .map(|keyspace| this.inner.build_keyspace(keyspace));
                    if probe.is_err() {
                        warn!("failed to build data cache keyspace");
                    }
                    probe
                }
            })
            .await
            .expect("retry forever")
    }
}

impl<S> Service<RawRequest> for HostKeyspaceService<S>
where
    S: Clone
        + Service<RawRequest, Response = Response<Body>, Error = hyper::Error>
        + Send
        + Sync
        + 'static,
    for<'b> <S as Service<RawRequest>>::Future: std::marker::Send + 'b,
{
    type Response = S::Response;
    type Error = Error;
    type Future = PinnedFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RawRequest) -> Self::Future {
        let mut this = self.clone();
        Box::pin(async move {
            Arc::clone(&this.inner)
                .initialize_once
                .get_or_init(|| this.initialized())
                .await;
            let req = this.add_host_to_request(req).await?;
            this.service.call(req).await.map_err(Error::Connection)
        })
    }
}

#[derive(Debug, Default)]
struct HostKeyspace {
    /// Hashring
    keyspace: ArcSwap<HashRing<ServiceNodeId>>,
    /// Map nodes to hostname.
    hostname_table: ArcSwap<HashMap<ServiceNodeId, String>>,
    /// A single init of the shared, clonable keyspace.
    /// (Note that the re-building of an invalidated keyspace, is separate from this init.)
    initialize_once: OnceCell<()>,
}

impl HostKeyspace {
    /// Lookup key in keyspace
    fn key_to_node(&self, key: &String) -> ServiceNodeId {
        self.keyspace
            .load()
            .as_ref()
            .primary_node(key)
            .unwrap()
            .to_owned()
    }

    /// Build keyspace for cache connector, from list of [`ServiceNode`]s.
    fn build_keyspace(&self, keyspace_nodes: Vec<ServiceNode>) {
        let mut keyspace = HashRing::new();
        let mut hostname_table = HashMap::new();

        for node in keyspace_nodes {
            keyspace.add(node.id);
            hostname_table.insert(node.id, node.hostname);
        }

        self.keyspace.swap(Arc::new(keyspace));
        self.hostname_table.swap(Arc::new(hostname_table));
    }
}

#[cfg(test)]
mod test {
    use std::collections::hash_map::Entry;

    use parking_lot::Mutex;
    use rand::seq::SliceRandom;
    use uuid::Uuid;

    use super::super::http::HttpService;
    use crate::data_types::ServiceNode;

    use super::*;

    async fn assert_consistent_hashing(
        mut cache_connector: HostKeyspaceService<HttpService>,
        prev_assignments: Arc<Mutex<HashMap<String, String>>>,
    ) {
        // test with 100 files
        for _ in 0..100 {
            let key = format!("unique/location/{}/file.parquet", Uuid::new_v4());
            for _ in 0..1000 {
                let key = key.clone();

                let got = cache_connector
                    .hostname(&key)
                    .await
                    .expect("should assign hostname");
                let expected = match prev_assignments.lock().entry(key) {
                    Entry::Vacant(v) => {
                        v.insert(got.clone());
                        got.clone()
                    }
                    Entry::Occupied(o) => o.get().clone(),
                };

                assert_eq!(
                    got, expected,
                    "should match previous assignment {}, instead got {}",
                    expected, got
                );
            }
        }
    }

    #[tokio::test]
    async fn test_keyspace_hashing_is_consistent() {
        let remote_cache_connector =
            HostKeyspaceService::new(HttpService::default(), "foo".to_string());

        let keyspace_nodes = (0..100)
            .map(|id| ServiceNode {
                id,
                hostname: format!("cache-server-hostname-{}", id),
            })
            .collect();
        remote_cache_connector.inner.build_keyspace(keyspace_nodes);

        let prev_assignments = Arc::new(Mutex::new(HashMap::new())); // location_key, hostname_assigned
        assert_consistent_hashing(remote_cache_connector, prev_assignments).await;
    }

    #[tokio::test]
    async fn test_keyspace_population_is_not_ordering_sensitive() {
        // Sanity check. Asserting that the expected hashing properties hold true.

        let remote_cache_connector =
            HostKeyspaceService::new(HttpService::default(), "foo".to_string());
        let prev_assignments = Arc::new(Mutex::new(HashMap::new())); // location_key, hostname_assigned

        // test with 0..100 ordered nodes, used when building keyspace
        let mut keyspace_nodes: Vec<ServiceNode> = (0..100)
            .map(|id| ServiceNode {
                id,
                hostname: format!("cache-server-hostname-{}", id),
            })
            .collect();
        remote_cache_connector
            .inner
            .build_keyspace(keyspace_nodes.clone());
        assert_consistent_hashing(
            remote_cache_connector.clone(),
            Arc::clone(&prev_assignments),
        )
        .await;

        // shuffled nodes, test against same/original assignments
        keyspace_nodes.shuffle(&mut rand::thread_rng());
        remote_cache_connector.inner.build_keyspace(keyspace_nodes);
        assert_consistent_hashing(remote_cache_connector, prev_assignments).await;
    }
}
