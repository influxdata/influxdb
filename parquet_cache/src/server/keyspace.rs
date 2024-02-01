use std::{path::Path, sync::Arc, task::Poll};

use arc_swap::ArcSwap;
use futures::Future;
use http::{Method, Request};
use hyper::Body;
use mpchash::HashRing;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use observability_deps::tracing::error;
use tokio::{sync::Notify, task::JoinHandle};
use tower::{Layer, Service};

use crate::{
    data_types::{
        InstanceState, KeyspaceResponseBody, KeyspaceVersion, ParquetCacheInstanceSet, ServiceNode,
        ServiceNodeHostname, ServiceNodeId,
    },
    server::response::Response,
};

use super::{error::Error, response::PinnedFuture};

struct BackgroundTask {
    path: String,
    fswatcher: RecommendedWatcher,
    notifier_handle: JoinHandle<()>,
}

impl Drop for BackgroundTask {
    fn drop(&mut self) {
        if let Err(e) = self.fswatcher.unwatch(Path::new(&self.path)) {
            error!("KeyspaceService fswatcher failed to unwatch: {}", e)
        }
        self.notifier_handle.abort();
    }
}

/// Service that applies the keyspace per request.
pub struct KeyspaceService<S> {
    shared: Arc<BackgroundTask>,
    ready_tx: Arc<Notify>,
    ready_rx: std::pin::Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>,
    keyspace: Arc<Keyspace>,
    inner: S,
}

impl<S> std::fmt::Debug for KeyspaceService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyspaceService")
            .field("keyspace", &self.keyspace)
            .finish_non_exhaustive()
    }
}

impl<S: Clone> Clone for KeyspaceService<S> {
    fn clone(&self) -> Self {
        let ready_rx = Arc::clone(&self.ready_tx);
        let ready_rx = Box::pin(async move {
            ready_rx.notified().await;
        });

        Self {
            shared: Arc::clone(&self.shared),
            ready_tx: Arc::clone(&self.ready_tx),
            ready_rx,
            keyspace: Arc::clone(&self.keyspace),
            inner: self.inner.clone(),
        }
    }
}

impl<S: Service<Request<Body>> + Clone + Send + Sync + 'static> KeyspaceService<S> {
    fn new(inner: S, configfile_path: String, node_hostname: String) -> Result<Self, Error> {
        let path = configfile_path.clone();

        let data = Arc::new(KeyspaceData::new(node_hostname));
        let keyspace = Arc::new(Keyspace {
            data: data.into(),
            configfile_path,
        });

        let ready_tx = Arc::new(Notify::new());
        let (fswatcher, notifier_handle) =
            Self::start_background_task(Arc::clone(&keyspace), Arc::clone(&ready_tx))?;

        let ready_rx = Arc::clone(&ready_tx);
        let ready_rx = Box::pin(async move {
            ready_rx.notified().await;
        });

        Ok(Self {
            shared: Arc::new(BackgroundTask {
                path,
                fswatcher,
                notifier_handle,
            }),
            ready_tx,
            ready_rx,
            keyspace,
            inner,
        })
    }

    fn start_background_task(
        keyspace: Arc<Keyspace>,
        ready_tx: Arc<Notify>,
    ) -> Result<(RecommendedWatcher, JoinHandle<()>), Error> {
        let changed = Arc::new(Notify::new());
        let has_changed = Arc::clone(&changed);

        let configfile_path = keyspace.configfile_path.clone();
        let ready_tx_ = Arc::clone(&ready_tx);
        let keyspace_ = Arc::clone(&keyspace);

        // start watcher -- default is to poll for changes every 30 seconds
        let watcher_and_listener =
            notify::recommended_watcher(move |res: notify::Result<notify::Event>| match res {
                Ok(notify::Event { kind, .. }) => {
                    if kind.is_modify() || kind.is_create() {
                        has_changed.notify_one();
                    }
                }
                Err(e) => error!(error=%e, "KeyspaceService fswatcher failed"),
            })
            .and_then(move |mut watcher| {
                watcher.watch(Path::new(&configfile_path), RecursiveMode::NonRecursive)?;
                Ok((
                    watcher,
                    tokio::spawn(async move {
                        loop {
                            changed.notified().await;
                            keyspace.update(Arc::clone(&ready_tx)).await;
                        }
                    }),
                ))
            })
            .map_err(|e| Error::Keyspace(e.to_string()))?;

        // handle race where the file is created before the watcher is started
        if Path::exists(Path::new(&keyspace_.configfile_path)) {
            tokio::spawn(async move {
                keyspace_.update(ready_tx_).await;
            });
        }

        Ok(watcher_and_listener)
    }
}

impl<S> Service<Request<Body>> for KeyspaceService<S>
where
    S: Service<Request<Body>, Future = PinnedFuture, Error = Error> + Clone + Send + Sync + 'static,
{
    type Response = super::response::Response;
    type Error = Error;
    type Future = PinnedFuture;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        if !self.keyspace.ready() {
            futures::ready!(self.ready_rx.as_mut().poll(cx));
        }
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/state") => {
                let this = self.clone();
                Box::pin(async move {
                    // return the version we have loaded
                    // serde serialization will add the CacheState enum, based on this version
                    Ok(Response::KeyspaceVersion(
                        this.keyspace.data.load().version.clone(),
                    ))
                })
            }
            (&Method::PATCH, "/warmed") => {
                let this = self.clone();
                Box::pin(async move {
                    this.keyspace.set_to_running();
                    Ok(Response::Ready)
                })
            }
            (&Method::GET, "/keyspace") => {
                let this = self.clone();
                Box::pin(async move {
                    let (_, _, keyspace) = this.keyspace.read_definition().await;
                    Ok(Response::Keyspace(keyspace))
                })
            }
            (&Method::GET, "/metadata")
            | (&Method::GET, "/object")
            | (&Method::POST, "/write-hint") => {
                let clone = self.inner.clone();
                let mut inner = std::mem::replace(&mut self.inner, clone);
                let this = self.clone();
                Box::pin(async move {
                    let as_url = url::Url::parse(req.uri().to_string().as_str())
                        .expect("should be already validated path & query");
                    let obj_location = match as_url.query_pairs().find(|(k, _v)| k.eq("location")) {
                        None => {
                            return Err(Error::Keyspace(
                                "invalid or missing object location".into(),
                            ));
                        }
                        Some((_key, location)) => location.to_string(),
                    };

                    // when keyspace is invalid (being re-built), return error such that
                    // cache client decides to (1) re-fetch keyspace, and/or (2) uses fallback
                    match this.keyspace.in_keyspace(&obj_location) {
                        true => inner.call(req).await,
                        false => Err(Error::Keyspace(format!(
                            "object {} is not found in keyspace",
                            obj_location
                        ))),
                    }
                })
            }
            (any_method, any_path) => {
                let msg = format!("invalid path: {} {}", any_method, any_path);
                Box::pin(async { Err(Error::BadRequest(msg)) })
            }
        }
    }
}

#[derive(Debug, Clone)]
struct KeyspaceData {
    /// ID self
    /// Is none if keyspace has been invalidated.
    own: Option<ServiceNodeId>,
    // Hashring
    keyspace: Arc<HashRing<ServiceNodeId>>,
    /// Versioning, so can provide current vs next, per GET `/state` request
    /// Is none if Self::Pending (a.k.a. no definition loaded yet)
    version: KeyspaceVersion,
}

impl KeyspaceData {
    pub fn new(self_node: ServiceNodeHostname) -> Self {
        Self {
            own: None,
            keyspace: Default::default(),
            version: KeyspaceVersion::new(self_node),
        }
    }
}

#[derive(Debug)]
struct Keyspace {
    /// Atomically updated keyspace data.
    data: ArcSwap<KeyspaceData>,
    /// Fs-accessible file containing the [`ParquetCacheInstanceSet`]
    configfile_path: String,
}

impl Keyspace {
    /// `Valid` as in able to check keyspace hashring.
    ///
    /// Returns true if the keyspace definition exists, and own-node is within keyspace.
    fn is_valid(&self) -> bool {
        self.data.load().own.is_some()
    }

    /// `Ready` as in poll_ready (to receive requests).
    /// Returns true if the keyspace is not in the init phase.
    ///
    /// Request include `GET /state` requests during warming and cooling phases.
    fn ready(&self) -> bool {
        let data = self.data.load();
        match InstanceState::from(&data.as_ref().version) {
            InstanceState::Pending => false,
            InstanceState::Warming | InstanceState::Running | InstanceState::Cooling => true,
        }
    }

    /// `Running` as in the [`InstanceState`].
    fn set_to_running(&self) {
        self.data.rcu(|data| KeyspaceData {
            own: data.own,
            keyspace: Arc::clone(&data.keyspace),
            version: data.version.clone_next_to_curr(),
        });
    }

    /// Returns true if the object location is in the keyspace.
    fn in_keyspace(&self, object: &String) -> bool {
        let data = self.data.load();
        self.is_valid()
            && match data.own {
                None => false,
                Some(id) => match data.keyspace.primary_node(object) {
                    Some(&assigned_node) => assigned_node == id,
                    None => false,
                },
            }
    }

    /// Read keyspace definition from file.
    async fn read_definition(
        &self,
    ) -> (
        ParquetCacheInstanceSet, /* KeyspaceVersion.next */
        Option<ServiceNodeId>,   /* None == current node is not in KeyspaceVersion.next */
        Vec<ServiceNode>,        /* full set of KeyspaceVersion.next hashring */
    ) {
        let current_instance_set_next = tokio::fs::read_to_string(self.configfile_path.clone())
            .await
            .expect("config map file should always exist on pod");
        let parquet_cache_instance_set: ParquetCacheInstanceSet =
            serde_json::from_str(current_instance_set_next.as_str())
                .expect("should have valid ParquetCacheInstanceSet format");

        let service_nodes = KeyspaceResponseBody::from(&parquet_cache_instance_set).nodes;

        let self_hostname = self.data.load().version.hostname().clone();
        (
            parquet_cache_instance_set,
            service_nodes
                .iter()
                .position(|node| node.hostname == self_hostname)
                .map(|node_id| node_id as u64),
            service_nodes,
        )
    }

    /// Update keyspace definition.
    async fn update(&self, ready: Arc<Notify>) {
        let (next_version, own, all_nodes) = self.read_definition().await;

        let mut keyspace = HashRing::new();
        for ServiceNode { id, hostname: _ } in all_nodes {
            keyspace.add(id);
        }
        let keyspace = Arc::new(keyspace);

        // determine if KeyspaceVersion changed
        let prev_data = self.data.rcu(|curr_data| {
            match &curr_data.version.next {
                Some(next) if next_version.revision == next.revision => {
                    // no change -- already knows about next
                    Arc::clone(curr_data)
                }
                _ => Arc::new(KeyspaceData {
                    own,
                    keyspace: Arc::clone(&keyspace),
                    version: curr_data.version.set_next(next_version.to_owned()),
                }),
            }
        });

        if InstanceState::from(&prev_data.version) == InstanceState::Pending && self.ready() {
            // Let anyone waiting on poll_ready know that we're no longer pending.
            ready.notify_waiters();
        }
    }
}

pub struct BuildKeyspaceService {
    pub configfile_path: String,
    pub node_hostname: String,
}

impl<S: Service<Request<Body>> + Clone + Send + Sync + 'static> Layer<S> for BuildKeyspaceService {
    type Service = KeyspaceService<S>;

    fn layer(&self, service: S) -> Self::Service {
        KeyspaceService::new(
            service,
            self.configfile_path.clone(),
            self.node_hostname.clone(),
        )
        .expect("cache server failed to deploy due to keyspace layer init error")
    }
}

#[cfg(test)]
mod test {
    use std::{
        io::{Seek, Write},
        sync::atomic::{AtomicU32, Ordering},
        task::Context,
        time::Duration,
    };

    use assert_matches::assert_matches;
    use futures::{future, task::noop_waker_ref};
    use tempfile::{NamedTempFile, TempDir};
    use tokio::io::AsyncWriteExt;
    use tokio_stream::StreamExt;
    use tower::{ServiceBuilder, ServiceExt};

    use super::super::response::Response;
    use super::*;

    const VALID_HOSTNAME: &str = "hostname-a";
    lazy_static::lazy_static! {
        static ref KEYSPACE_DEFINITION: String = serde_json::json!(ParquetCacheInstanceSet {
            revision: 0,
            // a single node in the keyspace, therefore all keys should hash to this keyspace
            instances: vec![VALID_HOSTNAME].into_iter().map(String::from).collect(),
        }).to_string();
    }

    #[derive(Clone, Default)]
    struct MockInnermostService {
        call: Arc<AtomicU32>,
        poll_ready: Arc<AtomicU32>,
    }

    impl Service<Request<Body>> for MockInnermostService {
        type Response = Response;
        type Error = Error;
        type Future = PinnedFuture;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.poll_ready.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _req: Request<Body>) -> Self::Future {
            self.call.fetch_add(1, Ordering::SeqCst);
            Box::pin(future::ok(Response::Ready))
        }
    }

    fn metadata_req() -> Request<Body> {
        Request::builder()
            .method(Method::GET)
            .uri("http://foo.io/metadata?location=bar")
            .body(Body::empty())
            .unwrap()
    }

    fn object_req() -> Request<Body> {
        Request::builder()
            .method(Method::GET)
            .uri("http://foo.io/object?location=bar")
            .body(Body::empty())
            .unwrap()
    }

    fn write_hint_req() -> Request<Body> {
        Request::builder()
            .method(Method::POST)
            .uri("http://foo.io/write-hint?location=bar")
            .body(Body::empty())
            .unwrap()
    }

    fn state_req() -> Request<Body> {
        Request::builder()
            .method(Method::GET)
            .uri("/state")
            .body(Body::empty())
            .unwrap()
    }

    fn warmed_req() -> Request<Body> {
        Request::builder()
            .method(Method::PATCH)
            .uri("/warmed")
            .body(Body::empty())
            .unwrap()
    }

    fn keyspace_defn_req() -> Request<Body> {
        Request::builder()
            .method(Method::GET)
            .uri("/keyspace")
            .body(Body::empty())
            .unwrap()
    }

    async fn write_defn_to_file(defn: &[u8], configfile_path: &Path) {
        let mut file = tokio::fs::File::create(&configfile_path).await.unwrap();
        file.write_all(defn)
            .await
            .expect("should write keyspace definition to configfile");

        // notify fswatcher will sometimes skip events when the file descriptor is still open
        file.shutdown()
            .await
            .expect("should shutdown file descriptor");
    }

    #[allow(clippy::future_not_send)]
    async fn wait_until_service_is_ready(server: &mut KeyspaceService<MockInnermostService>) {
        future::poll_fn(move |cx| server.poll_ready(cx))
            .await
            .expect("should not have failed");
    }

    #[tokio::test]
    async fn test_keyspace_can_load_definition() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "{}", KEYSPACE_DEFINITION.as_str())
            .expect("should write keyspace definition to configfile");

        let keyspace = Keyspace {
            configfile_path: file.path().to_str().unwrap().to_string(),
            data: Arc::new(KeyspaceData::new(VALID_HOSTNAME.into())).into(),
        };

        assert!(
            !keyspace.is_valid(),
            "default keyspace should be invalid, due to no definition loaded"
        );

        let notify = Arc::new(Notify::new());
        keyspace.update(Arc::clone(&notify)).await;
        assert!(
            keyspace.is_valid(),
            "keyspace should be valid, after definition is loaded"
        );

        // remove from keyspace, by changing keyspace definition
        let new_keyspace_definition = serde_json::json!(ParquetCacheInstanceSet {
            revision: 1,
            instances: vec!["another-node"].into_iter().map(String::from).collect(),
        })
        .to_string();
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(file.path())
            .unwrap();
        file.seek(std::io::SeekFrom::Start(0)).unwrap(); // move pointer to start, to overwrite
        writeln!(file, "{}", new_keyspace_definition.as_str())
            .expect("should write keyspace definition to configfile");
        file.sync_all().unwrap();

        // should no longer be in keyspace
        keyspace.update(Arc::clone(&notify)).await;
        assert!(
            !keyspace.is_valid(),
            "keyspace should not be valid, when own-hostname not in definition"
        );
    }

    #[tokio::test]
    async fn test_keyspace_poll_ready_during_instance_phases() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "{}", KEYSPACE_DEFINITION.as_str())
            .expect("should write keyspace definition to configfile");

        let keyspace = Keyspace {
            configfile_path: file.path().to_str().unwrap().to_string(),
            data: Arc::new(KeyspaceData::new(VALID_HOSTNAME.into())).into(),
        };

        // init phase
        assert!(!keyspace.ready(), "default keyspace should not poll_ready");

        // warming phase
        // this in when the outer service layers will be calling the inner KeyspaceService
        let notify = Arc::new(Notify::new());
        keyspace.update(Arc::clone(&notify)).await;
        assert!(
            keyspace.ready(),
            "keyspace should poll_ready, after definition (with own node) is loaded"
        );

        // running phase
        keyspace.set_to_running();
        assert!(keyspace.ready(), "keyspace should poll_ready, when running");

        // remove from keyspace, by changing keyspace definition
        let new_keyspace_definition = serde_json::json!(ParquetCacheInstanceSet {
            revision: 1,
            instances: vec!["another-node"].into_iter().map(String::from).collect(),
        })
        .to_string();
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(file.path())
            .unwrap();
        file.seek(std::io::SeekFrom::Start(0)).unwrap(); // move pointer to start, to overwrite
        writeln!(file, "{}", new_keyspace_definition.as_str())
            .expect("should write keyspace definition to configfile");
        file.sync_all().unwrap();

        // cooling phase
        keyspace.update(notify).await;
        assert!(
            keyspace.ready(),
            "keyspace should still poll_ready when cooling, to handle `GET /state` requests"
        );
    }

    #[tokio::test]
    async fn test_watcher_consumes_definition_file() {
        // no keyspace definition
        let dir = TempDir::new().unwrap();
        let configfile_path = dir.path().join("configfile.json");
        let mut file = tokio::fs::File::create(&configfile_path).await.unwrap();

        // start service
        let mut server = ServiceBuilder::new()
            .layer(BuildKeyspaceService {
                configfile_path: configfile_path.to_str().unwrap().to_string(),
                node_hostname: VALID_HOSTNAME.into(),
            })
            .service(MockInnermostService::default());

        // assert poll_ready returns pending, when no keyspace definition
        assert_matches!(
            server.poll_ready(&mut Context::from_waker(noop_waker_ref())),
            Poll::Pending,
            "should return pending status, as keyspace definition does not yet exist"
        );

        // write keyspace definition to configfile
        file.write_all(KEYSPACE_DEFINITION.as_bytes())
            .await
            .expect("should write keyspace definition to configfile");
        file.shutdown()
            .await
            .expect("should shutdown file descriptor");

        // wait for keyspace to be loaded by the watcher
        wait_until_service_is_ready(&mut server).await;

        // call service
        let res = server.call(state_req()).await;
        assert_matches!(
            res,
            Ok(Response::KeyspaceVersion(ver)) if InstanceState::from(&ver) == InstanceState::Warming,
            "should return successful response, instead found {:?}",
            res
        );
    }

    #[tokio::test]
    async fn test_service_instance_phases() {
        // provide keyspace definition
        let dir = TempDir::new().unwrap();
        let configfile_path = dir.path().join("configfile.json");
        write_defn_to_file(KEYSPACE_DEFINITION.as_bytes(), &configfile_path).await;

        // start service
        let innermost_service = MockInnermostService::default();
        let mut server = ServiceBuilder::new()
            .layer(BuildKeyspaceService {
                configfile_path: configfile_path.to_str().unwrap().to_string(),
                node_hostname: VALID_HOSTNAME.into(),
            })
            .service(innermost_service.clone());

        // wait for service.poll_ready to return ready
        wait_until_service_is_ready(&mut server).await;

        // call service when warming
        let res = server.call(state_req()).await;
        assert_matches!(
            res,
            Ok(Response::KeyspaceVersion(ver)) if InstanceState::from(&ver) == InstanceState::Warming,
            "should return InstanceState::Warming, instead found {:?}",
            res
        );

        // tell keyspace it's warmed
        assert!(
            server.call(warmed_req()).await.is_ok(),
            "should be able to PATCH /warmed"
        );

        // call poll_ready when warmed
        assert_matches!(
            server.poll_ready(&mut Context::from_waker(noop_waker_ref())),
            Poll::Ready(Ok(_)),
            "should return ready status"
        );

        // call `GET /state` when warmed
        let res = server.call(state_req()).await;
        assert_matches!(
            res,
            Ok(Response::KeyspaceVersion(ver)) if InstanceState::from(&ver) == InstanceState::Running,
            "should return InstanceState::Running, instead found {:?}",
            res
        );

        // tell keyspace to cool, by changing keyspace definition
        let new_keyspace_definition = serde_json::json!(ParquetCacheInstanceSet {
            revision: 1,
            instances: vec!["another-node"].into_iter().map(String::from).collect(),
        })
        .to_string();
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&configfile_path)
            .unwrap();
        file.seek(std::io::SeekFrom::Start(0)).unwrap(); // move pointer to start, to overwrite
        writeln!(file, "{}", new_keyspace_definition.as_str())
            .expect("should write keyspace definition to configfile");
        file.sync_all().unwrap();

        // waiting for new_keyspace_definition to load
        // cannot use poll_ready, as it is already returning ready (to accept `GET /state` requests)
        tokio::time::sleep(Duration::from_secs(10)).await;

        // call poll_ready when cooling
        assert_matches!(
            server.poll_ready(&mut Context::from_waker(noop_waker_ref())),
            Poll::Ready(Ok(_)),
            "should return ready status"
        );
        // call `GET /state` when cooling
        let res = server.call(state_req()).await;
        assert_matches!(
            res,
            Ok(Response::KeyspaceVersion(ver)) if InstanceState::from(&ver) == InstanceState::Cooling,
            "should return InstanceState::Cooling, instead found {:?}",
            res
        );
    }

    #[tokio::test]
    async fn test_keyspace_service_oks_for_included_key() {
        // provide keyspace definition
        let dir = TempDir::new().unwrap();
        let configfile_path = dir.path().join("configfile.json");
        write_defn_to_file(KEYSPACE_DEFINITION.as_bytes(), &configfile_path).await;

        // start service
        let innermost_service = MockInnermostService::default();
        let mut server = ServiceBuilder::new()
            .layer(BuildKeyspaceService {
                configfile_path: configfile_path.to_str().unwrap().to_string(),
                node_hostname: VALID_HOSTNAME.into(),
            })
            .service(innermost_service.clone());

        // wait for service.poll_ready to return ready
        wait_until_service_is_ready(&mut server).await;

        // GET /metadata
        let res = server.call(metadata_req()).await;
        assert!(
            res.is_ok(),
            "should return successful `GET /metadata`, instead found {:?}",
            res
        );

        // GET /object
        let res = server.call(object_req()).await;
        assert!(
            res.is_ok(),
            "should return successful `GET /object`, instead found {:?}",
            res
        );

        // GET /write-hint
        let res = server.call(write_hint_req()).await;
        assert!(
            res.is_ok(),
            "should return successful `POST /write-hint`, instead found {:?}",
            res
        );
    }

    #[tokio::test]
    async fn test_keyspace_service_errs_for_excluded_key() {
        // provide keyspace definition
        let dir = TempDir::new().unwrap();
        let configfile_path = dir.path().join("configfile.json");
        write_defn_to_file(KEYSPACE_DEFINITION.as_bytes(), &configfile_path).await;

        // start service
        let innermost_service = MockInnermostService::default();
        let mut server = ServiceBuilder::new()
            .layer(BuildKeyspaceService {
                configfile_path: configfile_path.to_str().unwrap().to_string(),
                node_hostname: VALID_HOSTNAME.into(),
            })
            .service(innermost_service.clone());

        // wait for keyspace to be loaded by the watcher
        wait_until_service_is_ready(&mut server).await;

        // update, to remove self from keyspace
        server.keyspace.data.rcu(|data| {
            Arc::new(KeyspaceData {
                own: None,
                keyspace: Arc::clone(&data.keyspace),
                version: data.version.set_next(ParquetCacheInstanceSet {
                    revision: data.version.next.as_ref().unwrap().revision + 1,
                    instances: vec!["another-node"].into_iter().map(String::from).collect(),
                }),
            })
        });

        // GET /metadata
        let res = server.call(metadata_req()).await;
        assert_matches!(
            res,
            Err(Error::Keyspace(_)),
            "should return errored `GET /metadata`, instead found {:?}",
            res
        );

        // GET /object
        let res = server.call(object_req()).await;
        assert_matches!(
            res,
            Err(Error::Keyspace(_)),
            "should return errored `GET /object`, instead found {:?}",
            res
        );

        // GET /write-hint
        let res = server.call(write_hint_req()).await;
        assert_matches!(
            res,
            Err(Error::Keyspace(_)),
            "should return errored `POST /write-hint`, instead found {:?}",
            res
        );
    }

    #[tokio::test]
    async fn test_keyspace_service_fetch_keyspace() {
        // provide keyspace definition
        let dir = TempDir::new().unwrap();
        let configfile_path = dir.path().join("configfile.json");
        write_defn_to_file(KEYSPACE_DEFINITION.as_bytes(), &configfile_path).await;

        // start service
        let innermost_service = MockInnermostService::default();
        let mut server = ServiceBuilder::new()
            .layer(BuildKeyspaceService {
                configfile_path: configfile_path.to_str().unwrap().to_string(),
                node_hostname: VALID_HOSTNAME.into(),
            })
            .service(innermost_service.clone());

        // wait for service.poll_ready to return ready
        wait_until_service_is_ready(&mut server).await;

        // GET /keyspace
        let res = server.call(keyspace_defn_req()).await;
        assert_matches!(
            res,
            Ok(Response::Keyspace(nodes)) if matches!(
                &nodes[..],
                [ServiceNode { id: 0, hostname }] if hostname == VALID_HOSTNAME
            ),
            "should return successful `GET /keyspace`, instead found {:?}",
            res
        );
    }

    mod usage_of_poll_ready {
        use super::*;

        #[tokio::test]
        async fn test_poll_ready_is_not_triggered_on_call() {
            // provide keyspace definition
            let dir = TempDir::new().unwrap();
            let configfile_path = dir.path().join("configfile.json");
            write_defn_to_file(KEYSPACE_DEFINITION.as_bytes(), &configfile_path).await;

            // start service
            let innermost_service = MockInnermostService::default();
            let mut server = ServiceBuilder::new()
                .layer(BuildKeyspaceService {
                    configfile_path: configfile_path.to_str().unwrap().to_string(),
                    node_hostname: VALID_HOSTNAME.into(),
                })
                .service(innermost_service.clone());

            // wait for keyspace to be loaded by the watcher
            wait_until_service_is_ready(&mut server).await;
            let init_poll_ready = innermost_service.poll_ready.load(Ordering::SeqCst);

            // call service
            // use `GET /object` since it calls inner service
            let res = server.call(object_req()).await;
            assert!(
                res.is_ok(),
                "should return successful response, instead found {:?}",
                res
            );

            // assert that poll_ready was not called
            assert_eq!(
                innermost_service.call.load(Ordering::SeqCst),
                1,
                "should call innermost service once"
            );
            assert_eq!(
                innermost_service.poll_ready.load(Ordering::SeqCst),
                init_poll_ready,
                "should not have called innermost poll_ready, on Service::call()"
            );
        }

        #[tokio::test]
        async fn test_poll_ready_used_when_connected_to_stream() {
            // provide keyspace definition
            let dir = TempDir::new().unwrap();
            let configfile_path = dir.path().join("configfile.json");
            write_defn_to_file(KEYSPACE_DEFINITION.as_bytes(), &configfile_path).await;

            // start service
            let innermost_service = MockInnermostService::default();
            let mut server = ServiceBuilder::new()
                .layer(BuildKeyspaceService {
                    configfile_path: configfile_path.to_str().unwrap().to_string(),
                    node_hostname: VALID_HOSTNAME.into(),
                })
                .service(innermost_service.clone());

            // Stream of requests, processed by service.
            let (reqs, rx) = futures::channel::mpsc::unbounded();
            let mut resps = server.clone().call_all(rx);

            // wait for service.poll_ready to return ready
            wait_until_service_is_ready(&mut server).await;
            let init_poll_ready = innermost_service.poll_ready.load(Ordering::SeqCst);

            // stream Service::call() requests
            vec![metadata_req(), object_req(), write_hint_req()]
                .into_iter()
                .for_each(|req| {
                    reqs.unbounded_send(req).unwrap();
                });
            drop(reqs);

            // await responses
            while let Some(rsp) = resps.next().await {
                assert!(
                    rsp.is_ok(),
                    "should return successful response, instead found {:?}",
                    rsp
                );
            }

            // assert that Service::poll_ready() was called at least as many times as Service::call()
            assert_eq!(
                innermost_service.call.load(Ordering::SeqCst),
                3,
                "should call innermost service once"
            );
            assert!(
                innermost_service.poll_ready.load(Ordering::SeqCst) >= 3 + init_poll_ready,
                "should have called innermost poll_ready"
            );
        }
    }
}
