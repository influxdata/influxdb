#![allow(dead_code)]
//! Contains the cache server.

use std::sync::Arc;

use iox_catalog::interface::Catalog;
use object_store::ObjectStore;
use tower::ServiceBuilder;

use crate::data_types::PolicyConfig;

use self::{
    cache::{BuildCacheService, CacheService},
    data::DataService,
    keyspace::{BuildKeyspaceService, KeyspaceService},
    precondition::{BuildPreconditionService, PreconditionService},
};

// Layers in the cache server:
mod cache;
mod data;
mod keyspace;
mod precondition;

// Shared server types:
mod error;
pub use error::Error as ServerError;
mod response;

#[cfg(test)]
pub(crate) mod mock;

/// The cache server type.
pub type ParquetCacheServer = CacheService<KeyspaceService<PreconditionService<DataService>>>;

/// Config for cache server.
#[derive(Debug)]
pub struct ParquetCacheServerConfig {
    /// The path to the config file for the keyspace.
    pub keyspace_config_path: String,
    /// The hostname of the cache instance (k8s pod) running this process.
    pub hostname: String,
    /// The local directory to store data.
    pub local_dir: String,
    /// The policy config for the cache eviction.
    pub policy_config: PolicyConfig,
}

/// Build a cache server.
pub async fn build_cache_server(
    config: ParquetCacheServerConfig,
    direct_store: Arc<dyn ObjectStore>,
    catalog: Arc<dyn Catalog>,
) -> ParquetCacheServer {
    let ParquetCacheServerConfig {
        keyspace_config_path: configfile_path,
        hostname: node_hostname,
        local_dir,
        policy_config,
    } = config;

    ServiceBuilder::new()
        // outermost layer 0
        .layer(BuildCacheService)
        // layer 1
        .layer(BuildKeyspaceService {
            configfile_path,
            node_hostname,
        })
        // layer 2
        .layer(BuildPreconditionService)
        // innermost layer 3
        .service(DataService::new(direct_store, catalog, policy_config, Some(local_dir)).await)
}

#[cfg(test)]
mod integration_tests {
    use std::{
        fs::create_dir_all,
        io::{Seek, Write},
        path::Path,
        time::Duration,
    };

    use bytes::{Buf, BufMut, BytesMut};
    use http::{Method, StatusCode};
    use hyper::{Body, Request};
    use iox_tests::{TestCatalog, TestParquetFileBuilder};
    use object_store::{local::LocalFileSystem, ObjectMeta};
    use serde::Deserialize;
    use serde_json::Deserializer;
    use tempfile::{tempdir, NamedTempFile, TempDir};
    use tower::Service;

    use crate::data_types::{
        GetObjectMetaResponse, InstanceState, KeyspaceResponseBody, ParquetCacheInstanceSet,
        ServiceNode, State, WriteHint, WriteHintRequestBody,
    };
    use crate::server::response::Response as ServerInternalResponse;

    use super::*;

    fn create_fs_direct_store(local_dir: &Path) -> Arc<dyn ObjectStore> {
        create_dir_all(local_dir).unwrap();
        Arc::new(LocalFileSystem::new_with_prefix(local_dir).expect("should create fs ObjectStore"))
    }

    #[tokio::test]
    async fn test_invalid_path() {
        let tmpdir = tempdir().unwrap();
        let direct_store = create_fs_direct_store(tmpdir.path());
        let catalog = iox_tests::TestCatalog::new();

        let config = ParquetCacheServerConfig {
            keyspace_config_path: "/tmp".to_string(),
            hostname: "localhost".to_string(),
            local_dir: tmpdir.path().to_str().unwrap().to_string(),
            policy_config: PolicyConfig::default(),
        };

        let mut server = build_cache_server(config, direct_store, catalog.catalog()).await;

        let req = Request::get("http://foo.io/invalid-path/")
            .body(Body::empty())
            .unwrap();
        let resp = server.call(req).await;

        // assert expected http response
        assert_matches::assert_matches!(
            resp,
            Err(ServerError::BadRequest(msg)) if msg.contains("invalid path"),
            "expected bad request, instead found {:?}", resp
        );
    }

    const VALID_HOSTNAME: &str = "hostname-a";
    lazy_static::lazy_static! {
        static ref KEYSPACE_DEFINITION: ParquetCacheInstanceSet = ParquetCacheInstanceSet {
            revision: 0,
            // a single node in the keyspace, therefore all keys should hash to this keyspace
            instances: vec![VALID_HOSTNAME].into_iter().map(String::from).collect(),
        };
    }

    const LOCATION: &str = "0/0/partition_key/00000000-0000-0000-0000-000000000001.parquet";
    const DATA: &[u8] = b"all my pretty words";

    async fn setup_service_and_direct_store(
        direct_store: Arc<dyn ObjectStore>,
        cache_tmpdir: TempDir,
        file: &mut NamedTempFile,
    ) -> (ParquetCacheServer, Arc<TestCatalog>, ObjectMeta) {
        let catalog = iox_tests::TestCatalog::new();

        let policy_config = PolicyConfig {
            max_capacity: 3_200_000_000,
            event_recency_max_duration_nanoseconds: 1_000_000_000 * 5, // 5 seconds
        };

        writeln!(file, "{}", serde_json::json!(*KEYSPACE_DEFINITION))
            .expect("should write keyspace definition to configfile");

        let obj_store_path = object_store::path::Path::from(LOCATION);

        let config = ParquetCacheServerConfig {
            keyspace_config_path: file.path().to_str().unwrap().to_string(),
            hostname: VALID_HOSTNAME.to_string(),
            local_dir: cache_tmpdir.path().to_str().unwrap().to_string(),
            policy_config,
        };

        let server = build_cache_server(config, Arc::clone(&direct_store), catalog.catalog()).await;

        // add object to direct store
        direct_store
            .put(&obj_store_path, DATA.into())
            .await
            .expect("should write object to direct store");
        let expected_meta = direct_store
            .head(&obj_store_path)
            .await
            .expect("should have object in direct store");

        // wait until service is ready
        let mut this = server.clone();
        futures::future::poll_fn(move |cx| this.poll_ready(cx))
            .await
            .expect("should not have failed");

        (server, catalog, expected_meta)
    }

    async fn confirm_data_exists(expected_meta: ObjectMeta, server: &mut ParquetCacheServer) {
        // issue read metadata
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("http://foo.io/metadata?location={}", LOCATION))
            .body(Body::empty())
            .unwrap();
        let resp = server.call(req).await.expect("should get a response");

        // assert expected http response for metadata
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "expected http 200, instead found {:?}",
            resp
        );
        let resp_body: GetObjectMetaResponse = serde_json::from_reader(
            hyper::body::aggregate(resp.into_body())
                .await
                .expect("should create reader")
                .reader(),
        )
        .expect("should read response body");
        let resp_meta: object_store::ObjectMeta = resp_body.into();
        assert_eq!(
            resp_meta, expected_meta,
            "expected proper metadata, instead found {:?}",
            resp_meta
        );

        // issue read object
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("http://foo.io/object?location={}", LOCATION))
            .body(Body::empty())
            .unwrap();
        let resp = server.call(req).await.expect("should get a response");

        // assert expected http response for object
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "expected http 200, instead found {:?}",
            resp
        );
        let body = hyper::body::to_bytes(resp.into_body())
            .await
            .expect("reading response body");
        assert_eq!(
            body.len(),
            DATA.to_vec().len(),
            "expected data in body, instead found {}",
            std::str::from_utf8(&body).unwrap()
        );
    }

    #[tokio::test]
    async fn test_write_hint_and_read() {
        // keep in scope so they are not dropped
        let dir_store_tmpdir = tempdir().unwrap();
        let cache_tmpdir = tempdir().unwrap();
        let mut configfile = NamedTempFile::new().unwrap();
        let direct_store = create_fs_direct_store(dir_store_tmpdir.path());

        // setup server
        let (mut server, _, expected_meta) =
            setup_service_and_direct_store(direct_store, cache_tmpdir, &mut configfile).await;

        // issue write-hint
        let mut buf = BytesMut::new().writer();
        serde_json::to_writer(
            &mut buf,
            &WriteHintRequestBody {
                location: LOCATION.into(),
                hint: WriteHint {
                    file_size_bytes: DATA.to_vec().len() as i64,
                    ..Default::default()
                },
                ack_setting: crate::data_types::WriteHintAck::Completed,
            },
        )
        .expect("should write request body");
        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("http://foo.io/write-hint?location={}", LOCATION))
            .body(hyper::Body::from(buf.into_inner().freeze()))
            .unwrap();
        let resp = server.call(req).await.expect("should get a response");

        // assert expected http response for write-hint
        let expected_resp = ServerInternalResponse::Written;
        assert_eq!(
            resp.status(),
            expected_resp.code(),
            "expected http response status code to match, instead found {:?}",
            resp
        );
        let body = hyper::body::to_bytes(resp.into_body())
            .await
            .expect("reading response body");
        assert_eq!(
            body.len(),
            0,
            "expected empty body, instead found {}",
            std::str::from_utf8(&body).unwrap()
        );

        confirm_data_exists(expected_meta, &mut server).await;
    }

    #[tokio::test]
    async fn test_cache_miss_writeback_and_read() {
        // keep in scope so they are not dropped
        let dir_store_tmpdir = tempdir().unwrap();
        let cache_tmpdir = tempdir().unwrap();
        let mut configfile = NamedTempFile::new().unwrap();
        let direct_store = create_fs_direct_store(dir_store_tmpdir.path());

        // setup server
        let (mut server, catalog, expected_meta) =
            setup_service_and_direct_store(direct_store, cache_tmpdir, &mut configfile).await;

        // write-back requires catalog data, therefore insert into catalog
        let namespace = catalog.create_namespace_1hr_retention("ns0").await;
        let table = namespace.create_table("table0").await;
        let partition = table.create_partition("partition_key").await;

        // insert parquet file into catalog, with proper matching object store id
        let parquet_file_path = parquet_file::ParquetFilePath::try_from(&LOCATION.to_string())
            .expect("should be valid parquet file path");
        let parquet_file = TestParquetFileBuilder::default()
            .with_creation_time(iox_time::Time::from_date_time(expected_meta.last_modified))
            .with_file_size_bytes(DATA.to_vec().len() as u64)
            .with_object_store_id(parquet_file_path.object_store_id());
        partition
            .create_parquet_file_catalog_record(parquet_file)
            .await;

        // trigger cache miss
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("http://foo.io/metadata?location={}", LOCATION))
            .body(Body::empty())
            .unwrap();
        let resp = server.call(req).await;
        assert_matches::assert_matches!(
            resp,
            Err(ServerError::CacheMiss),
            "expected cache miss, instead found {:?}",
            resp
        );

        // wait for write-back to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        confirm_data_exists(expected_meta, &mut server).await;
    }

    #[tokio::test]
    async fn test_state_responses() {
        // keep in scope so they are not dropped
        let dir_store_tmpdir = tempdir().unwrap();
        let cache_tmpdir = tempdir().unwrap();
        let mut configfile = NamedTempFile::new().unwrap();
        let direct_store = create_fs_direct_store(dir_store_tmpdir.path());

        // setup server
        let (mut server, _, _meta) =
            setup_service_and_direct_store(direct_store, cache_tmpdir, &mut configfile).await;

        // check keyspace status is running
        let req = Request::builder()
            .method(Method::GET)
            .uri("http://foo.io/state")
            .body(Body::empty())
            .unwrap();
        let resp = server.call(req).await.expect("should get a response");
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "expected http 200, instead found {:?}",
            resp
        );
        let resp_body_json = hyper::body::to_bytes(resp.into_body())
            .await
            .expect("should read response body");
        let mut de = Deserializer::from_slice(&resp_body_json);
        let mut state = State::deserialize(&mut de).expect("valid State object");
        state.state_changed = 0; // ignore the timestamp
        assert_eq!(
            state,
            State {
                state: InstanceState::Running,
                state_changed: 0,
                current_node_set_revision: 0,
                next_node_set_revision: 0,
            },
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
            .open(configfile.path())
            .unwrap();
        file.seek(std::io::SeekFrom::Start(0)).unwrap(); // move pointer to start, to overwrite
        writeln!(file, "{}", new_keyspace_definition.as_str())
            .expect("should write keyspace definition to configfile");
        file.sync_all().unwrap();

        // waiting for new_keyspace_definition to load
        // cannot use poll_ready, as it is already returning ready (to accept `GET /state` requests)
        tokio::time::sleep(Duration::from_secs(10)).await;

        // check keyspace status is cooling
        let req = Request::builder()
            .method(Method::GET)
            .uri("http://foo.io/state")
            .body(Body::empty())
            .unwrap();
        let resp = server.call(req).await.expect("should get a response");
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "expected http 200, instead found {:?}",
            resp
        );
        let resp_body_json = hyper::body::to_bytes(resp.into_body())
            .await
            .expect("should read response body");
        let mut de = Deserializer::from_slice(&resp_body_json);
        let mut state = State::deserialize(&mut de).expect("valid State object");
        state.state_changed = 0; // ignore the timestamp
        assert_eq!(
            state,
            State {
                state: InstanceState::Cooling,
                state_changed: 0,
                current_node_set_revision: 0,
                next_node_set_revision: 1,
            },
        );
    }

    #[tokio::test]
    async fn test_keyspace_nodes() {
        // keep in scope so they are not dropped
        let dir_store_tmpdir = tempdir().unwrap();
        let cache_tmpdir = tempdir().unwrap();
        let mut configfile = NamedTempFile::new().unwrap();
        let direct_store = create_fs_direct_store(dir_store_tmpdir.path());

        // setup server
        let (mut server, _, _meta) =
            setup_service_and_direct_store(direct_store, cache_tmpdir, &mut configfile).await;

        // get keyspace nodes
        let req = Request::builder()
            .method(Method::GET)
            .uri("http://foo.io/keyspace")
            .body(Body::empty())
            .unwrap();
        let resp = server.call(req).await.expect("should get a response");
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "expected http 200, instead found {:?}",
            resp
        );
        let resp_body: KeyspaceResponseBody = serde_json::from_reader(
            hyper::body::aggregate(resp.into_body())
                .await
                .expect("should create reader")
                .reader(),
        )
        .expect("should read response body");
        assert_matches::assert_matches!(
            resp_body,
            KeyspaceResponseBody { nodes } if matches!(
                &nodes[..],
                [ServiceNode { id: 0, hostname }] if hostname == VALID_HOSTNAME
            )
        );
    }
}
