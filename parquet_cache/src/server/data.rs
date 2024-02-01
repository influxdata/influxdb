mod manager;
mod reads;
mod store;
mod writes;

use std::{sync::Arc, task::Poll};

use backoff::{Backoff, BackoffConfig};
use bytes::Buf;
use http::{Request, Uri};
use hyper::{Body, Method};
use iox_catalog::interface::Catalog;
use object_store::ObjectStore;
use observability_deps::tracing::{error, warn};
use tokio::task::JoinHandle;
use tower::Service;

use self::{
    manager::{CacheManager, CacheManagerValue},
    reads::ReadHandler,
    store::LocalStore,
    writes::WriteHandler,
};
use super::{error::Error, response::Response};
use crate::data_types::{PolicyConfig, WriteHint, WriteHintRequestBody};

#[derive(Debug, thiserror::Error)]
pub enum DataError {
    #[error("Read error: {0}")]
    Read(String),
    #[error("Write-stream error: {0}")]
    Stream(String),
    #[error("Write-file error: {0}")]
    File(String),
    #[error("Bad Request: {0}")]
    BadRequest(String),
    #[error("Bad Request: object location does not exist in catalog or object store")]
    DoesNotExist,
}

/// Service that provides access to the data.
#[derive(Debug, Clone)]
pub struct DataService {
    catalog: Arc<dyn Catalog>,
    cache_manager: Arc<CacheManager>,
    read_handler: ReadHandler,
    write_hander: WriteHandler,
    handle: Arc<JoinHandle<()>>,
    backoff_config: BackoffConfig,
}

impl DataService {
    pub async fn new(
        direct_store: Arc<dyn ObjectStore>,
        catalog: Arc<dyn Catalog>,
        config: PolicyConfig,
        dir: Option<impl ToString + Send>,
    ) -> Self {
        let data_accessor = Arc::new(LocalStore::new(dir));

        // TODO: use a bounded channel
        // Apply back pressure if we can't keep up (a.k.a. the actual eviction from the local store).
        let (evict_tx, evict_rx) = async_channel::unbounded();

        // start background task to evict from local store
        let data_accessor_ = Arc::clone(&data_accessor);
        let handle = tokio::spawn(async move {
            while let Ok(key) = evict_rx.recv().await {
                let _ = data_accessor_.delete_object(&key).await;
            }
        });

        Self {
            catalog,
            read_handler: ReadHandler::new(Arc::clone(&data_accessor)),
            write_hander: WriteHandler::new(Arc::clone(&data_accessor), direct_store),
            cache_manager: Arc::new(CacheManager::new(config, evict_tx)),
            handle: Arc::new(handle),
            backoff_config: Default::default(),
        }
    }

    async fn create_write_hint(&self, location: &String) -> Result<WriteHint, Error> {
        let parquet_file_path = parquet_file::ParquetFilePath::try_from(location)
            .map_err(|e| Error::BadRequest(e.to_string()))?;

        let maybe_parquet_file = Backoff::new(&self.backoff_config)
            .retry_all_errors("lookup write-hint in catalog", || async {
                self.catalog
                    .repositories()
                    .parquet_files()
                    .get_by_object_store_id(parquet_file_path.object_store_id())
                    .await
            })
            .await
            .expect("retry forever");

        match maybe_parquet_file {
            None => Err(Error::DoesNotExist),
            Some(parquet_file) => Ok(WriteHint::from(&parquet_file)),
        }
    }

    async fn write_back(&self, location: String, write_hint: WriteHint) -> Result<(), Error> {
        // confirm valid location
        parquet_file::ParquetFilePath::try_from(&location)
            .map_err(|e| Error::BadRequest(e.to_string()))?;

        // write to local store
        let metadata = self
            .write_hander
            .write_local(&location, &write_hint)
            .await?;

        // update cache manager
        self.cache_manager
            .insert(
                location,
                CacheManagerValue {
                    params: write_hint,
                    metadata,
                },
            )
            .await;

        Ok(())
    }
}

impl Service<Request<Body>> for DataService {
    type Response = Response;
    type Error = Error;
    type Future = super::response::PinnedFuture;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/state")
            | (&Method::PATCH, "/warmed")
            | (&Method::GET, "/keyspace") => {
                unreachable!("`this request should have already been handled in the KeyspaceLayer`")
            }
            (&Method::GET, "/metadata") | (&Method::GET, "/object") => {
                let this = self.clone();
                Box::pin(async move {
                    let obj_location = parse_object_location(req.uri())?;
                    match this.cache_manager.in_cache(&obj_location).await {
                        Ok(_) => match req.uri().path() {
                            "/metadata" => {
                                let meta = this.cache_manager.fetch_metadata(&obj_location).await?;
                                Ok(Response::Head(meta.into()))
                            }
                            "/object" => {
                                let stream = this.read_handler.read_local(&obj_location).await?;
                                Ok(Response::Data(stream))
                            }
                            _ => unreachable!(),
                        },
                        Err(Error::CacheMiss) => {
                            // trigger write-back on another thread
                            let this_ = this.clone();
                            tokio::spawn(async move {
                                let write_hint = match this_.create_write_hint(&obj_location).await
                                {
                                    Ok(write_hint) => write_hint,
                                    Err(error) => {
                                        warn!(%error, "write-back failed to create write-hint (likely missing from catalog)");
                                        return;
                                    }
                                };

                                if let Err(error) = this_.write_back(obj_location, write_hint).await
                                {
                                    error!(%error, "write-back failed to perform local-store write");
                                }
                            });

                            // still return immediate response, such that client will use direct_store fallback
                            Err(Error::CacheMiss)
                        }
                        Err(e) => Err(e),
                    }
                })
            }
            (&Method::POST, "/write-hint") => {
                let this = self.clone();
                Box::pin(async move {
                    let reader = hyper::body::aggregate(req.into_body())
                        .await
                        .map_err(|e| Error::BadRequest(e.to_string()))?
                        .reader();
                    let write_hint: WriteHintRequestBody = serde_json::from_reader(reader)
                        .map_err(|e| Error::BadRequest(e.to_string()))?;

                    match this.cache_manager.in_cache(&write_hint.location).await {
                        Ok(_) => Ok(Response::Written),
                        Err(_) => {
                            this.write_back(write_hint.location, write_hint.hint)
                                .await?;
                            Ok(Response::Written)
                        }
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

fn parse_object_location(uri: &Uri) -> Result<String, Error> {
    let as_url = url::Url::parse(uri.to_string().as_str())
        .expect("should be already validated path & query");
    match as_url.query_pairs().find(|(k, _v)| k.eq("location")) {
        None => Err(Error::BadRequest(
            "missing required query parameter: location".into(),
        )),
        Some((_key, location)) => Ok(location.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs::File, io::Write, ops::Range, path::PathBuf};

    use assert_matches::assert_matches;
    use bytes::{BufMut, Bytes, BytesMut};
    use chrono::{DateTime, Utc};
    use futures::{stream::BoxStream, TryStreamExt};
    use iox_tests::TestParquetFileBuilder;
    use object_store::{
        path::Path, GetOptions, GetResult, GetResultPayload, ListResult, MultipartId, ObjectMeta,
        ObjectStore, PutOptions, PutResult,
    };
    use tempfile::{tempdir, TempDir};
    use tokio::{fs::create_dir_all, io::AsyncWrite};

    use crate::data_types::GetObjectMetaResponse;

    use super::*;

    const ONE_SECOND: u64 = 1_000_000_000;

    // refer to valid path in parquet_file::ParquetFilePath
    const LOCATION_F: &str = "0/0/partition_key/00000000-0000-0000-0000-000000000000.parquet";
    const LOCATION_S: &str = "0/0/partition_key/00000000-0000-0000-0000-000000000001.parquet";
    const LOCATION_MISSING: &str = "0/0/partition_key/00000000-0000-0000-0000-000000000002.parquet"; // not in catalog, nor remote store

    const DATA: &[u8] = b"all my pretty words";

    lazy_static::lazy_static! {
        static ref LAST_MODIFIED: DateTime<Utc> = Utc::now();
    }

    #[derive(Debug)]
    struct MockData(Bytes, bool /* as_stream */);

    #[derive(Debug)]
    struct MockDirectStore {
        mocked: HashMap<String /* location */, MockData>,
        temp_dir: TempDir,
    }

    impl MockDirectStore {
        fn default() -> Self {
            Self {
                mocked: HashMap::new(),
                temp_dir: tempdir().expect("should create temp dir"),
            }
        }

        fn put_mock(&mut self, location: String, data: MockData) {
            self.mocked.insert(location, data);
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for MockDirectStore {
        async fn get_opts(
            &self,
            location: &Path,
            _options: GetOptions,
        ) -> object_store::Result<GetResult> {
            let MockData(bytes, as_stream) = match self.mocked.get(&location.to_string()) {
                Some(data) => data,
                _ => {
                    return Err(object_store::Error::NotFound {
                        path: location.to_string(),
                        source: "not found in remote store".into(),
                    })
                }
            };

            let meta = ObjectMeta {
                location: location.clone(),
                last_modified: *LAST_MODIFIED,
                size: DATA.to_vec().len(),
                e_tag: Default::default(),
                version: Default::default(),
            };

            let bytes = bytes.to_owned();
            let payload =
                match as_stream {
                    true => GetResultPayload::Stream(Box::pin(futures::stream::once(async move {
                        Ok(bytes)
                    }))),
                    false => {
                        let path = self.temp_dir.path().join(location.to_string());
                        create_dir_all(path.parent().unwrap())
                            .await
                            .expect("should create nested path");
                        let mut file =
                            File::create(path.as_path()).expect("should be able to open temp file");
                        file.write_all(&bytes)
                            .expect("should be able to write to temp file");
                        file.flush().expect("should be able to flush temp file");
                        GetResultPayload::File(file, path)
                    }
                };

            Ok(GetResult {
                payload,
                meta,
                range: Range {
                    start: 0,
                    end: DATA.to_vec().len(),
                },
            })
        }

        async fn put_opts(
            &self,
            _location: &Path,
            _bytes: Bytes,
            _opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            unimplemented!()
        }
        async fn put_multipart(
            &self,
            _location: &Path,
        ) -> object_store::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
            unimplemented!()
        }
        async fn abort_multipart(
            &self,
            _location: &Path,
            _multipart_id: &MultipartId,
        ) -> object_store::Result<()> {
            unimplemented!()
        }
        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
            unimplemented!()
        }
        fn list(&self, _prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            unimplemented!()
        }
        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            unimplemented!()
        }
        async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }
        async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }
    }

    impl std::fmt::Display for MockDirectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockDirectStore")
        }
    }

    fn make_parquet_file(location: &str) -> TestParquetFileBuilder {
        let parquet_file_path = parquet_file::ParquetFilePath::try_from(&location.to_string())
            .expect("should be valid parquet file path");

        TestParquetFileBuilder::default()
            .with_creation_time(iox_time::Time::from_date_time(*LAST_MODIFIED))
            .with_file_size_bytes(DATA.to_vec().len() as u64)
            .with_object_store_id(parquet_file_path.object_store_id())
    }

    async fn make_service(temp_dir: PathBuf, policy_config: Option<PolicyConfig>) -> DataService {
        let mut direct_store = MockDirectStore::default();
        // data returned as file, for write-back
        direct_store.put_mock(
            LOCATION_F.to_string(),
            MockData(Bytes::from(DATA.to_vec()), false),
        );
        // data returned as stream, for write-back
        direct_store.put_mock(
            LOCATION_S.to_string(),
            MockData(Bytes::from(DATA.to_vec()), true),
        );

        // create catalog
        let test_catalog = iox_tests::TestCatalog::new();
        let namespace = test_catalog.create_namespace_1hr_retention("ns0").await;
        let table = namespace.create_table("table0").await;
        let partition = table.create_partition("partition_key").await;

        // add parquet files to catalog
        partition
            .create_parquet_file_catalog_record(make_parquet_file(LOCATION_F))
            .await;
        partition
            .create_parquet_file_catalog_record(make_parquet_file(LOCATION_S))
            .await;

        DataService::new(
            Arc::new(direct_store),
            test_catalog.catalog(),
            policy_config.unwrap_or(PolicyConfig {
                max_capacity: 3_200_000,
                event_recency_max_duration_nanoseconds: ONE_SECOND * 60 * 2,
            }),
            Some(temp_dir.to_str().unwrap()),
        )
        .await
    }

    // note: uses file for write-back
    #[tokio::test]
    async fn test_metadata_writeback_on_cache_miss() {
        // setup
        let dir = tempdir().expect("should create temp dir");
        let mut service = make_service(PathBuf::from(dir.path()), None).await;

        // return cache miss
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("http://foo.io/metadata?location={}", LOCATION_F))
            .body(Body::empty())
            .unwrap();
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Err(Error::CacheMiss),
            "should return cache miss, instead found {:?}",
            resp
        );

        // wait for write-back to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // return cache hit
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("http://foo.io/metadata?location={}", LOCATION_F))
            .body(Body::empty())
            .unwrap();
        let resp = service.call(req).await;
        let expected = GetObjectMetaResponse::from(ObjectMeta {
            location: LOCATION_F.into(),
            size: DATA.to_vec().len(),
            last_modified: *LAST_MODIFIED,
            e_tag: Default::default(),
            version: Default::default(),
        });
        assert_matches!(
            resp,
            Ok(Response::Head(meta)) if meta == expected,
            "should return metadata for location, instead found {:?}", resp
        );
    }

    // note: uses file for write-back
    #[tokio::test]
    async fn test_object_writeback_on_cache_miss() {
        // setup
        let dir = tempdir().expect("should create temp dir");
        let mut service = make_service(PathBuf::from(dir.path()), None).await;

        // return cache miss
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("http://foo.io/object?location={}", LOCATION_F))
            .body(Body::empty())
            .unwrap();
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Err(Error::CacheMiss),
            "should return cache miss, instead found {:?}",
            resp
        );

        // wait for write-back to complete
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // return cache hit
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("http://foo.io/object?location={}", LOCATION_F))
            .body(Body::empty())
            .unwrap();
        let resp = service.call(req).await;
        match resp {
            Ok(Response::Data(stream)) => {
                let data = stream.try_collect::<Vec<_>>().await.unwrap();
                assert_eq!(
                    data,
                    vec![DATA.to_vec()],
                    "should have returned matching bytes"
                );
            }
            _ => panic!("should return data for location, instead found {:?}", resp),
        }
    }

    // note: uses stream for write-back
    #[tokio::test]
    async fn test_write_hint() {
        // setup
        let dir = tempdir().expect("should create temp dir");
        let mut service = make_service(PathBuf::from(dir.path()), None).await;

        // issue write-hint
        let mut buf = BytesMut::new().writer();
        serde_json::to_writer(
            &mut buf,
            &WriteHintRequestBody {
                location: LOCATION_S.into(),
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
            .uri("http://foo.io/write-hint")
            .body(hyper::Body::from(buf.into_inner().freeze()))
            .unwrap();
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Ok(Response::Written),
            "should return successful write-back, instead found {:?}",
            resp
        );

        // return cache hit -- metadata
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("http://foo.io/metadata?location={}", LOCATION_S))
            .body(Body::empty())
            .unwrap();
        let resp = service.call(req).await;
        let expected = GetObjectMetaResponse::from(ObjectMeta {
            location: LOCATION_S.into(),
            size: DATA.to_vec().len(),
            last_modified: *LAST_MODIFIED,
            e_tag: Default::default(),
            version: Default::default(),
        });
        assert_matches!(
            resp,
            Ok(Response::Head(meta)) if meta == expected,
            "should return metadata for location, instead found {:?}", resp
        );

        // return cache hit -- object
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("http://foo.io/object?location={}", LOCATION_S))
            .body(Body::empty())
            .unwrap();
        let resp = service.call(req).await;
        match resp {
            Ok(Response::Data(stream)) => {
                let data = stream.try_collect::<Vec<_>>().await.unwrap();
                assert_eq!(
                    data,
                    vec![DATA.to_vec()],
                    "should have returned matching bytes"
                );
            }
            _ => panic!("should return data for location, instead found {:?}", resp),
        }
    }

    #[tokio::test]
    async fn test_write_hint_fails_for_invalid_path() {
        // setup
        let dir = tempdir().expect("should create temp dir");
        let mut service = make_service(PathBuf::from(dir.path()), None).await;

        // issue write-hint
        let mut buf = BytesMut::new().writer();
        serde_json::to_writer(
            &mut buf,
            &WriteHintRequestBody {
                location: "not_a_valid_path.parquet".into(),
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
            .uri("http://foo.io/write-hint")
            .body(hyper::Body::from(buf.into_inner().freeze()))
            .unwrap();
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Err(Error::BadRequest(_)),
            "should return failed write-back, instead found {:?}",
            resp
        );
    }

    #[tokio::test]
    async fn test_write_hint_fails_for_incorrect_size() {
        // setup
        let dir = tempdir().expect("should create temp dir");
        let mut service = make_service(PathBuf::from(dir.path()), None).await;

        // issue write-hint
        let mut buf = BytesMut::new().writer();
        serde_json::to_writer(
            &mut buf,
            &WriteHintRequestBody {
                location: LOCATION_S.into(),
                hint: WriteHint {
                    file_size_bytes: 12312,
                    ..Default::default()
                },
                ack_setting: crate::data_types::WriteHintAck::Completed,
            },
        )
        .expect("should write request body");
        let req = Request::builder()
            .method(Method::POST)
            .uri("http://foo.io/write-hint")
            .body(hyper::Body::from(buf.into_inner().freeze()))
            .unwrap();
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Err(Error::Data(_)),
            "should error for incorrect file size in write-hint, instead found {:?}",
            resp
        );
    }

    #[tokio::test]
    async fn test_fails_for_nonexistent_object() {
        // setup
        let dir = tempdir().expect("should create temp dir");
        let mut service = make_service(PathBuf::from(dir.path()), None).await;

        // issue write-hint
        // Fails when looking up in remote store. Does not check catalog first.
        let mut buf = BytesMut::new().writer();
        serde_json::to_writer(
            &mut buf,
            &WriteHintRequestBody {
                location: LOCATION_MISSING.into(),
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
            .uri("http://foo.io/write-hint")
            .body(hyper::Body::from(buf.into_inner().freeze()))
            .unwrap();
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Err(Error::Data(DataError::DoesNotExist)),
            "should return failed write-back, instead found {:?}",
            resp
        );
    }

    #[tokio::test]
    async fn test_eviction() {
        // setup
        let policy_config = PolicyConfig {
            max_capacity: DATA.to_vec().len() as u64 + 1,
            event_recency_max_duration_nanoseconds: ONE_SECOND * 60 * 2,
        };
        let dir = tempdir().expect("should create temp dir");
        let mut service = make_service(PathBuf::from(dir.path()), Some(policy_config)).await;

        // issue write-hint
        let mut buf = BytesMut::new().writer();
        serde_json::to_writer(
            &mut buf,
            &WriteHintRequestBody {
                location: LOCATION_S.into(),
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
            .uri("http://foo.io/write-hint")
            .body(hyper::Body::from(buf.into_inner().freeze()))
            .unwrap();
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Ok(Response::Written),
            "should return successful write-back, instead found {:?}",
            resp
        );

        // return cache hit
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("http://foo.io/metadata?location={}", LOCATION_S))
            .body(Body::empty())
            .unwrap();
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Ok(Response::Head(_)),
            "should return metadata for location, instead found {:?}",
            resp
        );
        service.cache_manager.flush_pending().await;

        // issue 2nd write-hint
        let mut buf = BytesMut::new().writer();
        serde_json::to_writer(
            &mut buf,
            &WriteHintRequestBody {
                location: LOCATION_F.into(),
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
            .uri("http://foo.io/write-hint")
            .body(hyper::Body::from(buf.into_inner().freeze()))
            .unwrap();
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Ok(Response::Written),
            "should return successful write-back, instead found {:?}",
            resp
        );
        service.cache_manager.flush_pending().await;

        // eviction should have happened
        // should return cache miss
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("http://foo.io/metadata?location={}", LOCATION_S))
            .body(Body::empty())
            .unwrap();
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Err(Error::CacheMiss),
            "should return cache miss, instead found {:?}",
            resp
        );

        // other object should still be in cache
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("http://foo.io/metadata?location={}", LOCATION_F))
            .body(Body::empty())
            .unwrap();
        let resp = service.call(req).await;
        assert_matches!(
            resp,
            Ok(Response::Head(_)),
            "should return metadata for location, instead found {:?}",
            resp
        );

        dir.close().expect("should close temp dir");
    }
}
