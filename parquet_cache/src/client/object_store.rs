use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::{ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use http::Method;
use hyper::StatusCode;
use hyper::{Body, Response};
use object_store::{
    path::Path, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartId,
    ObjectMeta, ObjectStore, PutOptions, PutResult, Result,
};
use tokio::io::AsyncWrite;
use tower::{Service, ServiceExt};

use crate::data_types::{
    extract_usize_header, GetObjectMetaResponse, X_RANGE_END_HEADER, X_RANGE_START_HEADER,
};

use super::cache_connector::{ClientCacheConnector, Error as CacheClientError};
use super::request::RawRequest;

/// identifier for `object_store::Error::Generic`
const DATA_CACHE: &str = "object store to data cache";

/// Data cache, consumable by IOX Components.
pub struct DataCacheObjectStore {
    pub(crate) cache: ClientCacheConnector,
    pub(crate) direct_passthru: Arc<dyn ObjectStore>,
}

impl DataCacheObjectStore {
    /// Create a new [`DataCacheObjectStore`].
    pub fn new(cache: ClientCacheConnector, direct_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            cache,
            direct_passthru: Arc::new(direct_store),
        }
    }
}

/// ObjectStore client for using the data cache.
///
/// Defines when to use the direct (passthru) object store,
/// versus the data cache.
///
/// Iox components all utilize the [`ObjectStore`] for store connection.
/// Based upon startup configuration, this may be the data cache.
#[async_trait]
impl ObjectStore for DataCacheObjectStore {
    async fn put_opts(&self, location: &Path, bytes: Bytes, opts: PutOptions) -> Result<PutResult> {
        self.direct_passthru.put_opts(location, bytes, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.direct_passthru.put_multipart(location).await
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> Result<()> {
        self.direct_passthru
            .abort_multipart(location, multipart_id)
            .await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let object_meta: ObjectMeta = self.head(location).await?;

        let key = location.to_string();

        let uri_parts = format!("/object?location={}", key)
            .parse::<http::Uri>()
            .map(http::uri::Parts::from)
            .expect("should be valid uri");

        let GetOptions {
            if_match,
            if_none_match,
            if_modified_since,
            if_unmodified_since,
            range,
            version,
            head,
        } = &options;
        let headers = Headers(&mut HashMap::new())
            .add_header("If-Match", if_match)
            .add_header("If-None-Match", if_none_match)
            .add_header("If-Modified-Since", if_modified_since)
            .add_header("If-Unmodified-Since", if_unmodified_since)
            // Pass other options as non standard headers
            .add_header("X-Version", version)
            .add_header("X-Head", &Some(head))
            .add_range(range)
            .0
            .to_owned();

        let req = RawRequest {
            method: Method::GET,
            uri_parts,
            headers,
            key: Some(key),
            ..Default::default()
        };

        let mut cache = self.cache.clone();
        let service = cache.ready().await.map_err(|e| ObjectStoreError::Generic {
            store: DATA_CACHE,
            source: Box::new(e),
        })?;

        match service.call(req).await {
            Ok(resp) => match resp.status() {
                StatusCode::OK => {
                    match transform_get_object_response(resp, object_meta, range) {
                        Ok(res) => Ok(res),
                        Err(_) => self.direct_passthru.get_opts(location, options).await, // read_data error
                    }
                }
                code => {
                    if use_fallback(code) {
                        self.direct_passthru.get_opts(location, options).await // http code error
                    } else {
                        let source = Box::new(Error::new(ErrorKind::Other, code.to_string()));
                        Err(ObjectStoreError::Generic {
                            store: DATA_CACHE,
                            source,
                        })
                    }
                }
            },
            Err(_) => self.direct_passthru.get_opts(location, options).await, // connection error
        }
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        self.get_opts(
            location,
            GetOptions {
                range: Some(range),
                ..Default::default()
            },
        )
        .await?
        .bytes()
        .await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let key = location.to_string();

        let uri_parts = format!("/metadata?location={}", key)
            .parse::<http::Uri>()
            .map(http::uri::Parts::from)
            .expect("should be valid uri");

        let req = RawRequest {
            method: Method::GET,
            uri_parts,
            key: Some(key),
            ..Default::default()
        };

        let mut cache = self.cache.clone();
        let service = cache.ready().await.map_err(|e| ObjectStoreError::Generic {
            store: DATA_CACHE,
            source: Box::new(e),
        })?;

        match service.call(req).await {
            Ok(mut resp) => match resp.status() {
                StatusCode::OK => {
                    let maybe_meta: Result<ObjectMeta, CacheClientError> =
                        hyper::body::aggregate(resp.body_mut())
                            .await
                            .map_err(|e| CacheClientError::ReadData(e.to_string()))
                            .map(|buf| buf.reader())
                            .and_then(|reader| {
                                serde_json::from_reader(reader)
                                    .map_err(|e| CacheClientError::ReadData(e.to_string()))
                            })
                            .map(|get_meta_resp: GetObjectMetaResponse| {
                                ObjectMeta::from(get_meta_resp)
                            });

                    match maybe_meta {
                        Ok(meta) => Ok(meta),
                        Err(_) => self.direct_passthru.head(location).await, // read_data error
                    }
                }
                code => {
                    if use_fallback(code) {
                        self.direct_passthru.head(location).await // http code error
                    } else {
                        let source = Box::new(Error::new(ErrorKind::Other, code.to_string()));
                        Err(ObjectStoreError::Generic {
                            store: DATA_CACHE,
                            source,
                        })
                    }
                }
            },
            Err(_) => self.direct_passthru.head(location).await, // connection error
        }
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        // Do not delete from cache, instead let it age out.
        // Querier runs off of catalog snapshots of object_store state.
        self.direct_passthru.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        // Use object_store directly as src of truth for currently existing files.
        // Because cache cannot know about completeness of the file set.
        self.direct_passthru.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        // Use object_store directly as src of truth for currently existing files.
        // Because cache cannot know about completeness of the file set.
        self.direct_passthru.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.direct_passthru.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.direct_passthru.copy_if_not_exists(from, to).await
    }
}

impl std::fmt::Display for DataCacheObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DataCacheObjectStore")
    }
}

impl std::fmt::Debug for DataCacheObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DataCacheObjectStore")
    }
}

fn use_fallback(code: StatusCode) -> bool {
    match code {
        StatusCode::OK => unreachable!("should not be requesting fallback if response is OK"),
        // Errors which should not result in trying the fallback.
        StatusCode::BAD_REQUEST
        | StatusCode::PRECONDITION_FAILED
        | StatusCode::FORBIDDEN
        | StatusCode::UNAUTHORIZED
        | StatusCode::MOVED_PERMANENTLY
        | StatusCode::NETWORK_AUTHENTICATION_REQUIRED => false,
        // All other errors => use fallback.
        _ => true,
    }
}

fn transform_get_object_response(
    resp: Response<Body>,
    meta: ObjectMeta,
    expected_range: &Option<Range<usize>>,
) -> Result<GetResult, CacheClientError> {
    let headers = resp.headers();
    let range = Range {
        start: extract_usize_header(X_RANGE_START_HEADER, headers)?,
        end: extract_usize_header(X_RANGE_END_HEADER, headers)?,
    };

    if let Some(expected_range) = expected_range {
        if !expected_range.start.eq(&range.start) || !expected_range.end.eq(&range.end) {
            return Err(CacheClientError::ReadData(format!(
                "expected range {:?} but found range {:?}",
                expected_range, range
            )));
        }
    };

    let stream = resp
        .into_body()
        .map_err(|e| ObjectStoreError::Generic {
            store: DATA_CACHE,
            source: Box::new(e),
        })
        .boxed();

    Ok(GetResult {
        payload: object_store::GetResultPayload::Stream(stream),
        meta,
        range,
    })
}

/// Newtype around headers, for convenience methods.
struct Headers<'a>(pub &'a mut HashMap<&'static str, String>);

impl<'a> Headers<'a> {
    fn add_header<T: ToString>(&mut self, k: &'static str, v: &Option<T>) -> &mut Self {
        if let Some(v) = v {
            // let header_name = k.to_owned();
            self.0.insert(k, v.to_string());
        }
        self
    }

    fn add_range(&mut self, range: &Option<Range<usize>>) -> &mut Self {
        if let Some(v) = range {
            self.0
                .insert("Range", format!("bytes={}-{}", v.start, v.end));
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use crate::client::mock::{build_cache_server_client, MockDirectStore};
    use crate::server::mock::{build_resp_body, ExpectedResponse};

    use super::*;

    static FILE: &[u8] = "All my pretty data.".as_bytes();

    #[tokio::test]
    async fn test_writes_are_passed_to_store() {
        let direct_to_store = Arc::new(MockDirectStore::default());

        let casted_object_store = Arc::clone(&direct_to_store) as Arc<dyn ObjectStore>;
        let (object_store, cache_server) = build_cache_server_client(casted_object_store).await;

        assert!(object_store
            .put(&Path::default(), FILE.into())
            .await
            .is_ok());
        assert!(
            Arc::clone(&direct_to_store).was_called("put"),
            "put should be passed to direct store"
        );

        assert!(object_store.put_multipart(&Path::default()).await.is_ok());
        assert!(
            Arc::clone(&direct_to_store).was_called("put_multipart"),
            "put_multipart should be passed to direct store"
        );

        assert!(object_store
            .abort_multipart(&Path::default(), &MultipartId::default())
            .await
            .is_ok());
        assert!(
            Arc::clone(&direct_to_store).was_called("abort_multipart"),
            "abort_multipart should be passed to direct store"
        );

        assert!(object_store.delete(&Path::default()).await.is_ok());
        assert!(
            Arc::clone(&direct_to_store).was_called("delete"),
            "delete should be passed to direct store"
        );

        assert!(object_store
            .copy(&Path::default(), &Path::default())
            .await
            .is_ok());
        assert!(
            Arc::clone(&direct_to_store).was_called("copy"),
            "copy should be passed to direct store"
        );

        assert!(object_store
            .copy_if_not_exists(&Path::default(), &Path::default())
            .await
            .is_ok());
        assert!(
            Arc::clone(&direct_to_store).was_called("copy_if_not_exists"),
            "copy_if_not_exists should be passed to direct store"
        );

        cache_server.close().await;
    }

    #[tokio::test]
    async fn test_list_all_objects_are_passed_to_store() {
        let direct_to_store = Arc::new(MockDirectStore::default());

        let casted_object_store = Arc::clone(&direct_to_store) as Arc<dyn ObjectStore>;
        let (object_store, cache_server) = build_cache_server_client(casted_object_store).await;

        object_store.list(Some(&Path::default()));
        assert!(
            Arc::clone(&direct_to_store).was_called("list"),
            "list should be passed to direct store"
        );

        assert!(object_store
            .list_with_delimiter(Some(&Path::default()))
            .await
            .is_ok());
        assert!(
            Arc::clone(&direct_to_store).was_called("list_with_delimiter"),
            "list_with_delimiter should be passed to direct store"
        );

        cache_server.close().await;
    }

    #[tokio::test]
    async fn test_fetch_requests_hit_the_cache() {
        let direct_to_store = Arc::new(MockDirectStore::default());

        let casted_object_store = Arc::clone(&direct_to_store) as Arc<dyn ObjectStore>;
        let (object_store, cache_server) = build_cache_server_client(casted_object_store).await;

        let path = Path::from("my/scoped/data/file.parquet");

        // GET /metadata
        let route = format!("/metadata?location={}", &path.to_string());
        let expected_metadata_resp = GetObjectMetaResponse {
            location: path.to_string(),
            last_modified: Default::default(),
            size: 42,
            e_tag: None,
            version: None,
        };
        cache_server.respond_with(
            route.clone(),
            ExpectedResponse {
                bytes: build_resp_body(&expected_metadata_resp),
                range: None,
            },
        );
        assert_matches!(
            object_store.head(&path).await,
            Ok(res) if res == ObjectMeta::from(expected_metadata_resp.clone()),
            "payload was returned and parsed properly"
        );
        assert!(
            cache_server.was_called(&route),
            "head should hit the cache server"
        );

        // GET fetch /object
        // note: all fetch object requests use ObjectStore::get_opts()
        let route = format!("/object?location={}", path);
        cache_server.respond_with(
            route.clone(),
            ExpectedResponse {
                bytes: std::str::from_utf8(FILE).unwrap().into(),
                range: Some(Range {
                    start: 0,
                    end: FILE.len(),
                }),
            },
        );
        let object_resp = object_store.get(&path).await;
        assert_matches!(
            &object_resp,
            Ok(GetResult {payload: _, meta, range: _}) if meta == &ObjectMeta::from(expected_metadata_resp),
            "object metadata was returned and parsed properly"
        ); // note: payload bytes will be asserted separately with the (non-mock-)server integration tests.
        assert!(
            cache_server.was_called(&route),
            "get should hit the cache server"
        );

        cache_server.close().await;
    }

    #[tokio::test]
    async fn test_fetch_range_request() {
        let direct_to_store = Arc::new(MockDirectStore::default());

        let casted_object_store = Arc::clone(&direct_to_store) as Arc<dyn ObjectStore>;
        let (object_store, cache_server) = build_cache_server_client(casted_object_store).await;

        let path = Path::from("my/scoped/data/file.parquet");

        // add mock metadata
        let route = format!("/metadata?location={}", &path.to_string());
        let expected_metadata_resp = GetObjectMetaResponse {
            location: path.to_string(),
            last_modified: Default::default(),
            size: 42,
            e_tag: None,
            version: None,
        };
        cache_server.respond_with(
            route.clone(),
            ExpectedResponse {
                bytes: build_resp_body(&expected_metadata_resp),
                range: None,
            },
        );

        // add mock file
        let route = format!("/object?location={}", &path.to_string());
        cache_server.respond_with(
            route.clone(),
            ExpectedResponse {
                bytes: std::str::from_utf8(&FILE[3..9]).unwrap().into(),
                range: Some(Range { start: 3, end: 9 }),
            },
        );

        // TEST: get_range()
        let range = Range { start: 3, end: 9 };
        let object_resp = object_store.get_range(&path, range.clone()).await;
        assert_matches!(
            &object_resp,
            Ok(bytes) if bytes.len() == range.len(),
            "returns proper bytes size for the range"
        );
        assert!(
            cache_server.was_called(&route),
            "get should hit the cache server"
        );

        // TEST: multiple get_ranges()
        let object_resp = object_store
            .get_ranges(&path, &[range.clone(), range.clone()])
            .await;
        assert_matches!(
            &object_resp,
            Ok(vec_bytes) if matches!(
                &vec_bytes[..],
                [bytes, bytes_2] if bytes.len() == range.len() && bytes_2.len() == range.len()
            ),
            "returns proper bytes size for multiple ranges"
        );

        cache_server.close().await;
    }

    mod test_range_failures {
        use super::*;

        #[should_panic(expected = "direct_store.get_opts() was called during test")]
        #[tokio::test]
        async fn test_get_opts_will_use_fallback_if_returned_range_does_not_match() {
            let direct_to_store = Arc::new(MockDirectStore::default());

            let casted_object_store = Arc::clone(&direct_to_store) as Arc<dyn ObjectStore>;
            let (object_store, cache_server) = build_cache_server_client(casted_object_store).await;

            let path = Path::from("my/scoped/data/file.parquet");

            // add mock metadata
            let route = format!("/metadata?location={}", &path.to_string());
            let expected_metadata_resp = GetObjectMetaResponse {
                location: path.to_string(),
                last_modified: Default::default(),
                size: 42,
                e_tag: None,
                version: None,
            };
            cache_server.respond_with(
                route.clone(),
                ExpectedResponse {
                    bytes: build_resp_body(&expected_metadata_resp),
                    range: None,
                },
            );

            // add mock file
            let route = format!("/object?location={}", &path.to_string());
            cache_server.respond_with(
                route.clone(),
                ExpectedResponse {
                    bytes: std::str::from_utf8(&FILE[3..9]).unwrap().into(),
                    range: Some(Range { start: 3, end: 9 }),
                },
            );

            // TEST: get_range()
            let range = Range { start: 1, end: 7 };
            let _ = object_store.get_range(&path, range.clone()).await;

            cache_server.close().await;
        }
    }

    mod test_head_failures {
        use super::*;

        #[should_panic(expected = "direct_store.head() was called during test")]
        #[tokio::test]
        async fn test_use_fallback_when_missing_data() {
            let direct_to_store = Arc::new(MockDirectStore::default());

            let casted_object_store = Arc::clone(&direct_to_store) as Arc<dyn ObjectStore>;
            let (object_store, _cache_server) =
                build_cache_server_client(casted_object_store).await;

            let path = Path::from("my/scoped/data/file.parquet");

            // TEST: metadata never provided to mock
            let _ = object_store.head(&path).await;
        }

        #[should_panic(expected = "direct_store.head() was called during test")]
        #[tokio::test]
        async fn test_use_fallback_when_bad_data() {
            let direct_to_store = Arc::new(MockDirectStore::default());

            let casted_object_store = Arc::clone(&direct_to_store) as Arc<dyn ObjectStore>;
            let (object_store, cache_server) = build_cache_server_client(casted_object_store).await;

            let path = Path::from("my/scoped/data/file.parquet");

            // TEST: incorrect metadata provided to mock
            let route = format!("/metadata?location={}", &path.to_string());
            cache_server.respond_with(
                route.clone(),
                ExpectedResponse {
                    bytes: vec![].into(), // BAD: should be metadata
                    range: None,
                },
            );
            let _ = object_store.head(&path).await;
        }

        #[should_panic(expected = "direct_store.head() was called during test")]
        #[tokio::test]
        async fn test_use_fallback_on_connection_failed() {
            let direct_to_store = Arc::new(MockDirectStore::default());

            let casted_object_store = Arc::clone(&direct_to_store) as Arc<dyn ObjectStore>;
            let (object_store, cache_server) = build_cache_server_client(casted_object_store).await;

            let path = Path::from("my/scoped/data/file.parquet");

            // GET /metadata is working
            let route = format!("/metadata?location={}", &path.to_string());
            let expected_metadata_resp = GetObjectMetaResponse {
                location: path.to_string(),
                last_modified: Default::default(),
                size: 42,
                e_tag: None,
                version: None,
            };
            cache_server.respond_with(
                route.clone(),
                ExpectedResponse {
                    bytes: build_resp_body(&expected_metadata_resp),
                    range: None,
                },
            );
            assert_matches!(
                object_store.head(&path).await,
                Ok(res) if res == ObjectMeta::from(expected_metadata_resp.clone()),
                "payload was returned and parsed properly"
            );

            // kill server
            cache_server.close().await;

            // TEST: connection fails
            let _ = object_store.head(&path).await;
        }
    }

    mod test_get_opts_failures {
        use crate::MockCacheServer;

        use super::*;

        async fn setup_metadata_head(path: &Path, cache_server: &MockCacheServer) {
            // GET /metadata is working
            let route = format!("/metadata?location={}", path);
            let expected_metadata_resp = GetObjectMetaResponse {
                location: path.to_string(),
                last_modified: Default::default(),
                size: 42,
                e_tag: None,
                version: None,
            };
            cache_server.respond_with(
                route.clone(),
                ExpectedResponse {
                    bytes: build_resp_body(&expected_metadata_resp),
                    range: None,
                },
            );
        }

        #[should_panic(expected = "direct_store.get_opts() was called during test")]
        #[tokio::test]
        async fn test_use_fallback_when_missing_data() {
            let direct_to_store = Arc::new(MockDirectStore::default());

            let casted_object_store = Arc::clone(&direct_to_store) as Arc<dyn ObjectStore>;
            let (object_store, cache_server) = build_cache_server_client(casted_object_store).await;

            let path = Path::from("my/scoped/data/file.parquet");
            setup_metadata_head(&path, &cache_server).await;
            assert!(
                object_store.head(&path).await.is_ok(),
                "should have functioning metadata/head request"
            );

            // TEST: object never provided to mock
            let _ = object_store.get(&path).await;
        }

        #[should_panic(expected = "direct_store.get_opts() was called during test")]
        #[tokio::test]
        async fn test_use_fallback_when_bad_data() {
            let direct_to_store = Arc::new(MockDirectStore::default());

            let casted_object_store = Arc::clone(&direct_to_store) as Arc<dyn ObjectStore>;
            let (object_store, cache_server) = build_cache_server_client(casted_object_store).await;

            let path = Path::from("my/scoped/data/file.parquet");
            setup_metadata_head(&path, &cache_server).await;
            assert!(
                object_store.head(&path).await.is_ok(),
                "should have functioning metadata/head request"
            );

            // TEST: incorrect metadata provided to mock
            let route = format!("/object?location={}", &path.to_string());
            cache_server.respond_with(
                route.clone(),
                ExpectedResponse {
                    bytes: vec![].into(), // BAD: should be object
                    range: None,
                },
            );
            let _ = object_store.get(&path).await;
        }

        // since server is shutdown, will fail on head() request before get_opts() request
        #[should_panic(expected = "direct_store.head() was called during test")]
        #[tokio::test]
        async fn test_use_fallback_on_connection_failed() {
            let direct_to_store = Arc::new(MockDirectStore::default());

            let casted_object_store = Arc::clone(&direct_to_store) as Arc<dyn ObjectStore>;
            let (object_store, cache_server) = build_cache_server_client(casted_object_store).await;

            let path = Path::from("my/scoped/data/file.parquet");
            setup_metadata_head(&path, &cache_server).await;
            assert!(
                object_store.head(&path).await.is_ok(),
                "should have functioning metadata/head request"
            );

            // GET /object is working
            let route = format!("/object?location={}", path);
            cache_server.respond_with(
                route.clone(),
                ExpectedResponse {
                    bytes: std::str::from_utf8(FILE).unwrap().into(),
                    range: Some(Range {
                        start: 0,
                        end: FILE.len(),
                    }),
                },
            );
            assert!(object_store.get(&path).await.is_ok());

            // kill server
            cache_server.close().await;

            // TEST: connection fails
            let _ = object_store.get(&path).await;
        }
    }
}
