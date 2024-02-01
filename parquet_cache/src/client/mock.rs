use std::collections::HashSet;
use std::{ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use data_types::ParquetFileParams;
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
    PutOptions, PutResult, Result,
};
use parking_lot::Mutex;
use tokio::io::AsyncWrite;

use crate::{
    data_types::WriteHintAck, DataCacheObjectStore, MockCacheServer, WriteHintingObjectStore,
};

use super::cache_connector::build_cache_connector;

/// Build a cache client,
/// with a mocked server and mocked direct-to-store fallback.
pub async fn build_cache_server_client(
    direct_to_store: Arc<dyn ObjectStore>,
) -> (DataCacheObjectStore, MockCacheServer) {
    // build server and client
    let dst = "localhost:0";
    let cache_server = MockCacheServer::create(dst, Arc::clone(&direct_to_store)).await;
    let cache_client = build_cache_connector(cache_server.addr());

    // build object_store
    let object_store = DataCacheObjectStore::new(cache_client, direct_to_store);

    (object_store, cache_server)
}

/// A mocked direct-to-object-store, with the following characteristics:
///   * panics when used as fallback (for GET requests)
///   * tracks when called for PUT requests
#[derive(Debug, Default)]
pub struct MockDirectStore {
    called: Mutex<HashSet<String>>,
}

impl MockDirectStore {
    pub fn was_called(&self, fn_name: &str) -> bool {
        self.called.lock().contains(&String::from(fn_name))
    }
}

#[async_trait]
impl ObjectStore for MockDirectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _bytes: Bytes,
        _opts: PutOptions,
    ) -> Result<PutResult> {
        self.called.lock().insert(String::from("put"));
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.called.lock().insert(String::from("put_multipart"));
        Ok((
            String::from("AsyncWriter for MockDirectStore"),
            Box::new(tokio::io::BufWriter::new(vec![])),
        ))
    }

    async fn abort_multipart(&self, _location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        self.called.lock().insert(String::from("abort_multipart"));
        Ok(())
    }

    async fn get(&self, _location: &Path) -> Result<GetResult> {
        panic!("object was not found in test cache")
    }

    async fn get_opts(&self, _location: &Path, _options: GetOptions) -> Result<GetResult> {
        // test may intentionally test fallback behavior of get_opts()
        panic!("direct_store.get_opts() was called during test")
    }

    async fn get_range(&self, _location: &Path, _range: Range<usize>) -> Result<Bytes> {
        panic!("direct_store should not be called during test")
    }

    async fn get_ranges(&self, _location: &Path, _ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        panic!("direct_store should not be called during test")
    }

    async fn head(&self, _location: &Path) -> Result<ObjectMeta> {
        // test may intentionally test fallback behavior of get_opts()
        panic!("direct_store.head() was called during test")
    }

    async fn delete(&self, _location: &Path) -> Result<()> {
        self.called.lock().insert(String::from("delete"));
        Ok(())
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        self.called.lock().insert(String::from("list"));
        Box::pin(tokio_stream::iter(vec![]))
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        self.called
            .lock()
            .insert(String::from("list_with_delimiter"));
        Ok(ListResult {
            common_prefixes: vec![],
            objects: vec![],
        })
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        self.called.lock().insert(String::from("copy"));
        Ok(())
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        self.called
            .lock()
            .insert(String::from("copy_if_not_exists"));
        Ok(())
    }
}

#[async_trait]
impl WriteHintingObjectStore for MockDirectStore {
    async fn write_hint<'a>(
        &self,
        _location: &'a Path,
        _new_file: &'a ParquetFileParams,
        _ack_setting: WriteHintAck,
    ) -> Result<()> {
        panic!("direct_store should not be called during test");
    }
}

impl std::fmt::Display for MockDirectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DirectStore")
    }
}
