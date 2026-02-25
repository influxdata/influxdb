use std::{num::NonZeroUsize, ops::Range, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{FutureExt, StreamExt, TryStreamExt, stream::BoxStream};
use linear_buffer::Slice;
use metric::U64Counter;
use object_store::{
    AttributeValue, Attributes, DynObjectStore, Error, GetOptions, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result, path::Path,
};
use object_store_metrics::cache_state::{ATTR_CACHE_STATE, CacheStateKind};
use object_store_size_hinting::{extract_size_hint, hint_size};

use crate::cache_system::{
    AsyncDrop, Cache, InUse,
    s3_fifo_cache::{S3Config, S3FifoCache},
};
use crate::{
    cache_system::{
        HasSize,
        hook::{chain::HookChain, observer::ObserverHook},
    },
    object_store_helpers::{any_options_set, dyn_error_to_object_store_error},
};

const CACHE_NAME: &str = "object_store";
const STORE_NAME: &str = "mem_cache";

#[derive(Debug)]
enum CacheValueData {
    Owned(Bytes),
    Shared(Slice),
}

impl CacheValueData {
    fn size(&self) -> usize {
        match self {
            Self::Owned(bytes) => bytes.len(),
            Self::Shared(slice) => slice.allocation_size(),
        }
    }

    fn as_bytes(&self) -> Bytes {
        match self {
            Self::Owned(bytes) => bytes.clone(),
            Self::Shared(slice) => Bytes::from_owner(slice.clone()),
        }
    }

    fn is_unique(&mut self) -> bool {
        match self {
            Self::Owned(bytes) => bytes.is_unique(),
            Self::Shared(slice) => slice.is_unique(),
        }
    }
}

#[derive(Debug)]
struct CacheValue {
    data: CacheValueData,
    meta: ObjectMeta,
}

impl CacheValue {
    async fn fetch(store: &DynObjectStore, location: &Path, size_hint: u64) -> Result<Self> {
        let mut options = hint_size(size_hint);

        let (buffer_tx, buffer_rx) = crate::buffer_channel::channel();
        options.extensions.insert(buffer_tx);

        let res = store.get_opts(location, options).await?;
        let meta = res.meta.clone();

        let data = if let Some(buffer_rx) = buffer_rx.accepted() {
            // drain stream because metric wrappers might depend on it
            let mut stream = res.into_stream();
            while stream.try_next().await?.is_some() {}

            CacheValueData::Shared(buffer_rx.await.map_err(|e| Error::Generic {
                store: STORE_NAME,
                source: Box::new(e),
            })?)
        } else {
            // HACK: `Bytes` is a view-based type and may reference and underlying larger buffer. Maybe that causes
            //        https://github.com/influxdata/influxdb_iox/issues/13765 (there it was a catalog issue, but we
            //        seem to have a similar issue with the disk cache interaction?) . So we "unshare" the buffer by
            //        round-tripping it through an owned type.
            //
            // We try to be clever by creating 1 "landing buffer" instead of using `res.bytes()` and then an
            // additional clone. See https://github.com/influxdata/influxdb_iox/issues/15078#issuecomment-3223376485
            let mut stream = res.into_stream();
            let mut buffer = Vec::with_capacity(meta.size as usize);
            while let Some(next) = stream.try_next().await? {
                buffer.extend_from_slice(&next);
            }
            CacheValueData::Owned(buffer.into())
        };

        Ok(Self { data, meta })
    }

    fn data(&self) -> Bytes {
        self.data.as_bytes()
    }
}

impl HasSize for CacheValue {
    fn size(&self) -> usize {
        let Self { data, meta } = self;
        let ObjectMeta {
            location,
            last_modified: _,
            size: _,
            e_tag,
            version,
        } = meta;

        data.size()
            + location.as_ref().len()
            + e_tag.as_ref().map(|s| s.capacity()).unwrap_or_default()
            + version.as_ref().map(|s| s.capacity()).unwrap_or_default()
    }
}

impl InUse for CacheValue {
    fn in_use(&mut self) -> bool {
        // destruct self so we don't forget new fields in the future
        let Self {
            data,
            // meta is owned
            meta: _,
        } = self;

        !data.is_unique()
    }
}

impl AsyncDrop for CacheValue {
    async fn async_drop(self) {
        drop(self);
    }
}

#[derive(Debug)]
struct HitMetrics {
    cached: U64Counter,
    miss: U64Counter,
    miss_already_loading: U64Counter,
}

impl HitMetrics {
    fn new(metrics: &metric::Registry) -> Self {
        let m = metrics.register_metric::<U64Counter>(
            "object_store_in_mem_cache_access",
            "Counts acccesses to object store in mem cache",
        );
        Self {
            cached: m.recorder(&[("status", "cached")]),
            miss: m.recorder(&[("status", "miss")]),
            miss_already_loading: m.recorder(&[("status", "miss_already_loading")]),
        }
    }
}

/// Parameters for [`MemCacheObjectStore`].
#[derive(Debug)]
pub struct MemCacheObjectStoreParams<'a> {
    /// Underlying, uncached object store.
    pub inner: Arc<DynObjectStore>,

    /// Memory limit in bytes.
    pub memory_limit: NonZeroUsize,

    /// Metric registry for metrics.
    pub metrics: &'a metric::Registry,

    /// The relative size (in percentage) of the "small" S3-FIFO queue.
    pub s3fifo_main_threshold: usize,

    /// Size of S3-FIFO ghost set in bytes.
    pub s3_fifo_ghost_memory_limit: NonZeroUsize,

    /// Maximum amount of in-flight data in bytes.
    pub inflight_bytes: usize,
}

impl MemCacheObjectStoreParams<'_> {
    /// Build store from parameters.
    pub fn build(self) -> MemCacheObjectStore {
        let Self {
            inner,
            memory_limit,
            metrics,
            s3fifo_main_threshold,
            s3_fifo_ghost_memory_limit,
            inflight_bytes,
        } = self;

        let cache = Arc::new(S3FifoCache::<_, _, ()>::new(
            S3Config {
                cache_name: CACHE_NAME,
                max_memory_size: memory_limit.get(),
                max_ghost_memory_size: s3_fifo_ghost_memory_limit.get(),
                move_to_main_threshold: s3fifo_main_threshold as f64 / 100.0,
                hook: Arc::new(HookChain::new([Arc::new(ObserverHook::new(
                    CACHE_NAME,
                    metrics,
                    Some(memory_limit.get() as u64),
                )) as _])),
                inflight_bytes,
            },
            metrics,
        ));

        MemCacheObjectStore {
            store: inner,
            hit_metrics: HitMetrics::new(metrics),
            cache,
        }
    }
}

#[derive(Debug)]
pub struct MemCacheObjectStore {
    store: Arc<DynObjectStore>,
    hit_metrics: HitMetrics,
    cache: Arc<dyn Cache<Path, Arc<CacheValue>, ()>>,
}

impl MemCacheObjectStore {
    async fn get_or_fetch(
        &self,
        location: &Path,
        size_hint: u64,
    ) -> Result<(Arc<CacheValue>, CacheStateKind)> {
        let captured_store = Arc::clone(&self.store);
        let captured_location = Arc::new(location.clone());
        let cache_state = self.cache.get_or_fetch(
            &Arc::clone(&captured_location),
            Box::new(move || {
                async move {
                    CacheValue::fetch(&captured_store, &captured_location, size_hint)
                        .await
                        .map_err(|e| Arc::new(e) as _)
                        .map(Arc::new)
                }
                .boxed()
            }),
            (),
            size_hint as _,
        );

        let state_kind = cache_state.kind();
        let res = cache_state.await_inner().await;

        match state_kind {
            CacheStateKind::WasCached => &self.hit_metrics.cached,
            CacheStateKind::NewEntry => &self.hit_metrics.miss,
            CacheStateKind::AlreadyLoading => &self.hit_metrics.miss_already_loading,
        }
        .inc(1);

        res.map(|val| (val, state_kind))
            .map_err(|e| dyn_error_to_object_store_error(e, STORE_NAME))
    }
}

impl std::fmt::Display for MemCacheObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemCache({})", self.store)
    }
}

#[async_trait]
impl ObjectStore for MemCacheObjectStore {
    async fn put(&self, _location: &Path, _bytes: PutPayload) -> Result<PutResult> {
        Err(Error::NotImplemented)
    }

    async fn put_opts(
        &self,
        _location: &Path,
        _bytes: PutPayload,
        _opts: PutOptions,
    ) -> Result<PutResult> {
        Err(Error::NotImplemented)
    }

    async fn put_multipart(&self, _location: &Path) -> Result<Box<dyn MultipartUpload>> {
        Err(Error::NotImplemented)
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(Error::NotImplemented)
    }

    async fn get(&self, _location: &Path) -> Result<GetResult> {
        Err(Error::NotImplemented)
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let (options, size_hint) = extract_size_hint(options);
        let size_hint = size_hint.ok_or_else(|| Error::Generic {
            store: STORE_NAME,
            source: "object store mem cache requires size hint".into(),
        })?;

        // be rather conservative
        if any_options_set(&options) {
            return Err(Error::NotImplemented);
        }

        let (v, state) = self.get_or_fetch(location, size_hint).await?;
        let data = v.data();
        let data_len = data.len();

        Ok(GetResult {
            payload: GetResultPayload::Stream(futures::stream::iter([Ok(data)]).boxed()),
            meta: v.meta.clone(),
            range: 0..(data_len as u64),
            attributes: Attributes::from_iter([(ATTR_CACHE_STATE, AttributeValue::from(state))]),
        })
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        Ok(self
            .get_ranges(location, &[range])
            .await?
            .into_iter()
            .next()
            .expect("requested one range"))
    }

    async fn get_ranges(&self, _location: &Path, _ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        Err(Error::NotImplemented)
    }

    async fn head(&self, _location: &Path) -> Result<ObjectMeta> {
        Err(Error::NotImplemented)
    }

    async fn delete(&self, _location: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        locations
            .and_then(|_| futures::future::err(Error::NotImplemented))
            .boxed()
    }

    fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        futures::stream::iter([Err(Error::NotImplemented)]).boxed()
    }

    fn list_with_offset(
        &self,
        _prefix: Option<&Path>,
        _offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        futures::stream::iter([Err(Error::NotImplemented)]).boxed()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        Err(Error::NotImplemented)
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }

    async fn rename_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(Error::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
    use http::Extensions;
    use linear_buffer::{LinearBuffer, LinearBufferExtend};
    use object_store::memory::InMemory;
    use object_store_mock::{MockCall, MockParam, MockStore, path};
    use tokio::sync::Barrier;

    use crate::{buffer_channel::BufferSender, gen_store_tests, object_store_cache_tests::Setup};

    use super::*;

    struct TestSetup {
        store: Arc<DynObjectStore>,
        inner: Arc<MockStore>,
    }

    impl Setup for TestSetup {
        fn new() -> futures::future::BoxFuture<'static, Self> {
            async move {
                let inner = MockStore::new();

                let store = Arc::new(
                    MemCacheObjectStoreParams {
                        inner: Arc::clone(&inner).as_store(),
                        memory_limit: NonZeroUsize::MAX,
                        s3_fifo_ghost_memory_limit: NonZeroUsize::MAX,
                        metrics: &metric::Registry::new(),
                        s3fifo_main_threshold: 25,
                        inflight_bytes: 1024 * 1024 * 1024, // 1GB
                    }
                    .build(),
                );

                Self { store, inner }
            }
            .boxed()
        }

        fn inner(&self) -> &Arc<MockStore> {
            &self.inner as _
        }

        fn outer(&self) -> &Arc<DynObjectStore> {
            &self.store
        }

        fn extensions(&self) -> Extensions {
            let mut ext = Extensions::default();
            let (tx, _rx) = crate::buffer_channel::channel();
            ext.insert(tx);
            ext
        }
    }

    gen_store_tests!(TestSetup);

    #[tokio::test]
    async fn test_cache_value_buffer_copy() {
        let location = path();
        let data = Bytes::from(b"foobar".to_vec());

        let (tx, _rx) = crate::buffer_channel::channel();
        let mut get_ops = hint_size(data.len() as _);
        get_ops.extensions.insert(tx);

        let store = MockStore::new()
            .mock_next(MockCall::GetOpts {
                params: (location.clone(), get_ops.clone().into()),
                barriers: vec![],
                res: Ok(GetResult {
                    payload: GetResultPayload::Stream(
                        futures::stream::iter([Ok(data.clone())]).boxed(),
                    ),
                    meta: meta(&location, &data),
                    range: 0..(data.len() as u64),
                    attributes: Default::default(),
                }),
            })
            .as_store();

        let mut value = CacheValue::fetch(&store, &location, data.len() as _)
            .await
            .unwrap();
        assert!(!value.in_use());

        let slice = value.data();
        assert_eq!(slice, data);
        assert_ne!(
            slice.as_ptr().expose_provenance(),
            data.as_ptr().expose_provenance(),
            "data was copied",
        );
        assert!(value.in_use());

        drop(slice);
        assert!(!value.in_use());
    }

    #[tokio::test]
    async fn test_cache_value_buffer_nocopy() {
        let location = path();
        let data = Bytes::from(b"foobar".to_vec());

        const OVERALLOCATE: usize = 10;
        let mut buffer = LinearBuffer::new(data.len() + OVERALLOCATE);
        buffer.append(&data);

        let (tx, _rx) = crate::buffer_channel::channel();
        let mut get_ops = hint_size(data.len() as _);
        get_ops.extensions.insert(tx);

        let barrier = Arc::new(Barrier::new(2));

        let store = MockStore::new().mock_next(MockCall::GetOpts {
            params: (location.clone(), get_ops.clone().into()),
            barriers: vec![Arc::clone(&barrier)],
            res: Ok(GetResult {
                payload: GetResultPayload::Stream(
                    futures::stream::iter([Ok(data.clone())]).boxed(),
                ),
                meta: meta(&location, &data),
                range: 0..(data.len() as u64),
                attributes: Default::default(),
            }),
        });
        let mut store_params = store.observed_params();
        let store = store.as_store();

        let fut_value = async {
            CacheValue::fetch(&store, &location, data.len() as _)
                .await
                .unwrap()
        };
        let fut_buffer = async {
            let param = store_params.recv().await.unwrap();
            let MockParam::GetOpts((_path, get_options)) = param else {
                unreachable!()
            };
            let tx = get_options.extensions.get::<BufferSender>().unwrap();
            let tx = tx.clone().accept();
            tx.send(buffer.slice_initialized_part(0..data.len()));
            barrier.wait().await;
        };

        let (mut value, ()) = tokio::join!(fut_value, fut_buffer);
        assert!(value.in_use());

        let buffer_ptr = buffer
            .slice_initialized_part(0..0)
            .as_ptr()
            .expose_provenance();
        drop(buffer);
        assert!(!value.in_use());

        let slice = value.data();
        assert_eq!(slice, data);
        assert_eq!(
            slice.as_ptr().expose_provenance(),
            buffer_ptr,
            "data was NOT copied",
        );
        assert!(value.in_use());

        drop(slice);
        assert!(!value.in_use());
    }

    #[tokio::test]
    async fn test_no_size_hint() {
        let inner = Arc::new(InMemory::new());
        let store = MemCacheObjectStoreParams {
            inner,
            memory_limit: NonZeroUsize::MAX,
            s3_fifo_ghost_memory_limit: NonZeroUsize::MAX,
            metrics: &metric::Registry::new(),
            s3fifo_main_threshold: 25,
            inflight_bytes: 1024 * 1024 * 1024, // 1GB
        }
        .build();

        assert_eq!(
            store
                .get_opts(&path(), Default::default())
                .await
                .unwrap_err()
                .to_string(),
            "Generic mem_cache error: object store mem cache requires size hint",
        );
    }

    fn meta(location: &Path, data: &[u8]) -> ObjectMeta {
        ObjectMeta {
            location: location.clone(),
            last_modified: Default::default(),
            size: data.len() as u64,
            e_tag: None,
            version: None,
        }
    }
}
