use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{ParquetFileMetrics, ParquetFileReaderFactory},
    },
    error::DataFusionError,
    parquet::{
        arrow::async_reader::AsyncFileReader, errors::ParquetError, file::metadata::ParquetMetaData,
    },
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{ExecutionPlan, metrics::ExecutionPlanMetricsSet},
};
use executor::spawn_io;
use futures::{
    FutureExt,
    future::{Shared, WeakShared},
    prelude::future::BoxFuture,
};
use object_store::{DynObjectStore, ObjectMeta};
use object_store_size_hinting::hint_size;

use crate::{config::IoxConfigExt, provider::PartitionedFileExt};

#[derive(Debug, Default)]
pub struct CachedParquetData;

impl PhysicalOptimizerRule for CachedParquetData {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let config_ext = config
            .extensions
            .get::<IoxConfigExt>()
            .cloned()
            .unwrap_or_default();
        if !config_ext.use_cached_parquet_loader {
            return Ok(plan);
        }

        plan.transform_up(|plan| {
            let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>() else {
                return Ok(Transformed::no(plan));
            };
            let Some(file_scan_config) = data_source_exec
                .data_source()
                .as_any()
                .downcast_ref::<FileScanConfig>()
            else {
                return Ok(Transformed::no(plan));
            };
            let Some(parquet_source) = file_scan_config
                .file_source()
                .as_any()
                .downcast_ref::<ParquetSource>()
            else {
                return Ok(Transformed::no(plan));
            };
            let mut files = file_scan_config
                .file_groups
                .iter()
                .flat_map(|g| g.iter())
                .peekable();

            if files.peek().is_none() {
                // no files
                return Ok(Transformed::no(Arc::clone(&plan)));
            }

            // find object store
            let Some(ext) = files
                .next()
                .and_then(|f| f.extensions.as_ref())
                .and_then(|ext| ext.downcast_ref::<PartitionedFileExt>())
            else {
                return Err(DataFusionError::Plan("lost PartitionFileExt".to_owned()));
            };

            let parquet_source = parquet_source
                .clone()
                .with_parquet_file_reader_factory(Arc::new(CachedParquetFileReaderFactory::new(
                    Arc::clone(&ext.object_store),
                    &config_ext,
                )));
            Ok(Transformed::yes(DataSourceExec::from_data_source(
                FileScanConfigBuilder::from(file_scan_config.clone())
                    .with_source(Arc::new(parquet_source))
                    .build(),
            )))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "cached_parquet_data"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Fallible fetch of a whole file's data.
type FetchFuture = BoxFuture<'static, Result<Bytes, Arc<dyn std::error::Error + Send + Sync>>>;

/// A [`ParquetFileReaderFactory`] that fetches file data only once per reader — and, when
/// [`IoxConfigExt::share_cached_parquet_loader_fetches`] is set, only once across a file's
/// concurrently live readers: readers for the same path (one per scan partition under
/// `repartition_file_scans`) share a single fetch, so a scan holds at most one buffer per
/// file at any moment. The sharing is weak — once every reader for a file has dropped, the
/// buffer is released and a reader created later fetches anew. With sharing off (the
/// default) every reader performs its own whole-file fetch, the historical behavior.
///
/// This does NOT support file parts / sub-ranges, we will always fetch the entire file!
///
/// Also supports the DataFusion [`ParquetFileMetrics`].
#[derive(Debug)]
struct CachedParquetFileReaderFactory {
    object_store: Arc<DynObjectStore>,
    hint_size_to_object_store: bool,
    share_fetches: bool,
    /// In-flight/live fetches by path. Weak handles: a file's buffer lives only as long as some
    /// reader for it does, so this map never pins the data. Unused when `share_fetches` is off.
    fetches:
        parking_lot::Mutex<hashbrown::HashMap<object_store::path::Path, WeakShared<FetchFuture>>>,
}

impl CachedParquetFileReaderFactory {
    /// Create new factory based on the given object store.
    pub(crate) fn new(object_store: Arc<DynObjectStore>, config: &IoxConfigExt) -> Self {
        Self {
            object_store,
            hint_size_to_object_store: config.hint_known_object_size_to_object_store,
            share_fetches: config.share_cached_parquet_loader_fetches,
            fetches: Default::default(),
        }
    }

    /// Get the fetch future for the given file: the reader's own with sharing off, otherwise
    /// shared with the file's other live readers, created if none currently holds one.
    fn fetch_file(&self, location: &object_store::path::Path, size: u64) -> Shared<FetchFuture> {
        if !self.share_fetches {
            return self.start_fetch(location, size);
        }

        let mut fetches = self.fetches.lock();
        if let Some(fut) = fetches.get(location).and_then(WeakShared::upgrade) {
            return fut;
        }

        let fut = self.start_fetch(location, size);
        fetches.retain(|_, weak| weak.upgrade().is_some());
        let weak = fut
            .downgrade()
            .expect("fetch future was just created and cannot have completed");
        fetches.insert(location.clone(), weak);
        fut
    }

    /// Build a whole-file fetch future.
    fn start_fetch(&self, location: &object_store::path::Path, size: u64) -> Shared<FetchFuture> {
        let object_store = Arc::clone(&self.object_store);
        let hint_size_to_object_store = self.hint_size_to_object_store;
        let fetch_location = location.clone();
        spawn_io(async move {
            let options = if hint_size_to_object_store {
                hint_size(size)
            } else {
                Default::default()
            };
            let res = object_store
                .get_opts(&fetch_location, options)
                .await
                .map_err(|e| Arc::new(e) as Arc<dyn std::error::Error + Send + Sync>)?;
            res.bytes().await.map_err(|e| Arc::new(e) as _)
        })
        .boxed()
        .shared()
    }
}

impl ParquetFileReaderFactory for CachedParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>, DataFusionError> {
        let file_metrics =
            ParquetFileMetrics::new(partition_index, partitioned_file.path().as_ref(), metrics);

        let data = self.fetch_file(
            &partitioned_file.object_meta.location,
            partitioned_file.object_meta.size,
        );
        let meta = Arc::new(partitioned_file.object_meta);

        let file_reader = ParquetFileReader {
            meta: Arc::clone(&meta),
            file_metrics: Some(file_metrics),
            metadata_size_hint,
            data,
        };

        Ok(Box::new(file_reader))
    }
}

/// A [`AsyncFileReader`] that fetches file data each time it is invoked (no cache).
///
/// This does NOT support file parts / sub-ranges, we will always fetch the entire file!
#[derive(Debug)]
struct ParquetFileReader {
    meta: Arc<ObjectMeta>,
    file_metrics: Option<ParquetFileMetrics>,
    metadata_size_hint: Option<usize>,
    data: Shared<FetchFuture>,
}

impl ParquetFileReader {
    /// Creates a new [`ParquetFileReader`] for loading metadata.
    ///
    /// This is a "partial" clone, but omits `file_metrics` because Datafusion excludes metadata
    /// loads from the "bytes scanned" metrics
    #[inline]
    fn clone_with_no_metrics(&self) -> Self {
        Self {
            meta: Arc::clone(&self.meta),
            file_metrics: None,
            metadata_size_hint: self.metadata_size_hint,
            data: self.data.clone(),
        }
    }
    /// Loads [`ParquetMetaData`] from file.
    #[inline]
    async fn load_metadata(&mut self) -> Result<ParquetMetaData, ParquetError> {
        let prefetch = self.metadata_size_hint;
        let file_size = self.meta.size;
        ParquetMetaDataReader::new()
            .with_prefetch_hint(prefetch)
            .with_page_index_policy(PageIndexPolicy::Required)
            .load_and_finish(self, file_size)
            .await
    }
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes, ParquetError>> {
        Box::pin(async move {
            Ok(self
                .get_byte_ranges(vec![range])
                .await?
                .into_iter()
                .next()
                .expect("requested one range"))
        })
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>, ParquetError>> {
        Box::pin(async move {
            let data = self
                .data
                .clone()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;

            ranges
                .into_iter()
                .map(|range| {
                    if range.end > data.len() as u64 {
                        return Err(ParquetError::IndexOutOfBound(
                            range.end as usize,
                            data.len(),
                        ));
                    }
                    if range.start > range.end {
                        return Err(ParquetError::IndexOutOfBound(
                            range.start as usize,
                            range.end as usize,
                        ));
                    }
                    if let Some(file_metrics) = &self.file_metrics {
                        file_metrics
                            .bytes_scanned
                            .add((range.end - range.start) as usize);
                    }
                    Ok(data.slice((range.start as usize)..(range.end as usize)))
                })
                .collect()
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>, ParquetError>> {
        Box::pin(async move {
            Ok(Arc::new(
                self.clone_with_no_metrics().load_metadata().await?,
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use async_trait::async_trait;
    use datafusion::{
        datasource::{object_store::ObjectStoreUrl, physical_plan::FileGroup},
        parquet::arrow::ArrowWriter,
        physical_plan::SendableRecordBatchStream,
        prelude::SessionContext,
    };
    use executor::register_current_runtime_for_io;
    use futures::{StreamExt, stream::BoxStream};
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectStore, PutMultipartOptions,
        PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
    };

    use super::*;

    /// Delegating store that counts `get_opts` calls, i.e. whole-file fetches issued by
    /// [`ParquetFileReader`].
    #[derive(Debug)]
    struct GetCountingStore {
        inner: Arc<DynObjectStore>,
        gets: AtomicUsize,
    }

    impl GetCountingStore {
        fn new() -> Self {
            Self {
                inner: Arc::new(InMemory::new()),
                gets: AtomicUsize::new(0),
            }
        }

        fn get_count(&self) -> usize {
            self.gets.load(Ordering::SeqCst)
        }
    }

    impl std::fmt::Display for GetCountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "GetCountingStore({})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for GetCountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            bytes: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, bytes, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    fn parquet_bytes() -> Bytes {
        let batch = RecordBatch::try_from_iter([(
            "x",
            Arc::new(Int64Array::from(vec![1i64, 2, 3])) as ArrayRef,
        )])
        .unwrap();
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        Bytes::from(buf)
    }

    async fn store_with_file(path: &Path) -> (Arc<GetCountingStore>, Bytes) {
        let store = Arc::new(GetCountingStore::new());
        let data = parquet_bytes();
        store
            .put(path, PutPayload::from_bytes(data.clone()))
            .await
            .unwrap();
        (store, data)
    }

    fn partitioned_file(path: &Path, size: usize) -> PartitionedFile {
        PartitionedFile::new(path.to_string(), size as u64)
    }

    fn sharing_enabled() -> IoxConfigExt {
        IoxConfigExt {
            share_cached_parquet_loader_fetches: true,
            hint_known_object_size_to_object_store: false,
            ..Default::default()
        }
    }

    /// The default configuration keeps the historical per-reader fetch — sharing is opt-in, so
    /// downstream consumers of this crate (IOx pins it) see no behavior change until they set
    /// [`IoxConfigExt::share_cached_parquet_loader_fetches`].
    #[tokio::test]
    async fn test_fetch_per_reader_when_sharing_disabled() {
        register_current_runtime_for_io();
        let path = Path::from("foo.parquet");
        let (store, data) = store_with_file(&path).await;

        let factory = CachedParquetFileReaderFactory::new(
            Arc::clone(&store) as Arc<DynObjectStore>,
            &IoxConfigExt::default(),
        );
        let metrics = ExecutionPlanMetricsSet::new();
        let mut readers = (0..4)
            .map(|i| {
                factory
                    .create_reader(i, partitioned_file(&path, data.len()), None, &metrics)
                    .unwrap()
            })
            .collect::<Vec<_>>();

        for reader in &mut readers {
            let bytes = reader.get_bytes(0..data.len() as u64).await.unwrap();
            assert_eq!(bytes, data);
        }

        assert_eq!(store.get_count(), 4);
    }

    /// `repartition_file_scans` splits one file across `target_partitions` scan partitions, and
    /// each partition creates its own reader for the same file. The file must only be fetched
    /// (and buffered) once per scan, not once per partition.
    #[tokio::test]
    async fn test_file_fetched_once_across_partition_readers() {
        register_current_runtime_for_io();
        let path = Path::from("foo.parquet");
        let (store, data) = store_with_file(&path).await;

        let factory = CachedParquetFileReaderFactory::new(
            Arc::clone(&store) as Arc<DynObjectStore>,
            &sharing_enabled(),
        );
        let metrics = ExecutionPlanMetricsSet::new();
        let mut readers = (0..4)
            .map(|i| {
                factory
                    .create_reader(i, partitioned_file(&path, data.len()), None, &metrics)
                    .unwrap()
            })
            .collect::<Vec<_>>();

        for reader in &mut readers {
            reader.get_metadata(None).await.unwrap();
            let bytes = reader.get_bytes(0..data.len() as u64).await.unwrap();
            assert_eq!(bytes, data);
        }

        assert_eq!(store.get_count(), 1);
    }

    /// End-to-end through DataFusion's parquet opener: a file range-split into two scan
    /// partitions (as `repartition_file_scans` splits it) is fetched once when the partitions
    /// execute concurrently.
    #[tokio::test]
    async fn test_range_split_scan_fetches_once() {
        register_current_runtime_for_io();
        let path = Path::from("foo.parquet");
        let (store, data) = store_with_file(&path).await;

        let schema =
            RecordBatch::try_from_iter([("x", Arc::new(Int64Array::from(vec![1i64])) as ArrayRef)])
                .unwrap()
                .schema();
        let parquet_source = ParquetSource::new(datafusion_util::config::table_parquet_options())
            .with_parquet_file_reader_factory(Arc::new(CachedParquetFileReaderFactory::new(
                Arc::clone(&store) as Arc<DynObjectStore>,
                &sharing_enabled(),
            )));
        let mid = data.len() as i64 / 2;
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            schema,
            Arc::new(parquet_source),
        )
        .with_file_groups(vec![
            FileGroup::new(vec![partitioned_file(&path, data.len()).with_range(0, mid)]),
            FileGroup::new(vec![
                partitioned_file(&path, data.len()).with_range(mid, data.len() as i64),
            ]),
        ])
        .build();
        let exec = DataSourceExec::from_data_source(config);

        let ctx = SessionContext::new();
        ctx.register_object_store(
            ObjectStoreUrl::parse("test://").unwrap().as_ref(),
            Arc::clone(&store) as Arc<DynObjectStore>,
        );
        let task_ctx = ctx.task_ctx();
        let (rows_a, rows_b) = futures::join!(
            collect_rows(exec.execute(0, Arc::clone(&task_ctx)).unwrap()),
            collect_rows(exec.execute(1, Arc::clone(&task_ctx)).unwrap()),
        );
        assert_eq!(rows_a + rows_b, 3);
        assert_eq!(store.get_count(), 1);
    }

    async fn collect_rows(mut stream: SendableRecordBatchStream) -> usize {
        let mut rows = 0;
        while let Some(batch) = stream.next().await {
            rows += batch.unwrap().num_rows();
        }
        rows
    }

    /// Sharing must not pin file data for the lifetime of the factory: once all readers for a
    /// file are dropped, its buffer is released and a later reader fetches again.
    #[tokio::test]
    async fn test_fetch_released_when_readers_drop() {
        register_current_runtime_for_io();
        let path = Path::from("foo.parquet");
        let (store, data) = store_with_file(&path).await;

        let factory = CachedParquetFileReaderFactory::new(
            Arc::clone(&store) as Arc<DynObjectStore>,
            &sharing_enabled(),
        );
        let metrics = ExecutionPlanMetricsSet::new();
        for _ in 0..2 {
            let mut reader = factory
                .create_reader(0, partitioned_file(&path, data.len()), None, &metrics)
                .unwrap();
            reader.get_bytes(0..data.len() as u64).await.unwrap();
            drop(reader);
        }

        assert_eq!(store.get_count(), 2);
    }

    /// Distinct files must not share a fetch.
    #[tokio::test]
    async fn test_distinct_files_fetched_separately() {
        register_current_runtime_for_io();
        let path_a = Path::from("a.parquet");
        let (store, data) = store_with_file(&path_a).await;
        let path_b = Path::from("b.parquet");
        store
            .put(&path_b, PutPayload::from_bytes(data.clone()))
            .await
            .unwrap();

        let factory = CachedParquetFileReaderFactory::new(
            Arc::clone(&store) as Arc<DynObjectStore>,
            &sharing_enabled(),
        );
        let metrics = ExecutionPlanMetricsSet::new();
        // Both readers alive at once, so a factory that shared one future regardless of
        // path would coalesce them and fail the count below:
        let mut reader_a = factory
            .create_reader(0, partitioned_file(&path_a, data.len()), None, &metrics)
            .unwrap();
        let mut reader_b = factory
            .create_reader(0, partitioned_file(&path_b, data.len()), None, &metrics)
            .unwrap();
        reader_a.get_bytes(0..data.len() as u64).await.unwrap();
        reader_b.get_bytes(0..data.len() as u64).await.unwrap();

        assert_eq!(store.get_count(), 2);
    }
}
