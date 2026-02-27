use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    datasource::physical_plan::{FileMeta, ParquetFileMetrics, ParquetFileReaderFactory},
    error::DataFusionError,
    parquet::{
        arrow::async_reader::AsyncFileReader, errors::ParquetError, file::metadata::ParquetMetaData,
    },
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{ExecutionPlan, metrics::ExecutionPlanMetricsSet},
};
use executor::spawn_io;
use futures::{FutureExt, future::Shared, prelude::future::BoxFuture};
use object_store::{DynObjectStore, Error as ObjectStoreError, ObjectMeta};
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
                    config_ext.hint_known_object_size_to_object_store,
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

/// A [`ParquetFileReaderFactory`] that fetches file data only onces.
///
/// This does NOT support file parts / sub-ranges, we will always fetch the entire file!
///
/// Also supports the DataFusion [`ParquetFileMetrics`].
#[derive(Debug)]
struct CachedParquetFileReaderFactory {
    object_store: Arc<DynObjectStore>,
    hint_size_to_object_store: bool,
}

impl CachedParquetFileReaderFactory {
    /// Create new factory based on the given object store.
    pub(crate) fn new(object_store: Arc<DynObjectStore>, hint_size_to_object_store: bool) -> Self {
        Self {
            object_store,
            hint_size_to_object_store,
        }
    }
}

impl ParquetFileReaderFactory for CachedParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>, DataFusionError> {
        let file_metrics =
            ParquetFileMetrics::new(partition_index, file_meta.location().as_ref(), metrics);

        let object_store = Arc::clone(&self.object_store);
        let location = file_meta.object_meta.location.clone();
        let size = file_meta.object_meta.size;
        let hint_size_to_object_store = self.hint_size_to_object_store;
        let data = spawn_io(async move {
            let options = if hint_size_to_object_store {
                hint_size(size)
            } else {
                Default::default()
            };
            let res = object_store
                .get_opts(&location, options)
                .await
                .map_err(Arc::new)?;
            res.bytes().await.map_err(Arc::new)
        })
        .boxed()
        .shared();
        let meta = Arc::new(file_meta.object_meta);

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
    data: Shared<BoxFuture<'static, Result<Bytes, Arc<ObjectStoreError>>>>,
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
