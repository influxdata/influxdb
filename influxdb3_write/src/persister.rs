//! This is the implementation of the `Persister` used to write data from the buffer to object
//! storage.

use crate::catalog::Catalog;
use crate::catalog::InnerCatalog;
use crate::paths::CatalogFilePath;
use crate::paths::ParquetFilePath;
use crate::paths::SegmentInfoFilePath;
use crate::{PersistedCatalog, PersistedSegment, Persister, SegmentId};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::execution::memory_pool::UnboundedMemoryPool;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use futures_util::stream::TryStreamExt;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::format::FileMetaData;
use parquet::format::SortingColumn;
use schema::SERIES_ID_COLUMN_NAME;
use schema::TIME_COLUMN_NAME;
use std::any::Any;
use std::io::Write;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("datafusion error: {0}")]
    DataFusion(#[from] DataFusionError),

    #[error("serde_json error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("object_store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),

    #[error("tried to serialize a parquet file with no rows")]
    NoRows,

    #[error("parse int error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct PersisterImpl {
    object_store: Arc<dyn ObjectStore>,
    mem_pool: Arc<dyn MemoryPool>,
}

impl PersisterImpl {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            mem_pool: Arc::new(UnboundedMemoryPool::default()),
        }
    }

    async fn serialize_to_parquet(
        &self,
        batches: SendableRecordBatchStream,
    ) -> Result<ParquetBytes> {
        // The ArrowWriter::write() call will return an error if any subsequent
        // batch does not match this schema, enforcing schema uniformity.
        let schema = batches.schema();

        let stream = batches;
        let mut bytes = Vec::new();
        pin_mut!(stream);

        // Construct the arrow serializer with the metadata as part of the parquet
        // file properties.
        let mut writer = TrackedMemoryArrowWriter::try_new(
            &mut bytes,
            Arc::clone(&schema),
            self.mem_pool.clone(),
        )?;

        while let Some(batch) = stream.try_next().await? {
            writer.write(batch)?;
        }

        let writer_meta = writer.close()?;
        if writer_meta.num_rows == 0 {
            return Err(Error::NoRows);
        }

        Ok(ParquetBytes {
            meta_data: writer_meta,
            bytes: Bytes::from(bytes),
        })
    }
}

#[async_trait]
impl Persister for PersisterImpl {
    type Error = Error;

    async fn load_catalog(&self) -> Result<Option<PersistedCatalog>> {
        let mut list = self.object_store.list(Some(&CatalogFilePath::dir()));
        let mut catalog_path: Option<ObjPath> = None;
        while let Some(item) = list.next().await {
            let item = item?;
            catalog_path = match catalog_path {
                Some(old_path) => {
                    let Some(new_catalog_name) = item.location.filename() else {
                        // Skip this iteration as this listed file has no
                        // filename
                        catalog_path = Some(old_path);
                        continue;
                    };
                    let old_catalog_name = old_path
                        .filename()
                        // NOTE: this holds so long as CatalogFilePath is used
                        // from crate::paths
                        .expect("catalog file paths are guaranteed to have a filename");

                    // We order catalogs by number starting with u32::MAX and
                    // then decrease it, therefore if the new catalog file name
                    // is less than the old one this is the path we want
                    if new_catalog_name < old_catalog_name {
                        Some(item.location)
                    } else {
                        Some(old_path)
                    }
                }
                None => Some(item.location),
            };
        }

        match catalog_path {
            None => Ok(None),
            Some(path) => {
                let bytes = self.object_store.get(&path).await?.bytes().await?;
                let catalog: InnerCatalog = serde_json::from_slice(&bytes)?;
                let file_name = path
                    .filename()
                    // NOTE: this holds so long as CatalogFilePath is used
                    // from crate::paths
                    .expect("catalog file paths are guaranteed to have a filename");
                let parsed_number = file_name
                    .trim_end_matches(format!(".{}", crate::paths::CATALOG_FILE_EXTENSION).as_str())
                    .parse::<u32>()?;
                let segment_id = SegmentId::new(u32::MAX - parsed_number);
                Ok(Some(PersistedCatalog {
                    segment_id,
                    catalog,
                }))
            }
        }
    }

    async fn load_segments(&self, mut most_recent_n: usize) -> Result<Vec<PersistedSegment>> {
        let mut output = Vec::new();
        let mut offset: Option<ObjPath> = None;
        while most_recent_n > 0 {
            let count = if most_recent_n > 1000 {
                most_recent_n -= 1000;
                1000
            } else {
                let count = most_recent_n;
                most_recent_n = 0;
                count
            };

            let mut segment_list = if let Some(offset) = offset {
                self.object_store
                    .list_with_offset(Some(&SegmentInfoFilePath::dir()), &offset)
            } else {
                self.object_store.list(Some(&SegmentInfoFilePath::dir()))
            };

            // Why not collect into a Result<Vec<ObjectMeta>, object_store::Error>>
            // like we could with Iterators? Well because it's a stream it ends up
            // using different traits and can't really do that. So we need to loop
            // through to return any errors that might have occurred, then do an
            // unstable sort (which is faster and we know won't have any
            // duplicates) since these can arrive out of order, and then issue gets
            // on the n most recent segments that we want and is returned in order
            // of the moste recent to least.
            let mut list = Vec::new();
            while let Some(item) = segment_list.next().await {
                list.push(item?);
            }

            list.sort_unstable_by(|a, b| a.location.cmp(&b.location));

            let len = list.len();
            let end = if len <= count { len } else { count };

            for item in &list[0..end] {
                let bytes = self.object_store.get(&item.location).await?.bytes().await?;
                output.push(serde_json::from_slice(&bytes)?);
            }

            if end == 0 {
                break;
            }

            // Get the last path in the array to use as an offset. This assumes
            // we sorted the list as we can't guarantee otherwise the order of
            // the list call to the object store.
            offset = Some(list[end - 1].location.clone());
        }

        Ok(output)
    }

    async fn load_parquet_file(&self, path: ParquetFilePath) -> Result<Bytes> {
        Ok(self.object_store.get(&path).await?.bytes().await?)
    }

    async fn persist_catalog(&self, segment_id: SegmentId, catalog: Catalog) -> Result<()> {
        let catalog_path = CatalogFilePath::new(segment_id);
        let json = serde_json::to_vec_pretty(&catalog.into_inner())?;
        self.object_store
            .put(catalog_path.as_ref(), Bytes::from(json))
            .await?;
        Ok(())
    }

    async fn persist_segment(&self, persisted_segment: &PersistedSegment) -> Result<()> {
        let segment_file_path = SegmentInfoFilePath::new(persisted_segment.segment_id);
        let json = serde_json::to_vec_pretty(persisted_segment)?;
        self.object_store
            .put(segment_file_path.as_ref(), Bytes::from(json))
            .await?;
        Ok(())
    }

    async fn persist_parquet_file(
        &self,
        path: ParquetFilePath,
        record_batch: SendableRecordBatchStream,
    ) -> Result<(u64, FileMetaData)> {
        let parquet = self.serialize_to_parquet(record_batch).await?;
        let bytes_written = parquet.bytes.len() as u64;
        self.object_store.put(path.as_ref(), parquet.bytes).await?;

        Ok((bytes_written, parquet.meta_data))
    }

    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

pub struct ParquetBytes {
    pub bytes: Bytes,
    pub meta_data: FileMetaData,
}

/// Wraps an [`ArrowWriter`] to track its buffered memory in a
/// DataFusion [`MemoryPool`]
#[derive(Debug)]
pub struct TrackedMemoryArrowWriter<W: Write + Send> {
    /// The inner ArrowWriter
    inner: ArrowWriter<W>,
    /// DataFusion memory reservation with
    reservation: MemoryReservation,
}

/// Parquet row group write size
pub const ROW_GROUP_WRITE_SIZE: usize = 1024 * 1024;

impl<W: Write + Send> TrackedMemoryArrowWriter<W> {
    /// create a new `TrackedMemoryArrowWriter<`
    pub fn try_new(sink: W, schema: SchemaRef, mem_pool: Arc<dyn MemoryPool>) -> Result<Self> {
        let series_id_idx = schema.fields.find(SERIES_ID_COLUMN_NAME);
        let time_idx = schema.fields.find(TIME_COLUMN_NAME);
        let mut builder = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_max_row_group_size(ROW_GROUP_WRITE_SIZE);
        if let (Some((s, _)), Some((t, _))) = (series_id_idx, time_idx) {
            builder = builder.set_sorting_columns(Some(vec![
                SortingColumn::new(s as i32, false, false),
                SortingColumn::new(t as i32, false, false),
            ]));
        }
        let props = builder.build();
        let inner = ArrowWriter::try_new(sink, schema, Some(props))?;
        let consumer = MemoryConsumer::new("InfluxDB3 ParquetWriter (TrackedMemoryArrowWriter)");
        let reservation = consumer.register(&mem_pool);

        Ok(Self { inner, reservation })
    }

    /// Push a `RecordBatch` into the underlying writer, updating the
    /// tracked allocation
    pub fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // writer encodes the batch into its internal buffers
        self.inner.write(&batch)?;

        // In progress memory, in bytes
        let in_progress_size = self.inner.in_progress_size();

        // update the allocation with the pool.
        self.reservation.try_resize(in_progress_size)?;

        Ok(())
    }

    /// closes the writer, flushing any remaining data and returning
    /// the written [`FileMetaData`]
    ///
    /// [`FileMetaData`]: parquet::format::FileMetaData
    pub fn close(self) -> Result<parquet::format::FileMetaData> {
        // reservation is returned on drop
        Ok(self.inner.close()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use {
        arrow::array::Int32Array, arrow::datatypes::DataType, arrow::datatypes::Field,
        arrow::datatypes::Schema, chrono::Utc,
        datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder,
        object_store::local::LocalFileSystem, std::collections::HashMap,
    };

    #[tokio::test]
    async fn persist_catalog() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let persister = PersisterImpl::new(Arc::new(local_disk));
        let catalog = Catalog::new();
        let _ = catalog.db_or_create("my_db");

        persister
            .persist_catalog(SegmentId::new(0), catalog)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn persist_and_load_newest_catalog() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let persister = PersisterImpl::new(Arc::new(local_disk));
        let catalog = Catalog::new();
        let _ = catalog.db_or_create("my_db");

        persister
            .persist_catalog(SegmentId::new(0), catalog)
            .await
            .unwrap();

        let catalog = Catalog::new();
        let _ = catalog.db_or_create("my_second_db");

        persister
            .persist_catalog(SegmentId::new(1), catalog)
            .await
            .unwrap();

        let catalog = persister
            .load_catalog()
            .await
            .expect("loading the catalog did not cause an error")
            .expect("there was a catalog to load");

        assert_eq!(catalog.segment_id, SegmentId::new(1));
        assert!(catalog.catalog.db_exists("my_second_db"));
        assert!(!catalog.catalog.db_exists("my_db"));
    }

    #[tokio::test]
    async fn persist_segment_info_file() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let persister = PersisterImpl::new(Arc::new(local_disk));
        let info_file = PersistedSegment {
            segment_id: SegmentId::new(0),
            segment_wal_size_bytes: 0,
            databases: HashMap::new(),
            segment_min_time: 0,
            segment_max_time: 1,
            segment_row_count: 0,
            segment_parquet_size_bytes: 0,
        };

        persister.persist_segment(&info_file).await.unwrap();
    }

    #[tokio::test]
    async fn persist_and_load_segment_info_files() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let persister = PersisterImpl::new(Arc::new(local_disk));
        let info_file = PersistedSegment {
            segment_id: SegmentId::new(0),
            segment_wal_size_bytes: 0,
            databases: HashMap::new(),
            segment_min_time: 0,
            segment_max_time: 1,
            segment_row_count: 0,
            segment_parquet_size_bytes: 0,
        };
        let info_file_2 = PersistedSegment {
            segment_id: SegmentId::new(1),
            segment_wal_size_bytes: 0,
            databases: HashMap::new(),
            segment_min_time: 0,
            segment_max_time: 1,
            segment_row_count: 0,
            segment_parquet_size_bytes: 0,
        };
        let info_file_3 = PersistedSegment {
            segment_id: SegmentId::new(2),
            segment_wal_size_bytes: 0,
            databases: HashMap::new(),
            segment_min_time: 0,
            segment_max_time: 1,
            segment_row_count: 0,
            segment_parquet_size_bytes: 0,
        };

        persister.persist_segment(&info_file).await.unwrap();
        persister.persist_segment(&info_file_2).await.unwrap();
        persister.persist_segment(&info_file_3).await.unwrap();

        let segments = persister.load_segments(2).await.unwrap();
        assert_eq!(segments.len(), 2);
        // The most recent one is first
        assert_eq!(segments[0].segment_id.0, 2);
        assert_eq!(segments[1].segment_id.0, 1);
    }

    #[tokio::test]
    async fn persist_and_load_segment_info_files_with_fewer_than_requested() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let persister = PersisterImpl::new(Arc::new(local_disk));
        let info_file = PersistedSegment {
            segment_id: SegmentId::new(0),
            segment_wal_size_bytes: 0,
            databases: HashMap::new(),
            segment_min_time: 0,
            segment_max_time: 1,
            segment_row_count: 0,
            segment_parquet_size_bytes: 0,
        };
        persister.persist_segment(&info_file).await.unwrap();
        let segments = persister.load_segments(2).await.unwrap();
        // We asked for the most recent 2 but there should only be 1
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].segment_id.0, 0);
    }

    #[tokio::test]
    /// This test makes sure that the logic for offset lists works
    async fn persist_and_load_over_9000_segment_info_files() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let persister = PersisterImpl::new(Arc::new(local_disk));
        for id in 0..9001 {
            let info_file = PersistedSegment {
                segment_id: SegmentId::new(id),
                segment_wal_size_bytes: 0,
                databases: HashMap::new(),
                segment_min_time: 0,
                segment_max_time: 1,
                segment_row_count: 0,
                segment_parquet_size_bytes: 0,
            };
            persister.persist_segment(&info_file).await.unwrap();
        }
        let segments = persister.load_segments(9500).await.unwrap();
        // We asked for the most recent 9500 so there should be 9001 of them
        assert_eq!(segments.len(), 9001);
        assert_eq!(segments[0].segment_id.0, 9000);
    }

    #[tokio::test]
    async fn load_segments_works_with_no_segments() {
        let store = InMemory::new();
        let persister = PersisterImpl::new(Arc::new(store));

        let segments = persister.load_segments(100).await.unwrap();
        assert!(segments.is_empty());
    }

    #[tokio::test]
    async fn get_parquet_bytes() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let persister = PersisterImpl::new(Arc::new(local_disk));

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let stream_builder = RecordBatchReceiverStreamBuilder::new(schema.clone(), 5);

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array)]).unwrap();

        let id_array = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array)]).unwrap();

        stream_builder.tx().send(Ok(batch1)).await.unwrap();
        stream_builder.tx().send(Ok(batch2)).await.unwrap();

        let parquet = persister
            .serialize_to_parquet(stream_builder.build())
            .await
            .unwrap();

        // Assert we've written all the expected rows
        assert_eq!(parquet.meta_data.num_rows, 10);
    }

    #[tokio::test]
    async fn persist_and_load_parquet_bytes() {
        let local_disk =
            LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap();
        let persister = PersisterImpl::new(Arc::new(local_disk));

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let stream_builder = RecordBatchReceiverStreamBuilder::new(schema.clone(), 5);

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array)]).unwrap();

        let id_array = Int32Array::from(vec![6, 7, 8, 9, 10]);
        let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array)]).unwrap();

        stream_builder.tx().send(Ok(batch1)).await.unwrap();
        stream_builder.tx().send(Ok(batch2)).await.unwrap();

        let path = ParquetFilePath::new("db_one", "table_one", Utc::now(), 1);
        let (bytes_written, meta) = persister
            .persist_parquet_file(path.clone(), stream_builder.build())
            .await
            .unwrap();

        // Assert we've written all the expected rows
        assert_eq!(meta.num_rows, 10);

        let bytes = persister.load_parquet_file(path).await.unwrap();

        // Assert that we have a file of bytes > 0
        assert!(!bytes.is_empty());
        assert_eq!(bytes.len() as u64, bytes_written);
    }
}
