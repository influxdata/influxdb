use crate::persister::serialize_to_parquet;
use crate::persister::Error;
use crate::ParquetFile;
use bytes::Bytes;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::physical_plan::SendableRecordBatchStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::task;

type MetaData = RwLock<HashMap<String, HashMap<String, HashMap<String, ParquetFile>>>>;

#[derive(Debug)]
pub struct ParquetCache {
    object_store: Arc<dyn ObjectStore>,
    meta_data: MetaData,
    mem_pool: Arc<dyn MemoryPool>,
}

impl ParquetCache {
    /// Create a new `ParquetCache`
    pub fn new(mem_pool: &Arc<dyn MemoryPool>) -> Self {
        Self {
            object_store: Arc::new(InMemory::new()),
            meta_data: RwLock::new(HashMap::new()),
            mem_pool: Arc::clone(mem_pool),
        }
    }

    /// Get the parquet file metadata for a given database and table
    pub fn get_parquet_files(&self, database_name: &str, table_name: &str) -> Vec<ParquetFile> {
        self.meta_data
            .read()
            .get(database_name)
            .and_then(|db| db.get(table_name))
            .cloned()
            .unwrap_or_default()
            .into_values()
            .collect()
    }

    /// Persist a new parquet file to the cache or pass an object store path to update a currently
    /// existing file in the cache
    // Note we want to hold across await points until everything is cleared
    // before letting other tasks access the data
    pub async fn persist_parquet_file(
        &self,
        db_name: &str,
        table_name: &str,
        min_time: i64,
        max_time: i64,
        record_batches: SendableRecordBatchStream,
        path: Option<ObjPath>,
    ) -> Result<(), Error> {
        let parquet = serialize_to_parquet(Arc::clone(&self.mem_pool), record_batches).await?;
        // Generate a path for this
        let id = uuid::Uuid::new_v4();
        let parquet_path =
            path.unwrap_or_else(|| ObjPath::from(format!("{db_name}-{table_name}-{id}")));
        let size_bytes = parquet.bytes.len() as u64;
        let meta_data = parquet.meta_data;

        // Lock the data structure until everything is written into the object
        // store and metadata. We block on writing to the ObjectStore so that we
        // don't yield the thread while maintaining the lock
        let mut meta_data_lock = self.meta_data.write();
        let path = parquet_path.to_string();
        task::block_in_place(move || -> Result<_, Error> {
            Handle::current()
                .block_on(self.object_store.put(&parquet_path, parquet.bytes.into()))
                .map_err(Into::into)
        })?;

        meta_data_lock
            .entry(db_name.into())
            .and_modify(|db| {
                db.entry(table_name.into())
                    .and_modify(|files| {
                        files.insert(
                            path.clone(),
                            ParquetFile {
                                path: path.clone(),
                                size_bytes,
                                row_count: meta_data.num_rows as u64,
                                min_time,
                                max_time,
                            },
                        );
                    })
                    .or_insert_with(|| {
                        HashMap::from([(
                            path.clone(),
                            ParquetFile {
                                path: path.clone(),
                                size_bytes,
                                row_count: meta_data.num_rows as u64,
                                min_time,
                                max_time,
                            },
                        )])
                    });
            })
            .or_insert_with(|| {
                HashMap::from([(
                    table_name.into(),
                    HashMap::from([(
                        path.clone(),
                        ParquetFile {
                            path: path.clone(),
                            size_bytes,
                            row_count: meta_data.num_rows as u64,
                            min_time,
                            max_time,
                        },
                    )]),
                )])
            });

        Ok(())
    }

    /// Load the file from the cache
    pub async fn load_parquet_file(&self, path: ObjPath) -> Result<Bytes, Error> {
        Ok(self.object_store.get(&path).await?.bytes().await?)
    }

    /// Remove the file from the cache
    pub async fn remove_parquet_file(&self, path: ObjPath) -> Result<(), Error> {
        let closure_path = path.clone();
        let mut split = path.as_ref().split('-');
        let db = split
            .next()
            .expect("cache keys are in the form db-table-uuid");
        let table = split
            .next()
            .expect("cache keys are in the form db-table-uuid");

        // Lock the cache until this function is completed. We block on the
        // delete so that we don't hold the lock across await points
        let mut meta_data_lock = self.meta_data.write();
        task::block_in_place(move || -> Result<_, Error> {
            Handle::current()
                .block_on(self.object_store.delete(&closure_path))
                .map_err(Into::into)
        })?;
        meta_data_lock
            .get_mut(db)
            .and_then(|tables| tables.get_mut(table))
            .expect("the file exists in the meta_data table as well")
            .remove(path.as_ref());

        Ok(())
    }

    /// Purge the whole cache
    pub async fn purge_cache(&self) -> Result<(), Error> {
        // Lock the metadata table and thus all writing to the cache
        let mut meta_data_lock = self.meta_data.write();
        // Remove every object from the object store
        for db in meta_data_lock.values() {
            for table in db.values() {
                for file in table.values() {
                    // Block on deletes so that we don't hold the lock across
                    // the await point
                    task::block_in_place(move || -> Result<_, Error> {
                        Handle::current()
                            .block_on(self.object_store.delete(&file.path.as_str().into()))
                            .map_err(Into::into)
                    })?;
                }
            }
        }

        // Reset the metadata table back to a new state
        *meta_data_lock = HashMap::new();

        Ok(())
    }

    // Get a reference to the ObjectStore backing the cache
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.object_store)
    }
}

#[cfg(test)]
mod tests {
    use super::Error;
    use super::ParquetCache;
    use arrow::array::TimestampNanosecondArray;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::datatypes::TimeUnit;
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::memory_pool::MemoryPool;
    use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn cache_persist() -> Result<(), Error> {
        let cache = make_cache();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        let stream_builder = RecordBatchReceiverStreamBuilder::new(schema.clone(), 5);

        let time_array = TimestampNanosecondArray::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(time_array)]).unwrap();

        stream_builder.tx().send(Ok(batch)).await.unwrap();

        let stream = stream_builder.build();

        cache
            .persist_parquet_file("test_db", "test_table", 1, 5, stream, None)
            .await?;

        let tables = cache.get_parquet_files("test_db", "test_table");
        assert_eq!(tables.len(), 1);

        let _bytes = cache
            .load_parquet_file(tables[0].path.as_str().into())
            .await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn cache_update() -> Result<(), Error> {
        let cache = make_cache();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        let stream_builder = RecordBatchReceiverStreamBuilder::new(schema.clone(), 5);

        let time_array = TimestampNanosecondArray::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(time_array)]).unwrap();

        stream_builder.tx().send(Ok(batch)).await.unwrap();

        let stream = stream_builder.build();

        cache
            .persist_parquet_file("test_db", "test_table", 1, 5, stream, None)
            .await?;

        let tables = cache.get_parquet_files("test_db", "test_table");
        assert_eq!(tables.len(), 1);

        let path: object_store::path::Path = tables[0].path.as_str().into();
        let orig_bytes = cache.load_parquet_file(path.clone()).await?;

        let stream_builder = RecordBatchReceiverStreamBuilder::new(schema.clone(), 5);

        let time_array = TimestampNanosecondArray::from(vec![6, 7, 8, 9, 10]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(time_array)]).unwrap();

        stream_builder.tx().send(Ok(batch)).await.unwrap();

        let stream = stream_builder.build();

        cache
            .persist_parquet_file("test_db", "test_table", 6, 10, stream, Some(path.clone()))
            .await?;

        let new_bytes = cache.load_parquet_file(path).await?;

        assert_ne!(orig_bytes, new_bytes);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn multiple_parquet() -> Result<(), Error> {
        let cache = make_cache();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        let stream_builder = RecordBatchReceiverStreamBuilder::new(schema.clone(), 5);

        let time_array = TimestampNanosecondArray::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(time_array)]).unwrap();

        stream_builder.tx().send(Ok(batch)).await.unwrap();

        let stream = stream_builder.build();

        cache
            .persist_parquet_file("test_db", "test_table", 1, 5, stream, None)
            .await?;

        let tables = cache.get_parquet_files("test_db", "test_table");
        assert_eq!(tables.len(), 1);

        let path: object_store::path::Path = tables[0].path.as_str().into();
        let _ = cache.load_parquet_file(path.clone()).await?;

        let stream_builder = RecordBatchReceiverStreamBuilder::new(schema.clone(), 5);

        let time_array = TimestampNanosecondArray::from(vec![6, 7, 8, 9, 10]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(time_array)]).unwrap();

        stream_builder.tx().send(Ok(batch)).await.unwrap();

        let stream = stream_builder.build();

        cache
            .persist_parquet_file("test_db", "test_table", 6, 10, stream, None)
            .await?;

        let tables = cache.get_parquet_files("test_db", "test_table");
        assert_eq!(tables.len(), 2);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn purge_cache() -> Result<(), Error> {
        let cache = make_cache();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));

        let time_array = TimestampNanosecondArray::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(time_array)]).unwrap();

        let stream_builder = RecordBatchReceiverStreamBuilder::new(schema.clone(), 5);
        stream_builder.tx().send(Ok(batch.clone())).await.unwrap();
        let stream = stream_builder.build();
        cache
            .persist_parquet_file("test_db", "test_table", 1, 5, stream, None)
            .await?;
        let tables = cache.get_parquet_files("test_db", "test_table");
        assert_eq!(tables.len(), 1);

        let stream_builder = RecordBatchReceiverStreamBuilder::new(schema.clone(), 5);
        stream_builder.tx().send(Ok(batch.clone())).await.unwrap();
        let stream = stream_builder.build();
        cache
            .persist_parquet_file("test_db_2", "test_table", 1, 5, stream, None)
            .await?;
        let tables = cache.get_parquet_files("test_db_2", "test_table");
        assert_eq!(tables.len(), 1);

        let stream_builder = RecordBatchReceiverStreamBuilder::new(schema.clone(), 5);
        stream_builder.tx().send(Ok(batch.clone())).await.unwrap();
        let stream = stream_builder.build();
        cache
            .persist_parquet_file("test_db_3", "test_table", 1, 5, stream, None)
            .await?;
        let tables = cache.get_parquet_files("test_db_3", "test_table");
        assert_eq!(tables.len(), 1);

        let size = cache.object_store.list(None).size_hint().0;
        assert_eq!(size, 3);

        cache.purge_cache().await?;
        let size = cache.object_store.list(None).size_hint().0;
        assert_eq!(size, 0);
        assert_eq!(cache.meta_data.read().len(), 0);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn cache_remove_parquet() -> Result<(), Error> {
        let cache = make_cache();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        let stream_builder = RecordBatchReceiverStreamBuilder::new(schema.clone(), 5);

        let time_array = TimestampNanosecondArray::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(time_array)]).unwrap();

        stream_builder.tx().send(Ok(batch)).await.unwrap();

        let stream = stream_builder.build();

        cache
            .persist_parquet_file("test_db", "test_table", 1, 5, stream, None)
            .await?;

        let tables = cache.get_parquet_files("test_db", "test_table");
        assert_eq!(tables.len(), 1);

        let path = object_store::path::Path::from(tables[0].path.as_str());
        let _bytes = cache.load_parquet_file(path.clone()).await?;

        cache.remove_parquet_file(path.clone()).await?;
        let tables = cache.get_parquet_files("test_db", "test_table");
        assert_eq!(tables.len(), 0);
        assert!(cache.load_parquet_file(path.clone()).await.is_err());

        Ok(())
    }

    fn make_cache() -> ParquetCache {
        let mem_pool: Arc<dyn MemoryPool> =
            Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        ParquetCache::new(&mem_pool)
    }
}
