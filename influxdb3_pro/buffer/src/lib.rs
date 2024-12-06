use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::{catalog::Session, error::DataFusionError, logical_expr::Expr};
use influxdb3_cache::{
    last_cache::LastCacheProvider,
    meta_cache::{CreateMetaCacheArgs, MetaCacheProvider},
};
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
use influxdb3_id::{ColumnId, DbId, TableId};
use influxdb3_wal::{LastCacheDefinition, MetaCacheDefinition};
use influxdb3_write::{
    write_buffer::{persisted_files::PersistedFiles, Result as WriteBufferResult},
    BufferedWriteRequest, Bufferer, ChunkContainer, DatabaseManager, LastCacheManager,
    MetaCacheManager, ParquetFile, PersistedSnapshot, Precision, WriteBuffer,
};
use iox_query::QueryChunk;
use iox_time::Time;
use modes::{
    compactor::CompactorMode,
    read::{CreateReadModeArgs, ReadMode},
    read_write::{CreateReadWriteModeArgs, ReadWriteMode},
};
use tokio::sync::watch::Receiver;

pub mod modes;
pub mod replica;

#[derive(Debug)]
pub struct WriteBufferPro<Mode> {
    mode: Mode,
}

#[doc(hidden)]
#[derive(Debug, Copy, Clone)]
pub struct NoMode;

impl WriteBufferPro<NoMode> {
    pub async fn read(args: CreateReadModeArgs) -> Result<WriteBufferPro<ReadMode>, anyhow::Error> {
        let mode = ReadMode::new(args).await?;
        Ok(WriteBufferPro { mode })
    }

    pub async fn read_write(
        args: CreateReadWriteModeArgs,
    ) -> Result<WriteBufferPro<ReadWriteMode>, anyhow::Error> {
        let mode = ReadWriteMode::new(args).await?;
        Ok(WriteBufferPro { mode })
    }

    pub fn compactor() -> WriteBufferPro<CompactorMode> {
        let mode = CompactorMode::default();
        WriteBufferPro { mode }
    }
}

impl WriteBufferPro<ReadWriteMode> {
    pub fn persisted_files(&self) -> Arc<PersistedFiles> {
        self.mode.persisted_files()
    }
}

#[async_trait]
impl<Mode: Bufferer> Bufferer for WriteBufferPro<Mode> {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> WriteBufferResult<BufferedWriteRequest> {
        self.mode
            .write_lp(database, lp, ingest_time, accept_partial, precision)
            .await
    }

    async fn write_lp_v3(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> WriteBufferResult<BufferedWriteRequest> {
        self.mode
            .write_lp_v3(database, lp, ingest_time, accept_partial, precision)
            .await
    }

    fn catalog(&self) -> Arc<Catalog> {
        self.mode.catalog()
    }

    fn parquet_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFile> {
        self.mode.parquet_files(db_id, table_id)
    }

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshot>> {
        unimplemented!("watch_persisted_snapshots not implemented for WriteBufferPro")
    }
}

impl<Mode: ChunkContainer> ChunkContainer for WriteBufferPro<Mode> {
    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> influxdb3_write::Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        self.mode
            .get_table_chunks(database_name, table_name, filters, projection, ctx)
    }
}

#[async_trait::async_trait]
impl<Mode: LastCacheManager> LastCacheManager for WriteBufferPro<Mode> {
    fn last_cache_provider(&self) -> Arc<LastCacheProvider> {
        self.mode.last_cache_provider()
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_last_cache(
        &self,
        db_id: DbId,
        tbl_id: TableId,
        cache_name: Option<&str>,
        count: Option<usize>,
        ttl: Option<Duration>,
        key_columns: Option<Vec<ColumnId>>,
        value_columns: Option<Vec<ColumnId>>,
    ) -> WriteBufferResult<Option<LastCacheDefinition>> {
        self.mode
            .create_last_cache(
                db_id,
                tbl_id,
                cache_name,
                count,
                ttl,
                key_columns,
                value_columns,
            )
            .await
    }

    async fn delete_last_cache(
        &self,
        db_id: DbId,
        tbl_id: TableId,
        cache_name: &str,
    ) -> WriteBufferResult<()> {
        self.mode.delete_last_cache(db_id, tbl_id, cache_name).await
    }
}

#[async_trait::async_trait]
impl<Mode: MetaCacheManager> MetaCacheManager for WriteBufferPro<Mode> {
    fn meta_cache_provider(&self) -> Arc<MetaCacheProvider> {
        self.mode.meta_cache_provider()
    }

    async fn create_meta_cache(
        &self,
        db_schema: Arc<DatabaseSchema>,
        cache_name: Option<String>,
        args: CreateMetaCacheArgs,
    ) -> WriteBufferResult<Option<MetaCacheDefinition>> {
        self.mode
            .create_meta_cache(db_schema, cache_name, args)
            .await
    }

    async fn delete_meta_cache(
        &self,
        db_id: &DbId,
        tbl_id: &TableId,
        cache_name: &str,
    ) -> WriteBufferResult<()> {
        self.mode.delete_meta_cache(db_id, tbl_id, cache_name).await
    }
}

#[async_trait::async_trait]
impl<Mode: DatabaseManager> DatabaseManager for WriteBufferPro<Mode> {
    async fn soft_delete_database(&self, name: String) -> WriteBufferResult<()> {
        self.mode.soft_delete_database(name).await
    }

    async fn soft_delete_table(
        &self,
        db_name: String,
        table_name: String,
    ) -> WriteBufferResult<()> {
        self.mode.soft_delete_table(db_name, table_name).await
    }
}

impl<Mode: WriteBuffer> WriteBuffer for WriteBufferPro<Mode> {}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use datafusion::assert_batches_sorted_eq;
    use datafusion_util::config::register_iox_object_store;
    use influxdb3_cache::{last_cache::LastCacheProvider, meta_cache::MetaCacheProvider};
    use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
    use influxdb3_wal::{Gen1Duration, WalConfig};
    use influxdb3_write::{
        parquet_cache::test_cached_obj_store_and_oracle, persister::Persister, Bufferer,
        ChunkContainer,
    };
    use iox_query::exec::IOxSessionContext;
    use iox_time::{MockProvider, Time, TimeProvider};
    use metric::Registry;
    use object_store::{memory::InMemory, path::Path};

    use crate::{
        modes::read_write::CreateReadWriteModeArgs,
        test_helpers::{
            chunks_to_record_batches, do_writes, make_exec, verify_snapshot_count, TestWrite,
        },
        WriteBufferPro,
    };

    #[tokio::test]
    async fn read_write_mode_no_parquet_cache() {
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let non_cached_obj_store =
            Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let host_id = "picard";
        let persister = Arc::new(Persister::new(
            Arc::clone(&non_cached_obj_store) as _,
            host_id,
        ));
        let catalog = Arc::new(persister.load_or_create_catalog().await.unwrap());
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).unwrap();
        let meta_cache =
            MetaCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
                .unwrap();
        let metric_registry = Arc::new(Registry::new());
        let ctx = IOxSessionContext::with_testing();
        let rt = ctx.inner().runtime_env();
        register_iox_object_store(rt, "influxdb3", Arc::clone(&non_cached_obj_store) as _);
        // create a buffer that does not use a parquet cache:
        let buffer = WriteBufferPro::read_write(CreateReadWriteModeArgs {
            host_id: "picard".into(),
            persister,
            catalog,
            last_cache,
            meta_cache,
            time_provider,
            executor: make_exec(
                Arc::clone(&non_cached_obj_store) as _,
                Arc::clone(&metric_registry),
            ),
            wal_config: WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                // small snapshot to trigger persistence asap
                snapshot_size: 1,
            },
            metric_registry,
            replication_config: None,
            parquet_cache: None,
            compacted_data: None,
        })
        .await
        .expect("create a read_write buffer with no parquet cache");

        do_writes(
            "foo",
            &buffer,
            &[
                TestWrite {
                    time_seconds: 1,
                    lp: "bar,tag=a f1=0.1",
                },
                TestWrite {
                    time_seconds: 2,
                    lp: "bar,tag=a f1=0.2",
                },
                TestWrite {
                    time_seconds: 3,
                    lp: "bar,tag=a f1=0.3",
                },
            ],
        )
        .await;

        verify_snapshot_count(1, Arc::clone(&non_cached_obj_store) as _, host_id).await;

        let persisted_files = buffer.persisted_files();
        let (db_id, db_schema) = buffer.catalog().db_id_and_schema("foo").unwrap();
        let table_id = db_schema.table_name_to_id("bar").unwrap();
        let parquet_files = persisted_files.get_files(db_id, table_id);
        assert_eq!(1, parquet_files.len());
        let path = &parquet_files[0].path;
        let request_count =
            non_cached_obj_store.total_read_request_count(&Path::from(path.as_str()));
        // there should be no requests made for this file yet since there is no cache and no
        // queries have been made for it yet...
        assert_eq!(0, request_count);

        // do a query which will pull the files from object store:
        let chunks = buffer
            .get_table_chunks("foo", "bar", &[], None, &ctx.inner().state())
            .unwrap();
        let batches = chunks_to_record_batches(chunks, ctx.inner()).await;
        assert_batches_sorted_eq!(
            [
                "+-----+-----+---------------------+",
                "| f1  | tag | time                |",
                "+-----+-----+---------------------+",
                "| 0.1 | a   | 1970-01-01T00:00:01 |",
                "| 0.2 | a   | 1970-01-01T00:00:02 |",
                "| 0.3 | a   | 1970-01-01T00:00:03 |",
                "+-----+-----+---------------------+",
            ],
            &batches
        );

        let request_count =
            non_cached_obj_store.total_read_request_count(&Path::from(path.as_str()));
        // there will have been multiple requests made for the file now, it is multiple because
        // datafusion does not use a single GET but several GET_RANGE requests...
        assert_eq!(3, request_count);
    }

    #[tokio::test]
    async fn read_write_mode_with_parquet_cache() {
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let non_cached_obj_store =
            Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let (cached_obj_store, parquet_cache) = test_cached_obj_store_and_oracle(
            Arc::clone(&non_cached_obj_store) as _,
            Arc::clone(&time_provider),
        );
        let host_id = "picard";
        let persister = Arc::new(Persister::new(Arc::clone(&cached_obj_store) as _, host_id));
        let catalog = Arc::new(persister.load_or_create_catalog().await.unwrap());
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog)).unwrap();
        let meta_cache =
            MetaCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
                .unwrap();
        let metric_registry = Arc::new(Registry::new());
        let ctx = IOxSessionContext::with_testing();
        let rt = ctx.inner().runtime_env();
        register_iox_object_store(rt, "influxdb3", Arc::clone(&cached_obj_store) as _);
        // create a buffer that does not use a parquet cache:
        let buffer = WriteBufferPro::read_write(CreateReadWriteModeArgs {
            host_id: "picard".into(),
            persister,
            catalog,
            last_cache,
            meta_cache,
            time_provider,
            executor: make_exec(
                Arc::clone(&cached_obj_store) as _,
                Arc::clone(&metric_registry),
            ),
            wal_config: WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                // small snapshot to trigger persistence asap
                snapshot_size: 1,
            },
            metric_registry,
            replication_config: None,
            parquet_cache: Some(parquet_cache),
            compacted_data: None,
        })
        .await
        .expect("create a read_write buffer with no parquet cache");

        do_writes(
            "foo",
            &buffer,
            &[
                TestWrite {
                    time_seconds: 1,
                    lp: "bar,tag=a f1=0.1",
                },
                TestWrite {
                    time_seconds: 2,
                    lp: "bar,tag=a f1=0.2",
                },
                TestWrite {
                    time_seconds: 3,
                    lp: "bar,tag=a f1=0.3",
                },
            ],
        )
        .await;

        verify_snapshot_count(1, Arc::clone(&cached_obj_store) as _, host_id).await;

        let persisted_files = buffer.persisted_files();
        let (db_id, db_schema) = buffer.catalog().db_id_and_schema("foo").unwrap();
        let table_id = db_schema.table_name_to_id("bar").unwrap();
        let parquet_files = persisted_files.get_files(db_id, table_id);
        assert_eq!(1, parquet_files.len());
        let path = &parquet_files[0].path;
        let request_count =
            non_cached_obj_store.total_read_request_count(&Path::from(path.as_str()));
        assert_eq!(1, request_count);

        // do a query which will pull the files from object store:
        let chunks = buffer
            .get_table_chunks("foo", "bar", &[], None, &ctx.inner().state())
            .unwrap();
        let batches = chunks_to_record_batches(chunks, ctx.inner()).await;
        assert_batches_sorted_eq!(
            [
                "+-----+-----+---------------------+",
                "| f1  | tag | time                |",
                "+-----+-----+---------------------+",
                "| 0.1 | a   | 1970-01-01T00:00:01 |",
                "| 0.2 | a   | 1970-01-01T00:00:02 |",
                "| 0.3 | a   | 1970-01-01T00:00:03 |",
                "+-----+-----+---------------------+",
            ],
            &batches
        );

        let request_count =
            non_cached_obj_store.total_read_request_count(&Path::from(path.as_str()));
        assert_eq!(1, request_count);
    }
}

#[cfg(test)]
mod test_helpers {
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    use data_types::NamespaceName;
    use datafusion::{arrow::array::RecordBatch, execution::context::SessionContext};
    use influxdb3_write::{persister::Persister, Precision, WriteBuffer};
    use iox_query::{
        exec::{DedicatedExecutor, Executor, ExecutorConfig},
        QueryChunk,
    };
    use iox_time::Time;
    use metric::Registry;
    use object_store::ObjectStore;
    use parquet_file::storage::{ParquetStorage, StorageId};

    pub(crate) fn make_exec(
        object_store: Arc<dyn ObjectStore>,
        metric_registry: Arc<Registry>,
    ) -> Arc<Executor> {
        let parquet_store = ParquetStorage::new(
            Arc::clone(&object_store),
            StorageId::from("test_exec_storage"),
        );
        Arc::new(Executor::new_with_config_and_executor(
            ExecutorConfig {
                target_query_partitions: NonZeroUsize::new(1).unwrap(),
                object_stores: [&parquet_store]
                    .into_iter()
                    .map(|store| (store.id(), Arc::clone(store.object_store())))
                    .collect(),
                metric_registry,
                // Default to 1gb
                mem_pool_size: 1024 * 1024 * 1024, // 1024 (b/kb) * 1024 (kb/mb) * 1024 (mb/gb)
            },
            DedicatedExecutor::new_testing(),
        ))
    }

    pub(crate) struct TestWrite<LP> {
        pub(crate) lp: LP,
        pub(crate) time_seconds: i64,
    }

    pub(crate) async fn do_writes<LP: AsRef<str> + Send + Sync>(
        db: &'static str,
        buffer: &impl WriteBuffer,
        writes: &[TestWrite<LP>],
    ) {
        for w in writes {
            buffer
                .write_lp(
                    NamespaceName::new(db).unwrap(),
                    w.lp.as_ref(),
                    Time::from_timestamp_nanos(w.time_seconds * 1_000_000_000),
                    false,
                    Precision::Nanosecond,
                )
                .await
                .unwrap();
        }
    }

    pub(crate) async fn verify_snapshot_count(
        n: usize,
        object_store: Arc<dyn ObjectStore>,
        host_id: &str,
    ) {
        let mut checks = 0;
        let persister = Persister::new(object_store, host_id);
        loop {
            let persisted_snapshots = persister.load_snapshots(1000).await.unwrap();
            if persisted_snapshots.len() > n {
                panic!(
                    "checking for {} snapshots but found {}",
                    n,
                    persisted_snapshots.len()
                );
            } else if persisted_snapshots.len() == n && checks > 5 {
                // let enough checks happen to ensure extra snapshots aren't running ion the background
                break;
            } else {
                checks += 1;
                if checks > 10 {
                    panic!("not persisting snapshots");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    pub(crate) async fn chunks_to_record_batches(
        chunks: Vec<Arc<dyn QueryChunk>>,
        ctx: &SessionContext,
    ) -> Vec<RecordBatch> {
        let mut batches = vec![];
        for chunk in chunks {
            batches.append(&mut chunk.data().read_to_batches(chunk.schema(), ctx).await);
        }
        batches
    }
}
