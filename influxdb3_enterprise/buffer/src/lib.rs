use std::sync::Arc;

use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::{catalog::Session, error::DataFusionError};
use influxdb3_cache::{distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider};
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::Wal;
use influxdb3_write::{
    BufferedWriteRequest, Bufferer, ChunkContainer, ChunkFilter, DistinctCacheManager,
    LastCacheManager, ParquetFile, PersistedSnapshotVersion, Precision, WriteBuffer,
    write_buffer::{Result as WriteBufferResult, WriteBufferImpl, persisted_files::PersistedFiles},
};
use iox_query::QueryChunk;
use iox_time::Time;
use modes::{
    combined::{CreateIngestQueryModeArgs, IngestQueryMode},
    compactor::CompactorMode,
};
use tokio::sync::watch::Receiver;

pub mod modes;
pub mod replica;

#[derive(Debug)]
pub struct WriteBufferEnterprise<Mode> {
    mode: Mode,
}

#[doc(hidden)]
#[derive(Debug, Copy, Clone)]
pub struct NoMode;

impl WriteBufferEnterprise<NoMode> {
    pub fn compactor(catalog: Arc<Catalog>) -> WriteBufferEnterprise<CompactorMode> {
        let mode = CompactorMode::new(catalog);
        WriteBufferEnterprise { mode }
    }

    pub async fn combined_ingest_query(
        args: CreateIngestQueryModeArgs,
    ) -> Result<WriteBufferEnterprise<IngestQueryMode>, anyhow::Error> {
        let mode = IngestQueryMode::new(args).await?;
        Ok(WriteBufferEnterprise { mode })
    }
}

impl WriteBufferEnterprise<IngestQueryMode> {
    pub fn persisted_files(&self) -> Option<Arc<PersistedFiles>> {
        self.mode.persisted_files()
    }

    pub fn write_buffer_impl(&self) -> Option<Arc<WriteBufferImpl>> {
        self.mode.write_buffer_impl()
    }
}

#[async_trait]
impl<Mode: Bufferer> Bufferer for WriteBufferEnterprise<Mode> {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> WriteBufferResult<BufferedWriteRequest> {
        self.mode
            .write_lp(
                database,
                lp,
                ingest_time,
                accept_partial,
                precision,
                no_sync,
            )
            .await
    }

    fn catalog(&self) -> Arc<Catalog> {
        self.mode.catalog()
    }

    fn parquet_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        self.mode.parquet_files_filtered(db_id, table_id, filter)
    }

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshotVersion>> {
        unimplemented!("watch_persisted_snapshots not implemented for WriteBufferEnterprise")
    }

    fn wal(&self) -> Arc<dyn Wal> {
        self.mode.wal()
    }
}

impl<Mode: ChunkContainer> ChunkContainer for WriteBufferEnterprise<Mode> {
    fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filters: &ChunkFilter<'_>,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> influxdb3_write::Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        self.mode
            .get_table_chunks(db_schema, table_def, filters, projection, ctx)
    }
}

#[async_trait::async_trait]
impl<Mode: LastCacheManager> LastCacheManager for WriteBufferEnterprise<Mode> {
    fn last_cache_provider(&self) -> Arc<LastCacheProvider> {
        self.mode.last_cache_provider()
    }
}

#[async_trait::async_trait]
impl<Mode: DistinctCacheManager> DistinctCacheManager for WriteBufferEnterprise<Mode> {
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider> {
        self.mode.distinct_cache_provider()
    }
}

impl<Mode: WriteBuffer> WriteBuffer for WriteBufferEnterprise<Mode> {}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use datafusion::{assert_batches_sorted_eq, datasource::MemTable};
    use datafusion_util::config::register_iox_object_store;
    use futures::future::try_join_all;
    use influxdb3_cache::{
        distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider,
        parquet_cache::test_cached_obj_store_and_oracle,
    };
    use influxdb3_catalog::{catalog::Catalog, log::FieldDataType};
    use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
    use influxdb3_wal::{Gen1Duration, WalConfig};
    use influxdb3_write::{Bufferer, persister::Persister, test_helpers::WriteBufferTester};
    use iox_query::exec::IOxSessionContext;
    use iox_time::{MockProvider, Time, TimeProvider};
    use metric::Registry;
    use object_store::{memory::InMemory, path::Path};
    use observability_deps::tracing::debug;

    use crate::{
        WriteBufferEnterprise,
        modes::combined::{CreateIngestQueryModeArgs, IngestArgs, IngestQueryMode},
        test_helpers::{TestWrite, do_writes, make_exec, setup_read_write, verify_snapshot_count},
    };

    #[tokio::test]
    async fn read_write_mode_no_parquet_cache() {
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let non_cached_obj_store =
            Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let node_id = "picard";
        let persister = Arc::new(Persister::new(
            Arc::clone(&non_cached_obj_store) as _,
            node_id,
            Arc::clone(&time_provider),
        ));
        let catalog = Arc::new(
            Catalog::new(
                node_id,
                Arc::clone(&non_cached_obj_store) as _,
                Arc::clone(&time_provider),
            )
            .await
            .unwrap(),
        );
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let metric_registry = Arc::new(Registry::new());
        let ctx = IOxSessionContext::with_testing();
        let rt = ctx.inner().runtime_env();
        register_iox_object_store(rt, "influxdb3", Arc::clone(&non_cached_obj_store) as _);
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        // create a buffer that does not use a parquet cache:
        let buffer = WriteBufferEnterprise::combined_ingest_query(CreateIngestQueryModeArgs {
            query_args: None,
            ingest_args: Some(IngestArgs {
                node_id: "picard".into(),
                persister,
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
                snapshotted_wal_files_to_keep: 10,
            }),
            object_store: Arc::clone(&non_cached_obj_store) as _,
            catalog,
            last_cache,
            distinct_cache,
            time_provider: Arc::clone(&time_provider) as _,
            metric_registry,
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

        verify_snapshot_count(
            1,
            Arc::clone(&non_cached_obj_store) as _,
            node_id,
            Arc::clone(&time_provider) as _,
        )
        .await;

        let persisted_files = buffer
            .persisted_files()
            .expect("persisted files must exist");
        let db_schema = buffer.catalog().db_schema("foo").unwrap();
        let table_id = db_schema.table_name_to_id("bar").unwrap();
        let parquet_files = persisted_files.get_files(db_schema.id, table_id);
        assert_eq!(1, parquet_files.len());
        let path = &parquet_files[0].path;
        let request_count =
            non_cached_obj_store.total_read_request_count(&Path::from(path.as_str()));
        // there should be no requests made for this file yet since there is no cache and no
        // queries have been made for it yet...
        assert_eq!(0, request_count);

        // do a query which will pull the files from object store:
        let batches = buffer
            .get_record_batches_unchecked("foo", "bar", &ctx)
            .await;
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
            Default::default(),
        );
        let node_id = "picard";
        let persister = Arc::new(Persister::new(
            Arc::clone(&cached_obj_store) as _,
            node_id,
            Arc::clone(&time_provider),
        ));
        let catalog = Arc::new(
            Catalog::new(
                node_id,
                Arc::clone(&non_cached_obj_store) as _,
                Arc::clone(&time_provider),
            )
            .await
            .unwrap(),
        );
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let metric_registry = Arc::new(Registry::new());
        let ctx = IOxSessionContext::with_testing();
        let rt = ctx.inner().runtime_env();
        register_iox_object_store(rt, "influxdb3", Arc::clone(&cached_obj_store) as _);
        // create a buffer that does not use a parquet cache:
        let buffer = WriteBufferEnterprise::combined_ingest_query(CreateIngestQueryModeArgs {
            query_args: None,
            ingest_args: Some(IngestArgs {
                node_id: "picard".into(),
                persister,
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
                snapshotted_wal_files_to_keep: 10,
            }),
            object_store: Arc::clone(&cached_obj_store),
            catalog,
            last_cache,
            distinct_cache,
            time_provider: Arc::clone(&time_provider) as _,
            metric_registry,
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

        verify_snapshot_count(
            1,
            Arc::clone(&cached_obj_store) as _,
            node_id,
            Arc::clone(&time_provider) as _,
        )
        .await;

        let persisted_files = buffer
            .persisted_files()
            .expect("persisted_files must be available");
        let db_schema = buffer.catalog().db_schema("foo").unwrap();
        let table_id = db_schema.table_name_to_id("bar").unwrap();
        let parquet_files = persisted_files.get_files(db_schema.id, table_id);
        assert_eq!(1, parquet_files.len());
        let path = &parquet_files[0].path;
        let request_count =
            non_cached_obj_store.total_read_request_count(&Path::from(path.as_str()));
        // they're cached immediately so no requests to non_cached_obj_store
        assert_eq!(0, request_count);

        // do a query which will pull the files from object store:
        let batches = buffer
            .get_record_batches_unchecked("foo", "bar", &ctx)
            .await;
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
        // they're cached immediately so no requests to non_cached_obj_store
        assert_eq!(0, request_count);
    }

    /// Reproducer for <https://github.com/influxdata/influxdb_pro/issues/269>
    #[test_log::test(tokio::test)]
    async fn ha_configuration_simultaneous_start_with_writes() {
        // setup globals:
        let cluster_id = "test-cluster";
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let object_store = Arc::new(InMemory::new());

        // create two read_write nodes simultaneously:
        struct WorkerConfig {
            node_id: &'static str,
            read_from_node_ids: &'static [&'static str],
        }
        let mut handles = vec![];
        for WorkerConfig {
            node_id,
            read_from_node_ids,
        } in [
            WorkerConfig {
                node_id: "worker-0",
                read_from_node_ids: &["worker-1"],
            },
            WorkerConfig {
                node_id: "worker-1",
                read_from_node_ids: &["worker-0"],
            },
        ] {
            let tp = Arc::clone(&time_provider);
            let os = Arc::clone(&object_store);
            let h = tokio::spawn(setup_read_write(
                tp,
                os,
                cluster_id,
                node_id,
                read_from_node_ids.into(),
            ));
            handles.push(h);
        }
        let workers: Vec<Arc<WriteBufferEnterprise<IngestQueryMode>>> = try_join_all(handles)
            .await
            .unwrap()
            .into_iter()
            .map(Arc::new)
            .collect();

        // write to the first worker:
        do_writes(
            "test_db",
            workers[0].as_ref(),
            &[
                TestWrite {
                    lp: "cpu,worker=0 usage=99",
                    time_seconds: 1,
                },
                TestWrite {
                    lp: "cpu,worker=0 usage=88",
                    time_seconds: 2,
                },
                TestWrite {
                    lp: "cpu,worker=0 usage=77",
                    time_seconds: 3,
                },
            ],
        )
        .await;

        // allow second worker to replicate first worker:
        tokio::time::sleep(Duration::from_secs(1)).await;

        // now write to each node simultaneously:
        let mut handles = vec![];
        for (i, worker) in workers.iter().enumerate() {
            let w = Arc::clone(worker);
            let h = tokio::spawn(async move {
                do_writes(
                    "test_db",
                    w.as_ref(),
                    &[
                        TestWrite {
                            lp: format!("cpu,worker={i} usage=99"),
                            time_seconds: 4,
                        },
                        TestWrite {
                            lp: format!("cpu,worker={i} usage=88"),
                            time_seconds: 5,
                        },
                        TestWrite {
                            lp: format!("cpu,worker={i} usage=77"),
                            time_seconds: 6,
                        },
                    ],
                )
                .await
            });
            handles.push(h);
        }
        try_join_all(handles).await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let ctx = IOxSessionContext::with_testing();

        // worker 1 has writes from both hosts:
        let batches = workers[1]
            .get_record_batches_unchecked("test_db", "cpu", &ctx)
            .await;
        assert_batches_sorted_eq!(
            [
                "+---------------------+-------+--------+",
                "| time                | usage | worker |",
                "+---------------------+-------+--------+",
                "| 1970-01-01T00:00:01 | 99.0  | 0      |",
                "| 1970-01-01T00:00:02 | 88.0  | 0      |",
                "| 1970-01-01T00:00:03 | 77.0  | 0      |",
                "| 1970-01-01T00:00:04 | 99.0  | 0      |",
                "| 1970-01-01T00:00:04 | 99.0  | 1      |",
                "| 1970-01-01T00:00:05 | 88.0  | 0      |",
                "| 1970-01-01T00:00:05 | 88.0  | 1      |",
                "| 1970-01-01T00:00:06 | 77.0  | 0      |",
                "| 1970-01-01T00:00:06 | 77.0  | 1      |",
                "+---------------------+-------+--------+",
            ],
            &batches
        );

        // worker 0 also has writes from both hosts (this is done second because this fails in
        // the reproducer scenario):
        let batches = workers[0]
            .get_record_batches_unchecked("test_db", "cpu", &ctx)
            .await;
        assert_batches_sorted_eq!(
            [
                "+---------------------+-------+--------+",
                "| time                | usage | worker |",
                "+---------------------+-------+--------+",
                "| 1970-01-01T00:00:01 | 99.0  | 0      |",
                "| 1970-01-01T00:00:02 | 88.0  | 0      |",
                "| 1970-01-01T00:00:03 | 77.0  | 0      |",
                "| 1970-01-01T00:00:04 | 99.0  | 0      |",
                "| 1970-01-01T00:00:04 | 99.0  | 1      |",
                "| 1970-01-01T00:00:05 | 88.0  | 0      |",
                "| 1970-01-01T00:00:05 | 88.0  | 1      |",
                "| 1970-01-01T00:00:06 | 77.0  | 0      |",
                "| 1970-01-01T00:00:06 | 77.0  | 1      |",
                "+---------------------+-------+--------+",
            ],
            &batches
        );
    }

    #[test_log::test(tokio::test)]
    async fn write_buffer_should_not_replicate_itself() {
        // setup a single read_write host that replicates itself
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let object_store = Arc::new(InMemory::new());

        let cluster_id = "dethklok";
        let node_id = "skwisgaar";
        let write_buffer = setup_read_write(
            Arc::<MockProvider>::clone(&time_provider),
            object_store,
            cluster_id,
            node_id,
            // pass in the writer itself to be replicated (which we don't want it to do):
            vec![node_id],
        )
        .await;

        // write to the writer:
        do_writes(
            "test_db",
            &write_buffer,
            &[TestWrite {
                lp: format!("cpu,host={node_id} usage=99"),
                time_seconds: 1,
            }],
        )
        .await;

        // allow writer to replicate:
        tokio::time::sleep(Duration::from_secs(1)).await;

        // query the writer:
        let ctx = IOxSessionContext::with_testing();
        let batches = write_buffer
            .get_record_batches_unchecked("test_db", "cpu", &ctx)
            .await;
        // there should only be a single line, because the writer didn't replicate itself:
        assert_batches_sorted_eq!(
            [
                "+-----------+---------------------+-------+",
                "| host      | time                | usage |",
                "+-----------+---------------------+-------+",
                "| skwisgaar | 1970-01-01T00:00:01 | 99.0  |",
                "+-----------+---------------------+-------+",
            ],
            &batches
        );
    }

    #[test_log::test(tokio::test)]
    async fn create_db_and_table_on_separate_replicated_write_buffers() {
        // setup globals:
        let cluster_id = "nine_nine";
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let object_store = Arc::new(InMemory::new());

        // create two read_write nodes simultaneously that replicate each other:
        struct Config {
            node_id: &'static str,
            read_from_node_ids: &'static [&'static str],
        }
        let mut handles = vec![];
        for Config {
            node_id,
            read_from_node_ids,
        } in [
            Config {
                node_id: "holt",
                read_from_node_ids: &["cheddar"],
            },
            Config {
                node_id: "cheddar",
                read_from_node_ids: &["holt"],
            },
        ] {
            let tp = Arc::clone(&time_provider);
            let os = Arc::clone(&object_store);
            let h = tokio::spawn(setup_read_write(
                tp,
                os,
                cluster_id,
                node_id,
                read_from_node_ids.into(),
            ));
            handles.push(h);
        }
        let writer_buffers: Vec<Arc<WriteBufferEnterprise<IngestQueryMode>>> =
            try_join_all(handles)
                .await
                .unwrap()
                .into_iter()
                .map(Arc::new)
                .collect();

        // create the db on the first writer:
        writer_buffers[0]
            .catalog()
            .create_database("foo")
            .await
            .expect("create database foo");

        // now create the table on the second writer:
        writer_buffers[1]
            .catalog()
            .create_table(
                "foo",
                "bar",
                &["tag1"],
                &[("field1", FieldDataType::UInteger)],
            )
            .await
            .expect("create table bar on database foo");

        // now write to each node simultaneously, to the same table/db:
        let mut handles = vec![];
        for (i, write_buffer) in writer_buffers.iter().enumerate() {
            let write_buffer_cloned = Arc::clone(write_buffer);
            let handle = tokio::spawn(async move {
                do_writes(
                    "foo",
                    write_buffer_cloned.as_ref(),
                    &[
                        TestWrite {
                            lp: format!("bar,tag1={i} field1=1u"),
                            time_seconds: 1,
                        },
                        TestWrite {
                            lp: format!("bar,tag1={i} field1=2u"),
                            time_seconds: 2,
                        },
                        TestWrite {
                            lp: format!("bar,tag1={i} field1=3u"),
                            time_seconds: 3,
                        },
                    ],
                )
                .await
            });
            handles.push(handle);
        }
        try_join_all(handles).await.unwrap();

        // allow write buffers to replicate each other:
        tokio::time::sleep(Duration::from_secs(1)).await;

        // do a query to check that results are there from each write buffer (on each):
        let ctx = IOxSessionContext::with_testing();
        for write_buffer in writer_buffers {
            let batches = write_buffer
                .get_record_batches_unchecked("foo", "bar", &ctx)
                .await;
            assert_batches_sorted_eq!(
                [
                    "+--------+------+---------------------+",
                    "| field1 | tag1 | time                |",
                    "+--------+------+---------------------+",
                    "| 1      | 0    | 1970-01-01T00:00:01 |",
                    "| 1      | 1    | 1970-01-01T00:00:01 |",
                    "| 2      | 0    | 1970-01-01T00:00:02 |",
                    "| 2      | 1    | 1970-01-01T00:00:02 |",
                    "| 3      | 0    | 1970-01-01T00:00:03 |",
                    "| 3      | 1    | 1970-01-01T00:00:03 |",
                    "+--------+------+---------------------+",
                ],
                &batches
            );
        }
    }

    /// Attempted reproducer for https://github.com/influxdata/influxdb_pro/issues/390
    #[test_log::test(tokio::test)]
    async fn two_writers_deletes_do_not_propagate_endlessly() {
        // setup globals:
        let cluster_id = "nielson";
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let object_store = Arc::new(InMemory::new());

        // create two read_write nodes simultaneously that replicate each other:
        struct Config {
            writer_id: &'static str,
            read_from_writer_ids: &'static [&'static str],
        }
        let mut handles = vec![];
        for Config {
            writer_id,
            read_from_writer_ids,
        } in [
            Config {
                writer_id: "drebin",
                read_from_writer_ids: &["rumack"],
            },
            Config {
                writer_id: "rumack",
                read_from_writer_ids: &["drebin"],
            },
        ] {
            let tp = Arc::clone(&time_provider);
            let os = Arc::clone(&object_store);
            let h = tokio::spawn(setup_read_write(
                tp,
                os,
                cluster_id,
                writer_id,
                read_from_writer_ids.into(),
            ));
            handles.push(h);
        }
        let write_buffers: Vec<Arc<WriteBufferEnterprise<IngestQueryMode>>> = try_join_all(handles)
            .await
            .unwrap()
            .into_iter()
            .map(Arc::new)
            .collect();

        // set up a loop that writes to both hosts periodically
        let mut write_loop_handles = vec![];
        for (i, write_buffer) in write_buffers.iter().enumerate() {
            let writer_cloned = Arc::clone(write_buffer);
            let time_provider_cloned = Arc::clone(&time_provider);
            let handle = tokio::spawn(async move {
                let mut time_seconds = 0;
                loop {
                    do_writes(
                        "movie_ratings",
                        writer_cloned.as_ref(),
                        &[TestWrite {
                            lp: format!("movies,title=airplane,source={i} rating=5.0"),
                            time_seconds,
                        }],
                    )
                    .await;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    time_seconds += 1;
                    time_provider_cloned
                        .set(Time::from_timestamp_millis(time_seconds * 1000).unwrap());
                }
            });
            write_loop_handles.push(handle);
        }

        // wait a bit so some writes have gone through, note the wal config uses 10ms by default so
        // a few wals should have been flushed by the time this is done:
        tokio::time::sleep(Duration::from_millis(500)).await;

        // now send a delete to the database:
        write_buffers[0]
            .catalog()
            .soft_delete_database("movie_ratings")
            .await
            .unwrap();

        // now wait for some more wal flush intervals to go by:
        tokio::time::sleep(Duration::from_millis(500)).await;

        // now send a delete to the database again but this time through the other writer:
        write_buffers[1]
            .catalog()
            .soft_delete_database("movie_ratings")
            .await
            .unwrap();

        // now wait for some more wal flush intervals to go by:
        tokio::time::sleep(Duration::from_millis(500)).await;

        // now delete them in both writers at the same time, one of these will fail with NOT_FOUND:
        let mut handles = vec![];
        for write_buffer in &write_buffers {
            let write_buffer_cloned = Arc::clone(write_buffer);
            let handle = tokio::spawn(async move {
                write_buffer_cloned
                    .catalog()
                    .soft_delete_database("movie_ratings")
                    .await
            });
            handles.push(handle);
        }

        let delete_results = try_join_all(handles).await.unwrap();
        assert_eq!(delete_results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(
            delete_results
                .iter()
                .filter(|r| r
                    .as_ref()
                    .is_err_and(|e| matches!(e, influxdb3_catalog::CatalogError::NotFound)))
                .count(),
            1
        );

        // now wait for some more wal flush intervals to go by:
        tokio::time::sleep(Duration::from_millis(500)).await;

        // now check the databases on the writers:
        for write_buffer in &write_buffers {
            let dbs = write_buffer.catalog().list_db_schema();
            let n_dbs = dbs.len();
            let n_deleted_dbs = dbs.iter().filter(|db| db.deleted).count();
            debug!(n_dbs, n_deleted_dbs, "dbs in write buffer");
            assert_eq!(n_dbs, 4);
            assert_eq!(n_deleted_dbs, 3);
        }

        // kill our write loops
        for handle in &write_loop_handles {
            handle.abort();
        }

        // wait for them to fully close
        for handle in write_loop_handles {
            assert!(handle.await.unwrap_err().is_cancelled());
        }

        // check that each write buffer is still replaying writes from the other:
        for write_buffer in &write_buffers {
            let ctx = IOxSessionContext::with_testing();
            let batches = write_buffer
                .get_record_batches_unchecked("movie_ratings", "movies", &ctx)
                .await;
            ctx.inner()
                .register_table(
                    "movies",
                    Arc::new(
                        MemTable::try_new(batches.first().unwrap().schema(), vec![batches])
                            .unwrap(),
                    ),
                )
                .unwrap();
            for query_str in [
                "SELECT * FROM movies WHERE source = 0",
                "SELECT * FROM movies WHERE source = 1",
            ] {
                let batches = ctx
                    .inner()
                    .sql(query_str)
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                assert!(
                    batches.iter().any(|b| b.num_rows() > 0),
                    "there should be rows for source 1 in writer 0, due to replication"
                );
            }
        }
    }
}

#[cfg(test)]
mod test_helpers {
    use std::{num::NonZeroUsize, sync::Arc, time::Duration};

    use data_types::NamespaceName;
    use datafusion::{arrow::array::RecordBatch, execution::context::SessionContext};
    use influxdb3_cache::{distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider};
    use influxdb3_catalog::{catalog::Catalog, log::NodeMode};
    use influxdb3_wal::WalConfig;
    use influxdb3_write::{Precision, WriteBuffer, persister::Persister};
    use iox_query::{
        QueryChunk,
        exec::{DedicatedExecutor, Executor, ExecutorConfig},
    };
    use iox_time::{Time, TimeProvider};
    use metric::Registry;
    use object_store::ObjectStore;
    use parquet_file::storage::{ParquetStorage, StorageId};

    use crate::{
        WriteBufferEnterprise,
        modes::combined::{CreateIngestQueryModeArgs, IngestArgs, IngestQueryMode, QueryArgs},
        replica::ReplicationConfig,
    };

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
                    false,
                )
                .await
                .unwrap();
        }
    }

    pub(crate) async fn verify_snapshot_count(
        n: usize,
        object_store: Arc<dyn ObjectStore>,
        node_id: &str,
        time_provider: Arc<dyn TimeProvider>,
    ) {
        let mut checks = 0;
        let persister = Persister::new(object_store, node_id, time_provider);
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

    pub(crate) async fn setup_read_write(
        time_provider: Arc<dyn TimeProvider>,
        object_store: Arc<dyn ObjectStore>,
        cluster_id: &str,
        node_id: &str,
        // NOTE(trevor/catalog-refactor): cluster-wide catalog should eventually make this unnecessary
        // as nodes can be read from based on their registration in the catalog
        read_from_node_ids: Vec<&str>,
    ) -> WriteBufferEnterprise<IngestQueryMode> {
        let persister = Arc::new(Persister::new(
            Arc::clone(&object_store),
            node_id,
            Arc::clone(&time_provider),
        ));
        let catalog = Arc::new(
            Catalog::new_enterprise(
                node_id,
                cluster_id,
                Arc::clone(&object_store),
                Arc::clone(&time_provider),
            )
            .await
            .unwrap(),
        );
        for node_id in read_from_node_ids {
            catalog
                .register_node(node_id, 1, vec![NodeMode::Ingest])
                .await
                .expect("must register node");
        }
        catalog
            .register_node(node_id, 1, vec![NodeMode::Query, NodeMode::Ingest])
            .await
            .unwrap();
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog))
            .await
            .unwrap();
        let distinct_cache = DistinctCacheProvider::new_from_catalog(
            Arc::clone(&time_provider),
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let metric_registry = Arc::new(metric::Registry::new());
        let executor = make_exec(Arc::clone(&object_store), Arc::clone(&metric_registry));
        let replication_config = ReplicationConfig {
            interval: Duration::from_millis(250),
        };
        WriteBufferEnterprise::combined_ingest_query(CreateIngestQueryModeArgs {
            query_args: Some(QueryArgs { replication_config }),
            ingest_args: Some(IngestArgs {
                node_id: node_id.into(),
                persister,
                executor,
                wal_config: WalConfig::test_config(),
                snapshotted_wal_files_to_keep: 10,
            }),
            catalog,
            last_cache,
            distinct_cache,
            time_provider,
            metric_registry,
            parquet_cache: None,
            compacted_data: None,
            object_store,
        })
        .await
        .unwrap()
    }
}
