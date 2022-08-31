//! Ingest handler

use crate::{
    data::{IngesterData, IngesterQueryResponse, ShardData},
    lifecycle::{run_lifecycle_manager, LifecycleConfig, LifecycleManager},
    poison::PoisonCabinet,
    querier_handler::prepare_data_to_querier,
    stream_handler::{
        sink_adaptor::IngestSinkAdaptor, sink_instrumentation::SinkInstrumentation,
        PeriodicWatermarkFetcher, SequencedStreamHandler,
    },
};
use async_trait::async_trait;
use backoff::BackoffConfig;
use data_types::{Shard, ShardIndex, TopicMetadata};
use futures::{
    future::{BoxFuture, Shared},
    stream::FuturesUnordered,
    FutureExt, StreamExt, TryFutureExt,
};
use generated_types::ingester::IngesterQueryRequest;
use iox_catalog::interface::Catalog;
use iox_query::exec::Executor;
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, Metric, U64Counter};
use object_store::DynObjectStore;
use observability_deps::tracing::*;
use snafu::{ResultExt, Snafu};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    sync::{Semaphore, TryAcquireError},
    task::{JoinError, JoinHandle},
};
use tokio_util::sync::CancellationToken;
use write_buffer::core::WriteBufferReading;
use write_summary::ShardProgress;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display("Write buffer error: {}", source))]
    WriteBuffer {
        source: write_buffer::core::WriteBufferError,
    },
}

/// A specialized `Error` for Catalog errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An [`IngestHandler`] handles all ingest requests from persistence and queries
#[async_trait]
pub trait IngestHandler: Send + Sync {
    /// Return results from the in-memory data that match this query
    async fn query(
        &self,
        request: IngesterQueryRequest,
    ) -> Result<IngesterQueryResponse, crate::querier_handler::Error>;

    /// Return shard progress for the requested shard indexes
    async fn progresses(
        &self,
        shard_indexes: Vec<ShardIndex>,
    ) -> BTreeMap<ShardIndex, ShardProgress>;

    /// Wait until the handler finished  to shutdown.
    ///
    /// Use [`shutdown`](Self::shutdown) to trigger a shutdown.
    async fn join(&self);

    /// Shut down background workers.
    fn shutdown(&self);
}

/// A [`JoinHandle`] that can be cloned
type SharedJoinHandle = Shared<BoxFuture<'static, Result<(), Arc<JoinError>>>>;

/// Convert a [`JoinHandle`] into a [`SharedJoinHandle`].
fn shared_handle(handle: JoinHandle<()>) -> SharedJoinHandle {
    handle.map_err(Arc::new).boxed().shared()
}

/// Implementation of the `IngestHandler` trait to ingest from shards and manage
/// persistence and answer queries
#[derive(Debug)]
pub struct IngestHandlerImpl<T = SystemProvider> {
    /// Topic assigned to this ingester
    #[allow(dead_code)]
    topic: TopicMetadata,

    /// Future that resolves when the background worker exits
    join_handles: Vec<(String, SharedJoinHandle)>,

    /// A token that is used to trigger shutdown of the background worker
    shutdown: CancellationToken,

    /// The cache and buffered data for the ingester
    data: Arc<IngesterData>,

    time_provider: T,

    /// Query execution duration distribution for successes.
    query_duration_success: DurationHistogram,

    /// Query execution duration distribution for "not found" errors
    query_duration_error_not_found: DurationHistogram,

    /// Query execution duration distribution for all other errors
    query_duration_error_other: DurationHistogram,

    /// Query request rejected due to concurrency limits
    query_request_limit_rejected: U64Counter,

    // A request limiter to restrict the number of simultaneous requests this
    // ingester services.
    //
    // This allows the ingester to drop a portion of requests when experiencing an
    // unusual flood of requests
    request_sem: Semaphore,
}

impl IngestHandlerImpl {
    /// Initialize the Ingester
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        lifecycle_config: LifecycleConfig,
        topic: TopicMetadata,
        shard_states: BTreeMap<ShardIndex, Shard>,
        catalog: Arc<dyn Catalog>,
        object_store: Arc<DynObjectStore>,
        write_buffer: Arc<dyn WriteBufferReading>,
        exec: Arc<Executor>,
        metric_registry: Arc<metric::Registry>,
        skip_to_oldest_available: bool,
        max_requests: usize,
    ) -> Result<Self> {
        // build the initial ingester data state
        let mut shards = BTreeMap::new();
        for s in shard_states.values() {
            shards.insert(
                s.id,
                ShardData::new(s.shard_index, Arc::clone(&metric_registry)),
            );
        }
        let data = Arc::new(IngesterData::new(
            object_store,
            catalog,
            shards,
            exec,
            BackoffConfig::default(),
            Arc::clone(&metric_registry),
        ));

        let ingester_data = Arc::clone(&data);
        let topic_name = topic.name.clone();

        // start the lifecycle manager
        let persister = Arc::clone(&data);
        let lifecycle_manager = LifecycleManager::new(
            lifecycle_config,
            Arc::clone(&metric_registry),
            Arc::new(SystemProvider::new()),
        );
        let lifecycle_handle = lifecycle_manager.handle();
        let shutdown = CancellationToken::new();
        let handle = tokio::task::spawn(run_lifecycle_manager(
            lifecycle_manager,
            persister,
            shutdown.clone(),
            Arc::new(PoisonCabinet::new()),
        ));
        info!(
            "ingester handler and lifecycle started with config {:?}",
            lifecycle_config
        );

        let mut join_handles = Vec::with_capacity(shard_states.len() + 1);
        join_handles.push(("lifecycle manager".to_owned(), shared_handle(handle)));

        for (shard_index, shard) in shard_states {
            let metric_registry = Arc::clone(&metric_registry);

            // Acquire a write buffer stream and seek it to the last
            // definitely-already-persisted op
            let mut op_stream = write_buffer
                .stream_handler(shard_index)
                .await
                .context(WriteBufferSnafu)?;
            info!(
                shard_index = shard_index.get(),
                min_unpersisted_sequence_number = shard.min_unpersisted_sequence_number.get(),
                "Seek stream",
            );
            op_stream
                .seek(shard.min_unpersisted_sequence_number)
                .await
                .context(WriteBufferSnafu)?;

            // Initialise the DmlSink stack.
            let watermark_fetcher = PeriodicWatermarkFetcher::new(
                Arc::clone(&write_buffer),
                shard.shard_index,
                Duration::from_secs(10),
                &*metric_registry,
            );
            // Wrap the IngesterData in a DmlSink adapter
            let sink = IngestSinkAdaptor::new(
                Arc::clone(&ingester_data),
                lifecycle_handle.clone(),
                shard.id,
            );
            // Emit metrics when ops flow through the sink
            let sink = SinkInstrumentation::new(
                sink,
                watermark_fetcher,
                topic_name.clone(),
                shard.shard_index,
                &*metric_registry,
            );

            // Spawn a task to stream in ops from the op_stream and push them
            // into the sink
            let handle = tokio::task::spawn({
                let shutdown = shutdown.child_token();
                let lifecycle_handle = lifecycle_handle.clone();
                let topic_name = topic_name.clone();
                async move {
                    let handler = SequencedStreamHandler::new(
                        op_stream,
                        shard.min_unpersisted_sequence_number,
                        sink,
                        lifecycle_handle,
                        topic_name,
                        shard.shard_index,
                        &*metric_registry,
                        skip_to_oldest_available,
                    );

                    handler.run(shutdown).await
                }
            });

            let worker_name = format!("stream handler for shard index {}", shard_index.get());
            join_handles.push((worker_name, shared_handle(handle)));
        }

        // Record query duration metrics, broken down by query execution result
        let query_duration: Metric<DurationHistogram> = metric_registry.register_metric(
            "ingester_flight_query_duration",
            "flight request query execution duration",
        );
        let query_duration_success = query_duration.recorder(&[("result", "success")]);
        let query_duration_error_not_found =
            query_duration.recorder(&[("result", "error_not_found")]);
        let query_duration_error_other = query_duration.recorder(&[("result", "error_other")]);

        let query_request_limit_rejected = metric_registry
            .register_metric::<U64Counter>(
                "query_request_limit_rejected",
                "number of query requests rejected due to exceeding parallel request limit",
            )
            .recorder(&[]);

        Ok(Self {
            data,
            topic,
            join_handles,
            shutdown,
            query_duration_success,
            query_duration_error_not_found,
            query_duration_error_other,
            query_request_limit_rejected,
            request_sem: Semaphore::new(max_requests),
            time_provider: Default::default(),
        })
    }
}

#[async_trait]
impl IngestHandler for IngestHandlerImpl {
    async fn query(
        &self,
        request: IngesterQueryRequest,
    ) -> Result<IngesterQueryResponse, crate::querier_handler::Error> {
        // TODO(4567): move this into a instrumented query delegate

        // Acquire and hold a permit for the duration of this request, or return
        // a 503 if the existing requests have already exhausted the allocation.
        //
        // Our goal is to limit the number of concurrently executing queries as a
        // rough way of ensuring we don't explode memory by trying to do too much at
        // the same time.
        let _permit = match self.request_sem.try_acquire() {
            Ok(p) => p,
            Err(TryAcquireError::NoPermits) => {
                error!("simultaneous request limit exceeded - dropping request");
                self.query_request_limit_rejected.inc(1);
                return Err(crate::querier_handler::Error::RequestLimit);
            }
            Err(e) => panic!("request limiter error: {}", e),
        };

        // TEMP(alamb): Log details about what was requested
        // temporarily so we can track down potentially "killer"
        // requests from the querier to ingester
        info!(namespace=%request.namespace,
              table=%request.table,
              columns=?request.columns,
              predicate=?request.predicate,
              "Handling querier request");

        let t = self.time_provider.now();
        let request = Arc::new(request);
        let res = prepare_data_to_querier(&self.data, &request).await;

        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.query_duration_success.record(delta),
                Err(crate::querier_handler::Error::TableNotFound { .. })
                | Err(crate::querier_handler::Error::NamespaceNotFound { .. }) => {
                    self.query_duration_error_not_found.record(delta)
                }
                Err(_) => self.query_duration_error_other.record(delta),
            };
        }

        res
    }

    async fn join(&self) {
        // Need to poll handlers unordered to detect early exists of any worker in the list.
        let mut unordered: FuturesUnordered<_> = self
            .join_handles
            .iter()
            .cloned()
            .map(|(name, handle)| async move { handle.await.map(|_| name) })
            .collect();

        while let Some(e) = unordered.next().await {
            let name = e.unwrap();

            if !self.shutdown.is_cancelled() {
                panic!("Background worker '{name}' exited early!");
            }
        }

        self.data.exec().join().await;
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
        self.data.exec().shutdown();
    }

    /// Return the ingestion progress from each shard
    async fn progresses(
        &self,
        shard_indexes: Vec<ShardIndex>,
    ) -> BTreeMap<ShardIndex, ShardProgress> {
        self.data.progresses(shard_indexes).await
    }
}

impl<T> Drop for IngestHandlerImpl<T> {
    fn drop(&mut self) {
        if !self.shutdown.is_cancelled() {
            warn!("IngestHandlerImpl dropped without calling shutdown()");
            self.shutdown.cancel();
        }

        for (worker_name, handle) in &self.join_handles {
            if handle.clone().now_or_never().is_none() {
                warn!(
                    worker_name = worker_name.as_str(),
                    "IngestHandlerImpl dropped without waiting for worker termination",
                );
            }
        }

        // `self.data.exec` implements `Drop`, so we don't need to do anything
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::SnapshotBatch;
    use data_types::{Namespace, NamespaceSchema, QueryPool, Sequence, SequenceNumber};
    use dml::{DmlMeta, DmlWrite};
    use iox_catalog::{mem::MemCatalog, validate_or_insert_schema};
    use iox_time::Time;
    use metric::{Attributes, Metric, U64Counter, U64Gauge};
    use mutable_batch_lp::lines_to_batches;
    use object_store::memory::InMemory;
    use std::{num::NonZeroU32, ops::DerefMut};
    use test_helpers::maybe_start_logging;
    use write_buffer::mock::{MockBufferForReading, MockBufferSharedState};

    #[tokio::test]
    async fn read_from_write_buffer_write_to_mutable_buffer() {
        let ingester = TestIngester::new().await;

        let schema = NamespaceSchema::new(
            ingester.namespace.id,
            ingester.topic.id,
            ingester.query_pool.id,
        );
        let mut txn = ingester.catalog.start_transaction().await.unwrap();
        let ingest_ts1 = Time::from_timestamp_millis(42);
        let ingest_ts2 = Time::from_timestamp_millis(1337);
        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(0), SequenceNumber::new(0)),
                ingest_ts1,
                None,
                50,
            ),
        );
        let schema = validate_or_insert_schema(w1.tables(), &schema, txn.deref_mut())
            .await
            .unwrap()
            .unwrap();
        ingester.write_buffer_state.push_write(w1);
        let w2 = DmlWrite::new(
            "foo",
            lines_to_batches("cpu bar=2 20\ncpu bar=3 30", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(0), SequenceNumber::new(7)),
                ingest_ts2,
                None,
                150,
            ),
        );
        let _schema = validate_or_insert_schema(w2.tables(), &schema, txn.deref_mut())
            .await
            .unwrap()
            .unwrap();
        ingester.write_buffer_state.push_write(w2);
        let w3 = DmlWrite::new(
            "foo",
            lines_to_batches("a b=2 200", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(0), SequenceNumber::new(9)),
                ingest_ts2,
                None,
                150,
            ),
        );
        let _schema = validate_or_insert_schema(w3.tables(), &schema, txn.deref_mut())
            .await
            .unwrap()
            .unwrap();
        ingester.write_buffer_state.push_write(w3);
        txn.commit().await.unwrap();

        // give the writes some time to go through the buffer. Exit once we've verified there's
        // data in there from both writes.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let mut has_measurement = false;

                if let Some(data) = ingester.ingester.data.shard(ingester.shard.id) {
                    if let Some(data) = data.namespace(&ingester.namespace.name) {
                        // verify there's data in the buffer
                        if let Some((b, _)) = data.snapshot("a", &"1970-01-01".into()).await {
                            if let Some(b) = b.first() {
                                if b.data.num_rows() > 0 {
                                    has_measurement = true;
                                }
                            }
                        }
                    }
                }

                // and ensure that the shard state was actually updated
                let shard = ingester
                    .catalog
                    .repositories()
                    .await
                    .shards()
                    .create_or_get(&ingester.topic, ingester.shard_index)
                    .await
                    .unwrap();

                if has_measurement
                    && shard.min_unpersisted_sequence_number == SequenceNumber::new(9)
                {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("timeout");

        let observation = ingester
            .metrics
            .get_instrument::<Metric<DurationHistogram>>("ingester_op_apply_duration")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("kafka_topic", "whatevs"),
                ("kafka_partition", "0"),
                ("result", "success"),
            ]))
            .unwrap()
            .fetch();
        let hits = observation.buckets.iter().map(|b| b.count).sum::<u64>();
        assert_eq!(hits, 3);

        let observation = ingester
            .metrics
            .get_instrument::<Metric<U64Counter>>("ingester_write_buffer_read_bytes")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("kafka_topic", "whatevs"),
                ("kafka_partition", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(observation, 350);

        let observation = ingester
            .metrics
            .get_instrument::<Metric<U64Gauge>>("ingester_write_buffer_last_sequence_number")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("kafka_topic", "whatevs"),
                ("kafka_partition", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(observation, 9);

        let observation = ingester
            .metrics
            .get_instrument::<Metric<U64Gauge>>("ingester_write_buffer_sequence_number_lag")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("kafka_topic", "whatevs"),
                ("kafka_partition", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(observation, 0);

        let observation = ingester
            .metrics
            .get_instrument::<Metric<U64Gauge>>("ingester_write_buffer_last_ingest_ts")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("kafka_topic", "whatevs"),
                ("kafka_partition", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(observation, ingest_ts2.timestamp_nanos() as u64);
    }

    #[tokio::test]
    async fn test_shutdown() {
        let ingester = TestIngester::new().await.ingester;

        // does not exit w/o shutdown
        tokio::select! {
            _ = ingester.join() => panic!("ingester finished w/o shutdown"),
            _ = tokio::time::sleep(Duration::from_millis(10)) => {},
        };

        ingester.shutdown();

        tokio::time::timeout(Duration::from_millis(1000), ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "Background worker 'bad_task' exited early!")]
    async fn test_join_task_early_shutdown() {
        let mut ingester = TestIngester::new().await.ingester;

        let shutdown_task = tokio::spawn(async {
            // It does nothing! and stops.
        });
        ingester
            .join_handles
            .push(("bad_task".to_string(), shared_handle(shutdown_task)));

        tokio::time::timeout(Duration::from_millis(1000), ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "JoinError::Panic")]
    async fn test_join_task_panic() {
        let mut ingester = TestIngester::new().await.ingester;

        let shutdown_task = tokio::spawn(async {
            panic!("bananas");
        });
        ingester
            .join_handles
            .push(("bad_task".to_string(), shared_handle(shutdown_task)));

        tokio::time::timeout(Duration::from_millis(1000), ingester.join())
            .await
            .unwrap();
    }

    async fn ingester_test_setup(
        write_operations: Vec<DmlWrite>,
        min_unpersisted_sequence_number: i64,
        skip_to_oldest_available: bool,
    ) -> (IngestHandlerImpl, Shard, Namespace) {
        let metrics: Arc<metric::Registry> = Default::default();
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));

        let mut txn = catalog.start_transaction().await.unwrap();
        let topic = txn.topics().create_or_get("whatevs").await.unwrap();
        let query_pool = txn.query_pools().create_or_get("whatevs").await.unwrap();
        let shard_index = ShardIndex::new(0);
        let namespace = txn
            .namespaces()
            .create("foo", "inf", topic.id, query_pool.id)
            .await
            .unwrap();
        let mut shard = txn
            .shards()
            .create_or_get(&topic, shard_index)
            .await
            .unwrap();
        // update the min unpersisted
        shard.min_unpersisted_sequence_number =
            SequenceNumber::new(min_unpersisted_sequence_number);
        // this probably isn't necessary, but just in case something changes later
        txn.shards()
            .update_min_unpersisted_sequence_number(
                shard.id,
                SequenceNumber::new(min_unpersisted_sequence_number),
            )
            .await
            .unwrap();

        let mut shard_states = BTreeMap::new();
        shard_states.insert(shard_index, shard);

        let write_buffer_state =
            MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(1).unwrap());

        let schema = NamespaceSchema::new(namespace.id, topic.id, query_pool.id);
        for write_operation in write_operations {
            validate_or_insert_schema(write_operation.tables(), &schema, txn.deref_mut())
                .await
                .unwrap()
                .unwrap();
            write_buffer_state.push_write(write_operation);
        }
        txn.commit().await.unwrap();

        let reading: Arc<dyn WriteBufferReading> =
            Arc::new(MockBufferForReading::new(write_buffer_state.clone(), None).unwrap());
        let object_store = Arc::new(InMemory::new());

        let lifecycle_config = LifecycleConfig::new(
            1000000,
            1000,
            1000,
            Duration::from_secs(10),
            Duration::from_secs(10),
            100000000,
        );
        let ingester = IngestHandlerImpl::new(
            lifecycle_config,
            topic.clone(),
            shard_states,
            Arc::clone(&catalog),
            object_store,
            reading,
            Arc::new(Executor::new(1)),
            Arc::clone(&metrics),
            skip_to_oldest_available,
            1,
        )
        .await
        .unwrap();

        (ingester, shard, namespace)
    }

    async fn verify_ingester_buffer_has_data(
        ingester: IngestHandlerImpl,
        shard: Shard,
        namespace: Namespace,
        custom_batch_verification: impl Fn(&SnapshotBatch) + Send,
    ) {
        // give the writes some time to go through the buffer. Exit once we've verified there's
        // data in there
        tokio::time::timeout(Duration::from_secs(1), async move {
            loop {
                let mut has_measurement = false;

                if let Some(data) = ingester.data.shard(shard.id) {
                    if let Some(data) = data.namespace(&namespace.name) {
                        // verify there's data in the buffer
                        if let Some((b, _)) = data.snapshot("cpu", &"1970-01-01".into()).await {
                            if let Some(b) = b.first() {
                                custom_batch_verification(b);

                                if b.data.num_rows() == 1 {
                                    has_measurement = true;
                                }
                            }
                        }
                    }
                }

                if has_measurement {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .expect("timeout");
    }

    #[tokio::test]
    async fn seeks_on_initialization() {
        let ingest_ts1 = Time::from_timestamp_millis(42);
        let ingest_ts2 = Time::from_timestamp_millis(1337);
        let write_operations = vec![
            DmlWrite::new(
                "foo",
                lines_to_batches("cpu bar=2 20", 0).unwrap(),
                Some("1970-01-01".into()),
                DmlMeta::sequenced(
                    Sequence::new(ShardIndex::new(0), SequenceNumber::new(1)),
                    ingest_ts1,
                    None,
                    150,
                ),
            ),
            DmlWrite::new(
                "foo",
                lines_to_batches("cpu bar=2 30", 0).unwrap(),
                Some("1970-01-01".into()),
                DmlMeta::sequenced(
                    Sequence::new(ShardIndex::new(0), SequenceNumber::new(2)),
                    ingest_ts2,
                    None,
                    150,
                ),
            ),
        ];

        let (ingester, shard, namespace) = ingester_test_setup(write_operations, 2, false).await;

        verify_ingester_buffer_has_data(ingester, shard, namespace, |first_batch| {
            if first_batch.min_sequence_number == SequenceNumber::new(1) {
                panic!(
                    "initialization did a seek to the beginning rather than \
                    the min_unpersisted"
                );
            }
        })
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "JoinError::Panic")]
    async fn sequence_number_no_longer_exists() {
        maybe_start_logging();

        let ingest_ts1 = Time::from_timestamp_millis(42);
        let write_operations = vec![DmlWrite::new(
            "foo",
            lines_to_batches("cpu bar=2 20", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(0), SequenceNumber::new(10)),
                ingest_ts1,
                None,
                150,
            ),
        )];
        let (ingester, _shard, _namespace) = ingester_test_setup(write_operations, 2, false).await;

        tokio::time::timeout(Duration::from_millis(1000), ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "JoinError::Panic")]
    async fn sequence_number_after_watermark() {
        maybe_start_logging();

        let ingest_ts1 = Time::from_timestamp_millis(42);
        let write_operations = vec![DmlWrite::new(
            "foo",
            lines_to_batches("cpu bar=2 20", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(0), SequenceNumber::new(2)),
                ingest_ts1,
                None,
                150,
            ),
        )];
        let (ingester, _shard, _namespace) = ingester_test_setup(write_operations, 10, false).await;

        tokio::time::timeout(Duration::from_millis(1100), ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "JoinError::Panic")]
    async fn sequence_number_after_watermark_skip_to_oldest_available() {
        maybe_start_logging();

        let ingest_ts1 = Time::from_timestamp_millis(42);
        let write_operations = vec![DmlWrite::new(
            "foo",
            lines_to_batches("cpu bar=2 20", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(0), SequenceNumber::new(2)),
                ingest_ts1,
                None,
                150,
            ),
        )];
        let (ingester, _shard, _namespace) = ingester_test_setup(write_operations, 10, true).await;

        tokio::time::timeout(Duration::from_millis(1100), ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn skip_to_oldest_available() {
        maybe_start_logging();

        let ingest_ts1 = Time::from_timestamp_millis(42);
        let write_operations = vec![DmlWrite::new(
            "foo",
            lines_to_batches("cpu bar=2 20", 0).unwrap(),
            Some("1970-01-01".into()),
            DmlMeta::sequenced(
                Sequence::new(ShardIndex::new(0), SequenceNumber::new(10)),
                ingest_ts1,
                None,
                150,
            ),
        )];

        // Set the min unpersisted to something bigger than the write's sequence number to
        // cause an UnknownSequenceNumber error. Skip to oldest available = true, so ingester
        // should find data
        let (ingester, shard, namespace) = ingester_test_setup(write_operations, 1, true).await;

        verify_ingester_buffer_has_data(ingester, shard, namespace, |first_batch| {
            assert_eq!(
                first_batch.min_sequence_number,
                SequenceNumber::new(10),
                "re-initialization didn't seek to the beginning",
            );
        })
        .await;
    }

    #[tokio::test]
    async fn limits_concurrent_queries() {
        let mut ingester = TestIngester::new().await;
        let request = IngesterQueryRequest {
            namespace: "foo".to_string(),
            table: "cpu".to_string(),
            columns: vec!["asdf".to_string()],
            predicate: None,
        };

        let res = ingester.ingester.query(request.clone()).await.unwrap_err();
        assert!(matches!(
            res,
            crate::querier_handler::Error::NamespaceNotFound { .. }
        ));

        ingester.ingester.request_sem = Semaphore::new(0);
        let res = ingester.ingester.query(request).await.unwrap_err();
        assert!(matches!(res, crate::querier_handler::Error::RequestLimit));
    }

    struct TestIngester {
        catalog: Arc<dyn Catalog>,
        shard: Shard,
        namespace: Namespace,
        topic: TopicMetadata,
        shard_index: ShardIndex,
        query_pool: QueryPool,
        metrics: Arc<metric::Registry>,
        write_buffer_state: MockBufferSharedState,
        ingester: IngestHandlerImpl,
    }

    impl TestIngester {
        async fn new() -> Self {
            let metrics: Arc<metric::Registry> = Default::default();
            let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));

            let mut txn = catalog.start_transaction().await.unwrap();
            let topic = txn.topics().create_or_get("whatevs").await.unwrap();
            let query_pool = txn.query_pools().create_or_get("whatevs").await.unwrap();
            let shard_index = ShardIndex::new(0);
            let namespace = txn
                .namespaces()
                .create("foo", "inf", topic.id, query_pool.id)
                .await
                .unwrap();
            let shard = txn
                .shards()
                .create_or_get(&topic, shard_index)
                .await
                .unwrap();
            txn.commit().await.unwrap();

            let mut shard_states = BTreeMap::new();
            shard_states.insert(shard_index, shard);

            let write_buffer_state =
                MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(1).unwrap());
            let reading: Arc<dyn WriteBufferReading> =
                Arc::new(MockBufferForReading::new(write_buffer_state.clone(), None).unwrap());
            let object_store = Arc::new(InMemory::new());

            let lifecycle_config = LifecycleConfig::new(
                1000000,
                1000,
                1000,
                Duration::from_secs(10),
                Duration::from_secs(10),
                10000000,
            );
            let ingester = IngestHandlerImpl::new(
                lifecycle_config,
                topic.clone(),
                shard_states,
                Arc::clone(&catalog),
                object_store,
                reading,
                Arc::new(Executor::new(1)),
                Arc::clone(&metrics),
                false,
                1,
            )
            .await
            .unwrap();

            Self {
                catalog,
                shard,
                namespace,
                topic,
                shard_index,
                query_pool,
                metrics,
                write_buffer_state,
                ingester,
            }
        }
    }
}
