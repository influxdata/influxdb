//! Ingest handler

use crate::{
    data::{IngesterData, IngesterQueryRequest, IngesterQueryResponse, SequencerData},
    lifecycle::{run_lifecycle_manager, LifecycleConfig, LifecycleManager},
    poison::{PoisonCabinet, PoisonPill},
    querier_handler::prepare_data_to_querier,
};
use async_trait::async_trait;
use db::write_buffer::metrics::{SequencerMetrics, WriteBufferIngestMetrics};
use futures::{
    future::{BoxFuture, Shared},
    pin_mut,
    stream::FuturesUnordered,
    FutureExt, StreamExt, TryFutureExt,
};
use iox_catalog::interface::{Catalog, KafkaPartition, KafkaTopic, Sequencer, SequencerId};
use object_store::ObjectStore;
use observability_deps::tracing::{debug, error, info, warn};
use query::exec::Executor;
use snafu::{ResultExt, Snafu};
use std::collections::BTreeMap;
use std::{
    fmt::Formatter,
    sync::Arc,
    time::{Duration, Instant},
};
use time::SystemProvider;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use trace::span::SpanRecorder;
use write_buffer::core::{WriteBufferReading, WriteBufferStreamHandler};

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display(
        "No sequencer record found for kafka topic {} and partition {}",
        kafka_topic,
        kafka_partition
    ))]
    SequencerRecordNotFound {
        kafka_topic: String,
        kafka_partition: KafkaPartition,
    },

    #[snafu(display("Write buffer error: {}", source))]
    WriteBuffer {
        source: write_buffer::core::WriteBufferError,
    },
}

/// When the lifecycle manager indicates that ingest should be paused because of
/// memory pressure, the sequencer will loop, sleeping this long before checking
/// with the manager if it can resume ingest.
const INGEST_PAUSE_DELAY: Duration = Duration::from_millis(100);

/// A specialized `Error` for Catalog errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The [`IngestHandler`] handles all ingest from kafka, persistence and queries
#[async_trait]
pub trait IngestHandler: Send + Sync {
    /// Return results from the in-memory data that match this query
    async fn query(
        &self,
        request: IngesterQueryRequest,
    ) -> Result<IngesterQueryResponse, crate::querier_handler::Error>;

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

/// Implementation of the `IngestHandler` trait to ingest from kafka and manage persistence and answer queries
pub struct IngestHandlerImpl {
    /// Kafka Topic assigned to this ingester
    #[allow(dead_code)]
    kafka_topic: KafkaTopic,

    /// Future that resolves when the background worker exits
    join_handles: Vec<(String, SharedJoinHandle)>,

    /// A token that is used to trigger shutdown of the background worker
    shutdown: CancellationToken,

    /// Poison pills for testing.
    poison_cabinet: Arc<PoisonCabinet>,

    /// The cache and buffered data for the ingester
    data: Arc<IngesterData>,

    /// The lifecycle manager, keeping state of partitions across all sequencers
    lifecycle_manager: Arc<LifecycleManager>,
}

impl std::fmt::Debug for IngestHandlerImpl {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl IngestHandlerImpl {
    /// Initialize the Ingester
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        lifecycle_config: LifecycleConfig,
        topic: KafkaTopic,
        sequencer_states: BTreeMap<KafkaPartition, Sequencer>,
        catalog: Arc<dyn Catalog>,
        object_store: Arc<ObjectStore>,
        write_buffer: Arc<dyn WriteBufferReading>,
        exec: Executor,
        registry: &metric::Registry,
    ) -> Result<Self> {
        // build the initial ingester data state
        let mut sequencers = BTreeMap::new();
        for s in sequencer_states.values() {
            sequencers.insert(s.id, SequencerData::default());
        }
        let data = Arc::new(IngesterData {
            object_store,
            catalog,
            sequencers,
            exec,
        });

        let ingester_data = Arc::clone(&data);
        let kafka_topic_name = topic.name.clone();
        let ingest_metrics = WriteBufferIngestMetrics::new(registry, &topic.name);

        // start the lifecycle manager
        let persister = Arc::clone(&data);
        let lifecycle_manager = Arc::new(LifecycleManager::new(
            lifecycle_config,
            Arc::new(SystemProvider::new()),
        ));
        let manager = Arc::clone(&lifecycle_manager);
        let shutdown = CancellationToken::new();
        let poison_cabinet = Arc::new(PoisonCabinet::new());
        let handle = tokio::task::spawn(run_lifecycle_manager(
            manager,
            persister,
            shutdown.clone(),
            Arc::clone(&poison_cabinet),
        ));
        info!(
            "ingester handler and lifecycle started with config {:?}",
            lifecycle_config
        );

        let mut join_handles = Vec::with_capacity(sequencer_states.len() + 1);
        join_handles.push(("lifecycle manager".to_owned(), shared_handle(handle)));

        for (kafka_partition, sequencer) in sequencer_states {
            let worker_name = format!("stream handler for partition {}", kafka_partition.get());
            let metrics = ingest_metrics.new_sequencer_metrics(kafka_partition.get() as u32);
            let ingester_data = Arc::clone(&ingester_data);
            let kafka_topic_name = kafka_topic_name.clone();

            let stream_handler = write_buffer
                .stream_handler(kafka_partition.get() as u32)
                .await
                .context(WriteBufferSnafu)?;

            let handle = tokio::task::spawn(stream_in_sequenced_entries(
                Arc::clone(&lifecycle_manager),
                ingester_data,
                sequencer.id,
                kafka_topic_name,
                kafka_partition,
                Arc::clone(&write_buffer),
                stream_handler,
                metrics,
                shutdown.clone(),
                Arc::clone(&poison_cabinet),
            ));
            join_handles.push((worker_name, shared_handle(handle)));
        }

        Ok(Self {
            data,
            kafka_topic: topic,
            join_handles,
            shutdown,
            poison_cabinet,
            lifecycle_manager,
        })
    }
}

#[async_trait]
impl IngestHandler for IngestHandlerImpl {
    async fn query(
        &self,
        request: IngesterQueryRequest,
    ) -> Result<IngesterQueryResponse, crate::querier_handler::Error> {
        prepare_data_to_querier(&request).await
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
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

impl Drop for IngestHandlerImpl {
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
    }
}

/// This is used to take entries from a `Stream` and put them in the
/// mutable buffer, such as streaming entries from a write buffer.
///
/// Note all errors reading / parsing / writing entries from the write
/// buffer are ignored.
#[allow(clippy::too_many_arguments)]
async fn stream_in_sequenced_entries(
    lifecycle_manager: Arc<LifecycleManager>,
    ingester_data: Arc<IngesterData>,
    sequencer_id: SequencerId,
    kafka_topic: String,
    kafka_partition: KafkaPartition,
    write_buffer: Arc<dyn WriteBufferReading>,
    mut write_buffer_stream: Box<dyn WriteBufferStreamHandler>,
    mut metrics: SequencerMetrics,
    shutdown: CancellationToken,
    poison_cabinet: Arc<PoisonCabinet>,
) {
    let mut watermark_last_updated: Option<Instant> = None;
    let mut watermark = 0_u64;
    let mut stream = write_buffer_stream.stream().await;

    let shutdown_cancelled = shutdown.cancelled().fuse();
    let poison_wait_panic = poison_cabinet
        .wait_for(PoisonPill::StreamPanic(kafka_partition))
        .fuse();
    let poison_wait_exit = poison_cabinet
        .wait_for(PoisonPill::StreamExit(kafka_partition))
        .fuse();

    pin_mut!(shutdown_cancelled);
    pin_mut!(poison_wait_panic);
    pin_mut!(poison_wait_exit);

    while let Some(db_write_result) = futures::select!(next = stream.next().fuse() => next, _ = shutdown_cancelled => {
            info!(
                kafka_topic=kafka_topic.as_str(),
                %kafka_partition,
                "Stream handler shutdown",
            );
            return;
        }, _ = poison_wait_panic => {
            panic!("Stream {} poisened, panic", kafka_partition.get());
        },_ = poison_wait_exit => {
            error!("Stream {} poisened, exit early", kafka_partition.get());
            return;
        },
    ) {
        if poison_cabinet.contains(&PoisonPill::StreamPanic(kafka_partition)) {
            panic!("Stream {} poisened, panic", kafka_partition.get());
        }
        if poison_cabinet.contains(&PoisonPill::StreamExit(kafka_partition)) {
            error!("Stream {} poisened, exit early", kafka_partition.get());
            return;
        }

        if shutdown.is_cancelled() {
            info!(
                kafka_topic=kafka_topic.as_str(),
                %kafka_partition,
                "Stream handler shutdown",
            );
            return;
        }

        // maybe update sequencer watermark
        // We are not updating this watermark every round because asking the sequencer for that watermark can be
        // quite expensive.
        let now = Instant::now();
        if watermark_last_updated
            .map(|ts| now.duration_since(ts) > Duration::from_secs(10))
            .unwrap_or(true)
        {
            match write_buffer
                .fetch_high_watermark(sequencer_id.get() as u32)
                .await
            {
                Ok(w) => {
                    watermark = w;
                }
                // skip over invalid data in the write buffer so recovery can succeed
                Err(e) => {
                    debug!(
                        %e,
                        %kafka_topic,
                        %kafka_partition,
                        "Error while reading sequencer watermark",
                    )
                }
            }
            watermark_last_updated = Some(now);
        }

        let ingest_recorder = metrics.recorder(watermark);

        // get entry from sequencer
        let dml_operation = match db_write_result {
            Ok(db_write) => db_write,
            // skip over invalid data in the write buffer so recovery can succeed
            Err(e) => {
                warn!(
                    %e,
                    %kafka_topic,
                    %kafka_partition,
                    "Error converting write buffer data to SequencedEntry",
                );
                continue;
            }
        };

        let ingest_recorder = ingest_recorder.operation(&dml_operation);

        // store entry
        let mut span_recorder = SpanRecorder::new(
            dml_operation
                .meta()
                .span_context()
                .map(|parent| parent.child("IOx write buffer")),
        );

        let result = ingester_data
            .buffer_operation(sequencer_id, dml_operation.clone(), &lifecycle_manager)
            .await;

        match result {
            Ok(should_pause) => {
                ingest_recorder.success();
                span_recorder.ok("stored write");

                if should_pause {
                    warn!(%sequencer_id, "pausing ingest until persistence has run");
                    while !lifecycle_manager.can_resume_ingest() {
                        tokio::time::sleep(INGEST_PAUSE_DELAY).await;
                    }
                    warn!(%sequencer_id, "resuming ingest");
                }
            }
            Err(e) => {
                // skip over invalid data in the write buffer so recovery can succeed
                debug!(
                    %e,
                    %kafka_topic,
                    %sequencer_id,
                    "Error storing SequencedEntry from write buffer in ingester buffer"
                );
                span_recorder.error("cannot store write");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::poison::PoisonPill;

    use super::*;
    use data_types::sequence::Sequence;
    use dml::{DmlMeta, DmlWrite};
    use iox_catalog::interface::{Namespace, NamespaceSchema, QueryPool};
    use iox_catalog::mem::MemCatalog;
    use iox_catalog::validate_or_insert_schema;
    use metric::{Attributes, Metric, U64Counter, U64Gauge};
    use mutable_batch_lp::lines_to_batches;
    use std::{num::NonZeroU32, ops::DerefMut};
    use time::Time;
    use write_buffer::mock::{MockBufferForReading, MockBufferSharedState};

    #[tokio::test]
    async fn read_from_write_buffer_write_to_mutable_buffer() {
        let ingester = TestIngester::new().await;

        let schema = NamespaceSchema::new(
            ingester.namespace.id,
            ingester.kafka_topic.id,
            ingester.query_pool.id,
        );
        let mut txn = ingester.catalog.start_transaction().await.unwrap();
        let ingest_ts1 = Time::from_timestamp_millis(42);
        let ingest_ts2 = Time::from_timestamp_millis(1337);
        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("mem foo=1 10", 0).unwrap(),
            DmlMeta::sequenced(Sequence::new(0, 0), ingest_ts1, None, 50),
        );
        let schema = validate_or_insert_schema(w1.tables(), &schema, txn.deref_mut())
            .await
            .unwrap()
            .unwrap();
        ingester.write_buffer_state.push_write(w1);
        let w2 = DmlWrite::new(
            "foo",
            lines_to_batches("cpu bar=2 20\ncpu bar=3 30", 0).unwrap(),
            DmlMeta::sequenced(Sequence::new(0, 7), ingest_ts2, None, 150),
        );
        let _schema = validate_or_insert_schema(w2.tables(), &schema, txn.deref_mut())
            .await
            .unwrap()
            .unwrap();
        txn.commit().await.unwrap();
        ingester.write_buffer_state.push_write(w2);

        // give the writes some time to go through the buffer. Exit once we've verified there's
        // data in there from both writes.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let mut have_mem = false;
                let mut have_cpu = false;

                if let Some(data) = ingester
                    .ingester
                    .data
                    .sequencers
                    .get(&ingester.sequencer.id)
                {
                    if let Some(data) = data.namespace(&ingester.namespace.name) {
                        // verify mem table
                        if let Some(table) = data.table_data("mem") {
                            if let Some(partition) = table.partition_data("1970-01-01") {
                                let snapshots = partition.snapshot().unwrap();
                                if let Some(s) = snapshots.last() {
                                    if s.data.num_rows() > 0 {
                                        have_mem = true;
                                    }
                                }
                            }
                        }

                        // verify cpu table
                        if let Some(table) = data.table_data("cpu") {
                            if let Some(partition) = table.partition_data("1970-01-01") {
                                let snapshots = partition.snapshot().unwrap();
                                if let Some(s) = snapshots.last() {
                                    if s.data.num_rows() > 0 {
                                        have_cpu = true;
                                    }
                                }
                            }
                        }
                    }
                }

                if have_mem && have_cpu {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timeout");

        let observation = ingester
            .metrics
            .get_instrument::<Metric<U64Counter>>("write_buffer_ingest_requests")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("db_name", "whatevs"),
                ("sequencer_id", "0"),
                ("status", "ok"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(observation, 2);

        let observation = ingester
            .metrics
            .get_instrument::<Metric<U64Counter>>("write_buffer_read_bytes")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("db_name", "whatevs"),
                ("sequencer_id", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(observation, 200);

        let observation = ingester
            .metrics
            .get_instrument::<Metric<U64Gauge>>("write_buffer_last_sequence_number")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("db_name", "whatevs"),
                ("sequencer_id", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(observation, 7);

        let observation = ingester
            .metrics
            .get_instrument::<Metric<U64Gauge>>("write_buffer_sequence_number_lag")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("db_name", "whatevs"),
                ("sequencer_id", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(observation, 0);

        let observation = ingester
            .metrics
            .get_instrument::<Metric<U64Gauge>>("write_buffer_last_min_ts")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("db_name", "whatevs"),
                ("sequencer_id", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(observation, 20);

        let observation = ingester
            .metrics
            .get_instrument::<Metric<U64Gauge>>("write_buffer_last_max_ts")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("db_name", "whatevs"),
                ("sequencer_id", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(observation, 30);

        let observation = ingester
            .metrics
            .get_instrument::<Metric<U64Gauge>>("write_buffer_last_ingest_ts")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("db_name", "whatevs"),
                ("sequencer_id", "0"),
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
    #[should_panic(expected = "Background worker 'lifecycle manager' exited early!")]
    async fn test_supervise_lifecycle_manager_early_exit() {
        let ingester = TestIngester::new().await.ingester;
        ingester.poison_cabinet.add(PoisonPill::LifecycleExit);
        tokio::time::timeout(Duration::from_millis(1000), ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "JoinError::Panic")]
    async fn test_supervise_lifecycle_manager_panic() {
        let ingester = TestIngester::new().await.ingester;
        ingester.poison_cabinet.add(PoisonPill::LifecyclePanic);
        tokio::time::timeout(Duration::from_millis(1000), ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "Background worker 'stream handler for partition 0' exited early!")]
    async fn test_supervise_stream_early_exit() {
        let ingester = TestIngester::new().await;
        ingester
            .ingester
            .poison_cabinet
            .add(PoisonPill::StreamExit(ingester.kafka_partition));
        tokio::time::timeout(Duration::from_millis(1000), ingester.ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "JoinError::Panic")]
    async fn test_supervise_stream_panic() {
        let ingester = TestIngester::new().await;
        ingester
            .ingester
            .poison_cabinet
            .add(PoisonPill::StreamPanic(ingester.kafka_partition));
        tokio::time::timeout(Duration::from_millis(1000), ingester.ingester.join())
            .await
            .unwrap();
    }

    struct TestIngester {
        catalog: Arc<dyn Catalog>,
        sequencer: Sequencer,
        namespace: Namespace,
        kafka_topic: KafkaTopic,
        kafka_partition: KafkaPartition,
        query_pool: QueryPool,
        metrics: Arc<metric::Registry>,
        write_buffer_state: MockBufferSharedState,
        ingester: IngestHandlerImpl,
    }

    impl TestIngester {
        async fn new() -> Self {
            let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new());

            let mut txn = catalog.start_transaction().await.unwrap();
            let kafka_topic = txn.kafka_topics().create_or_get("whatevs").await.unwrap();
            let query_pool = txn.query_pools().create_or_get("whatevs").await.unwrap();
            let kafka_partition = KafkaPartition::new(0);
            let namespace = txn
                .namespaces()
                .create("foo", "inf", kafka_topic.id, query_pool.id)
                .await
                .unwrap();
            let sequencer = txn
                .sequencers()
                .create_or_get(&kafka_topic, kafka_partition)
                .await
                .unwrap();
            txn.commit().await.unwrap();

            let mut sequencer_states = BTreeMap::new();
            sequencer_states.insert(kafka_partition, sequencer);

            let write_buffer_state =
                MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
            let reading: Arc<dyn WriteBufferReading> =
                Arc::new(MockBufferForReading::new(write_buffer_state.clone(), None).unwrap());
            let object_store = Arc::new(ObjectStore::new_in_memory());
            let metrics: Arc<metric::Registry> = Default::default();

            let lifecycle_config =
                LifecycleConfig::new(1000000, 1000, 1000, Duration::from_secs(10));
            let ingester = IngestHandlerImpl::new(
                lifecycle_config,
                kafka_topic.clone(),
                sequencer_states,
                Arc::clone(&catalog),
                object_store,
                reading,
                Executor::new(1),
                &metrics,
            )
            .await
            .unwrap();

            Self {
                catalog,
                sequencer,
                namespace,
                kafka_topic,
                kafka_partition,
                query_pool,
                metrics,
                write_buffer_state,
                ingester,
            }
        }
    }
}
