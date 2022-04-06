//! Ingest handler

use crate::{
    data::{IngesterData, IngesterQueryResponse, SequencerData},
    lifecycle::{run_lifecycle_manager, LifecycleConfig, LifecycleHandle, LifecycleManager},
    poison::{PoisonCabinet, PoisonPill},
    querier_handler::prepare_data_to_querier,
};
use async_trait::async_trait;
use backoff::BackoffConfig;
use data_types2::{IngesterQueryRequest, KafkaPartition, KafkaTopic, Sequencer, SequencerId};
use db::write_buffer::metrics::{SequencerMetrics, WriteBufferIngestMetrics};
use dml::DmlOperation;
use futures::{
    future::{BoxFuture, Shared},
    pin_mut,
    stream::FuturesUnordered,
    FutureExt, StreamExt, TryFutureExt,
};
use iox_catalog::interface::Catalog;
use metric::Attributes;
use metric::U64Counter;
use metric::U64Gauge;
use object_store::DynObjectStore;
use observability_deps::tracing::{debug, error, info, warn};
use query::exec::Executor;
use snafu::{ResultExt, Snafu};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use time::{SystemProvider, TimeProvider};
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;
use trace::span::SpanRecorder;
use write_buffer::core::{WriteBufferReading, WriteBufferStreamHandler};
use write_summary::SequencerProgress;

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

    #[snafu(display("Data Error: {}", source))]
    Data { source: crate::data::Error },
}

/// When the lifecycle manager indicates that ingest should be paused because of
/// memory pressure, the sequencer will loop, sleeping this long before checking
/// with the manager if it can resume ingest.
const INGEST_PAUSE_DELAY: Duration = Duration::from_millis(100);

/// A specialized `Error` for Catalog errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An [`IngestHandler`] handles all ingest requests from kafka, persistence and queries
#[async_trait]
pub trait IngestHandler: Send + Sync {
    /// Return results from the in-memory data that match this query
    async fn query(
        &self,
        request: IngesterQueryRequest,
    ) -> Result<IngesterQueryResponse, crate::querier_handler::Error>;

    /// Return sequencer progress for the requested kafka partitions
    async fn progresses(
        &self,
        sequencers: Vec<KafkaPartition>,
    ) -> Result<BTreeMap<KafkaPartition, SequencerProgress>>;

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

/// Implementation of the `IngestHandler` trait to ingest from kafka and manage
/// persistence and answer queries
#[derive(Debug)]
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
}

impl IngestHandlerImpl {
    /// Initialize the Ingester
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        lifecycle_config: LifecycleConfig,
        topic: KafkaTopic,
        sequencer_states: BTreeMap<KafkaPartition, Sequencer>,
        catalog: Arc<dyn Catalog>,
        object_store: Arc<DynObjectStore>,
        write_buffer: Arc<dyn WriteBufferReading>,
        exec: Arc<Executor>,
        metric_registry: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Result<Self> {
        // build the initial ingester data state
        let mut sequencers = BTreeMap::new();
        for s in sequencer_states.values() {
            sequencers.insert(
                s.id,
                SequencerData::new(s.kafka_partition, Arc::clone(&metric_registry)),
            );
        }
        let data = Arc::new(IngesterData {
            object_store,
            catalog,
            sequencers,
            exec,
            backoff_config: BackoffConfig::default(),
        });

        let ingester_data = Arc::clone(&data);
        let kafka_topic_name = topic.name.clone();
        let ingest_metrics = WriteBufferIngestMetrics::new(&metric_registry, &topic.name);

        // start the lifecycle manager
        let persister = Arc::clone(&data);
        let lifecycle_manager = LifecycleManager::new(
            lifecycle_config,
            Arc::clone(&metric_registry),
            Arc::new(SystemProvider::new()),
        );
        let lifecycle_handle = lifecycle_manager.handle();
        let shutdown = CancellationToken::new();
        let poison_cabinet = Arc::new(PoisonCabinet::new());
        let handle = tokio::task::spawn(run_lifecycle_manager(
            lifecycle_manager,
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
            let metric_registry = Arc::clone(&metric_registry);

            let mut stream_handler = write_buffer
                .stream_handler(kafka_partition.get() as u32)
                .await
                .context(WriteBufferSnafu)?;

            stream_handler
                .seek(sequencer.min_unpersisted_sequence_number as u64)
                .await
                .context(WriteBufferSnafu)?;

            let handle = tokio::task::spawn(stream_in_sequenced_entries(
                lifecycle_handle.clone(),
                ingester_data,
                sequencer.id,
                kafka_topic_name,
                kafka_partition,
                Arc::clone(&write_buffer),
                stream_handler,
                metrics,
                metric_registry,
                shutdown.clone(),
                Arc::clone(&poison_cabinet),
                Arc::clone(&time_provider),
            ));
            join_handles.push((worker_name, shared_handle(handle)));
        }

        Ok(Self {
            data,
            kafka_topic: topic,
            join_handles,
            shutdown,
            poison_cabinet,
        })
    }
}

#[async_trait]
impl IngestHandler for IngestHandlerImpl {
    async fn query(
        &self,
        request: IngesterQueryRequest,
    ) -> Result<IngesterQueryResponse, crate::querier_handler::Error> {
        prepare_data_to_querier(&self.data, &request).await
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

        self.data.exec.join().await;
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
        self.data.exec.shutdown();
    }

    /// Return the ingestion progress from each sequencer
    async fn progresses(
        &self,
        partitions: Vec<KafkaPartition>,
    ) -> Result<BTreeMap<KafkaPartition, SequencerProgress>> {
        self.data.progresses(partitions).await.context(DataSnafu)
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

        // `self.data.exec` implements `Drop`, so we don't need to do anything
    }
}

/// This is used to take entries from a `Stream` and put them in the
/// mutable buffer, such as streaming entries from a write buffer.
///
/// Note all errors reading / parsing / writing entries from the write
/// buffer are ignored.
#[allow(clippy::too_many_arguments)]
async fn stream_in_sequenced_entries(
    lifecycle_manager: LifecycleHandle,
    ingester_data: Arc<IngesterData>,
    sequencer_id: SequencerId,
    kafka_topic: String,
    kafka_partition: KafkaPartition,
    write_buffer: Arc<dyn WriteBufferReading>,
    mut write_buffer_stream: Box<dyn WriteBufferStreamHandler>,
    mut metrics: SequencerMetrics,
    metric_registry: Arc<metric::Registry>,
    shutdown: CancellationToken,
    poison_cabinet: Arc<PoisonCabinet>,
    time_provider: Arc<dyn TimeProvider>,
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

    let pause_duration_ms = metric_registry
        .register_metric::<U64Counter>(
            "ingest_paused_duration_ms_total",
            "Duration of time ingestion has been paused by the lifecycle manager in milliseconds",
        )
        .recorder(&[]);

    // using the kafka partition number to keep metrics consistent with write buffer which calls
    // the sequencer_id the kafka partition number.
    let sequencer_id_attr = format!("{}", kafka_partition.get());
    let attributes = Attributes::from([
        ("sequencer_id", sequencer_id_attr.into()),
        ("kafka_topic", kafka_topic.clone().into()),
    ]);
    let time_to_be_readable_ms = metric_registry.register_metric::<U64Gauge>(
        "ingester_ttbr_ms",
        "Duration of time between producer writing to consumer putting into queryable cache in milliseconds",
    ).recorder(attributes);

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
        let dml_operation: DmlOperation = match db_write_result {
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
                        // Incrementally report on the sleeps (as opposed to
                        // measuring the start/end duration) in order to report
                        // a blocked ingester _before_ it recovers.
                        //
                        // While the actual sleep may be slightly longer than
                        // INGEST_PAUSE_DELAY, it's not likely to be a useful
                        // distinction in the metrics.
                        pause_duration_ms.inc(INGEST_PAUSE_DELAY.as_millis() as _);
                    }
                    info!(%sequencer_id, "resuming ingest");
                }

                if let Some(ts) = dml_operation.meta().producer_ts() {
                    if let Some(ttbr) = time_provider.now().checked_duration_since(ts) {
                        time_to_be_readable_ms.set(ttbr.as_millis() as u64);
                    }
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
    use super::*;
    use crate::poison::PoisonPill;
    use data_types2::{Namespace, NamespaceSchema, QueryPool, Sequence, SequenceNumber};
    use dml::{DmlMeta, DmlWrite};
    use iox_catalog::{mem::MemCatalog, validate_or_insert_schema};
    use metric::{Attributes, Metric, U64Counter, U64Gauge};
    use mutable_batch_lp::lines_to_batches;
    use object_store::ObjectStoreImpl;
    use std::{num::NonZeroU32, ops::DerefMut};
    use time::{MockProvider, Time};
    use write_buffer::mock::{MockBufferForReading, MockBufferSharedState};

    #[tokio::test]
    async fn read_from_write_buffer_write_to_mutable_buffer() {
        let start_time = Time::from_timestamp_millis(1340);
        let ingester = TestIngester::new(start_time).await;

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
        ingester.write_buffer_state.push_write(w2);
        let w3 = DmlWrite::new(
            "foo",
            lines_to_batches("a b=2 200", 0).unwrap(),
            DmlMeta::sequenced(Sequence::new(0, 9), ingest_ts2, None, 150),
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

                if let Some(data) = ingester
                    .ingester
                    .data
                    .sequencers
                    .get(&ingester.sequencer.id)
                {
                    if let Some(data) = data.namespace(&ingester.namespace.name) {
                        // verify there's data in the buffer
                        if let Some((b, _)) = data.snapshot("a", "1970-01-01").await {
                            if let Some(b) = b.first() {
                                if b.data.num_rows() > 0 {
                                    has_measurement = true;
                                }
                            }
                        }
                    }
                }

                // and ensure that the sequencer state was actually updated
                let seq = ingester
                    .catalog
                    .repositories()
                    .await
                    .sequencers()
                    .create_or_get(&ingester.kafka_topic, ingester.kafka_partition)
                    .await
                    .unwrap();

                if has_measurement && seq.min_unpersisted_sequence_number == 9 {
                    break;
                }

                tokio::time::sleep(Duration::from_millis(200)).await;
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
        assert_eq!(observation, 3);

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
        assert_eq!(observation, 350);

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
        assert_eq!(observation, 9);

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
        assert_eq!(observation, 200);

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
        assert_eq!(observation, 200);

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

        let observation = ingester
            .metrics
            .get_instrument::<Metric<U64Gauge>>("ingester_ttbr_ms")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("kafka_topic", "whatevs"),
                ("sequencer_id", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(
            observation,
            (start_time.timestamp_nanos() as u64 - ingest_ts2.timestamp_nanos() as u64)
                / 1000
                / 1000
        );
    }

    #[tokio::test]
    async fn test_shutdown() {
        let ingester = TestIngester::new(Time::from_timestamp_millis(10000))
            .await
            .ingester;

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
        let ingester = TestIngester::new(Time::from_timestamp_millis(10000))
            .await
            .ingester;
        ingester.poison_cabinet.add(PoisonPill::LifecycleExit);
        tokio::time::timeout(Duration::from_millis(1000), ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "JoinError::Panic")]
    async fn test_supervise_lifecycle_manager_panic() {
        let ingester = TestIngester::new(Time::from_timestamp_millis(10000))
            .await
            .ingester;
        ingester.poison_cabinet.add(PoisonPill::LifecyclePanic);
        tokio::time::timeout(Duration::from_millis(1000), ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "Background worker 'stream handler for partition 0' exited early!")]
    async fn test_supervise_stream_early_exit() {
        let ingester = TestIngester::new(Time::from_timestamp_millis(10000)).await;
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
        let ingester = TestIngester::new(Time::from_timestamp_millis(10000)).await;
        ingester
            .ingester
            .poison_cabinet
            .add(PoisonPill::StreamPanic(ingester.kafka_partition));
        tokio::time::timeout(Duration::from_millis(1000), ingester.ingester.join())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn seeks_on_initialization() {
        let metrics: Arc<metric::Registry> = Default::default();
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));

        let mut txn = catalog.start_transaction().await.unwrap();
        let kafka_topic = txn.kafka_topics().create_or_get("whatevs").await.unwrap();
        let query_pool = txn.query_pools().create_or_get("whatevs").await.unwrap();
        let kafka_partition = KafkaPartition::new(0);
        let namespace = txn
            .namespaces()
            .create("foo", "inf", kafka_topic.id, query_pool.id)
            .await
            .unwrap();
        let mut sequencer = txn
            .sequencers()
            .create_or_get(&kafka_topic, kafka_partition)
            .await
            .unwrap();
        // update the min unpersisted so we can verify this was what was seeked to later
        sequencer.min_unpersisted_sequence_number = 2;
        // this probably isn't necessary, but just in case something changes later
        txn.sequencers()
            .update_min_unpersisted_sequence_number(sequencer.id, SequenceNumber::new(2))
            .await
            .unwrap();

        let mut sequencer_states = BTreeMap::new();
        sequencer_states.insert(kafka_partition, sequencer);

        let write_buffer_state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());

        let schema = NamespaceSchema::new(namespace.id, kafka_topic.id, query_pool.id);
        let ingest_ts1 = Time::from_timestamp_millis(42);
        let ingest_ts2 = Time::from_timestamp_millis(1337);
        let w1 = DmlWrite::new(
            "foo",
            lines_to_batches("cpu bar=2 20", 0).unwrap(),
            DmlMeta::sequenced(Sequence::new(0, 1), ingest_ts1, None, 150),
        );
        let _schema = validate_or_insert_schema(w1.tables(), &schema, txn.deref_mut())
            .await
            .unwrap()
            .unwrap();
        let w2 = DmlWrite::new(
            "foo",
            lines_to_batches("cpu bar=2 30", 0).unwrap(),
            DmlMeta::sequenced(Sequence::new(0, 2), ingest_ts2, None, 150),
        );
        txn.commit().await.unwrap();
        write_buffer_state.push_write(w1);
        write_buffer_state.push_write(w2);

        let reading: Arc<dyn WriteBufferReading> =
            Arc::new(MockBufferForReading::new(write_buffer_state.clone(), None).unwrap());
        let object_store = Arc::new(ObjectStoreImpl::new_in_memory());

        let lifecycle_config = LifecycleConfig::new(
            1000000,
            1000,
            1000,
            Duration::from_secs(10),
            Duration::from_secs(10),
        );
        let ingester = IngestHandlerImpl::new(
            lifecycle_config,
            kafka_topic.clone(),
            sequencer_states,
            Arc::clone(&catalog),
            object_store,
            reading,
            Arc::new(Executor::new(1)),
            Arc::clone(&metrics),
            Arc::new(SystemProvider::new()),
        )
        .await
        .unwrap();

        // give the writes some time to go through the buffer. Exit once we've verified there's
        // data in there
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let mut has_measurement = false;

                if let Some(data) = ingester
                    .data
                    .sequencers
                    .get(&sequencer.id)
                {
                    if let Some(data) = data.namespace(&namespace.name) {
                        // verify there's data in the buffer
                        if let Some((b, _)) = data.snapshot("cpu", "1970-01-01").await {
                            if let Some(b) = b.first() {
                                if b.min_sequencer_number == SequenceNumber::new(1) {
                                    panic!("initialization did a seek to the beginning rather than the min_unpersisted");
                                }

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
        async fn new(start_time: Time) -> Self {
            let metrics: Arc<metric::Registry> = Default::default();
            let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metrics)));

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
            let object_store = Arc::new(ObjectStoreImpl::new_in_memory());

            let time_provider = Arc::new(MockProvider::new(start_time));

            let lifecycle_config = LifecycleConfig::new(
                1000000,
                1000,
                1000,
                Duration::from_secs(10),
                Duration::from_secs(10),
            );
            let ingester = IngestHandlerImpl::new(
                lifecycle_config,
                kafka_topic.clone(),
                sequencer_states,
                Arc::clone(&catalog),
                object_store,
                reading,
                Arc::new(Executor::new(1)),
                Arc::clone(&metrics),
                time_provider,
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
