//! Ingest handler

use crate::{
    data::{IngesterData, IngesterQueryRequest, QueryData, SequencerData},
    lifecycle::{run_lifecycle_manager, LifecycleConfig, LifecycleManager},
};
use db::write_buffer::metrics::{SequencerMetrics, WriteBufferIngestMetrics};
use futures::StreamExt;
use iox_catalog::interface::{Catalog, KafkaPartition, KafkaTopic, Sequencer, SequencerId};
use object_store::ObjectStore;
use observability_deps::tracing::{debug, info, warn};
use query::exec::Executor;
use snafu::{ResultExt, Snafu};
use std::collections::BTreeMap;
use std::{
    fmt::Formatter,
    sync::Arc,
    time::{Duration, Instant},
};
use time::SystemProvider;
use tokio::task::JoinHandle;
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
pub trait IngestHandler {
    /// Return results from the in-memory data that match this query
    fn query(&self, request: IngesterQueryRequest) -> Result<QueryData>;
}

/// Implementation of the `IngestHandler` trait to ingest from kafka and manage persistence and answer queries
pub struct IngestHandlerImpl {
    /// Kafka Topic assigned to this ingester
    #[allow(dead_code)]
    kafka_topic: KafkaTopic,
    /// Future that resolves when the background worker exits
    join_handles: Vec<JoinHandle<()>>,
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
        let handle = tokio::task::spawn(async move {
            run_lifecycle_manager(manager, persister).await;
        });
        info!(
            "ingester handler and lifecycle started with config {:?}",
            lifecycle_config
        );

        let mut join_handles = Vec::with_capacity(sequencer_states.len());
        join_handles.push(handle);

        for (kafka_partition, sequencer) in sequencer_states {
            let metrics = ingest_metrics.new_sequencer_metrics(kafka_partition.get() as u32);
            let ingester_data = Arc::clone(&ingester_data);
            let kafka_topic_name = kafka_topic_name.clone();

            let stream_handler = write_buffer
                .stream_handler(kafka_partition.get() as u32)
                .await
                .context(WriteBufferSnafu)?;

            join_handles.push(tokio::task::spawn(stream_in_sequenced_entries(
                Arc::clone(&lifecycle_manager),
                ingester_data,
                sequencer.id,
                kafka_topic_name,
                kafka_partition,
                Arc::clone(&write_buffer),
                stream_handler,
                metrics,
            )));
        }

        Ok(Self {
            data,
            kafka_topic: topic,
            join_handles,
            lifecycle_manager,
        })
    }
}

impl IngestHandler for IngestHandlerImpl {
    fn query(&self, _request: IngesterQueryRequest) -> Result<QueryData> {
        todo!()
    }
}

impl Drop for IngestHandlerImpl {
    fn drop(&mut self) {
        for h in &mut self.join_handles {
            h.abort();
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
) {
    let mut watermark_last_updated: Option<Instant> = None;
    let mut watermark = 0_u64;
    let mut stream = write_buffer_stream.stream().await;

    while let Some(db_write_result) = stream.next().await {
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
    use super::*;
    use data_types::sequence::Sequence;
    use dml::{DmlMeta, DmlWrite};
    use iox_catalog::interface::NamespaceSchema;
    use iox_catalog::mem::MemCatalog;
    use iox_catalog::validate_or_insert_schema;
    use metric::{Attributes, Metric, U64Counter, U64Gauge};
    use mutable_batch_lp::lines_to_batches;
    use std::{num::NonZeroU32, ops::DerefMut};
    use time::Time;
    use write_buffer::mock::{MockBufferForReading, MockBufferSharedState};

    #[tokio::test]
    async fn read_from_write_buffer_write_to_mutable_buffer() {
        let catalog = MemCatalog::new();
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

        let mut sequencer_states = BTreeMap::new();
        sequencer_states.insert(kafka_partition, sequencer);

        let schema = NamespaceSchema::new(namespace.id, kafka_topic.id, query_pool.id);

        let write_buffer_state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
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
        write_buffer_state.push_write(w1);
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
        write_buffer_state.push_write(w2);
        let reading: Arc<dyn WriteBufferReading> =
            Arc::new(MockBufferForReading::new(write_buffer_state, None).unwrap());
        let object_store = Arc::new(ObjectStore::new_in_memory());
        let metrics: Arc<metric::Registry> = Default::default();

        let lifecycle_config = LifecycleConfig::new(1000000, 1000, 1000, Duration::from_secs(10));
        let ingester = IngestHandlerImpl::new(
            lifecycle_config,
            kafka_topic,
            sequencer_states,
            Arc::new(catalog),
            object_store,
            reading,
            Executor::new(1),
            &metrics,
        )
        .await
        .unwrap();

        // give the writes some time to go through the buffer. Exit once we've verified there's
        // data in there from both writes.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let mut have_mem = false;
                let mut have_cpu = false;

                if let Some(data) = ingester.data.sequencers.get(&sequencer.id) {
                    if let Some(data) = data.namespace(&namespace.name) {
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

        let observation = metrics
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

        let observation = metrics
            .get_instrument::<Metric<U64Counter>>("write_buffer_read_bytes")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("db_name", "whatevs"),
                ("sequencer_id", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(observation, 200);

        let observation = metrics
            .get_instrument::<Metric<U64Gauge>>("write_buffer_last_sequence_number")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("db_name", "whatevs"),
                ("sequencer_id", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(observation, 7);

        let observation = metrics
            .get_instrument::<Metric<U64Gauge>>("write_buffer_sequence_number_lag")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("db_name", "whatevs"),
                ("sequencer_id", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(observation, 0);

        let observation = metrics
            .get_instrument::<Metric<U64Gauge>>("write_buffer_last_min_ts")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("db_name", "whatevs"),
                ("sequencer_id", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(observation, 20);

        let observation = metrics
            .get_instrument::<Metric<U64Gauge>>("write_buffer_last_max_ts")
            .unwrap()
            .get_observer(&Attributes::from(&[
                ("db_name", "whatevs"),
                ("sequencer_id", "0"),
            ]))
            .unwrap()
            .fetch();
        assert_eq!(observation, 30);

        let observation = metrics
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
}
