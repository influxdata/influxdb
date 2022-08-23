//! A representation of a single operation sequencer.

use data_types::KafkaPartition;
use dml::{DmlMeta, DmlOperation};
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, Metric};
use std::{borrow::Cow, hash::Hash, sync::Arc};
use write_buffer::core::{WriteBufferError, WriteBufferWriting};

/// A sequencer tags an write buffer with a Kafka partition index.
#[derive(Debug)]
pub struct Sequencer<P = SystemProvider> {
    /// The 0..N index / identifier for the Kakfa partition.
    ///
    /// NOTE: this is NOT the ID of the Sequencer row in the catalog this
    /// Sequencer represents.
    kafka_index: KafkaPartition,

    inner: Arc<dyn WriteBufferWriting>,
    time_provider: P,

    enqueue_success: DurationHistogram,
    enqueue_error: DurationHistogram,
}

impl Eq for Sequencer {}

impl PartialEq for Sequencer {
    fn eq(&self, other: &Self) -> bool {
        self.kafka_index == other.kafka_index
    }
}

impl Hash for Sequencer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.kafka_index.hash(state);
    }
}

impl Sequencer {
    /// Tag `inner` with the specified `kafka_index`.
    pub fn new(
        kafka_index: KafkaPartition,
        inner: Arc<dyn WriteBufferWriting>,
        metrics: &metric::Registry,
    ) -> Self {
        let write: Metric<DurationHistogram> = metrics.register_metric(
            "sequencer_enqueue_duration",
            "sequencer enqueue call duration",
        );

        let enqueue_success = write.recorder([
            ("kafka_partition", Cow::from(kafka_index.to_string())),
            ("result", Cow::from("success")),
        ]);
        let enqueue_error = write.recorder([
            ("kafka_partition", Cow::from(kafka_index.to_string())),
            ("result", Cow::from("error")),
        ]);

        // Validate the conversion between the kafka partition type's inner
        // numerical type (an i32) can be cast correctly to the write buffer
        // abstractions numerical type used to identify the partition (a u32).
        assert!(u32::try_from(kafka_index.get()).is_ok());

        Self {
            kafka_index,
            inner,
            enqueue_success,
            enqueue_error,
            time_provider: Default::default(),
        }
    }

    /// Return the 0..N index / identifier for the Kafka partition.
    ///
    /// NOTE: this is NOT the ID of the Sequencer row in the catalog this
    /// Sequencer represents.
    pub fn kafka_index(&self) -> KafkaPartition {
        self.kafka_index
    }

    /// Enqueue `op` into this sequencer.
    ///
    /// The buffering / async return behaviour of this method is defined by the
    /// behaviour of the [`WriteBufferWriting::store_operation()`]
    /// implementation this [`Sequencer`] wraps.
    pub async fn enqueue<'a>(&self, op: DmlOperation) -> Result<DmlMeta, WriteBufferError> {
        let t = self.time_provider.now();

        let res = self
            .inner
            .store_operation(self.kafka_index.get() as _, op)
            .await;

        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.enqueue_success.record(delta),
                Err(_) => self.enqueue_error.record(delta),
            }
        }

        res
    }
}
