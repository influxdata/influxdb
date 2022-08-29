//! A representation of a single operation shard.

use data_types::ShardIndex;
use dml::{DmlMeta, DmlOperation};
use iox_time::{SystemProvider, TimeProvider};
use metric::{DurationHistogram, Metric};
use std::{borrow::Cow, hash::Hash, sync::Arc};
use write_buffer::core::{WriteBufferError, WriteBufferWriting};

/// A shard tags a write buffer with a shard index (Kafka partition).
#[derive(Debug)]
pub struct Shard<P = SystemProvider> {
    /// The 0..N index / identifier for the Kakfa partition.
    ///
    /// NOTE: this is NOT the ID of the Shard row in the catalog this
    /// Shard represents.
    shard_index: ShardIndex,

    inner: Arc<dyn WriteBufferWriting>,
    time_provider: P,

    enqueue_success: DurationHistogram,
    enqueue_error: DurationHistogram,
}

impl Eq for Shard {}

impl PartialEq for Shard {
    fn eq(&self, other: &Self) -> bool {
        self.shard_index == other.shard_index
    }
}

impl Hash for Shard {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.shard_index.hash(state);
    }
}

impl Shard {
    /// Tag `inner` with the specified `shard_index`.
    pub fn new(
        shard_index: ShardIndex,
        inner: Arc<dyn WriteBufferWriting>,
        metrics: &metric::Registry,
    ) -> Self {
        let write: Metric<DurationHistogram> =
            metrics.register_metric("shard_enqueue_duration", "shard enqueue call duration");

        let enqueue_success = write.recorder([
            ("kafka_partition", Cow::from(shard_index.to_string())),
            ("result", Cow::from("success")),
        ]);
        let enqueue_error = write.recorder([
            ("kafka_partition", Cow::from(shard_index.to_string())),
            ("result", Cow::from("error")),
        ]);

        // Validate the conversion between the kafka partition type's inner
        // numerical type (an i32) can be cast correctly to the write buffer
        // abstractions numerical type used to identify the partition (a u32).
        assert!(u32::try_from(shard_index.get()).is_ok());

        Self {
            shard_index,
            inner,
            enqueue_success,
            enqueue_error,
            time_provider: Default::default(),
        }
    }

    /// Return the 0..N index / identifier for the shard (Kafka partition).
    ///
    /// NOTE: this is NOT the ID of the Shard row in the catalog this
    /// Shard represents.
    pub fn shard_index(&self) -> ShardIndex {
        self.shard_index
    }

    /// Enqueue `op` into this shard.
    ///
    /// The buffering / async return behaviour of this method is defined by the
    /// behaviour of the [`WriteBufferWriting::store_operation()`]
    /// implementation this [`Shard`] wraps.
    pub async fn enqueue<'a>(&self, op: DmlOperation) -> Result<DmlMeta, WriteBufferError> {
        let t = self.time_provider.now();

        let res = self.inner.store_operation(self.shard_index, op).await;

        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.enqueue_success.record(delta),
                Err(_) => self.enqueue_error.record(delta),
            }
        }

        res
    }
}
