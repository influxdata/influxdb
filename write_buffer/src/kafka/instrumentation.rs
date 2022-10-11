use std::result::Result;

use data_types::ShardIndex;
use futures::future::BoxFuture;
use iox_time::{SystemProvider, TimeProvider};
use metric::{Attributes, DurationHistogram, U64Histogram, U64HistogramOptions};
use rskafka::{
    client::{partition::Compression, producer::ProducerClient},
    record::Record,
};

/// An instrumentation layer that decorates a [`ProducerClient`] implementation,
/// recording the latency distribution and success/error result of the
/// underlying [`ProducerClient::produce()`] call, which includes serialisation
/// & protocol overhead, as well as the actual network I/O.
///
/// Captures the approximate, uncompressed size of the resulting Kafka message's
/// payload wrote to the wire by summing the [`Record::approximate_size()`] of
/// the batch. This value reflects the size of the message before client
/// compression, or broker compression - messages on the wire may be
/// significantly smaller.
///
/// The metrics created by this instrumentation are labelled with the kafka
/// topic & partition specified at initialisation.
#[derive(Debug)]
pub struct KafkaProducerMetrics<P = SystemProvider> {
    inner: Box<dyn ProducerClient>,
    time_provider: P,

    enqueue_success: DurationHistogram,
    enqueue_error: DurationHistogram,

    msg_size: U64Histogram,
}

impl KafkaProducerMetrics {
    /// Decorate the specified [`ProducerClient`] implementation with an
    /// instrumentation layer.
    pub fn new(
        client: Box<dyn ProducerClient>,
        kafka_topic_name: String,
        shard_index: ShardIndex,
        metrics: &metric::Registry,
    ) -> Self {
        let attr = Attributes::from([
            ("kafka_partition", shard_index.to_string().into()),
            ("kafka_topic", kafka_topic_name.into()),
        ]);

        // Capture the distribution of message sizes (sum of Record size)
        let msg_size = metrics
            .register_metric_with_options::<U64Histogram, _>(
                "write_buffer_client_payload_size",
                "distribution of approximate uncompressed message \
                payload size wrote to Kafka",
                || {
                    U64HistogramOptions::new(
                        // 512 bytes to 16MiB buckets.
                        [
                            512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144,
                            524288, 1048576, 2097152, 4194304, 8388608, 16777216,
                        ],
                    )
                },
            )
            .recorder(attr.clone());

        let enqueue = metrics.register_metric::<DurationHistogram>(
            "write_buffer_client_produce_duration",
            "duration of time taken to push a set of records to kafka \
             - includes codec, protocol, and network overhead",
        );
        let enqueue_success = enqueue.recorder({
            let mut attr = attr.clone();
            attr.insert("result", "success");
            attr
        });
        let enqueue_error = enqueue.recorder({
            let mut attr = attr;
            attr.insert("result", "error");
            attr
        });

        Self {
            inner: client,
            time_provider: Default::default(),
            enqueue_success,
            enqueue_error,
            msg_size,
        }
    }
}

impl<P> KafkaProducerMetrics<P>
where
    P: TimeProvider,
{
    #[cfg(test)]
    fn with_time_provider<T>(self, time_provider: T) -> KafkaProducerMetrics<T> {
        KafkaProducerMetrics {
            inner: self.inner,
            time_provider,
            enqueue_error: self.enqueue_error,
            enqueue_success: self.enqueue_success,
            msg_size: self.msg_size,
        }
    }

    /// Call the inner [`ProducerClient`] implementation and record latency in
    /// the appropriate success/error metric depending on the result.
    async fn instrument(
        &self,
        records: Vec<Record>,
        compression: Compression,
    ) -> Result<Vec<i64>, rskafka::client::error::Error> {
        // Capture the approximate message size.
        self.msg_size
            .record(records.iter().map(|v| v.approximate_size() as u64).sum());

        let t = self.time_provider.now();

        let res = self.inner.produce(records, compression).await;

        if let Some(delta) = self.time_provider.now().checked_duration_since(t) {
            match &res {
                Ok(_) => self.enqueue_success.record(delta),
                Err(_) => self.enqueue_error.record(delta),
            }
        }

        res
    }
}

impl<P> rskafka::client::producer::ProducerClient for KafkaProducerMetrics<P>
where
    P: TimeProvider,
{
    fn produce(
        &self,
        records: Vec<Record>,
        compression: Compression,
    ) -> BoxFuture<'_, Result<Vec<i64>, rskafka::client::error::Error>> {
        Box::pin(self.instrument(records, compression))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use iox_time::Time;
    use metric::Metric;
    use parking_lot::Mutex;
    use rskafka::chrono::{self, Utc};

    use super::*;

    const KAFKA_TOPIC: &str = "bananas";
    const SHARD_INDEX: ShardIndex = ShardIndex::new(42);

    /// The duration of time the MockProducer::produce() takes to "execute"
    const CALL_LATENCY: Duration = Duration::from_secs(1);

    #[derive(Debug)]
    pub struct MockProducer {
        clock: Arc<iox_time::MockProvider>,
        ret: Mutex<Option<Result<Vec<i64>, rskafka::client::error::Error>>>,
    }

    impl MockProducer {
        pub fn new(
            clock: Arc<iox_time::MockProvider>,
            ret: Result<Vec<i64>, rskafka::client::error::Error>,
        ) -> Self {
            Self {
                clock,
                ret: Mutex::new(Some(ret)),
            }
        }
    }

    impl rskafka::client::producer::ProducerClient for MockProducer {
        fn produce(
            &self,
            _records: Vec<Record>,
            _compression: Compression,
        ) -> BoxFuture<'_, Result<Vec<i64>, rskafka::client::error::Error>> {
            // Jump the clock by 1s so the metrics to observe a 1s latency
            self.clock.inc(CALL_LATENCY);
            // And return the configured response
            Box::pin(async { self.ret.lock().take().expect("multiple calls to mock") })
        }
    }

    #[tokio::test]
    async fn test_produce_instrumentation_success() {
        let clock = Arc::new(iox_time::MockProvider::new(Time::MIN));
        let producer = Box::new(MockProducer::new(Arc::clone(&clock), Ok(Vec::default())));
        let metrics = metric::Registry::default();

        let wrapper =
            KafkaProducerMetrics::new(producer, KAFKA_TOPIC.to_string(), SHARD_INDEX, &metrics)
                .with_time_provider(Arc::clone(&clock));

        let record = Record {
            key: Some("bananas".into()),
            value: None,
            headers: Default::default(),
            timestamp: chrono::DateTime::<Utc>::MIN_UTC,
        };

        wrapper
            .produce(vec![record.clone()], Compression::Snappy)
            .await
            .expect("produce call should succeed");

        // Ensure the latency was correctly recorded.
        let histogram = metrics
            .get_instrument::<Metric<DurationHistogram>>("write_buffer_client_produce_duration")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[
                ("kafka_topic", KAFKA_TOPIC),
                ("kafka_partition", "42"),
                ("result", "success"),
            ]))
            .expect("failed to get observer")
            .fetch();

        assert_eq!(histogram.sample_count(), 1);
        assert_eq!(histogram.total, CALL_LATENCY);

        // Ensure the size was captured
        let histogram = metrics
            .get_instrument::<Metric<U64Histogram>>("write_buffer_client_payload_size")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[
                ("kafka_topic", KAFKA_TOPIC),
                ("kafka_partition", "42"),
            ]))
            .expect("failed to get observer")
            .fetch();

        assert_eq!(histogram.sample_count(), 1);
        assert_eq!(histogram.total, record.approximate_size() as u64);
    }

    #[tokio::test]
    async fn test_produce_instrumentation_error() {
        let clock = Arc::new(iox_time::MockProvider::new(Time::MIN));
        let producer = Box::new(MockProducer::new(
            Arc::clone(&clock),
            Err(rskafka::client::error::Error::InvalidResponse(
                "bananas".to_string(),
            )),
        ));
        let metrics = metric::Registry::default();

        let wrapper =
            KafkaProducerMetrics::new(producer, KAFKA_TOPIC.to_string(), SHARD_INDEX, &metrics)
                .with_time_provider(Arc::clone(&clock));

        wrapper
            .produce(Vec::new(), Compression::Snappy)
            .await
            .expect_err("produce call should fail");

        // Ensure the latency was correctly recorded.
        let histogram = metrics
            .get_instrument::<Metric<DurationHistogram>>("write_buffer_client_produce_duration")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[
                ("kafka_topic", KAFKA_TOPIC),
                ("kafka_partition", "42"),
                ("result", "error"),
            ]))
            .expect("failed to get observer")
            .fetch();

        assert_eq!(histogram.sample_count(), 1);
        assert_eq!(histogram.total, CALL_LATENCY);

        // Ensure the size was captured
        let histogram = metrics
            .get_instrument::<Metric<U64Histogram>>("write_buffer_client_payload_size")
            .expect("failed to read metric")
            .get_observer(&Attributes::from(&[
                ("kafka_topic", KAFKA_TOPIC),
                ("kafka_partition", "42"),
            ]))
            .expect("failed to get observer")
            .fetch();

        assert_eq!(histogram.sample_count(), 1);
        assert_eq!(histogram.total, 0);
    }
}
