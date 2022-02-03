use std::{
    collections::BTreeMap,
    num::NonZeroU32,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use crate::{
    core::{
        test_utils::{perform_generic_tests, random_topic_name, TestAdapter, TestContext},
        WriteBufferError,
    },
    kafka::{test_utils::kafka_sequencer_options, KafkaBufferConsumer, KafkaBufferProducer},
    maybe_skip_kafka_integration,
    rskafka::{RSKafkaConsumer, RSKafkaProducer},
};

use async_trait::async_trait;
use data_types::{server_id::ServerId, write_buffer::WriteBufferCreationConfig};
use test_helpers::maybe_start_logging;
use time::TimeProvider;
use trace::RingBufferTraceCollector;

/// A struct that uses `rdkafka` to write messages to kafka
/// and `rskafka` to read messages back to ensure
/// messages written during the transition period from
/// `rdkafka` to `rskafka` can be read correctly
struct RdRsTestAdapter {
    conn: String,
}

impl RdRsTestAdapter {
    fn new(conn: String) -> Self {
        Self { conn }
    }
}

#[async_trait]
impl TestAdapter for RdRsTestAdapter {
    type Context = RdRsTestContext;

    async fn new_context_with_time(
        &self,
        n_sequencers: NonZeroU32,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self::Context {
        RdRsTestContext {
            conn: self.conn.clone(),
            database_name: random_topic_name(),
            n_sequencers,
            time_provider,
            metric_registry: metric::Registry::new(),
            trace_collector: Arc::new(RingBufferTraceCollector::new(100)),
        }
    }
}

/// A struct that uses `rskafka` to write messages to kafka
/// and `rdkafka` to read messages back, for the reason
/// explained in [RdRsTestAdapter]
struct RsRdTestAdapter {
    conn: String,
}

impl RsRdTestAdapter {
    fn new(conn: String) -> Self {
        Self { conn }
    }
}

#[async_trait]
impl TestAdapter for RsRdTestAdapter {
    type Context = RsRdTestContext;

    async fn new_context_with_time(
        &self,
        n_sequencers: NonZeroU32,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self::Context {
        RsRdTestContext {
            conn: self.conn.clone(),
            database_name: random_topic_name(),
            server_id_counter: AtomicU32::new(1),
            n_sequencers,
            time_provider,
            metric_registry: metric::Registry::new(),
            trace_collector: Arc::new(RingBufferTraceCollector::new(100)),
        }
    }
}

struct RdRsTestContext {
    conn: String,
    database_name: String,
    n_sequencers: NonZeroU32,
    time_provider: Arc<dyn TimeProvider>,
    metric_registry: metric::Registry,
    trace_collector: Arc<RingBufferTraceCollector>,
}

impl RdRsTestContext {
    fn creation_config(&self, value: bool) -> Option<WriteBufferCreationConfig> {
        value.then(|| WriteBufferCreationConfig {
            n_sequencers: self.n_sequencers,
            options: kafka_sequencer_options(),
        })
    }

    fn connection_config(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            // WARNING: Don't set `statistics.interval.ms` to a too lower value, otherwise rdkafka will become
            // overloaded and will not keep up delivering the statistics, leading to very long or infinite thread
            // blocking during process shutdown.
            ("statistics.interval.ms".to_owned(), "1000".to_owned()),
        ])
    }
}

#[async_trait]
impl TestContext for RdRsTestContext {
    type Writing = KafkaBufferProducer;

    type Reading = RSKafkaConsumer;

    async fn writing(&self, creation_config: bool) -> Result<Self::Writing, WriteBufferError> {
        KafkaBufferProducer::new(
            &self.conn,
            &self.database_name,
            &self.connection_config(),
            self.creation_config(creation_config).as_ref(),
            Arc::clone(&self.time_provider),
            &self.metric_registry,
        )
        .await
    }

    async fn reading(&self, creation_config: bool) -> Result<Self::Reading, WriteBufferError> {
        RSKafkaConsumer::new(
            self.conn.clone(),
            self.database_name.clone(),
            &BTreeMap::default(),
            self.creation_config(creation_config).as_ref(),
            Some(self.trace_collector() as Arc<_>),
        )
        .await
    }

    fn trace_collector(&self) -> Arc<RingBufferTraceCollector> {
        Arc::clone(&self.trace_collector)
    }
}

struct RsRdTestContext {
    conn: String,
    database_name: String,
    server_id_counter: AtomicU32,
    n_sequencers: NonZeroU32,
    time_provider: Arc<dyn TimeProvider>,
    metric_registry: metric::Registry,
    trace_collector: Arc<RingBufferTraceCollector>,
}

impl RsRdTestContext {
    fn creation_config(&self, value: bool) -> Option<WriteBufferCreationConfig> {
        value.then(|| WriteBufferCreationConfig {
            n_sequencers: self.n_sequencers,
            options: kafka_sequencer_options(),
        })
    }

    fn connection_config(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            // WARNING: Don't set `statistics.interval.ms` to a too lower value, otherwise rdkafka will become
            // overloaded and will not keep up delivering the statistics, leading to very long or infinite thread
            // blocking during process shutdown.
            ("statistics.interval.ms".to_owned(), "1000".to_owned()),
        ])
    }
}

#[async_trait]
impl TestContext for RsRdTestContext {
    type Writing = RSKafkaProducer;

    type Reading = KafkaBufferConsumer;

    async fn writing(&self, creation_config: bool) -> Result<Self::Writing, WriteBufferError> {
        RSKafkaProducer::new(
            self.conn.clone(),
            self.database_name.clone(),
            &BTreeMap::default(),
            self.creation_config(creation_config).as_ref(),
            Arc::clone(&self.time_provider),
            Some(self.trace_collector() as Arc<_>),
        )
        .await
    }

    async fn reading(&self, creation_config: bool) -> Result<Self::Reading, WriteBufferError> {
        let server_id = self.server_id_counter.fetch_add(1, Ordering::SeqCst);
        let server_id = ServerId::try_from(server_id).unwrap();

        KafkaBufferConsumer::new(
            &self.conn,
            server_id,
            &self.database_name,
            &self.connection_config(),
            self.creation_config(creation_config).as_ref(),
            Some(&(self.trace_collector() as Arc<_>)),
            &self.metric_registry,
        )
        .await
    }

    fn trace_collector(&self) -> Arc<RingBufferTraceCollector> {
        Arc::clone(&self.trace_collector)
    }
}

#[tokio::test]
async fn test_rdrs() {
    let conn = maybe_skip_kafka_integration!();
    maybe_start_logging();

    perform_generic_tests(RdRsTestAdapter::new(conn)).await;
}

#[tokio::test]
async fn test_rsrd() {
    let conn = maybe_skip_kafka_integration!();
    maybe_start_logging();

    perform_generic_tests(RsRdTestAdapter::new(conn)).await;
}
