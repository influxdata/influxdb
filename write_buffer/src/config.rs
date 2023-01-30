use crate::{
    core::{WriteBufferError, WriteBufferReading, WriteBufferWriting},
    file::{FileBufferConsumer, FileBufferProducer},
    kafka::{RSKafkaConsumer, RSKafkaProducer},
    mock::{
        MockBufferForReading, MockBufferForReadingThatAlwaysErrors, MockBufferForWriting,
        MockBufferForWritingThatAlwaysErrors, MockBufferSharedState,
    },
};
use iox_time::TimeProvider;
use parking_lot::RwLock;
use std::{
    collections::{btree_map::Entry, BTreeMap},
    num::NonZeroU32,
    ops::Range,
    path::PathBuf,
    sync::Arc,
};
use trace::TraceCollector;

pub const DEFAULT_N_SHARDS: u32 = 1;

#[derive(Debug, Clone)]
enum Mock {
    Normal(MockBufferSharedState),
    AlwaysFailing,
}

/// Configures the use of a write buffer.
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct WriteBufferConnection {
    /// Which type should be used (e.g. "kafka", "mock")
    pub type_: String,

    /// Connection string, depends on [`type_`](Self::type_).
    /// When Kafka type is selected, multiple bootstrap_broker can be separated by commas.
    pub connection: String,

    /// Special configs to be applied when establishing the connection.
    ///
    /// This depends on [`type_`](Self::type_) and can configure aspects like timeouts.
    ///
    /// Note: This config should be a [`BTreeMap`] to ensure that a stable hash.
    pub connection_config: BTreeMap<String, String>,

    /// Specifies if the shards (e.g. for Kafka in form of a topic) should be automatically
    /// created if they do not existing prior to reading or writing.
    pub creation_config: Option<WriteBufferCreationConfig>,
}

impl Default for WriteBufferConnection {
    fn default() -> Self {
        Self {
            type_: "unspecified".to_string(),
            connection: Default::default(),
            connection_config: Default::default(),
            creation_config: Default::default(),
        }
    }
}

/// Configs shard auto-creation for write buffers.
///
/// What that means depends on the used write buffer, e.g. for Kafka this will create a new topic w/
/// [`n_shards`](Self::n_shards) partitions.
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct WriteBufferCreationConfig {
    /// Number of shards.
    ///
    /// How they are implemented depends on [type](WriteBufferConnection::type_), e.g. for Kafka
    /// this is mapped to the number of partitions.
    pub n_shards: NonZeroU32,

    /// Special configs to by applied when shards are created.
    ///
    /// This depends on [type](WriteBufferConnection::type_) and can setup parameters like
    /// retention policy.
    ///
    /// Note: This config should be a [`BTreeMap`] to ensure that a stable hash.
    pub options: BTreeMap<String, String>,
}

impl Default for WriteBufferCreationConfig {
    fn default() -> Self {
        Self {
            n_shards: NonZeroU32::try_from(DEFAULT_N_SHARDS).unwrap(),
            options: Default::default(),
        }
    }
}

/// Factory that creates [`WriteBufferReading`] and [`WriteBufferWriting`]
/// from [`WriteBufferConnection`].
#[derive(Debug)]
pub struct WriteBufferConfigFactory {
    mocks: RwLock<BTreeMap<String, Mock>>,
    time_provider: Arc<dyn TimeProvider>,
    #[allow(dead_code)] // this field is only used in optionally-compiled kafka code
    metric_registry: Arc<metric::Registry>,
}

impl WriteBufferConfigFactory {
    /// Create new factory w/o any mocks.
    pub fn new(
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: Arc<metric::Registry>,
    ) -> Self {
        Self {
            mocks: Default::default(),
            time_provider,
            metric_registry,
        }
    }

    /// Registers new mock.
    ///
    /// # Panics
    /// When mock with identical name is already registered.
    pub fn register_mock(&self, name: String, state: MockBufferSharedState) {
        self.set_mock(name, Mock::Normal(state));
    }

    /// Registers new mock that always fail.
    ///
    /// # Panics
    /// When mock with identical name is already registered.
    pub fn register_always_fail_mock(&self, name: String) {
        self.set_mock(name, Mock::AlwaysFailing);
    }

    fn set_mock(&self, name: String, mock: Mock) {
        let mut mocks = self.mocks.write();
        match mocks.entry(name) {
            Entry::Vacant(v) => {
                v.insert(mock);
            }
            Entry::Occupied(o) => {
                panic!("Mock with the name '{}' already registered", o.key());
            }
        }
    }

    fn get_mock(&self, name: &str) -> Result<Mock, WriteBufferError> {
        self.mocks
            .read()
            .get(name)
            .cloned()
            .ok_or_else::<WriteBufferError, _>(|| format!("Unknown mock ID: {name}").into())
    }

    /// Returns a new [`WriteBufferWriting`] for the provided [`WriteBufferConnection`]
    ///
    pub async fn new_config_write(
        &self,
        db_name: &str,
        partitions: Option<Range<i32>>,
        trace_collector: Option<&Arc<dyn TraceCollector>>,
        cfg: &WriteBufferConnection,
    ) -> Result<Arc<dyn WriteBufferWriting>, WriteBufferError> {
        let writer = match &cfg.type_[..] {
            "file" => {
                let root = PathBuf::from(&cfg.connection);
                let file_buffer = FileBufferProducer::new(
                    &root,
                    db_name,
                    cfg.creation_config.as_ref(),
                    Arc::clone(&self.time_provider),
                )
                .await?;
                Arc::new(file_buffer) as _
            }
            "kafka" => {
                let rskafa_buffer = RSKafkaProducer::new(
                    cfg.connection.clone(),
                    db_name.to_owned(),
                    &cfg.connection_config,
                    Arc::clone(&self.time_provider),
                    cfg.creation_config.as_ref(),
                    partitions,
                    trace_collector.map(Arc::clone),
                    &self.metric_registry,
                )
                .await?;
                Arc::new(rskafa_buffer) as _
            }
            "mock" => match self.get_mock(&cfg.connection)? {
                Mock::Normal(state) => {
                    let mock_buffer = MockBufferForWriting::new(
                        state,
                        cfg.creation_config.as_ref(),
                        Arc::clone(&self.time_provider),
                    )?;
                    Arc::new(mock_buffer) as _
                }
                Mock::AlwaysFailing => {
                    let mock_buffer = MockBufferForWritingThatAlwaysErrors {};
                    Arc::new(mock_buffer) as _
                }
            },
            other => {
                return Err(format!("Unknown write buffer type: {other}").into());
            }
        };

        Ok(writer)
    }

    /// Returns a new [`WriteBufferReading`] for the provided [`WriteBufferConnection`]
    pub async fn new_config_read(
        &self,
        db_name: &str,
        partitions: Option<Range<i32>>,
        trace_collector: Option<&Arc<dyn TraceCollector>>,
        cfg: &WriteBufferConnection,
    ) -> Result<Arc<dyn WriteBufferReading>, WriteBufferError> {
        let reader = match &cfg.type_[..] {
            "file" => {
                let root = PathBuf::from(&cfg.connection);
                let file_buffer = FileBufferConsumer::new(
                    &root,
                    db_name,
                    cfg.creation_config.as_ref(),
                    trace_collector,
                )
                .await?;
                Arc::new(file_buffer) as _
            }
            "kafka" => {
                let rskafka_buffer = RSKafkaConsumer::new(
                    cfg.connection.clone(),
                    db_name.to_owned(),
                    &cfg.connection_config,
                    cfg.creation_config.as_ref(),
                    partitions,
                    trace_collector.map(Arc::clone),
                )
                .await?;
                Arc::new(rskafka_buffer) as _
            }
            "mock" => match self.get_mock(&cfg.connection)? {
                Mock::Normal(state) => {
                    let mock_buffer =
                        MockBufferForReading::new(state, cfg.creation_config.as_ref())?;
                    Arc::new(mock_buffer) as _
                }
                Mock::AlwaysFailing => {
                    let mock_buffer = MockBufferForReadingThatAlwaysErrors {};
                    Arc::new(mock_buffer) as _
                }
            },
            other => {
                return Err(format!("Unknown write buffer type: {other}").into());
            }
        };

        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::test_utils::random_topic_name, maybe_skip_kafka_integration,
        mock::MockBufferSharedState,
    };
    use data_types::NamespaceName;
    use std::{convert::TryFrom, num::NonZeroU32};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_writing_file() {
        let root = TempDir::new().unwrap();
        let factory = factory();
        let db_name = NamespaceName::try_from("foo").unwrap();
        let cfg = WriteBufferConnection {
            type_: "file".to_string(),
            connection: root.path().display().to_string(),
            creation_config: Some(WriteBufferCreationConfig::default()),
            ..Default::default()
        };

        let conn = factory
            .new_config_write(db_name.as_str(), None, None, &cfg)
            .await
            .unwrap();
        assert_eq!(conn.type_name(), "file");
    }

    #[tokio::test]
    async fn test_reading_file() {
        let root = TempDir::new().unwrap();
        let factory = factory();
        let db_name = NamespaceName::try_from("foo").unwrap();
        let cfg = WriteBufferConnection {
            type_: "file".to_string(),
            connection: root.path().display().to_string(),
            creation_config: Some(WriteBufferCreationConfig::default()),
            ..Default::default()
        };

        let conn = factory
            .new_config_read(db_name.as_str(), None, None, &cfg)
            .await
            .unwrap();
        assert_eq!(conn.type_name(), "file");
    }

    #[tokio::test]
    async fn test_writing_mock() {
        let factory = factory();

        let state = MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(1).unwrap());
        let mock_name = "some_mock";
        factory.register_mock(mock_name.to_string(), state);

        let db_name = NamespaceName::try_from(random_topic_name()).unwrap();
        let cfg = WriteBufferConnection {
            type_: "mock".to_string(),
            connection: mock_name.to_string(),
            ..Default::default()
        };

        let conn = factory
            .new_config_write(db_name.as_str(), None, None, &cfg)
            .await
            .unwrap();
        assert_eq!(conn.type_name(), "mock");

        // will error when state is unknown
        let cfg = WriteBufferConnection {
            type_: "mock".to_string(),
            connection: "bar".to_string(),
            ..Default::default()
        };
        let err = factory
            .new_config_write(db_name.as_str(), None, None, &cfg)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Unknown mock ID:"));
    }

    #[tokio::test]
    async fn test_reading_mock() {
        let factory = factory();

        let state = MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(1).unwrap());
        let mock_name = "some_mock";
        factory.register_mock(mock_name.to_string(), state);

        let db_name = NamespaceName::try_from(random_topic_name()).unwrap();
        let cfg = WriteBufferConnection {
            type_: "mock".to_string(),
            connection: mock_name.to_string(),
            ..Default::default()
        };

        let conn = factory
            .new_config_read(db_name.as_str(), None, None, &cfg)
            .await
            .unwrap();
        assert_eq!(conn.type_name(), "mock");

        // will error when state is unknown
        let cfg = WriteBufferConnection {
            type_: "mock".to_string(),
            connection: "bar".to_string(),
            ..Default::default()
        };
        let err = factory
            .new_config_read(db_name.as_str(), None, None, &cfg)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Unknown mock ID:"));
    }

    #[tokio::test]
    async fn test_writing_mock_failing() {
        let factory = factory();

        let mock_name = "some_mock";
        factory.register_always_fail_mock(mock_name.to_string());

        let db_name = NamespaceName::try_from(random_topic_name()).unwrap();
        let cfg = WriteBufferConnection {
            type_: "mock".to_string(),
            connection: mock_name.to_string(),
            ..Default::default()
        };

        let conn = factory
            .new_config_write(db_name.as_str(), None, None, &cfg)
            .await
            .unwrap();
        assert_eq!(conn.type_name(), "mock_failing");

        // will error when state is unknown
        let cfg = WriteBufferConnection {
            type_: "mock".to_string(),
            connection: "bar".to_string(),
            ..Default::default()
        };
        let err = factory
            .new_config_write(db_name.as_str(), None, None, &cfg)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Unknown mock ID:"));
    }

    #[tokio::test]
    async fn test_reading_mock_failing() {
        let factory = factory();

        let mock_name = "some_mock";
        factory.register_always_fail_mock(mock_name.to_string());

        let db_name = NamespaceName::new("foo").unwrap();
        let cfg = WriteBufferConnection {
            type_: "mock".to_string(),
            connection: mock_name.to_string(),
            ..Default::default()
        };

        let conn = factory
            .new_config_read(db_name.as_str(), None, None, &cfg)
            .await
            .unwrap();
        assert_eq!(conn.type_name(), "mock_failing");

        // will error when state is unknown
        let cfg = WriteBufferConnection {
            type_: "mock".to_string(),
            connection: "bar".to_string(),
            ..Default::default()
        };
        let err = factory
            .new_config_read(db_name.as_str(), None, None, &cfg)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Unknown mock ID:"));
    }

    #[test]
    #[should_panic(expected = "Mock with the name 'some_mock' already registered")]
    fn test_register_mock_twice_panics() {
        let factory = factory();

        let state = MockBufferSharedState::empty_with_n_shards(NonZeroU32::try_from(1).unwrap());
        let mock_name = "some_mock";
        factory.register_always_fail_mock(mock_name.to_string());
        factory.register_mock(mock_name.to_string(), state);
    }

    fn factory() -> WriteBufferConfigFactory {
        let time = Arc::new(iox_time::SystemProvider::new());
        let registry = Arc::new(metric::Registry::new());
        WriteBufferConfigFactory::new(time, registry)
    }

    #[tokio::test]
    async fn test_writing_kafka() {
        let conn = maybe_skip_kafka_integration!();
        let factory = factory();
        let db_name = NamespaceName::try_from(random_topic_name()).unwrap();
        let cfg = WriteBufferConnection {
            type_: "kafka".to_string(),
            connection: conn,
            creation_config: Some(WriteBufferCreationConfig::default()),
            ..Default::default()
        };

        let conn = factory
            .new_config_write(db_name.as_str(), None, None, &cfg)
            .await
            .unwrap();
        assert_eq!(conn.type_name(), "kafka");
    }

    #[tokio::test]
    async fn test_reading_kafka() {
        let conn = maybe_skip_kafka_integration!();
        let factory = factory();

        let db_name = NamespaceName::try_from(random_topic_name()).unwrap();
        let cfg = WriteBufferConnection {
            type_: "kafka".to_string(),
            connection: conn,
            creation_config: Some(WriteBufferCreationConfig::default()),
            ..Default::default()
        };

        let conn = factory
            .new_config_read(db_name.as_str(), None, None, &cfg)
            .await
            .unwrap();
        assert_eq!(conn.type_name(), "kafka");
    }
}
