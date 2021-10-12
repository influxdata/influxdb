use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use data_types::{
    database_rules::{WriteBufferConnection, WriteBufferDirection},
    server_id::ServerId,
};
use time::TimeProvider;

use crate::{
    core::{WriteBufferError, WriteBufferReading, WriteBufferWriting},
    kafka::{KafkaBufferConsumer, KafkaBufferProducer},
    mock::{
        MockBufferForReading, MockBufferForReadingThatAlwaysErrors, MockBufferForWriting,
        MockBufferForWritingThatAlwaysErrors, MockBufferSharedState,
    },
};

#[derive(Debug)]
pub enum WriteBufferConfig {
    Writing(Arc<dyn WriteBufferWriting>),
    Reading(Arc<tokio::sync::Mutex<Box<dyn WriteBufferReading>>>),
}

#[derive(Debug, Clone)]
enum Mock {
    Normal(MockBufferSharedState),
    AlwaysFailing,
}

/// Factory that creates [`WriteBufferReading`] and [`WriteBufferWriting`]
/// from [`WriteBufferConnection`].
#[derive(Debug)]
pub struct WriteBufferConfigFactory {
    mocks: BTreeMap<String, Mock>,
    time_provider: Arc<dyn TimeProvider>,
}

impl WriteBufferConfigFactory {
    /// Create new factory w/o any mocks.
    pub fn new(time_provider: Arc<dyn TimeProvider>) -> Self {
        Self {
            mocks: Default::default(),
            time_provider,
        }
    }

    /// Registers new mock.
    ///
    /// # Panics
    /// When mock with identical name is already registered.
    pub fn register_mock(&mut self, name: String, state: MockBufferSharedState) {
        self.set_mock(name, Mock::Normal(state));
    }

    /// Registers new mock that always fail.
    ///
    /// # Panics
    /// When mock with identical name is already registered.
    pub fn register_always_fail_mock(&mut self, name: String) {
        self.set_mock(name, Mock::AlwaysFailing);
    }

    fn set_mock(&mut self, name: String, mock: Mock) {
        match self.mocks.entry(name) {
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
            .get(name)
            .cloned()
            .ok_or_else::<WriteBufferError, _>(|| format!("Unknown mock ID: {}", name).into())
    }

    /// Returns a new [`WriteBufferWriting`] for the provided [`WriteBufferConnection`]
    ///
    /// # Panics
    /// When the provided connection is not [`WriteBufferDirection::Write`]
    ///
    pub async fn new_config_write(
        &self,
        db_name: &str,
        cfg: &WriteBufferConnection,
    ) -> Result<Arc<dyn WriteBufferWriting>, WriteBufferError> {
        assert_eq!(cfg.direction, WriteBufferDirection::Write);

        let writer = match &cfg.type_[..] {
            "kafka" => {
                let kafka_buffer = KafkaBufferProducer::new(
                    &cfg.connection,
                    db_name,
                    &cfg.connection_config,
                    cfg.creation_config.as_ref(),
                    Arc::clone(&self.time_provider),
                )
                .await?;
                Arc::new(kafka_buffer) as _
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
                return Err(format!("Unknown write buffer type: {}", other).into());
            }
        };

        Ok(writer)
    }

    /// Returns a new [`WriteBufferReading`] for the provided [`WriteBufferConnection`]
    ///
    /// # Panics
    /// When the provided connection is not [`WriteBufferDirection::Read`]
    pub async fn new_config_read(
        &self,
        server_id: ServerId,
        db_name: &str,
        cfg: &WriteBufferConnection,
    ) -> Result<Box<dyn WriteBufferReading>, WriteBufferError> {
        assert_eq!(cfg.direction, WriteBufferDirection::Read);

        let reader = match &cfg.type_[..] {
            "kafka" => {
                let kafka_buffer = KafkaBufferConsumer::new(
                    &cfg.connection,
                    server_id,
                    db_name,
                    &cfg.connection_config,
                    cfg.creation_config.as_ref(),
                )
                .await?;
                Box::new(kafka_buffer) as _
            }
            "mock" => match self.get_mock(&cfg.connection)? {
                Mock::Normal(state) => {
                    let mock_buffer =
                        MockBufferForReading::new(state, cfg.creation_config.as_ref())?;
                    Box::new(mock_buffer) as _
                }
                Mock::AlwaysFailing => {
                    let mock_buffer = MockBufferForReadingThatAlwaysErrors {};
                    Box::new(mock_buffer) as _
                }
            },
            other => {
                return Err(format!("Unknown write buffer type: {}", other).into());
            }
        };

        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::TryFrom, num::NonZeroU32};

    use data_types::{database_rules::WriteBufferCreationConfig, DatabaseName};

    use crate::{
        kafka::test_utils::random_kafka_topic, maybe_skip_kafka_integration,
        mock::MockBufferSharedState,
    };

    use super::*;

    #[tokio::test]
    async fn test_writing_kafka() {
        let conn = maybe_skip_kafka_integration!();
        let time = Arc::new(time::SystemProvider::new());
        let factory = WriteBufferConfigFactory::new(time);
        let db_name = DatabaseName::try_from(random_kafka_topic()).unwrap();
        let cfg = WriteBufferConnection {
            direction: WriteBufferDirection::Write,
            type_: "kafka".to_string(),
            connection: conn,
            creation_config: Some(WriteBufferCreationConfig::default()),
            ..Default::default()
        };

        let conn = factory
            .new_config_write(db_name.as_str(), &cfg)
            .await
            .unwrap();
        assert_eq!(conn.type_name(), "kafka");
    }

    #[tokio::test]
    async fn test_reading_kafka() {
        let conn = maybe_skip_kafka_integration!();
        let time = Arc::new(time::SystemProvider::new());
        let factory = WriteBufferConfigFactory::new(time);
        let server_id = ServerId::try_from(1).unwrap();

        let db_name = DatabaseName::try_from(random_kafka_topic()).unwrap();
        let cfg = WriteBufferConnection {
            direction: WriteBufferDirection::Read,
            type_: "kafka".to_string(),
            connection: conn,
            creation_config: Some(WriteBufferCreationConfig::default()),
            ..Default::default()
        };

        let conn = factory
            .new_config_read(server_id, db_name.as_str(), &cfg)
            .await
            .unwrap();
        assert_eq!(conn.type_name(), "kafka");
    }

    #[tokio::test]
    async fn test_writing_mock() {
        let time = Arc::new(time::SystemProvider::new());
        let mut factory = WriteBufferConfigFactory::new(time);

        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
        let mock_name = "some_mock";
        factory.register_mock(mock_name.to_string(), state);

        let db_name = DatabaseName::try_from(random_kafka_topic()).unwrap();
        let cfg = WriteBufferConnection {
            direction: WriteBufferDirection::Write,
            type_: "mock".to_string(),
            connection: mock_name.to_string(),
            ..Default::default()
        };

        let conn = factory
            .new_config_write(db_name.as_str(), &cfg)
            .await
            .unwrap();
        assert_eq!(conn.type_name(), "mock");

        // will error when state is unknown
        let cfg = WriteBufferConnection {
            direction: WriteBufferDirection::Write,
            type_: "mock".to_string(),
            connection: "bar".to_string(),
            ..Default::default()
        };
        let err = factory
            .new_config_write(db_name.as_str(), &cfg)
            .await
            .unwrap_err();
        assert!(err.to_string().starts_with("Unknown mock ID:"));
    }

    #[tokio::test]
    async fn test_reading_mock() {
        let time = Arc::new(time::SystemProvider::new());
        let mut factory = WriteBufferConfigFactory::new(time);

        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
        let mock_name = "some_mock";
        factory.register_mock(mock_name.to_string(), state);

        let server_id = ServerId::try_from(1).unwrap();
        let db_name = DatabaseName::try_from(random_kafka_topic()).unwrap();
        let cfg = WriteBufferConnection {
            direction: WriteBufferDirection::Read,
            type_: "mock".to_string(),
            connection: mock_name.to_string(),
            ..Default::default()
        };

        let conn = factory
            .new_config_read(server_id, db_name.as_str(), &cfg)
            .await
            .unwrap();
        assert_eq!(conn.type_name(), "mock");

        // will error when state is unknown
        let cfg = WriteBufferConnection {
            direction: WriteBufferDirection::Read,
            type_: "mock".to_string(),
            connection: "bar".to_string(),
            ..Default::default()
        };
        let err = factory
            .new_config_read(server_id, db_name.as_str(), &cfg)
            .await
            .unwrap_err();
        assert!(err.to_string().starts_with("Unknown mock ID:"));
    }

    #[tokio::test]
    async fn test_writing_mock_failing() {
        let time = Arc::new(time::SystemProvider::new());
        let mut factory = WriteBufferConfigFactory::new(time);

        let mock_name = "some_mock";
        factory.register_always_fail_mock(mock_name.to_string());

        let db_name = DatabaseName::try_from(random_kafka_topic()).unwrap();
        let cfg = WriteBufferConnection {
            direction: WriteBufferDirection::Write,
            type_: "mock".to_string(),
            connection: mock_name.to_string(),
            ..Default::default()
        };

        let conn = factory
            .new_config_write(db_name.as_str(), &cfg)
            .await
            .unwrap();
        assert_eq!(conn.type_name(), "mock_failing");

        // will error when state is unknown
        let cfg = WriteBufferConnection {
            direction: WriteBufferDirection::Write,
            type_: "mock".to_string(),
            connection: "bar".to_string(),
            ..Default::default()
        };
        let err = factory
            .new_config_write(db_name.as_str(), &cfg)
            .await
            .unwrap_err();
        assert!(err.to_string().starts_with("Unknown mock ID:"));
    }

    #[tokio::test]
    async fn test_reading_mock_failing() {
        let time = Arc::new(time::SystemProvider::new());
        let mut factory = WriteBufferConfigFactory::new(time);

        let mock_name = "some_mock";
        factory.register_always_fail_mock(mock_name.to_string());

        let server_id = ServerId::try_from(1).unwrap();

        let db_name = DatabaseName::new("foo").unwrap();
        let cfg = WriteBufferConnection {
            direction: WriteBufferDirection::Read,
            type_: "mock".to_string(),
            connection: mock_name.to_string(),
            ..Default::default()
        };

        let conn = factory
            .new_config_read(server_id, db_name.as_str(), &cfg)
            .await
            .unwrap();
        assert_eq!(conn.type_name(), "mock_failing");

        // will error when state is unknown
        let cfg = WriteBufferConnection {
            direction: WriteBufferDirection::Read,
            type_: "mock".to_string(),
            connection: "bar".to_string(),
            ..Default::default()
        };
        let err = factory
            .new_config_read(server_id, db_name.as_str(), &cfg)
            .await
            .unwrap_err();
        assert!(err.to_string().starts_with("Unknown mock ID:"));
    }

    #[test]
    #[should_panic(expected = "Mock with the name 'some_mock' already registered")]
    fn test_register_mock_twice_panics() {
        let time = Arc::new(time::SystemProvider::new());
        let mut factory = WriteBufferConfigFactory::new(time);

        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
        let mock_name = "some_mock";
        factory.register_always_fail_mock(mock_name.to_string());
        factory.register_mock(mock_name.to_string(), state);
    }
}
