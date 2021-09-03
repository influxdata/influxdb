use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use data_types::{
    database_rules::{DatabaseRules, WriteBufferConnection, WriteBufferDirection},
    server_id::ServerId,
};

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

/// Factory that creates [`WriteBufferConfig`] from [`DatabaseRules`].
#[derive(Debug)]
pub struct WriteBufferConfigFactory {
    mocks: BTreeMap<String, Mock>,
}

impl WriteBufferConfigFactory {
    /// Create new factory w/o any mocks.
    pub fn new() -> Self {
        Self {
            mocks: Default::default(),
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

    /// Create new config.
    pub async fn new_config(
        &self,
        server_id: ServerId,
        rules: &DatabaseRules,
    ) -> Result<Option<WriteBufferConfig>, WriteBufferError> {
        let db_name = rules.db_name();

        let cfg = match rules.write_buffer_connection.as_ref() {
            Some(cfg) => match cfg.direction {
                WriteBufferDirection::Write => Some(WriteBufferConfig::Writing(
                    self.new_config_write(db_name, cfg).await?,
                )),
                WriteBufferDirection::Read => Some(WriteBufferConfig::Reading(Arc::new(
                    tokio::sync::Mutex::new(self.new_config_read(server_id, db_name, cfg).await?),
                ))),
            },
            None => None,
        };

        Ok(cfg)
    }

    async fn new_config_write(
        &self,
        db_name: &str,
        cfg: &WriteBufferConnection,
    ) -> Result<Arc<dyn WriteBufferWriting>, WriteBufferError> {
        assert_eq!(cfg.direction, WriteBufferDirection::Write);

        let writer = match &cfg.type_[..] {
            "kafka" => {
                let kafka_buffer =
                    KafkaBufferProducer::new(&cfg.connection, db_name, &cfg.connection_config)?;
                Arc::new(kafka_buffer) as _
            }
            "mock" => match self.get_mock(&cfg.connection)? {
                Mock::Normal(state) => {
                    let mock_buffer = MockBufferForWriting::new(state);
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

    async fn new_config_read(
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
                )
                .await?;
                Box::new(kafka_buffer) as _
            }
            "mock" => match self.get_mock(&cfg.connection)? {
                Mock::Normal(state) => {
                    let mock_buffer = MockBufferForReading::new(state);
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

impl Default for WriteBufferConfigFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::TryFrom, num::NonZeroU32};

    use data_types::DatabaseName;

    use crate::mock::MockBufferSharedState;

    use super::*;

    #[tokio::test]
    async fn test_none() {
        let factory = WriteBufferConfigFactory::new();

        let server_id = ServerId::try_from(1).unwrap();

        let mut rules = DatabaseRules::new(DatabaseName::new("foo").unwrap());
        rules.write_buffer_connection = None;

        assert!(factory
            .new_config(server_id, &rules)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_writing_kafka() {
        let factory = WriteBufferConfigFactory::new();

        let server_id = ServerId::try_from(1).unwrap();

        let mut rules = DatabaseRules::new(DatabaseName::new("foo").unwrap());
        rules.write_buffer_connection = Some(WriteBufferConnection {
            direction: WriteBufferDirection::Write,
            type_: "kafka".to_string(),
            connection: "127.0.0.1:2".to_string(),
            ..Default::default()
        });

        if let WriteBufferConfig::Writing(conn) = factory
            .new_config(server_id, &rules)
            .await
            .unwrap()
            .unwrap()
        {
            assert_eq!(conn.type_name(), "kafka");
        } else {
            panic!("not a writing connection");
        }
    }

    #[tokio::test]
    #[ignore = "waits forever to connect until https://github.com/influxdata/influxdb_iox/issues/2189 is solved"]
    async fn test_reading_kafka() {
        let factory = WriteBufferConfigFactory::new();

        let server_id = ServerId::try_from(1).unwrap();

        let mut rules = DatabaseRules::new(DatabaseName::new("foo").unwrap());
        rules.write_buffer_connection = Some(WriteBufferConnection {
            direction: WriteBufferDirection::Read,
            type_: "kafka".to_string(),
            connection: "127.0.0.1:2".to_string(),
            ..Default::default()
        });

        if let WriteBufferConfig::Reading(conn) = factory
            .new_config(server_id, &rules)
            .await
            .unwrap()
            .unwrap()
        {
            let conn = conn.lock().await;
            assert_eq!(conn.type_name(), "kafka");
        } else {
            panic!("not a reading connection");
        }
    }

    #[tokio::test]
    async fn test_writing_mock() {
        let mut factory = WriteBufferConfigFactory::new();

        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
        let mock_name = "some_mock";
        factory.register_mock(mock_name.to_string(), state);

        let server_id = ServerId::try_from(1).unwrap();

        let mut rules = DatabaseRules::new(DatabaseName::new("foo").unwrap());
        rules.write_buffer_connection = Some(WriteBufferConnection {
            direction: WriteBufferDirection::Write,
            type_: "mock".to_string(),
            connection: mock_name.to_string(),
            ..Default::default()
        });

        if let WriteBufferConfig::Writing(conn) = factory
            .new_config(server_id, &rules)
            .await
            .unwrap()
            .unwrap()
        {
            assert_eq!(conn.type_name(), "mock");
        } else {
            panic!("not a writing connection");
        }

        // will error when state is unknown
        rules.write_buffer_connection = Some(WriteBufferConnection {
            direction: WriteBufferDirection::Write,
            type_: "mock".to_string(),
            connection: "bar".to_string(),
            ..Default::default()
        });
        let err = factory.new_config(server_id, &rules).await.unwrap_err();
        assert!(err.to_string().starts_with("Unknown mock ID:"));
    }

    #[tokio::test]
    async fn test_reading_mock() {
        let mut factory = WriteBufferConfigFactory::new();

        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
        let mock_name = "some_mock";
        factory.register_mock(mock_name.to_string(), state);

        let server_id = ServerId::try_from(1).unwrap();

        let mut rules = DatabaseRules::new(DatabaseName::new("foo").unwrap());
        rules.write_buffer_connection = Some(WriteBufferConnection {
            direction: WriteBufferDirection::Read,
            type_: "mock".to_string(),
            connection: mock_name.to_string(),
            ..Default::default()
        });

        if let WriteBufferConfig::Reading(conn) = factory
            .new_config(server_id, &rules)
            .await
            .unwrap()
            .unwrap()
        {
            let conn = conn.lock().await;
            assert_eq!(conn.type_name(), "mock");
        } else {
            panic!("not a reading connection");
        }

        // will error when state is unknown
        rules.write_buffer_connection = Some(WriteBufferConnection {
            direction: WriteBufferDirection::Read,
            type_: "mock".to_string(),
            connection: "bar".to_string(),
            ..Default::default()
        });
        let err = factory.new_config(server_id, &rules).await.unwrap_err();
        assert!(err.to_string().starts_with("Unknown mock ID:"));
    }

    #[tokio::test]
    async fn test_writing_mock_failing() {
        let mut factory = WriteBufferConfigFactory::new();

        let mock_name = "some_mock";
        factory.register_always_fail_mock(mock_name.to_string());

        let server_id = ServerId::try_from(1).unwrap();

        let mut rules = DatabaseRules::new(DatabaseName::new("foo").unwrap());
        rules.write_buffer_connection = Some(WriteBufferConnection {
            direction: WriteBufferDirection::Write,
            type_: "mock".to_string(),
            connection: mock_name.to_string(),
            ..Default::default()
        });

        if let WriteBufferConfig::Writing(conn) = factory
            .new_config(server_id, &rules)
            .await
            .unwrap()
            .unwrap()
        {
            assert_eq!(conn.type_name(), "mock_failing");
        } else {
            panic!("not a writing connection");
        }

        // will error when state is unknown
        rules.write_buffer_connection = Some(WriteBufferConnection {
            direction: WriteBufferDirection::Write,
            type_: "mock".to_string(),
            connection: "bar".to_string(),
            ..Default::default()
        });
        let err = factory.new_config(server_id, &rules).await.unwrap_err();
        assert!(err.to_string().starts_with("Unknown mock ID:"));
    }

    #[tokio::test]
    async fn test_reading_mock_failing() {
        let mut factory = WriteBufferConfigFactory::new();

        let mock_name = "some_mock";
        factory.register_always_fail_mock(mock_name.to_string());

        let server_id = ServerId::try_from(1).unwrap();

        let mut rules = DatabaseRules::new(DatabaseName::new("foo").unwrap());
        rules.write_buffer_connection = Some(WriteBufferConnection {
            direction: WriteBufferDirection::Read,
            type_: "mock".to_string(),
            connection: mock_name.to_string(),
            ..Default::default()
        });

        if let WriteBufferConfig::Reading(conn) = factory
            .new_config(server_id, &rules)
            .await
            .unwrap()
            .unwrap()
        {
            let conn = conn.lock().await;
            assert_eq!(conn.type_name(), "mock_failing");
        } else {
            panic!("not a reading connection");
        }

        // will error when state is unknown
        rules.write_buffer_connection = Some(WriteBufferConnection {
            direction: WriteBufferDirection::Read,
            type_: "mock".to_string(),
            connection: "bar".to_string(),
            ..Default::default()
        });
        let err = factory.new_config(server_id, &rules).await.unwrap_err();
        assert!(err.to_string().starts_with("Unknown mock ID:"));
    }

    #[test]
    #[should_panic(expected = "Mock with the name 'some_mock' already registered")]
    fn test_register_mock_twice_panics() {
        let mut factory = WriteBufferConfigFactory::new();

        let state =
            MockBufferSharedState::empty_with_n_sequencers(NonZeroU32::try_from(1).unwrap());
        let mock_name = "some_mock";
        factory.register_always_fail_mock(mock_name.to_string());
        factory.register_mock(mock_name.to_string(), state);
    }
}
