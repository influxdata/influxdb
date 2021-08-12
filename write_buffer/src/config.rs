use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use data_types::{
    database_rules::{DatabaseRules, WriteBufferConnection},
    server_id::ServerId,
};

use crate::{
    core::{WriteBufferError, WriteBufferReading, WriteBufferWriting},
    kafka::{KafkaBufferConsumer, KafkaBufferProducer},
    mock::{MockBufferForReading, MockBufferForWriting, MockBufferSharedState},
};

/// Prefix for mocked connections.
pub const PREFIX_MOCK: &str = "mock://";

#[derive(Debug)]
pub enum WriteBufferConfig {
    Writing(Arc<dyn WriteBufferWriting>),
    Reading(Arc<tokio::sync::Mutex<Box<dyn WriteBufferReading>>>),
}

/// Factory that creates [`WriteBufferConfig`] from [`DatabaseRules`].
#[derive(Debug)]
pub struct WriteBufferConfigFactory {
    mocks: BTreeMap<String, MockBufferSharedState>,
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
        match self.mocks.entry(name) {
            Entry::Vacant(v) => {
                v.insert(state);
            }
            Entry::Occupied(o) => {
                panic!("Mock with the name '{}' already registered", o.key());
            }
        }
    }

    fn get_mock(&self, name: &str) -> Result<MockBufferSharedState, WriteBufferError> {
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
        let name = rules.db_name();

        match rules.write_buffer_connection.as_ref() {
            Some(WriteBufferConnection::Writing(conn)) => {
                let writer: Arc<dyn WriteBufferWriting> =
                    if let Some(conn) = conn.strip_prefix(PREFIX_MOCK) {
                        let state = self.get_mock(conn)?;
                        let mock_buffer = MockBufferForWriting::new(state);
                        Arc::new(mock_buffer) as _
                    } else {
                        let kafka_buffer = KafkaBufferProducer::new(conn, name)?;
                        Arc::new(kafka_buffer) as _
                    };

                Ok(Some(WriteBufferConfig::Writing(writer)))
            }
            Some(WriteBufferConnection::Reading(conn)) => {
                let reader: Box<dyn WriteBufferReading> =
                    if let Some(conn) = conn.strip_prefix(PREFIX_MOCK) {
                        let state = self.get_mock(conn)?;
                        let mock_buffer = MockBufferForReading::new(state);
                        Box::new(mock_buffer) as _
                    } else {
                        let kafka_buffer = KafkaBufferConsumer::new(conn, server_id, name).await?;
                        Box::new(kafka_buffer) as _
                    };

                Ok(Some(WriteBufferConfig::Reading(Arc::new(
                    tokio::sync::Mutex::new(reader),
                ))))
            }
            None => Ok(None),
        }
    }
}

impl Default for WriteBufferConfigFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

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
        rules.write_buffer_connection =
            Some(WriteBufferConnection::Writing("127.0.0.1:2".to_string()));

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
        rules.write_buffer_connection = Some(WriteBufferConnection::Reading("test".to_string()));

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

        let state = MockBufferSharedState::empty_with_n_sequencers(1);
        let mock_name = "some_mock";
        factory.register_mock(mock_name.to_string(), state);

        let server_id = ServerId::try_from(1).unwrap();

        let mut rules = DatabaseRules::new(DatabaseName::new("foo").unwrap());
        rules.write_buffer_connection = Some(WriteBufferConnection::Writing(format!(
            "mock://{}",
            mock_name,
        )));

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
        rules.write_buffer_connection =
            Some(WriteBufferConnection::Writing("mock://bar".to_string()));
        let err = factory.new_config(server_id, &rules).await.unwrap_err();
        assert!(err.to_string().starts_with("Unknown mock ID:"));
    }

    #[tokio::test]
    async fn test_reading_mock() {
        let mut factory = WriteBufferConfigFactory::new();

        let state = MockBufferSharedState::empty_with_n_sequencers(1);
        let mock_name = "some_mock";
        factory.register_mock(mock_name.to_string(), state);

        let server_id = ServerId::try_from(1).unwrap();

        let mut rules = DatabaseRules::new(DatabaseName::new("foo").unwrap());
        rules.write_buffer_connection = Some(WriteBufferConnection::Reading(format!(
            "mock://{}",
            mock_name,
        )));

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
        rules.write_buffer_connection =
            Some(WriteBufferConnection::Reading("mock://bar".to_string()));
        let err = factory.new_config(server_id, &rules).await.unwrap_err();
        assert!(err.to_string().starts_with("Unknown mock ID:"));
    }

    #[test]
    #[should_panic(expected = "Mock with the name 'some_mock' already registered")]
    fn test_register_mock_twice_panics() {
        let mut factory = WriteBufferConfigFactory::new();

        let state = MockBufferSharedState::empty_with_n_sequencers(1);
        let mock_name = "some_mock";
        factory.register_mock(mock_name.to_string(), state.clone());
        factory.register_mock(mock_name.to_string(), state);
    }
}
