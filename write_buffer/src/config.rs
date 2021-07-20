use std::sync::Arc;

use data_types::{
    database_rules::{DatabaseRules, WriteBufferConnection},
    server_id::ServerId,
};

use crate::{
    core::{WriteBufferError, WriteBufferReading, WriteBufferWriting},
    kafka::{KafkaBufferConsumer, KafkaBufferProducer},
};

#[derive(Debug)]
pub enum WriteBufferConfig {
    Writing(Arc<dyn WriteBufferWriting>),
    Reading(Arc<tokio::sync::Mutex<Box<dyn WriteBufferReading>>>),
}

impl WriteBufferConfig {
    pub async fn new(
        server_id: ServerId,
        rules: &DatabaseRules,
    ) -> Result<Option<Self>, WriteBufferError> {
        let name = rules.db_name();

        // Right now, the Kafka producer and consumers ar the only production implementations of the
        // `WriteBufferWriting` and `WriteBufferReading` traits. If/when there are other kinds of
        // write buffers, additional configuration will be needed to determine what kind of write
        // buffer to use here.
        match rules.write_buffer_connection.as_ref() {
            Some(WriteBufferConnection::Writing(conn)) => {
                let kafka_buffer = KafkaBufferProducer::new(conn, name)?;

                Ok(Some(Self::Writing(Arc::new(kafka_buffer) as _)))
            }
            Some(WriteBufferConnection::Reading(conn)) => {
                let kafka_buffer = KafkaBufferConsumer::new(conn, server_id, name).await?;

                Ok(Some(Self::Reading(Arc::new(tokio::sync::Mutex::new(
                    Box::new(kafka_buffer) as _,
                )))))
            }
            None => Ok(None),
        }
    }
}
