//! gRPC clients abastraction.
//!
//! This abstraction was created for easier testing.
use std::{
    any::Any,
    sync::atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use mutable_batch::DbWrite;
use parking_lot::RwLock;

/// Generic write error.
pub type WriteError = Box<dyn std::error::Error + Send + Sync>;

/// An abstract IOx gRPC client.
#[async_trait]
pub trait GrpcClient: Sync + Send + std::fmt::Debug + 'static {
    /// Write data to the given database.
    async fn write(&self, db_name: &str, write: &DbWrite) -> Result<(), WriteError>;

    /// Cast client to [`Any`], useful for downcasting.
    fn as_any(&self) -> &dyn Any;
}

/// A real, network-driven gRPC client.
#[derive(Debug)]
pub struct RealClient {
    /// Write client for IOx.
    write_client: influxdb_iox_client::write::Client,
}

impl RealClient {
    /// Create new client from established connection.
    pub fn new(connection: influxdb_iox_client::connection::Connection) -> Self {
        Self {
            write_client: influxdb_iox_client::write::Client::new(connection),
        }
    }
}

#[async_trait]
impl GrpcClient for RealClient {
    async fn write(&self, db_name: &str, write: &DbWrite) -> Result<(), WriteError> {
        use influxdb_iox_client::write::generated_types::WriteRequest;
        use mutable_batch_pb::encode::encode_write;

        let write_request = WriteRequest {
            database_batch: Some(encode_write(db_name, write)),
        };

        // cheap, see https://docs.rs/tonic/0.4.2/tonic/client/index.html#concurrent-usage
        let mut client = self.write_client.clone();

        client
            .write_pb(write_request)
            .await
            .map_err(|e| Box::new(e) as _)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Mock client for testing.
#[derive(Debug, Default)]
pub struct MockClient {
    /// All writes recorded by this client.
    writes: RwLock<Vec<(String, DbWrite)>>,

    /// Poisen pill.
    ///
    /// If set to `true` all writes will fail.
    poisoned: AtomicBool,
}

impl MockClient {
    /// Take poison pill.
    ///
    /// All subsequent writes will fail.
    pub fn poison(&self) {
        self.poisoned.store(true, Ordering::SeqCst)
    }

    /// Get a copy of all recorded writes.
    pub fn writes(&self) -> Vec<(String, DbWrite)> {
        self.writes.read().clone()
    }

    /// Assert that writes are as expected.
    pub fn assert_writes(&self, expected: &[(String, DbWrite)]) {
        use mutable_batch::test_util::assert_writes_eq;

        let actual = self.writes();

        assert_eq!(
            actual.len(),
            expected.len(),
            "number of writes differ ({} VS {})",
            actual.len(),
            expected.len()
        );

        for ((actual_db, actual_write), (expected_db, expected_write)) in
            actual.iter().zip(expected)
        {
            assert_eq!(
                actual_db, expected_db,
                "database names differ (\"{}\" VS \"{}\")",
                actual_db, expected_db
            );
            assert_writes_eq(actual_write, expected_write);
        }
    }
}

#[async_trait]
impl GrpcClient for MockClient {
    async fn write(&self, db_name: &str, write: &DbWrite) -> Result<(), WriteError> {
        if self.poisoned.load(Ordering::SeqCst) {
            return Err("poisened".to_string().into());
        }

        self.writes
            .write()
            .push((db_name.to_string(), write.clone()));
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use mutable_batch_lp::lines_to_batches;

    use super::*;

    #[tokio::test]
    async fn test_mock() {
        let client = MockClient::default();

        let write1 = DbWrite::new(
            lines_to_batches("foo x=1 1", 0).unwrap(),
            Default::default(),
        );
        let write2 = DbWrite::new(
            lines_to_batches("foo x=2 2", 0).unwrap(),
            Default::default(),
        );
        let write3 = DbWrite::new(
            lines_to_batches("foo x=3 3", 0).unwrap(),
            Default::default(),
        );

        client.write("db1", &write1).await.unwrap();
        client.write("db2", &write1).await.unwrap();
        client.write("db1", &write2).await.unwrap();

        let expected_writes = vec![
            (String::from("db1"), write1.clone()),
            (String::from("db2"), write1.clone()),
            (String::from("db1"), write2.clone()),
        ];
        client.assert_writes(&expected_writes);

        client.poison();
        client.write("db1", &write3).await.unwrap_err();
        client.assert_writes(&expected_writes);
    }

    #[tokio::test]
    #[should_panic(expected = "number of writes differ (1 VS 0)")]
    async fn test_assert_writes_fail_count() {
        let client = MockClient::default();

        let write1 = DbWrite::new(
            lines_to_batches("foo x=1 1", 0).unwrap(),
            Default::default(),
        );

        client.write("db1", &write1).await.unwrap();

        let expected_writes = [];
        client.assert_writes(&expected_writes);
    }

    #[tokio::test]
    #[should_panic(expected = "database names differ (\"db1\" VS \"db2\")")]
    async fn test_assert_writes_fail_db_name() {
        let client = MockClient::default();

        let write = DbWrite::new(
            lines_to_batches("foo x=1 1", 0).unwrap(),
            Default::default(),
        );

        client.write("db1", &write).await.unwrap();

        let expected_writes = vec![(String::from("db2"), write)];
        client.assert_writes(&expected_writes);
    }

    #[tokio::test]
    #[should_panic(expected = "batches for table \"foo\" differ")]
    async fn test_assert_writes_fail_batch() {
        let client = MockClient::default();

        let write1 = DbWrite::new(
            lines_to_batches("foo x=1 1", 0).unwrap(),
            Default::default(),
        );
        let write2 = DbWrite::new(
            lines_to_batches("foo x=2 2", 0).unwrap(),
            Default::default(),
        );

        client.write("db1", &write1).await.unwrap();

        let expected_writes = vec![(String::from("db1"), write2)];
        client.assert_writes(&expected_writes);
    }
}
