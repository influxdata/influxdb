use super::{DeleteManagerArgs, ObjectDeleter, run};
use influxdb3_catalog::catalog::{Catalog, HardDeletionTime};
use influxdb3_catalog::log::FieldDataType;
use influxdb3_id::{DbId, TableId};
use influxdb3_shutdown::ShutdownManager;
use iox_time::{MockProvider, Time};
use std::sync::Arc;
use std::time::Duration;
use test_helpers::timeout::FutureTimeout;
use tokio::sync::mpsc;

#[derive(Debug)]
struct MockObjectDeleter {
    db_sender: mpsc::UnboundedSender<DbId>,
    table_sender: mpsc::UnboundedSender<(DbId, TableId)>,
}

#[allow(dead_code, reason = "used in future PR")]
struct MockObjectDeleterWaiter {
    db_receiver: mpsc::UnboundedReceiver<DbId>,
    table_receiver: mpsc::UnboundedReceiver<(DbId, TableId)>,
}

impl MockObjectDeleter {
    fn new() -> (Self, MockObjectDeleterWaiter) {
        let (db_sender, db_receiver) = mpsc::unbounded_channel();
        let (table_sender, table_receiver) = mpsc::unbounded_channel();
        (
            Self {
                db_sender,
                table_sender,
            },
            MockObjectDeleterWaiter {
                db_receiver,
                table_receiver,
            },
        )
    }
}

impl MockObjectDeleterWaiter {
    async fn wait_delete_database(&mut self) -> Option<DbId> {
        self.db_receiver.recv().await
    }
    async fn wait_delete_table(&mut self) -> Option<(DbId, TableId)> {
        self.table_receiver.recv().await
    }
}

#[async_trait::async_trait]
impl ObjectDeleter for MockObjectDeleter {
    async fn delete_database(
        &self,
        db_id: DbId,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.db_sender
            .send(db_id)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>)
    }
    async fn delete_table(
        &self,
        db_id: DbId,
        table_id: TableId,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.table_sender
            .send((db_id, table_id))
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>)
    }
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_delete_table() {
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let args = influxdb3_catalog::catalog::CatalogArgs::new(Duration::from_millis(10));
    let catalog = Arc::new(
        Catalog::new_in_memory_with_args("cats", Arc::clone(&time_provider) as _, args)
            .await
            .unwrap(),
    );

    catalog.create_database("foo").await.unwrap();
    let db_id = catalog.db_name_to_id("foo").unwrap();

    let new_table = async |name: &str| {
        catalog
            .create_table("foo", name, &["tag"], &[("field", FieldDataType::String)])
            .await
            .expect("create table");

        catalog
            .db_schema("foo")
            .unwrap()
            .table_name_to_id(name)
            .expect("table schema")
    };

    let t1_id = new_table("table_1").await;
    let t2_id = new_table("table_2").await;
    let t3_id = new_table("table_3").await;
    let t4_id = new_table("table_4").await;

    // Mark table_2 as deleted, which will default to a hard delete time of 10 ms from now.
    catalog
        .soft_delete_table("foo", "table_2", HardDeletionTime::Default)
        .await
        .expect("soft delete table");

    let (object_deleter, mut waiter) = MockObjectDeleter::new();
    let shutdown_manager = ShutdownManager::new_testing();

    run(
        DeleteManagerArgs {
            catalog: Arc::clone(&catalog),
            time_provider: Arc::clone(&time_provider) as _,
            object_deleter: Some(Arc::new(object_deleter)),
            delete_grace_period: Duration::from_millis(0),
        },
        shutdown_manager.register("test"),
    );

    // First check that the object deleter was not called before the hard delete time
    waiter
        .wait_delete_table()
        // We wait 15 ms, which is greater than the hard delete time of 10 ms, and the longest interval
        // the deleter would wait to retry.
        .with_timeout(Duration::from_millis(15))
        .await
        .expect_err("should not have been called");

    // Advance time to trigger the deletion.
    time_provider.inc(Duration::from_millis(15));

    // Now the object deleter should have been called to delete table_2.
    let res = waiter
        .wait_delete_table()
        .with_timeout(Duration::from_millis(15))
        .await
        .expect("should have been called")
        .unwrap();
    assert_eq!(res, (db_id, t2_id));

    // Delete a table and verify the object deleter is called.
    catalog
        .soft_delete_table("foo", "table_1", HardDeletionTime::Default)
        .await
        .expect("soft delete table");
    time_provider.inc(Duration::from_millis(15));
    let res = waiter
        .wait_delete_table()
        .with_timeout(Duration::from_millis(15))
        .await
        .expect("should have been called")
        .unwrap();
    assert_eq!(res, (db_id, t1_id));

    // Delete a table without a delay and verify the object deleter is called immediately.
    catalog
        .soft_delete_table("foo", "table_3", HardDeletionTime::Now)
        .await
        .expect("soft delete table");
    let res = waiter
        .wait_delete_table()
        .with_timeout(Duration::from_millis(15))
        .await
        .expect("should have been called")
        .unwrap();
    assert_eq!(res, (db_id, t3_id));

    // Soft-delete the table only.
    catalog
        .soft_delete_table("foo", "table_4", HardDeletionTime::Never)
        .await
        .expect("soft delete table");
    waiter
        .wait_delete_table()
        // Wait for a time that is greater than the default hard delete time of 10 ms.
        .with_timeout(Duration::from_millis(15))
        .await
        .expect_err("should be an error");

    assert!(catalog.db_schema("foo").unwrap().table_exists(&t4_id));

    shutdown_manager.shutdown();
    shutdown_manager.join().await;
}

#[test_log::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_delete_database() {
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let args = influxdb3_catalog::catalog::CatalogArgs::new(Duration::from_millis(10));
    let catalog = Arc::new(
        Catalog::new_in_memory_with_args("cats", Arc::clone(&time_provider) as _, args)
            .await
            .unwrap(),
    );

    // Create databases
    catalog.create_database("db1").await.unwrap();
    catalog.create_database("db2").await.unwrap();
    catalog.create_database("db3").await.unwrap();
    catalog.create_database("db4").await.unwrap();

    let db1_id = catalog.db_name_to_id("db1").unwrap();
    let db2_id = catalog.db_name_to_id("db2").unwrap();
    let db3_id = catalog.db_name_to_id("db3").unwrap();
    let db4_id = catalog.db_name_to_id("db4").unwrap();

    // Mark db2 as deleted, which will default to a hard delete time of 10 ms from now.
    catalog
        .soft_delete_database("db2", HardDeletionTime::Default)
        .await
        .expect("soft delete database");

    let (object_deleter, mut waiter) = MockObjectDeleter::new();
    let shutdown_manager = ShutdownManager::new_testing();

    run(
        DeleteManagerArgs {
            catalog: Arc::clone(&catalog),
            time_provider: Arc::clone(&time_provider) as _,
            object_deleter: Some(Arc::new(object_deleter)),
            delete_grace_period: Duration::from_millis(0),
        },
        shutdown_manager.register("test"),
    );

    // First check that the object deleter was not called before the hard delete time
    waiter
        .wait_delete_database()
        // We wait 15 ms, which is greater than the hard delete time of 10 ms, and the longest interval
        // the deleter would wait to retry.
        .with_timeout(Duration::from_millis(15))
        .await
        .expect_err("should not have been called");

    // Advance time to trigger the deletion.
    time_provider.inc(Duration::from_millis(15));

    // Now the object deleter should have been called to delete db2.
    let res = waiter
        .wait_delete_database()
        .with_timeout(Duration::from_millis(15))
        .await
        .expect("should have been called")
        .unwrap();
    assert_eq!(res, db2_id);

    // Delete a database and verify the object deleter is called.
    catalog
        .soft_delete_database("db1", HardDeletionTime::Default)
        .await
        .expect("soft delete database");
    time_provider.inc(Duration::from_millis(15));
    let res = waiter
        .wait_delete_database()
        .with_timeout(Duration::from_millis(15))
        .await
        .expect("should have been called")
        .unwrap();
    assert_eq!(res, db1_id);

    // Delete a database without a delay and verify the object deleter is called immediately.
    catalog
        .soft_delete_database("db3", HardDeletionTime::Now)
        .await
        .expect("soft delete database");
    let res = waiter
        .wait_delete_database()
        .with_timeout(Duration::from_millis(15))
        .await
        .expect("should have been called")
        .unwrap();
    assert_eq!(res, db3_id);

    // Soft-delete the database only.
    catalog
        .soft_delete_database("db4", HardDeletionTime::Never)
        .await
        .expect("soft delete database");
    waiter
        .wait_delete_database()
        // Wait for a time that is greater than the default hard delete time of 10 ms.
        .with_timeout(Duration::from_millis(15))
        .await
        .expect_err("should be an error");

    // Verify db4 still exists in catalog (soft-deleted only)
    assert!(catalog.db_schema_by_id(&db4_id).is_some());

    shutdown_manager.shutdown();
    shutdown_manager.join().await;
}
