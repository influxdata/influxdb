use crate::async_collections;
use influxdb3_catalog::CatalogError;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::channel::CatalogUpdateReceiver;
use influxdb3_catalog::log::{
    CatalogBatch, DatabaseCatalogOp, SoftDeleteDatabaseLog, SoftDeleteTableLog,
};
use influxdb3_catalog::resource::CatalogResource;
use influxdb3_id::{DbId, TableId};
use influxdb3_shutdown::ShutdownToken;
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::{error, info};
use std::sync::Arc;

/// Trait for clients to be notified when a database or table should be deleted.
pub trait ObjectDeleter: std::fmt::Debug + Send + Sync {
    /// Deletes a database.
    fn delete_database(&self, db_id: DbId);
    /// Deletes a table.
    fn delete_table(&self, db_id: DbId, table_id: TableId);
}

#[derive(Debug)]
pub struct DeleteManagerArgs {
    pub catalog: Arc<Catalog>,
    pub time_provider: Arc<dyn TimeProvider>,
    pub object_deleter: Option<Arc<dyn ObjectDeleter>>,
    /// The grace period after the hard delete time that the object will be removed
    /// permanently from the catalog.
    pub delete_grace_period: std::time::Duration,
}

/// Starts the delete manager, which processes hard deletion tasks for database objects.
pub fn run(
    DeleteManagerArgs {
        catalog,
        time_provider,
        object_deleter,
        delete_grace_period,
    }: DeleteManagerArgs,
    shutdown_token: ShutdownToken,
) {
    let tasks = async_collections::PriorityQueue::<Task>::new(Arc::clone(&time_provider));
    // Find all databases and tables in catalog that are marked as deleted with a hard delete time.
    for db_schema in catalog.list_db_schema() {
        if let Some(hard_delete_time) = db_schema
            .deleted
            .then(|| db_schema.hard_delete_time)
            .flatten()
        {
            if let Some(object_deleter) = &object_deleter {
                tasks.push(
                    hard_delete_time,
                    Task::NotifyDeleteDatabase {
                        db_id: db_schema.id(),
                        object_deleter: Arc::clone(object_deleter),
                    },
                );
            }
            tasks.push(
                hard_delete_time + delete_grace_period,
                Task::DeleteDatabase {
                    db_id: db_schema.id(),
                    catalog: Arc::clone(&catalog),
                },
            );
        }

        for (time, table_def) in db_schema.tables().filter_map(|td| {
            // Table is marked as deleted
            td.deleted
                // and it has a hard delete time set.
                .then(|| td.hard_delete_time.map(|t| (t, td)))
                .flatten()
        }) {
            if let Some(object_deleter) = &object_deleter {
                tasks.push(
                    time,
                    Task::NotifyDeleteTable {
                        db_id: db_schema.id(),
                        table_id: table_def.id(),
                        object_deleter: Arc::clone(object_deleter),
                    },
                );
            }
            tasks.push(
                time + delete_grace_period,
                Task::DeleteTable {
                    db_id: db_schema.id(),
                    table_id: table_def.id(),
                    catalog: Arc::clone(&catalog),
                },
            );
        }
    }

    tokio::spawn(async move {
        info!(delete_grace_period = ?delete_grace_period, "Started catalog hard deleter task.");

        let delete_manager = DeleteManager {
            tasks: tasks.clone(),
            catalog: Arc::clone(&catalog),
            object_deleter,
            delete_grace_period,
        };

        background_catalog_update(
            delete_manager,
            catalog.subscribe_to_updates("object_deleter").await,
        );

        loop {
            tokio::select! {
                task = tasks.pop() => {
                    task.execute().await;
                }
                _ = shutdown_token.wait_for_shutdown() => {
                    info!("Shutdown signal received, exiting object deleter loop.");
                    break;
                }
            }
        }
        shutdown_token.complete();
    });
}

/// Represents a task to delete a database object's data.
#[derive(Clone)]
enum Task {
    /// Notify the object_deleter that the specified database should be deleted.
    NotifyDeleteDatabase {
        db_id: DbId,
        object_deleter: Arc<dyn ObjectDeleter>,
    },
    /// Notify the object_deleter that the specified table should be deleted.
    NotifyDeleteTable {
        db_id: DbId,
        table_id: TableId,
        object_deleter: Arc<dyn ObjectDeleter>,
    },
    /// Remove the database from the catalog.
    DeleteDatabase { db_id: DbId, catalog: Arc<Catalog> },
    /// Remove the table from the catalog.
    DeleteTable {
        db_id: DbId,
        table_id: TableId,
        catalog: Arc<Catalog>,
    },
}

impl Task {
    async fn execute(self) {
        match self {
            Task::NotifyDeleteDatabase {
                db_id,
                object_deleter,
            } => {
                info!(?db_id, "Notify object_deleter to delete database.");
                object_deleter.delete_database(db_id);
            }
            Task::NotifyDeleteTable {
                db_id,
                table_id,
                object_deleter,
            } => {
                info!(?db_id, ?table_id, "Notify object_deleter to delete table.");
                object_deleter.delete_table(db_id, table_id);
            }
            Task::DeleteDatabase { db_id, catalog } => {
                info!(?db_id, "Processing delete database task.");
                match catalog.hard_delete_database(&db_id).await {
                    Err(CatalogError::NotFound) | Ok(_) => {}
                    Err(CatalogError::CannotDeleteInternalDatabase) => {
                        // This should not happen
                        error!("Rejected request to delete internal database")
                    }
                    Err(err) => {
                        error!(%db_id, ?err, "Unexpected error deleting database from catalog.");
                    }
                }
            }
            Task::DeleteTable {
                db_id,
                table_id,
                catalog,
            } => {
                info!(?db_id, ?table_id, "Processing delete table task.");
                match catalog.hard_delete_table(&db_id, &table_id).await {
                    Err(CatalogError::NotFound) | Ok(_) => {}
                    Err(err) => {
                        error!(%db_id, %table_id, ?err, "Unexpected error deleting table from catalog.");
                    }
                }
            }
        }
    }
}

struct DeleteManager {
    tasks: async_collections::PriorityQueue<Task>,
    catalog: Arc<Catalog>,
    object_deleter: Option<Arc<dyn ObjectDeleter>>,
    delete_grace_period: std::time::Duration,
}

fn background_catalog_update(
    manager: DeleteManager,
    mut subscription: CatalogUpdateReceiver,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(catalog_update) = subscription.recv().await {
            for batch in catalog_update
                .batches()
                .filter_map(CatalogBatch::as_database)
            {
                for op in batch.ops.iter() {
                    match op {
                        DatabaseCatalogOp::SoftDeleteDatabase(SoftDeleteDatabaseLog {
                            database_id,
                            hard_deletion_time: Some(hard_delete_time),
                            ..
                        }) => {
                            let time = Time::from_timestamp_nanos(*hard_delete_time);
                            if let Some(object_deleter) = manager.object_deleter.as_ref() {
                                manager.tasks.push(
                                    time,
                                    Task::NotifyDeleteDatabase {
                                        db_id: *database_id,
                                        object_deleter: Arc::clone(object_deleter),
                                    },
                                );
                            }
                            manager.tasks.push(
                                time + manager.delete_grace_period,
                                Task::DeleteDatabase {
                                    db_id: *database_id,
                                    catalog: Arc::clone(&manager.catalog),
                                },
                            );
                        }
                        DatabaseCatalogOp::SoftDeleteTable(SoftDeleteTableLog {
                            database_id,
                            table_id,
                            hard_deletion_time: Some(hard_deletion_time),
                            ..
                        }) => {
                            let time = Time::from_timestamp_nanos(*hard_deletion_time);
                            if let Some(object_deleter) = manager.object_deleter.as_ref() {
                                manager.tasks.push(
                                    time,
                                    Task::NotifyDeleteTable {
                                        db_id: *database_id,
                                        table_id: *table_id,
                                        object_deleter: Arc::clone(object_deleter),
                                    },
                                );
                            }
                            manager.tasks.push(
                                time + manager.delete_grace_period,
                                Task::DeleteTable {
                                    db_id: *database_id,
                                    table_id: *table_id,
                                    catalog: Arc::clone(&manager.catalog),
                                },
                            );
                        }
                        _ => (),
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
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

    impl ObjectDeleter for MockObjectDeleter {
        fn delete_database(&self, db_id: DbId) {
            let _ = self.db_sender.send(db_id);
        }
        fn delete_table(&self, db_id: DbId, table_id: TableId) {
            let _ = self.table_sender.send((db_id, table_id));
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
            shutdown_manager.register(),
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
            shutdown_manager.register(),
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
}
