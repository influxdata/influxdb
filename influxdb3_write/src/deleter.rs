use crate::async_collections;
use async_trait::async_trait;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::channel::CatalogUpdateReceiver;
use influxdb3_catalog::log::{CatalogBatch, DatabaseCatalogOp, SoftDeleteTableLog};
use influxdb3_catalog::resource::CatalogResource;
use influxdb3_id::{DbId, TableId};
use influxdb3_shutdown::ShutdownToken;
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::info;
use std::sync::Arc;

/// Trait for deleting database objects from object storage.
#[async_trait]
pub trait ObjectDeleter: std::fmt::Debug + Send + Sync {
    /// Deletes a database from object storage.
    async fn delete_database(&self, db_id: DbId);
    /// Deletes a table from object storage.
    async fn delete_table(&self, db_id: DbId, table_id: TableId);
}

#[derive(Debug)]
pub struct DeleteManagerArgs {
    catalog: Arc<Catalog>,
    time_provider: Arc<dyn TimeProvider>,
    object_deleter: Arc<dyn ObjectDeleter>,
}

/// Starts the delete manager, which processes hard deletion tasks for database objects.
pub fn run(
    DeleteManagerArgs {
        catalog,
        time_provider,
        object_deleter,
    }: DeleteManagerArgs,
    shutdown: ShutdownToken,
) {
    let tasks = async_collections::PriorityQueue::<DeleteTask>::new(Arc::clone(&time_provider));
    // Find all tables in catalog that are marked as deleted with a hard delete time.
    for db_schema in catalog.list_db_schema() {
        // TODO(sgc): handle db_schema.deleted in the future.

        for (time, table_def) in db_schema.tables().filter_map(|td| {
            // Table is marked as deleted
            td.deleted
                // and it has a hard delete time set.
                .then(|| td.hard_delete_time.map(|t| (t, td)))
                .flatten()
        }) {
            tasks.push(
                time,
                DeleteTask::Table {
                    db_id: db_schema.id(),
                    table_id: table_def.id(),
                },
            );
        }
    }

    tokio::spawn(async move {
        let delete_manager = DeleteManager {
            tasks: tasks.clone(),
        };

        background_catalog_update(
            delete_manager,
            catalog.subscribe_to_updates("object_deleter").await,
        );

        loop {
            tokio::select! {
                task = tasks.pop() => {
                    match task {
                        DeleteTask::Table { db_id, table_id } => {
                            info!(?db_id, ?table_id, "Processing delete task for table.");
                            object_deleter.delete_table(db_id, table_id).await;
                        }
                    }
                }
                _ = shutdown.wait_for_shutdown() => {
                    info!("Shutdown signal received, exiting object deleter loop.");
                    break;
                }
            }
        }
    });
}

/// Represents a task to delete a database object's data.
#[derive(Clone)]
enum DeleteTask {
    Table { db_id: DbId, table_id: TableId },
}

struct DeleteManager {
    tasks: async_collections::PriorityQueue<DeleteTask>,
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
                    #[allow(
                        clippy::single_match,
                        reason = "we're going to handle database deletion in the future"
                    )]
                    match op {
                        DatabaseCatalogOp::SoftDeleteTable(SoftDeleteTableLog {
                            database_id,
                            table_id,
                            hard_deletion_time,
                            ..
                        }) => {
                            let Some(time) = hard_deletion_time.map(Time::from_timestamp_nanos)
                            else {
                                continue;
                            };
                            manager.tasks.push(
                                time,
                                DeleteTask::Table {
                                    db_id: *database_id,
                                    table_id: *table_id,
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
    use influxdb3_catalog::resource::CatalogResource;
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
        #[allow(dead_code, reason = "used in future PR")]
        async fn wait_delete_database(&mut self) -> Option<DbId> {
            self.db_receiver.recv().await
        }
        async fn wait_delete_table(&mut self) -> Option<(DbId, TableId)> {
            self.table_receiver.recv().await
        }
    }

    #[async_trait::async_trait]
    impl ObjectDeleter for MockObjectDeleter {
        async fn delete_database(&self, db_id: DbId) {
            let _ = self.db_sender.send(db_id);
        }
        async fn delete_table(&self, db_id: DbId, table_id: TableId) {
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
        let db_id = catalog.db_schema("foo").unwrap().id();

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
                object_deleter: Arc::new(object_deleter),
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
}
