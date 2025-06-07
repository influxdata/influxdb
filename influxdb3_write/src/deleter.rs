use crate::async_collections;
use async_trait::async_trait;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::channel::CatalogUpdateReceiver;
use influxdb3_catalog::log::{CatalogBatch, DatabaseCatalogOp, SoftDeleteTableLog};
use influxdb3_catalog::resource::CatalogResource;
use influxdb3_id::{DbId, ParquetFileId, TableId};
use influxdb3_shutdown::ShutdownToken;
use iox_time::{Time, TimeProvider};
use object_store::ObjectStore;
use observability_deps::tracing::info;
use std::sync::Arc;

/// Trait for deleting database objects from object storage.
#[async_trait]
pub trait ObjectDeleter: std::fmt::Debug + Send + Sync {
    /// Deletes a database from object storage.
    async fn delete_database(&self, db_id: DbId);
    /// Deletes a table from object storage.
    async fn delete_table(&self, db_id: DbId, table_id: TableId);
    /// Deletes a parquet file from object storage.
    async fn delete_parquet_file(&self, parquet_file_id: ParquetFileId, path: String);
}

#[derive(Debug)]
pub struct DeleteManagerArgs {
    pub catalog: Arc<Catalog>,
    pub time_provider: Arc<dyn TimeProvider>,
    pub shutdown: ShutdownToken,
}

/// Starts the delete manager, which processes hard deletion tasks for database objects.
pub fn queue_hard_deletes(manager: &DeleteManager) {
    // Find all tables in catalog that are marked as deleted with a hard delete time.
    for db_schema in manager.catalog.list_db_schema() {
        // TODO(sgc): handle db_schema.deleted in the future.

        for (time, table_def) in db_schema.tables().filter_map(|td| {
            // Table is marked as deleted
            td.deleted
                // and it has a hard delete time set.
                .then(|| td.hard_delete_time.map(|t| (t, td)))
                .flatten()
        }) {
            manager.tasks.push(
                time,
                DeleteTask::Table {
                    db_id: db_schema.id(),
                    table_id: table_def.id(),
                },
            );
        }
    }
}

fn spawn_background_deleter(manager: DeleteManager) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                task = manager.tasks.pop() => {
                    match task {
                        DeleteTask::Table { db_id, table_id } => {
                            info!(?db_id, ?table_id, "Processing delete task for table.");
                            manager.object_deleter.delete_table(db_id, table_id).await;
                        }
                        DeleteTask::ParquetFile {
                            parquet_file_id, path,
                        } => {
                            info!(?parquet_file_id, ?path, "Processing delete task for table.");
                            manager.object_deleter.delete_parquet_file(parquet_file_id, path).await;
                        }
                    }
                }
                _ = manager.shutdown.wait_for_shutdown() => {
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
    Table {
        db_id: DbId,
        table_id: TableId,
    },
    ParquetFile {
        parquet_file_id: ParquetFileId,
        path: String,
    },
}

#[derive(Clone)]
pub struct DeleteManager {
    tasks: async_collections::PriorityQueue<DeleteTask>,
    catalog: Arc<Catalog>,
    object_deleter: Arc<dyn ObjectDeleter>,
    shutdown: Arc<ShutdownToken>,
}

impl std::fmt::Debug for DeleteManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteManager").finish()
    }
}

impl DeleteManager {
    pub fn new(
        DeleteManagerArgs {
            catalog,
            time_provider,
            shutdown,
        }: DeleteManagerArgs,
    ) -> Self {
        let tasks = async_collections::PriorityQueue::<DeleteTask>::new(Arc::clone(&time_provider));
        let object_store = catalog.object_store();
        Self {
            tasks,
            catalog,
            object_deleter: Arc::new(ObjectStoreDeleter { object_store }),
            shutdown: Arc::new(shutdown),
        }
    }

    pub fn spawn_background_catalog_update(&self) {
        let manager = self.clone();
        tokio::spawn(async move {
            let subscription = manager.catalog.subscribe_to_updates("object_deleter").await;
            let queuer = manager.get_queuer();
            background_catalog_update(queuer, subscription).await;
        });
    }

    pub fn spawn_background_deleter(&self) {
        spawn_background_deleter(self.clone());
    }

    pub fn queue_hard_deletes(&self) {
        queue_hard_deletes(self);
    }

    pub fn get_queuer(&self) -> DeleteTaskQueuer {
        DeleteTaskQueuer {
            tasks: self.tasks.clone(),
        }
    }
}

async fn background_catalog_update(
    queuer: DeleteTaskQueuer,
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
                            queuer.tasks.push(
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

pub struct DeleteTaskQueuer {
    tasks: async_collections::PriorityQueue<DeleteTask>,
}

impl std::fmt::Debug for DeleteTaskQueuer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteTaskQueuer").finish()
    }
}

#[derive(Debug)]
struct ObjectStoreDeleter {
    object_store: Arc<dyn ObjectStore>,
}

#[async_trait::async_trait]
impl ObjectDeleter for ObjectStoreDeleter {
    /// Deletes a database from object storage.
    async fn delete_database(&self, _db_id: DbId) {
        unimplemented!()
    }
    /// Deletes a table from object storage.
    async fn delete_table(&self, _db_id: DbId, _table_id: TableId) {
        unimplemented!()
    }
    /// Deletes a parquet file from object storage.
    async fn delete_parquet_file(&self, _parquet_file_id: ParquetFileId, _path: String) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use crate::async_collections;
    use crate::deleter::DeleteManager;

    use super::{DeleteManagerArgs, DeleteTask, ObjectDeleter};
    use influxdb3_catalog::catalog::{Catalog, HardDeletionTime};
    use influxdb3_catalog::log::FieldDataType;
    use influxdb3_catalog::resource::CatalogResource;
    use influxdb3_id::{DbId, ParquetFileId, TableId};
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
        parquet_file_sender: mpsc::UnboundedSender<(ParquetFileId, String)>,
    }

    #[allow(dead_code, reason = "used in future PR")]
    struct MockObjectDeleterWaiter {
        db_receiver: mpsc::UnboundedReceiver<DbId>,
        table_receiver: mpsc::UnboundedReceiver<(DbId, TableId)>,
        parquet_file_receiver: mpsc::UnboundedReceiver<(ParquetFileId, String)>,
    }

    impl MockObjectDeleter {
        fn new() -> (Self, MockObjectDeleterWaiter) {
            let (db_sender, db_receiver) = mpsc::unbounded_channel();
            let (table_sender, table_receiver) = mpsc::unbounded_channel();
            let (parquet_file_sender, parquet_file_receiver) = mpsc::unbounded_channel();
            (
                Self {
                    db_sender,
                    table_sender,
                    parquet_file_sender,
                },
                MockObjectDeleterWaiter {
                    db_receiver,
                    table_receiver,
                    parquet_file_receiver,
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

    impl DeleteManager {
        fn new_testing(
            DeleteManagerArgs {
                catalog,
                time_provider,
                shutdown,
            }: DeleteManagerArgs,
            object_deleter: Arc<dyn ObjectDeleter>,
        ) -> Self {
            let tasks =
                async_collections::PriorityQueue::<DeleteTask>::new(Arc::clone(&time_provider));
            let s = Self {
                tasks,
                catalog,
                object_deleter,
                shutdown: Arc::new(shutdown),
            };
            s.spawn_background_catalog_update();

            s
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
        async fn delete_parquet_file(&self, parquet_file_id: ParquetFileId, path: String) {
            let _ = self.parquet_file_sender.send((parquet_file_id, path));
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

        let manager = DeleteManager::new_testing(
            DeleteManagerArgs {
                catalog: Arc::clone(&catalog),
                time_provider: Arc::clone(&time_provider) as _,
                shutdown: shutdown_manager.register(),
            },
            Arc::new(object_deleter),
        );

        manager.queue_hard_deletes();

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
