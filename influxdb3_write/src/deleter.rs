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
#[async_trait::async_trait]
pub trait ObjectDeleter: std::fmt::Debug + Send + Sync {
    /// Deletes a database.
    async fn delete_database(
        &self,
        db_id: DbId,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>;
    /// Deletes a table.
    async fn delete_table(
        &self,
        db_id: DbId,
        table_id: TableId,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>;
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
                match object_deleter.delete_database(db_id).await {
                    Ok(_) => {}
                    Err(err) => {
                        error!(?db_id, ?err, "Failed to delete database from object store.");
                    }
                }
            }
            Task::NotifyDeleteTable {
                db_id,
                table_id,
                object_deleter,
            } => {
                info!(?db_id, ?table_id, "Notify object_deleter to delete table.");
                match object_deleter.delete_table(db_id, table_id).await {
                    Ok(_) => {}
                    Err(err) => {
                        error!(
                            ?db_id,
                            ?table_id,
                            ?err,
                            "Failed to delete table from object store."
                        );
                    }
                }
            }
            Task::DeleteDatabase { db_id, catalog } => {
                info!(?db_id, "Processing delete database task.");
                match catalog.hard_delete_database(&db_id).await {
                    Err(CatalogError::NotFound(_)) | Ok(_) => {}
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
                    Err(CatalogError::NotFound(_)) | Ok(_) => {}
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
mod tests;
