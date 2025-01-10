use crate::manager::{ProcessingEngineError, ProcessingEngineManager};
#[cfg(feature = "system-py")]
use crate::plugins::PluginContext;
use anyhow::Context;
use hashbrown::HashMap;
use influxdb3_catalog::catalog;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_catalog::catalog::Error::ProcessingEngineTriggerNotFound;
use influxdb3_client::plugin_development::{WalPluginTestRequest, WalPluginTestResponse};
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_wal::{
    CatalogBatch, CatalogOp, DeletePluginDefinition, DeleteTriggerDefinition, PluginDefinition,
    PluginType, TriggerDefinition, TriggerIdentifier, TriggerSpecificationDefinition, Wal,
    WalContents, WalOp,
};
use influxdb3_write::WriteBuffer;
use iox_time::TimeProvider;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};

pub mod manager;
pub mod plugins;

#[derive(Debug)]
pub struct ProcessingEngineManagerImpl {
    plugin_dir: Option<std::path::PathBuf>,
    catalog: Arc<Catalog>,
    _write_buffer: Arc<dyn WriteBuffer>,
    _query_executor: Arc<dyn QueryExecutor>,
    time_provider: Arc<dyn TimeProvider>,
    wal: Arc<dyn Wal>,
    plugin_event_tx: Mutex<PluginChannels>,
}

#[derive(Debug, Default)]
struct PluginChannels {
    /// Map of database to trigger name to sender
    active_triggers: HashMap<String, HashMap<String, mpsc::Sender<PluginEvent>>>,
}

#[cfg(feature = "system-py")]
const PLUGIN_EVENT_BUFFER_SIZE: usize = 60;

impl PluginChannels {
    async fn shutdown(&mut self, db: String, trigger: String) {
        // create a one shot to wait for the shutdown to complete
        let (tx, rx) = oneshot::channel();
        if let Some(trigger_map) = self.active_triggers.get_mut(&db) {
            if let Some(sender) = trigger_map.remove(&trigger) {
                if sender.send(PluginEvent::Shutdown(tx)).await.is_ok() {
                    rx.await.ok();
                }
            }
        }
    }

    #[cfg(feature = "system-py")]
    fn add_trigger(&mut self, db: String, trigger: String) -> mpsc::Receiver<PluginEvent> {
        let (tx, rx) = mpsc::channel(PLUGIN_EVENT_BUFFER_SIZE);
        self.active_triggers
            .entry(db)
            .or_default()
            .insert(trigger, tx);
        rx
    }
}

impl ProcessingEngineManagerImpl {
    pub fn new(
        plugin_dir: Option<std::path::PathBuf>,
        catalog: Arc<Catalog>,
        write_buffer: Arc<dyn WriteBuffer>,
        query_executor: Arc<dyn QueryExecutor>,
        time_provider: Arc<dyn TimeProvider>,
        wal: Arc<dyn Wal>,
    ) -> Self {
        Self {
            plugin_dir,
            catalog,
            _write_buffer: write_buffer,
            _query_executor: query_executor,
            time_provider,
            wal,
            plugin_event_tx: Default::default(),
        }
    }

    pub fn read_plugin_code(&self, name: &str) -> Result<String, plugins::Error> {
        let plugin_dir = self.plugin_dir.clone().context("plugin dir not set")?;
        let path = plugin_dir.join(name);
        Ok(std::fs::read_to_string(path)?)
    }
}

#[async_trait::async_trait]
impl ProcessingEngineManager for ProcessingEngineManagerImpl {
    async fn insert_plugin(
        &self,
        db: &str,
        plugin_name: String,
        code: String,
        plugin_type: PluginType,
    ) -> Result<(), ProcessingEngineError> {
        let (db_id, db_schema) = self
            .catalog
            .db_id_and_schema(db)
            .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db.to_string()))?;

        let catalog_op = CatalogOp::CreatePlugin(PluginDefinition {
            plugin_name,
            code,
            plugin_type,
        });

        let creation_time = self.time_provider.now();
        let catalog_batch = CatalogBatch {
            time_ns: creation_time.timestamp_nanos(),
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            ops: vec![catalog_op],
        };
        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(&catalog_batch)? {
            let wal_op = WalOp::Catalog(catalog_batch);
            self.wal.write_ops(vec![wal_op]).await?;
        }
        Ok(())
    }

    async fn delete_plugin(
        &self,
        db: &str,
        plugin_name: &str,
    ) -> Result<(), ProcessingEngineError> {
        let (db_id, db_schema) = self
            .catalog
            .db_id_and_schema(db)
            .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db.to_string()))?;
        let catalog_op = CatalogOp::DeletePlugin(DeletePluginDefinition {
            plugin_name: plugin_name.to_string(),
        });
        let catalog_batch = CatalogBatch {
            time_ns: self.time_provider.now().timestamp_nanos(),
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            ops: vec![catalog_op],
        };

        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(&catalog_batch)? {
            self.wal
                .write_ops(vec![WalOp::Catalog(catalog_batch)])
                .await?;
        }
        Ok(())
    }

    async fn insert_trigger(
        &self,
        db_name: &str,
        trigger_name: String,
        plugin_name: String,
        trigger_specification: TriggerSpecificationDefinition,
        disabled: bool,
    ) -> Result<(), ProcessingEngineError> {
        let Some((db_id, db_schema)) = self.catalog.db_id_and_schema(db_name) else {
            return Err(ProcessingEngineError::DatabaseNotFound(db_name.to_string()));
        };
        let plugin = db_schema
            .processing_engine_plugins
            .get(&plugin_name)
            .ok_or_else(|| catalog::Error::ProcessingEnginePluginNotFound {
                plugin_name: plugin_name.to_string(),
                database_name: db_schema.name.to_string(),
            })?;
        let catalog_op = CatalogOp::CreateTrigger(TriggerDefinition {
            trigger_name,
            plugin_name,
            plugin: plugin.clone(),
            trigger: trigger_specification,
            disabled,
            database_name: db_name.to_string(),
        });
        let creation_time = self.time_provider.now();
        let catalog_batch = CatalogBatch {
            time_ns: creation_time.timestamp_nanos(),
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            ops: vec![catalog_op],
        };
        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(&catalog_batch)? {
            let wal_op = WalOp::Catalog(catalog_batch);
            self.wal.write_ops(vec![wal_op]).await?;
        }
        Ok(())
    }

    async fn delete_trigger(
        &self,
        db: &str,
        trigger_name: &str,
        force: bool,
    ) -> Result<(), ProcessingEngineError> {
        let (db_id, db_schema) = self
            .catalog
            .db_id_and_schema(db)
            .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db.to_string()))?;
        let catalog_op = CatalogOp::DeleteTrigger(DeleteTriggerDefinition {
            trigger_name: trigger_name.to_string(),
            force,
        });
        let catalog_batch = CatalogBatch {
            time_ns: self.time_provider.now().timestamp_nanos(),
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            ops: vec![catalog_op],
        };

        // Do this first to avoid a dangling running plugin.
        // Potential edge-case of a plugin being stopped but not deleted,
        // but should be okay given desire to force delete.
        let needs_deactivate = force
            && db_schema
                .processing_engine_triggers
                .get(trigger_name)
                .is_some_and(|trigger| !trigger.disabled);

        if needs_deactivate {
            self.deactivate_trigger(db, trigger_name).await?;
        }

        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(&catalog_batch)? {
            self.wal
                .write_ops(vec![WalOp::Catalog(catalog_batch)])
                .await?;
        }
        Ok(())
    }

    #[cfg_attr(not(feature = "system-py"), allow(unused))]
    async fn run_trigger(
        &self,
        write_buffer: Arc<dyn WriteBuffer>,
        query_executor: Arc<dyn QueryExecutor>,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError> {
        #[cfg(feature = "system-py")]
        {
            let db_schema = self
                .catalog
                .db_schema(db_name)
                .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db_name.to_string()))?;
            let trigger = db_schema
                .processing_engine_triggers
                .get(trigger_name)
                .ok_or_else(|| ProcessingEngineTriggerNotFound {
                    database_name: db_name.to_string(),
                    trigger_name: trigger_name.to_string(),
                })?
                .clone();

            let trigger_rx = self
                .plugin_event_tx
                .lock()
                .await
                .add_trigger(db_name.to_string(), trigger_name.to_string());

            let plugin_context = PluginContext {
                trigger_rx,
                write_buffer,
                query_executor,
            };
            plugins::run_plugin(db_name.to_string(), trigger, plugin_context);
        }

        Ok(())
    }

    async fn deactivate_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError> {
        let (db_id, db_schema) = self
            .catalog
            .db_id_and_schema(db_name)
            .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db_name.to_string()))?;
        let trigger = db_schema
            .processing_engine_triggers
            .get(trigger_name)
            .ok_or_else(|| ProcessingEngineTriggerNotFound {
                database_name: db_name.to_string(),
                trigger_name: trigger_name.to_string(),
            })?;
        // Already disabled, so this is a no-op
        if trigger.disabled {
            return Ok(());
        };

        let catalog_op = CatalogOp::DisableTrigger(TriggerIdentifier {
            db_name: db_name.to_string(),
            trigger_name: trigger_name.to_string(),
        });
        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(&CatalogBatch {
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            time_ns: self.time_provider.now().timestamp_nanos(),
            ops: vec![catalog_op],
        })? {
            let wal_op = WalOp::Catalog(catalog_batch);
            self.wal.write_ops(vec![wal_op]).await?;
        }

        let mut plugin_channels = self.plugin_event_tx.lock().await;
        plugin_channels
            .shutdown(db_name.to_string(), trigger_name.to_string())
            .await;

        Ok(())
    }

    async fn activate_trigger(
        &self,
        write_buffer: Arc<dyn WriteBuffer>,
        query_executor: Arc<dyn QueryExecutor>,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError> {
        let (db_id, db_schema) = self
            .catalog
            .db_id_and_schema(db_name)
            .ok_or_else(|| ProcessingEngineError::DatabaseNotFound(db_name.to_string()))?;
        let trigger = db_schema
            .processing_engine_triggers
            .get(trigger_name)
            .ok_or_else(|| ProcessingEngineTriggerNotFound {
                database_name: db_name.to_string(),
                trigger_name: trigger_name.to_string(),
            })?;
        // Already enabled, so this is a no-op
        if !trigger.disabled {
            return Ok(());
        };

        let catalog_op = CatalogOp::EnableTrigger(TriggerIdentifier {
            db_name: db_name.to_string(),
            trigger_name: trigger_name.to_string(),
        });
        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(&CatalogBatch {
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            time_ns: self.time_provider.now().timestamp_nanos(),
            ops: vec![catalog_op],
        })? {
            let wal_op = WalOp::Catalog(catalog_batch);
            self.wal.write_ops(vec![wal_op]).await?;
        }

        self.run_trigger(write_buffer, query_executor, db_name, trigger_name)
            .await?;
        Ok(())
    }

    #[cfg_attr(not(feature = "system-py"), allow(unused))]
    async fn test_wal_plugin(
        &self,
        request: WalPluginTestRequest,
        query_executor: Arc<dyn QueryExecutor>,
    ) -> Result<WalPluginTestResponse, plugins::Error> {
        #[cfg(feature = "system-py")]
        {
            // create a copy of the catalog so we don't modify the original
            let catalog = Arc::new(Catalog::from_inner(self.catalog.clone_inner()));
            let now = self.time_provider.now();

            let code = self.read_plugin_code(&request.filename)?;

            let res = plugins::run_test_wal_plugin(now, catalog, query_executor, code, request)
                .unwrap_or_else(|e| WalPluginTestResponse {
                    log_lines: vec![],
                    database_writes: Default::default(),
                    errors: vec![e.to_string()],
                });

            return Ok(res);
        }

        #[cfg(not(feature = "system-py"))]
        Err(plugins::Error::AnyhowError(anyhow::anyhow!(
            "system-py feature not enabled"
        )))
    }
}

#[allow(unused)]
pub(crate) enum PluginEvent {
    WriteWalContents(Arc<WalContents>),
    Shutdown(oneshot::Sender<()>),
}

#[cfg(test)]
mod tests {
    use crate::manager::{ProcessingEngineError, ProcessingEngineManager};
    use crate::ProcessingEngineManagerImpl;
    use data_types::NamespaceName;
    use datafusion_util::config::register_iox_object_store;
    use influxdb3_cache::last_cache::LastCacheProvider;
    use influxdb3_cache::meta_cache::MetaCacheProvider;
    use influxdb3_catalog::catalog;
    use influxdb3_internal_api::query_executor::UnimplementedQueryExecutor;
    use influxdb3_wal::{
        Gen1Duration, PluginDefinition, PluginType, TriggerSpecificationDefinition, WalConfig,
    };
    use influxdb3_write::persister::Persister;
    use influxdb3_write::write_buffer::{WriteBufferImpl, WriteBufferImplArgs};
    use influxdb3_write::{Precision, WriteBuffer};
    use iox_query::exec::{DedicatedExecutor, Executor, ExecutorConfig, IOxSessionContext};
    use iox_time::{MockProvider, Time, TimeProvider};
    use metric::Registry;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;
    use parquet_file::storage::{ParquetStorage, StorageId};
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_create_plugin() -> influxdb3_write::write_buffer::Result<()> {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let pem = setup(start_time, test_store, wal_config).await;

        pem._write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
            )
            .await?;

        let empty_udf = r#"def example(iterator, output):
                                   return"#;

        pem.insert_plugin(
            "foo",
            "my_plugin".to_string(),
            empty_udf.to_string(),
            PluginType::WalRows,
        )
        .await
        .unwrap();

        let plugin = pem
            .catalog
            .db_schema("foo")
            .expect("should have db named foo")
            .processing_engine_plugins
            .get("my_plugin")
            .unwrap()
            .clone();
        let expected = PluginDefinition {
            plugin_name: "my_plugin".to_string(),
            code: empty_udf.to_string(),
            plugin_type: PluginType::WalRows,
        };
        assert_eq!(expected, plugin);

        // confirm that creating it again is a no-op.
        pem.insert_plugin(
            "foo",
            "my_plugin".to_string(),
            empty_udf.to_string(),
            PluginType::WalRows,
        )
        .await
        .unwrap();

        // Confirm the same contents can be added to a new name.
        pem.insert_plugin(
            "foo",
            "my_second_plugin".to_string(),
            empty_udf.to_string(),
            PluginType::WalRows,
        )
        .await
        .unwrap();
        Ok(())
    }
    #[tokio::test]
    async fn test_delete_plugin() -> influxdb3_write::write_buffer::Result<()> {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let pem = setup(start_time, test_store, wal_config).await;

        // Create the DB by inserting a line.
        pem._write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
            )
            .await?;

        // First create a plugin
        pem.insert_plugin(
            "foo",
            "test_plugin".to_string(),
            "def process(iterator, output): pass".to_string(),
            PluginType::WalRows,
        )
        .await
        .unwrap();

        // Then delete it
        pem.delete_plugin("foo", "test_plugin").await.unwrap();

        // Verify plugin is gone from schema
        let schema = pem.catalog.db_schema("foo").unwrap();
        assert!(!schema.processing_engine_plugins.contains_key("test_plugin"));

        // Verify we can add a newly named plugin
        pem.insert_plugin(
            "foo",
            "test_plugin".to_string(),
            "def new_process(iterator, output): pass".to_string(),
            PluginType::WalRows,
        )
        .await
        .unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_plugin_with_active_trigger() -> influxdb3_write::write_buffer::Result<()> {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let pem = setup(start_time, test_store, wal_config).await;

        // Create the DB by inserting a line.
        pem._write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
            )
            .await?;

        // Create a plugin
        pem.insert_plugin(
            "foo",
            "test_plugin".to_string(),
            "def process(iterator, output): pass".to_string(),
            PluginType::WalRows,
        )
        .await
        .unwrap();

        // Create a trigger using the plugin
        pem.insert_trigger(
            "foo",
            "test_trigger".to_string(),
            "test_plugin".to_string(),
            TriggerSpecificationDefinition::AllTablesWalWrite,
            false,
        )
        .await
        .unwrap();

        // Try to delete the plugin - should fail because trigger exists
        let result = pem.delete_plugin("foo", "test_plugin").await;
        assert!(matches!(
            result,
            Err(ProcessingEngineError::CatalogUpdateError(catalog::Error::ProcessingEnginePluginInUse {
                database_name,
                plugin_name,
                trigger_name,
            })) if database_name == "foo" && plugin_name == "test_plugin" && trigger_name == "test_trigger"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_trigger_lifecycle() -> influxdb3_write::write_buffer::Result<()> {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let pem = setup(start_time, test_store, wal_config).await;

        // convert to Arc<WriteBuffer>
        let write_buffer: Arc<dyn WriteBuffer> = Arc::clone(&pem._write_buffer);

        // Create the DB by inserting a line.
        write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
            )
            .await?;

        // Create a plugin
        pem.insert_plugin(
            "foo",
            "test_plugin".to_string(),
            "def process(iterator, output): pass".to_string(),
            PluginType::WalRows,
        )
        .await
        .unwrap();

        // Create an enabled trigger
        pem.insert_trigger(
            "foo",
            "test_trigger".to_string(),
            "test_plugin".to_string(),
            TriggerSpecificationDefinition::AllTablesWalWrite,
            false,
        )
        .await
        .unwrap();
        // Run the trigger
        pem.run_trigger(
            Arc::clone(&write_buffer),
            Arc::clone(&pem._query_executor),
            "foo",
            "test_trigger",
        )
        .await
        .unwrap();

        // Deactivate the trigger
        let result = pem.deactivate_trigger("foo", "test_trigger").await;
        assert!(result.is_ok());

        // Verify trigger is disabled in schema
        let schema = write_buffer.catalog().db_schema("foo").unwrap();
        let trigger = schema
            .processing_engine_triggers
            .get("test_trigger")
            .unwrap();
        assert!(trigger.disabled);

        // Activate the trigger
        let result = pem
            .activate_trigger(
                Arc::clone(&write_buffer),
                Arc::clone(&pem._query_executor),
                "foo",
                "test_trigger",
            )
            .await;
        assert!(result.is_ok());

        // Verify trigger is enabled and running
        let schema = write_buffer.catalog().db_schema("foo").unwrap();
        let trigger = schema
            .processing_engine_triggers
            .get("test_trigger")
            .unwrap();
        assert!(!trigger.disabled);
        Ok(())
    }

    #[tokio::test]
    async fn test_create_disabled_trigger() -> influxdb3_write::write_buffer::Result<()> {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let pem = setup(start_time, test_store, wal_config).await;

        // Create the DB by inserting a line.
        pem._write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
            )
            .await?;

        // Create a plugin
        pem.insert_plugin(
            "foo",
            "test_plugin".to_string(),
            "def process(iterator, output): pass".to_string(),
            PluginType::WalRows,
        )
        .await
        .unwrap();

        // Create a disabled trigger
        pem.insert_trigger(
            "foo",
            "test_trigger".to_string(),
            "test_plugin".to_string(),
            TriggerSpecificationDefinition::AllTablesWalWrite,
            true,
        )
        .await
        .unwrap();

        // Verify trigger is created but disabled
        let schema = pem.catalog.db_schema("foo").unwrap();
        let trigger = schema
            .processing_engine_triggers
            .get("test_trigger")
            .unwrap();
        assert!(trigger.disabled);

        // Verify trigger is not in active triggers list
        assert!(pem.catalog.triggers().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_activate_nonexistent_trigger() -> influxdb3_write::write_buffer::Result<()> {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let pem = setup(start_time, test_store, wal_config).await;

        let write_buffer: Arc<dyn WriteBuffer> = Arc::clone(&pem._write_buffer);

        // Create the DB by inserting a line.
        write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
            )
            .await?;

        let result = pem
            .activate_trigger(
                Arc::clone(&write_buffer),
                Arc::clone(&pem._query_executor),
                "foo",
                "nonexistent_trigger",
            )
            .await;

        assert!(matches!(
            result,
            Err(ProcessingEngineError::CatalogUpdateError(catalog::Error::ProcessingEngineTriggerNotFound {
                database_name,
                trigger_name,
            })) if database_name == "foo" && trigger_name == "nonexistent_trigger"
        ));
        Ok(())
    }

    async fn setup(
        start: Time,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
    ) -> ProcessingEngineManagerImpl {
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start));
        let metric_registry = Arc::new(Registry::new());
        let persister = Arc::new(Persister::new(Arc::clone(&object_store), "test_host"));
        let catalog = Arc::new(persister.load_or_create_catalog().await.unwrap());
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _).unwrap();
        let meta_cache =
            MetaCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
                .unwrap();
        let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
            persister,
            catalog: Arc::clone(&catalog),
            last_cache,
            meta_cache,
            time_provider: Arc::clone(&time_provider),
            executor: make_exec(),
            wal_config,
            parquet_cache: None,
            metric_registry: Arc::clone(&metric_registry),
        })
        .await
        .unwrap();
        let ctx = IOxSessionContext::with_testing();
        let runtime_env = ctx.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&object_store));

        let qe = Arc::new(UnimplementedQueryExecutor);
        let wal = wbuf.wal();

        ProcessingEngineManagerImpl::new(None, catalog, wbuf, qe, time_provider, wal)
    }

    pub(crate) fn make_exec() -> Arc<Executor> {
        let metrics = Arc::new(metric::Registry::default());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let parquet_store = ParquetStorage::new(
            Arc::clone(&object_store),
            StorageId::from("test_exec_storage"),
        );
        Arc::new(Executor::new_with_config_and_executor(
            ExecutorConfig {
                target_query_partitions: NonZeroUsize::new(1).unwrap(),
                object_stores: [&parquet_store]
                    .into_iter()
                    .map(|store| (store.id(), Arc::clone(store.object_store())))
                    .collect(),
                metric_registry: Arc::clone(&metrics),
                // Default to 1gb
                mem_pool_size: 1024 * 1024 * 1024, // 1024 (b/kb) * 1024 (kb/mb) * 1024 (mb/gb)
            },
            DedicatedExecutor::new_testing(),
        ))
    }
}
