use influxdb3_client::plugin_development::{WalPluginTestRequest, WalPluginTestResponse};
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb3_wal::{PluginType, TriggerSpecificationDefinition};
use influxdb3_write::WriteBuffer;
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProcessingEngineError {
    #[error("database not found: {0}")]
    DatabaseNotFound(String),

    #[error("catalog update error: {0}")]
    CatalogUpdateError(#[from] influxdb3_catalog::catalog::Error),

    #[error("write buffer error: {0}")]
    WriteBufferError(#[from] influxdb3_write::write_buffer::Error),

    #[error("wal error: {0}")]
    WalError(#[from] influxdb3_wal::Error),
}

/// `[ProcessingEngineManager]` is used to interact with the processing engine,
/// in particular plugins and triggers.
///
#[async_trait::async_trait]
pub trait ProcessingEngineManager: Debug + Send + Sync + 'static {
    /// Inserts a plugin
    async fn insert_plugin(
        &self,
        db: &str,
        plugin_name: String,
        code: String,
        plugin_type: PluginType,
    ) -> Result<(), ProcessingEngineError>;

    async fn delete_plugin(&self, db: &str, plugin_name: &str)
        -> Result<(), ProcessingEngineError>;

    async fn insert_trigger(
        &self,
        db_name: &str,
        trigger_name: String,
        plugin_name: String,
        trigger_specification: TriggerSpecificationDefinition,
        disabled: bool,
    ) -> Result<(), ProcessingEngineError>;

    async fn delete_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
        force: bool,
    ) -> Result<(), ProcessingEngineError>;

    /// Starts running the trigger, which will run in the background.
    async fn run_trigger(
        &self,
        write_buffer: Arc<dyn WriteBuffer>,
        query_executor: Arc<dyn QueryExecutor>,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError>;

    async fn deactivate_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError>;

    async fn activate_trigger(
        &self,
        write_buffer: Arc<dyn WriteBuffer>,
        query_executor: Arc<dyn QueryExecutor>,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), ProcessingEngineError>;

    async fn test_wal_plugin(
        &self,
        request: WalPluginTestRequest,
        query_executor: Arc<dyn QueryExecutor>,
    ) -> Result<WalPluginTestResponse, crate::plugins::Error>;
}

// from the queryable_bufer
//     #[cfg(feature = "system-py")]
//     pub(crate) async fn subscribe_to_plugin_events(
//         &self,
//         trigger_name: String,
//     ) -> mpsc::Receiver<PluginEvent> {
//         let mut senders = self.plugin_event_tx.lock().await;
//
//         // TODO: should we be checking for replacements?
//         let (plugin_tx, plugin_rx) = mpsc::channel(4);
//         senders.insert(trigger_name, plugin_tx);
//         plugin_rx
//     }
//
//     /// Deactivates a running trigger by sending it a oneshot sender. It should send back a message and then immediately shut down.
//     pub(crate) async fn deactivate_trigger(
//         &self,
//         #[allow(unused)] trigger_name: String,
//     ) -> Result<(), anyhow::Error> {
//         #[cfg(feature = "system-py")]
//         {
//             let Some(sender) = self.plugin_event_tx.lock().await.remove(&trigger_name) else {
//                 anyhow::bail!("no trigger named '{}' found", trigger_name);
//             };
//             let (oneshot_tx, oneshot_rx) = oneshot::channel();
//             sender.send(PluginEvent::Shutdown(oneshot_tx)).await?;
//             oneshot_rx.await?;
//         }
//         Ok(())
//     }
//
//     async fn send_to_plugins(&self, wal_contents: &WalContents) {
//         let senders = self.plugin_event_tx.lock().await;
//         if !senders.is_empty() {
//             let wal_contents = Arc::new(wal_contents.clone());
//             for (plugin, sender) in senders.iter() {
//                 if let Err(err) = sender
//                     .send(PluginEvent::WriteWalContents(Arc::clone(&wal_contents)))
//                     .await
//                 {
//                     error!("failed to send plugin event to plugin {}: {}", plugin, err);
//                 }
//             }
//         }
//     }
