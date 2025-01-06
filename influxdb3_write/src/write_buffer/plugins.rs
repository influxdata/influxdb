use crate::write_buffer::PluginEvent;
use crate::{write_buffer, WriteBuffer};
use influxdb3_wal::{PluginType, TriggerDefinition, TriggerSpecificationDefinition};
use observability_deps::tracing::warn;
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::broadcast::error::RecvError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("couldn't find db")]
    MissingDb,

    #[cfg(feature = "system-py")]
    #[error(transparent)]
    PyError(#[from] pyo3::PyErr),

    #[error(transparent)]
    WriteBufferError(#[from] write_buffer::Error),
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
        function_name: String,
        plugin_type: PluginType,
    ) -> crate::Result<(), write_buffer::Error>;

    async fn insert_trigger(
        &self,
        db_name: &str,
        trigger_name: String,
        plugin_name: String,
        trigger_specification: TriggerSpecificationDefinition,
    ) -> crate::Result<(), write_buffer::Error>;

    /// Starts running the trigger, which will run in the background.
    async fn run_trigger(
        &self,
        write_buffer: Arc<dyn WriteBuffer>,
        db_name: &str,
        trigger_name: &str,
    ) -> crate::Result<(), write_buffer::Error>;
}

#[cfg(feature = "system-py")]
pub(crate) fn run_plugin(
    db_name: String,
    trigger_definition: TriggerDefinition,
    mut context: PluginContext,
) {
    let trigger_plugin = TriggerPlugin {
        trigger_definition,
        db_name,
    };
    tokio::task::spawn(async move {
        trigger_plugin
            .run_plugin(&mut context)
            .await
            .expect("trigger plugin failed");
    });
}

pub(crate) struct PluginContext {
    // tokio channel for inputs
    pub(crate) trigger_rx: tokio::sync::broadcast::Receiver<PluginEvent>,
    // handler to write data back to the DB.
    pub(crate) write_buffer: Arc<dyn WriteBuffer>,
}

#[async_trait::async_trait]
trait RunnablePlugin {
    async fn process_event(
        &self,
        event: PluginEvent,
        write_buffer: Arc<dyn WriteBuffer>,
    ) -> Result<(), Error>;
    async fn run_plugin(&self, context: &mut PluginContext) -> Result<(), Error> {
        loop {
            match context.trigger_rx.recv().await {
                Err(RecvError::Closed) => {
                    break;
                }
                Err(RecvError::Lagged(_)) => {
                    warn!("plugin lagged");
                }
                Ok(event) => {
                    self.process_event(event, context.write_buffer.clone())
                        .await?;
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug)]
struct TriggerPlugin {
    trigger_definition: TriggerDefinition,
    db_name: String,
}

#[cfg(feature = "system-py")]
mod python_plugin {
    use super::*;
    use crate::Precision;
    use data_types::NamespaceName;
    use influxdb3_py_api::system_py::PyWriteBatch;
    use influxdb3_wal::WalOp;
    use iox_time::Time;
    use std::time::SystemTime;

    #[async_trait::async_trait]
    impl RunnablePlugin for TriggerPlugin {
        async fn process_event(
            &self,
            event: PluginEvent,
            write_buffer: Arc<dyn WriteBuffer>,
        ) -> Result<(), Error> {
            let Some(schema) = write_buffer.catalog().db_schema(self.db_name.as_str()) else {
                return Err(Error::MissingDb);
            };

            let mut output_lines = Vec::new();

            match event {
                PluginEvent::WriteWalContents(wal_contents) => {
                    for wal_op in &wal_contents.ops {
                        match wal_op {
                            WalOp::Write(write_batch) => {
                                let py_write_batch = PyWriteBatch {
                                    // TODO: don't clone the write batch
                                    write_batch: write_batch.clone(),
                                    schema: Arc::clone(&schema),
                                };
                                match &self.trigger_definition.trigger {
                                    TriggerSpecificationDefinition::SingleTableWalWrite {
                                        table_name,
                                    } => {
                                        output_lines.extend(py_write_batch.call_against_table(
                                            table_name,
                                            &self.trigger_definition.plugin.code,
                                            &self.trigger_definition.plugin.function_name,
                                        )?);
                                    }
                                    TriggerSpecificationDefinition::AllTablesWalWrite => {
                                        for table in schema.table_map.right_values() {
                                            output_lines.extend(
                                                py_write_batch.call_against_table(
                                                    table.as_ref(),
                                                    &self.trigger_definition.plugin.code,
                                                    &self.trigger_definition.plugin.function_name,
                                                )?,
                                            );
                                        }
                                    }
                                }
                            }
                            WalOp::Catalog(_) => {}
                            WalOp::ForcedSnapshot(_) => {}
                            WalOp::Snapshot(_) => {}
                        }
                    }
                }
            }
            if !output_lines.is_empty() {
                let ingest_time = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap();
                write_buffer
                    .write_lp(
                        NamespaceName::new(self.db_name.to_string()).unwrap(),
                        output_lines.join("\n").as_str(),
                        Time::from_timestamp_nanos(ingest_time.as_nanos() as i64),
                        false,
                        Precision::Nanosecond,
                    )
                    .await?;
            }

            Ok(())
        }
    }
}
