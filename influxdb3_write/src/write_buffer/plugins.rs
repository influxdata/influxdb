use crate::write_buffer::PluginEvent;
use crate::{write_buffer, WriteBuffer};
use influxdb3_client::plugin_test::{WalPluginTestRequest, WalPluginTestResponse};
use influxdb3_wal::{PluginType, TriggerDefinition, TriggerSpecificationDefinition};
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub enum Error {
    #[error("couldn't find db")]
    MissingDb,

    #[cfg(feature = "system-py")]
    #[error(transparent)]
    PyError(#[from] pyo3::PyErr),

    #[error(transparent)]
    WriteBufferError(#[from] write_buffer::Error),

    #[error("failed to send shutdown message back")]
    FailedToShutdown,

    #[error(transparent)]
    AnyhowError(#[from] anyhow::Error),
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

    async fn delete_plugin(
        &self,
        db: &str,
        plugin_name: &str,
    ) -> crate::Result<(), write_buffer::Error>;

    async fn insert_trigger(
        &self,
        db_name: &str,
        trigger_name: String,
        plugin_name: String,
        trigger_specification: TriggerSpecificationDefinition,
        disabled: bool,
    ) -> crate::Result<(), write_buffer::Error>;

    async fn delete_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
        force: bool,
    ) -> crate::Result<(), write_buffer::Error>;

    /// Starts running the trigger, which will run in the background.
    async fn run_trigger(
        &self,
        write_buffer: Arc<dyn WriteBuffer>,
        db_name: &str,
        trigger_name: &str,
    ) -> crate::Result<(), write_buffer::Error>;

    async fn deactivate_trigger(
        &self,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), write_buffer::Error>;

    async fn activate_trigger(
        &self,
        write_buffer: Arc<dyn WriteBuffer>,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), write_buffer::Error>;

    async fn test_wal_plugin(
        &self,
        request: WalPluginTestRequest,
    ) -> crate::Result<WalPluginTestResponse, write_buffer::Error>;
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
    pub(crate) trigger_rx: mpsc::Receiver<PluginEvent>,
    // handler to write data back to the DB.
    pub(crate) write_buffer: Arc<dyn WriteBuffer>,
}

#[async_trait::async_trait]
trait RunnablePlugin {
    // Returns true if it should exit
    async fn process_event(
        &self,
        event: PluginEvent,
        write_buffer: Arc<dyn WriteBuffer>,
    ) -> Result<bool, Error>;
    async fn run_plugin(&self, context: &mut PluginContext) -> Result<(), Error> {
        while let Some(event) = context.trigger_rx.recv().await {
            if self
                .process_event(event, context.write_buffer.clone())
                .await?
            {
                break;
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
        ) -> Result<bool, Error> {
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
                        }
                    }
                }
                PluginEvent::Shutdown(sender) => {
                    sender.send(()).map_err(|_| Error::FailedToShutdown)?;
                    return Ok(true);
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

            Ok(false)
        }
    }
}

#[cfg(feature = "system-py")]
pub(crate) fn run_test_wal_plugin(
    now_time: iox_time::Time,
    catalog: Arc<influxdb3_catalog::catalog::Catalog>,
    code: String,
    request: WalPluginTestRequest,
) -> Result<WalPluginTestResponse, Error> {
    use crate::write_buffer::validator::WriteValidator;
    use crate::Precision;
    use data_types::NamespaceName;
    use influxdb3_wal::Gen1Duration;

    // parse the lp into a write batch
    let namespace = NamespaceName::new("_testdb").unwrap();
    let validator =
        WriteValidator::initialize(namespace, Arc::clone(&catalog), now_time.timestamp_nanos())?;
    let data = validator.v1_parse_lines_and_update_schema(
        &request.input_lp.unwrap(),
        false,
        now_time,
        Precision::Nanosecond,
    )?;
    let data = data.convert_lines_to_buffer(Gen1Duration::new_1m());
    let db = catalog.db_schema("_testdb").unwrap();

    let log_lines = influxdb3_py_api::system_py::execute_python_with_batch(
        &code,
        &data.valid_data,
        db,
        catalog,
    )?;
    Ok(WalPluginTestResponse {
        log_lines,
        database_writes: Default::default(),
        errors: vec![],
    })
}

#[cfg(feature = "system-py")]
#[cfg(test)]
mod tests {
    use super::*;
    use influxdb3_catalog::catalog::Catalog;
    use iox_time::Time;

    #[test]
    fn test_wal_plugin() {
        let now = Time::from_timestamp_nanos(1);
        let catalog = Catalog::new("foo".into(), "bar".into());
        let code = r#"
for table_batch in table_batches:
    api.info("table: " + table_batch["table_name"])

    for row in table_batch["rows"]:
        api.info("row: " + str(row))

api.info("done")
        ""#;

        let request = WalPluginTestRequest {
            name: "test".into(),
            input_lp: Some("cpu,host=A,region=west usage=1i,system=23.2 100".into()),
            input_file: None,
            save_output_to_file: None,
            validate_output_file: None,
        };

        let reesponse =
            run_test_wal_plugin(now, Arc::new(catalog), code.to_string(), request).unwrap();

        let expected_log_lines = vec![
            "table: cpu",
            "row: {\"host\": \"A\", \"region\": \"west\", \"usage\": 1, \"system\": 23.2, \"_time\": 100}",
            "done",
        ];
        assert_eq!(reesponse.log_lines, expected_log_lines);
    }
}
