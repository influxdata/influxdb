use crate::write_buffer::PluginEvent;
use crate::{write_buffer, WriteBuffer};
use influxdb3_client::plugin_development::{WalPluginTestRequest, WalPluginTestResponse};
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
    let validator = WriteValidator::initialize(
        namespace.clone(),
        Arc::clone(&catalog),
        now_time.timestamp_nanos(),
    )?;
    let data = validator.v1_parse_lines_and_update_schema(
        &request.input_lp.unwrap(),
        false,
        now_time,
        Precision::Nanosecond,
    )?;
    let data = data.convert_lines_to_buffer(Gen1Duration::new_1m());
    let db = catalog.db_schema("_testdb").unwrap();

    let plugin_return_state = influxdb3_py_api::system_py::execute_python_with_batch(
        &code,
        &data.valid_data,
        db,
        Arc::clone(&catalog),
        request.input_arguments,
    )?;

    // validate the generated output lines
    let mut errors = Vec::new();

    // first for the write back database
    let validator =
        WriteValidator::initialize(namespace, Arc::clone(&catalog), now_time.timestamp_nanos())?;
    let lp = plugin_return_state.write_back_lines.join("\n");
    match validator.v1_parse_lines_and_update_schema(&lp, false, now_time, Precision::Nanosecond) {
        Ok(data) => {
            let data = data.convert_lines_to_buffer(Gen1Duration::new_1m());

            for err in data.errors {
                errors.push(format!("{:?}", err));
            }
        }
        Err(write_buffer::Error::ParseError(e)) => {
            errors.push(format!("line protocol parse error on write back: {:?}", e));
        }
        Err(e) => {
            errors.push(format!(
                "Failed to validate output lines on write back: {}",
                e
            ));
        }
    }

    // now for any other dbs that received writes
    for (db_name, lines) in &plugin_return_state.write_db_lines {
        let namespace = match NamespaceName::new(db_name.to_string()) {
            Ok(namespace) => namespace,
            Err(e) => {
                errors.push(format!("database name {} is invalid: {}", db_name, e));
                continue;
            }
        };

        let validator = WriteValidator::initialize(
            namespace,
            Arc::clone(&catalog),
            now_time.timestamp_nanos(),
        )?;
        let lp = lines.join("\n");
        match validator.v1_parse_lines_and_update_schema(
            &lp,
            false,
            now_time,
            Precision::Nanosecond,
        ) {
            Ok(data) => {
                let data = data.convert_lines_to_buffer(Gen1Duration::new_1m());
                for err in data.errors {
                    errors.push(format!("{:?}", err));
                }
            }
            Err(write_buffer::Error::ParseError(e)) => {
                errors.push(format!(
                    "line protocol parse error on write to db {}: {:?}",
                    db_name, e
                ));
            }
            Err(e) => {
                errors.push(format!(
                    "Failed to validate output lines to db {}: {}",
                    db_name, e
                ));
            }
        }
    }

    let log_lines = plugin_return_state.log();
    let mut database_writes = plugin_return_state.write_db_lines;
    database_writes.insert("_testdb".to_string(), plugin_return_state.write_back_lines);

    Ok(WalPluginTestResponse {
        log_lines,
        database_writes,
        errors,
    })
}

#[cfg(feature = "system-py")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::write_buffer::validator::WriteValidator;
    use crate::Precision;
    use data_types::NamespaceName;
    use influxdb3_catalog::catalog::Catalog;
    use iox_time::Time;
    use std::collections::HashMap;

    #[test]
    fn test_wal_plugin() {
        let now = Time::from_timestamp_nanos(1);
        let catalog = Catalog::new("foo".into(), "bar".into());
        let code = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info("arg1: " + args["arg1"])

    for table_batch in table_batches:
        influxdb3_local.info("table: " + table_batch["table_name"])

        for row in table_batch["rows"]:
            influxdb3_local.info("row: " + str(row))

    line = LineBuilder("some_table")\
        .tag("tag1", "tag1_value")\
        .tag("tag2", "tag2_value")\
        .int64_field("field1", 1)\
        .float64_field("field2", 2.0)\
        .string_field("field3", "number three")
    influxdb3_local.write(line)

    other_line = LineBuilder("other_table")
    other_line.int64_field("other_field", 1)
    other_line.float64_field("other_field2", 3.14)
    other_line.time_ns(1302)

    influxdb3_local.write_to_db("mytestdb", other_line)

    influxdb3_local.info("done")"#;

        let lp = [
            "cpu,host=A,region=west usage=1i,system=23.2 100",
            "mem,host=B user=43.1 120",
        ]
        .join("\n");

        let request = WalPluginTestRequest {
            name: "test".into(),
            input_lp: Some(lp),
            input_file: None,
            save_output_to_file: None,
            validate_output_file: None,
            input_arguments: Some(HashMap::from([(
                String::from("arg1"),
                String::from("val1"),
            )])),
        };

        let reesponse =
            run_test_wal_plugin(now, Arc::new(catalog), code.to_string(), request).unwrap();

        let expected_log_lines = vec![
            "INFO: arg1: val1",
            "INFO: table: cpu",
            "INFO: row: {'time': 100, 'fields': [{'name': 'host', 'value': 'A'}, {'name': 'region', 'value': 'west'}, {'name': 'usage', 'value': 1}, {'name': 'system', 'value': 23.2}]}",
            "INFO: table: mem", "INFO: row: {'time': 120, 'fields': [{'name': 'host', 'value': 'B'}, {'name': 'user', 'value': 43.1}]}",
            "INFO: done",
        ].into_iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(reesponse.log_lines, expected_log_lines);

        let expected_testdb_lines = vec![
            "some_table,tag1=tag1_value,tag2=tag2_value field1=1i,field2=2.0,field3=\"number three\""
                .to_string(),
        ];
        assert_eq!(
            reesponse.database_writes.get("_testdb").unwrap(),
            &expected_testdb_lines
        );
        let expected_mytestdb_lines =
            vec!["other_table other_field=1i,other_field2=3.14 1302".to_string()];
        assert_eq!(
            reesponse.database_writes.get("mytestdb").unwrap(),
            &expected_mytestdb_lines
        );
    }

    #[test]
    fn test_wal_plugin_invalid_lines() {
        // set up a catalog and write some data into it to create a schema
        let now = Time::from_timestamp_nanos(1);
        let catalog = Arc::new(Catalog::new("foo".into(), "bar".into()));
        let namespace = NamespaceName::new("foodb").unwrap();
        let validator = WriteValidator::initialize(
            namespace.clone(),
            Arc::clone(&catalog),
            now.timestamp_nanos(),
        )
        .unwrap();
        let _data = validator
            .v1_parse_lines_and_update_schema(
                "cpu,host=A f1=10i 100",
                false,
                now,
                Precision::Nanosecond,
            )
            .unwrap();

        let code = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    line = LineBuilder("some_table")\
        .tag("tag1", "tag1_value")\
        .tag("tag2", "tag2_value")\
        .int64_field("field1", 1)\
        .float64_field("field2", 2.0)\
        .string_field("field3", "number three")
    influxdb3_local.write(line)

    cpu_valid = LineBuilder("cpu")\
        .tag("host", "A")\
        .int64_field("f1", 10)
    influxdb3_local.write_to_db("foodb", cpu_valid)

    cpu_invalid = LineBuilder("cpu")\
        .tag("host", "A")\
        .string_field("f1", "not_an_int")
    influxdb3_local.write_to_db("foodb", cpu_invalid)"#;

        let lp = ["mem,host=B user=43.1 120"].join("\n");

        let request = WalPluginTestRequest {
            name: "test".into(),
            input_lp: Some(lp),
            input_file: None,
            save_output_to_file: None,
            validate_output_file: None,
            input_arguments: None,
        };

        let reesponse =
            run_test_wal_plugin(now, Arc::clone(&catalog), code.to_string(), request).unwrap();

        let expected_testdb_lines = vec![
            "some_table,tag1=tag1_value,tag2=tag2_value field1=1i,field2=2.0,field3=\"number three\""
                .to_string(),
        ];
        assert_eq!(
            reesponse.database_writes.get("_testdb").unwrap(),
            &expected_testdb_lines
        );

        // the lines should still come through in the output because that's what Python sent
        let expected_foodb_lines = vec![
            "cpu,host=A f1=10i".to_string(),
            "cpu,host=A f1=\"not_an_int\"".to_string(),
        ];
        assert_eq!(
            reesponse.database_writes.get("foodb").unwrap(),
            &expected_foodb_lines
        );

        // there should be an error for the invalid line
        assert_eq!(reesponse.errors.len(), 1);
        let expected_error = "line protocol parse error on write to db foodb: WriteLineError { original_line: \"cpu,host=A f1=not_an_int\", line_number: 2, error_message: \"invalid field value in line protocol for field 'f1' on line 1: expected type iox::column_type::field::integer, but got iox::column_type::field::string\" }";
        assert_eq!(reesponse.errors[0], expected_error);
    }
}
