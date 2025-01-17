#[cfg(feature = "system-py")]
use crate::PluginEvent;
use influxdb3_catalog::catalog::Catalog;
#[cfg(feature = "system-py")]
use influxdb3_client::plugin_development::{WalPluginTestRequest, WalPluginTestResponse};
#[cfg(feature = "system-py")]
use influxdb3_internal_api::query_executor::QueryExecutor;
#[cfg(feature = "system-py")]
use influxdb3_wal::TriggerDefinition;
#[cfg(feature = "system-py")]
use influxdb3_wal::TriggerSpecificationDefinition;
use influxdb3_write::write_buffer;
#[cfg(feature = "system-py")]
use influxdb3_write::WriteBuffer;
use observability_deps::tracing::error;
use std::fmt::Debug;
#[cfg(feature = "system-py")]
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;
#[cfg(feature = "system-py")]
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid database {0}")]
    InvalidDatabase(String),

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

    #[error("reading plugin file: {0}")]
    ReadPluginError(#[from] std::io::Error),

    #[error("error executing plugin: {0}")]
    PluginExecutionError(#[from] influxdb3_py_api::ExecutePluginError),

    #[error("invalid cron syntax: {0}")]
    InvalidCronSyntax(#[from] cron::error::Error),

    #[error("cron schedule never triggers: {0}")]
    CronScheduleNeverTriggers(String),
}

#[cfg(feature = "system-py")]
pub(crate) fn run_wal_contents_plugin(
    db_name: String,
    plugin_code: String,
    trigger_definition: TriggerDefinition,
    context: PluginContext,
) {
    let trigger_plugin = TriggerPlugin {
        trigger_definition,
        db_name,
        plugin_code,
        write_buffer: context.write_buffer,
        query_executor: context.query_executor,
    };
    tokio::task::spawn(async move {
        trigger_plugin
            .run_wal_contents_plugin(context.trigger_rx)
            .await
            .expect("trigger plugin failed");
    });
}

#[cfg(feature = "system-py")]
pub(crate) fn run_cron_plugin(
    db_name: String,
    plugin_code: String,
    trigger_definition: TriggerDefinition,
    context: PluginContext,
) {
    let TriggerSpecificationDefinition::CronSchedule { schedule } = &trigger_definition.trigger
    else {
        // TODO: these linkages should be guaranteed by code.
        unreachable!("this should've been checked");
    };
    let schedule = schedule.to_string();
    let trigger_plugin = TriggerPlugin {
        trigger_definition,
        db_name,
        plugin_code,
        write_buffer: context.write_buffer,
        query_executor: context.query_executor,
    };
    tokio::task::spawn(async move {
        trigger_plugin
            .run_cron_plugin(context.trigger_rx, schedule)
            .await
            .expect("cron trigger plugin failed");
    });
}

#[cfg(feature = "system-py")]
pub(crate) struct PluginContext {
    // tokio channel for inputs
    pub(crate) trigger_rx: mpsc::Receiver<PluginEvent>,
    // handler to write data back to the DB.
    pub(crate) write_buffer: Arc<dyn WriteBuffer>,
    // query executor to hand off to the plugin
    pub(crate) query_executor: Arc<dyn QueryExecutor>,
}

#[cfg(feature = "system-py")]
#[derive(Debug)]
struct TriggerPlugin {
    trigger_definition: TriggerDefinition,
    plugin_code: String,
    db_name: String,
    write_buffer: Arc<dyn WriteBuffer>,
    query_executor: Arc<dyn QueryExecutor>,
}

#[cfg(feature = "system-py")]
mod python_plugin {
    use super::*;
    use anyhow::{anyhow, Context};
    use chrono::{DateTime, Utc};
    use cron::{OwnedScheduleIterator, Schedule};
    use data_types::NamespaceName;
    use hashbrown::HashMap;
    use influxdb3_catalog::catalog::DatabaseSchema;
    use influxdb3_py_api::system_py::{execute_cron_trigger, execute_python_with_batch};
    use influxdb3_wal::WalOp;
    use influxdb3_write::Precision;
    use iox_time::Time;
    use observability_deps::tracing::{info, warn};
    use std::str::FromStr;
    use std::time::{Duration, SystemTime};
    use tokio::sync::mpsc::Receiver;
    use tokio::time::Instant;

    impl TriggerPlugin {
        pub(crate) async fn run_wal_contents_plugin(
            &self,
            mut receiver: Receiver<PluginEvent>,
        ) -> Result<(), Error> {
            info!(?self.trigger_definition.trigger_name, ?self.trigger_definition.database_name, ?self.trigger_definition.plugin_name, "starting trigger plugin");

            loop {
                let event = match receiver.recv().await {
                    Some(event) => event,
                    None => {
                        warn!(?self.trigger_definition, "trigger plugin receiver closed");
                        break;
                    }
                };

                match self.process_event(event).await {
                    Ok(stop) => {
                        if stop {
                            break;
                        }
                    }
                    Err(e) => {
                        error!(?self.trigger_definition, "error processing event: {}", e);
                    }
                }
            }

            Ok(())
        }

        pub(crate) async fn run_cron_plugin(
            &self,
            mut receiver: Receiver<PluginEvent>,
            schedule: String,
        ) -> Result<(), Error> {
            let schedule = Schedule::from_str(schedule.as_str())?;
            let mut cron_runner = CronTriggerRunner::new(schedule);

            loop {
                let Some(next_run_instant) = cron_runner.next_run_time() else {
                    break;
                };

                tokio::select! {
                    _ = tokio::time::sleep_until(next_run_instant) => {
                        info!("running");
                        let Some(schema) = self.write_buffer.catalog().db_schema(self.db_name.as_str()) else {
                            return Err(Error::MissingDb);
                        };
                        cron_runner.run_at_time(self, schema).await?;
                    }
                    event = receiver.recv() => {
                        match event {
                            None => {
                                warn!(?self.trigger_definition, "trigger plugin receiver closed");
                                break;
                            }
                            Some(PluginEvent::WriteWalContents(_)) => {
                                info!("ignoring wal contents in cron plugin.")
                            }
                            Some(PluginEvent::Shutdown(sender)) => {
                                sender.send(()).map_err(|_| Error::FailedToShutdown)?;
                                break;
                            }
                        }
                    }
                }
            }

            Ok(())
        }
        async fn process_event(&self, event: PluginEvent) -> Result<bool, Error> {
            let Some(schema) = self.write_buffer.catalog().db_schema(self.db_name.as_str()) else {
                return Err(Error::MissingDb);
            };

            let mut db_writes = DatabaseWriteBuffer::new();

            match event {
                PluginEvent::WriteWalContents(wal_contents) => {
                    for wal_op in &wal_contents.ops {
                        match wal_op {
                            WalOp::Write(write_batch) => {
                                // determine if this write batch is for this database
                                if write_batch.database_name.as_ref()
                                    != self.trigger_definition.database_name
                                {
                                    continue;
                                }
                                let table_filter = match &self.trigger_definition.trigger {
                                    TriggerSpecificationDefinition::AllTablesWalWrite => {
                                        // no filter
                                        None
                                    }
                                    TriggerSpecificationDefinition::SingleTableWalWrite {
                                        table_name,
                                    } => {
                                        let table_id = schema
                                            .table_name_to_id(table_name.as_ref())
                                            .context("table not found")?;
                                        Some(table_id)
                                    }
                                    // This should not occur
                                    TriggerSpecificationDefinition::CronSchedule {
                                        schedule
                                    } => {
                                        return Err(anyhow!("unexpectedly found cron trigger specification {} for WAL plugin {}", schedule, self.trigger_definition.trigger_name).into())
                                    }
                                };

                                let result = execute_python_with_batch(
                                    &self.plugin_code,
                                    write_batch,
                                    Arc::clone(&schema),
                                    Arc::clone(&self.query_executor),
                                    table_filter,
                                    &self.trigger_definition.trigger_arguments,
                                )?;

                                // write the output lines to the appropriate database
                                if !result.write_back_lines.is_empty() {
                                    db_writes
                                        .add_lines(schema.name.as_ref(), result.write_back_lines);
                                }

                                for (db_name, add_lines) in result.write_db_lines {
                                    db_writes.add_lines(&db_name, add_lines);
                                }
                            }
                            WalOp::Catalog(_) => {}
                            WalOp::Noop(_) => {}
                        }
                    }
                }
                PluginEvent::Shutdown(sender) => {
                    sender.send(()).map_err(|_| Error::FailedToShutdown)?;
                    return Ok(true);
                }
            }

            if !db_writes.is_empty() {
                db_writes.execute(&self.write_buffer).await?;
            }

            Ok(false)
        }
    }

    struct DatabaseWriteBuffer {
        writes: HashMap<String, Vec<String>>,
    }
    impl DatabaseWriteBuffer {
        fn new() -> Self {
            Self {
                writes: HashMap::new(),
            }
        }

        fn add_lines(&mut self, db_name: &str, lines: Vec<String>) {
            self.writes.entry_ref(db_name).or_default().extend(lines);
        }

        fn is_empty(&self) -> bool {
            self.writes.is_empty()
        }

        async fn execute(self, write_buffer: &Arc<dyn WriteBuffer>) -> Result<(), Error> {
            for (db_name, output_lines) in self.writes {
                let ingest_time = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap();
                write_buffer
                    .write_lp(
                        NamespaceName::new(db_name).unwrap(),
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

    struct CronTriggerRunner {
        schedule: OwnedScheduleIterator<Utc>,
        next_trigger_time: Option<DateTime<Utc>>,
    }
    impl CronTriggerRunner {
        fn new(cron_schedule: Schedule) -> Self {
            let mut schedule = cron_schedule.upcoming_owned(Utc);
            let next_trigger_time = schedule.next();
            Self {
                schedule,
                next_trigger_time,
            }
        }

        async fn run_at_time(
            &mut self,
            plugin: &TriggerPlugin,
            db_schema: Arc<DatabaseSchema>,
        ) -> Result<(), Error> {
            let Some(trigger_time) = self.next_trigger_time else {
                return Err(anyhow!("running a cron trigger that is finished.").into());
            };
            let result = execute_cron_trigger(
                &plugin.plugin_code,
                trigger_time,
                Arc::clone(&db_schema),
                Arc::clone(&plugin.query_executor),
                &plugin.trigger_definition.trigger_arguments,
            )
            .expect("should be able to run trigger");

            let mut db_writes = DatabaseWriteBuffer::new();
            // write the output lines to the appropriate database
            if !result.write_back_lines.is_empty() {
                db_writes.add_lines(db_schema.name.as_ref(), result.write_back_lines);
            }

            for (db_name, add_lines) in result.write_db_lines {
                db_writes.add_lines(&db_name, add_lines);
            }
            db_writes.execute(&plugin.write_buffer).await?;
            self.advance_time();
            Ok(())
        }
        fn advance_time(&mut self) {
            self.next_trigger_time = self.schedule.next();
        }

        /// A funky little method to get a tokio Instant that we can call `tokio::time::sleep_until()` on.
        fn next_run_time(&self) -> Option<Instant> {
            let next_trigger_time = self.next_trigger_time.as_ref()?;
            let wait = next_trigger_time.signed_duration_since(Utc::now());
            if wait.num_milliseconds() < 0 {
                Some(Instant::now())
            } else {
                Some(Instant::now() + Duration::from_millis(wait.num_milliseconds() as u64))
            }
        }
    }
}

#[cfg(feature = "system-py")]
pub(crate) fn run_test_wal_plugin(
    now_time: iox_time::Time,
    catalog: Arc<influxdb3_catalog::catalog::Catalog>,
    query_executor: Arc<dyn QueryExecutor>,
    code: String,
    request: WalPluginTestRequest,
) -> Result<WalPluginTestResponse, Error> {
    use data_types::NamespaceName;
    use influxdb3_wal::Gen1Duration;
    use influxdb3_write::write_buffer::validator::WriteValidator;
    use influxdb3_write::Precision;

    let database = request.database;
    let namespace = NamespaceName::new(database.clone())
        .map_err(|_e| Error::InvalidDatabase(database.clone()))?;
    // parse the lp into a write batch
    let validator = WriteValidator::initialize(
        namespace.clone(),
        Arc::clone(&catalog),
        now_time.timestamp_nanos(),
    )?;
    let data = validator.v1_parse_lines_and_update_schema(
        &request.input_lp,
        false,
        now_time,
        Precision::Nanosecond,
    )?;
    let data = data.convert_lines_to_buffer(Gen1Duration::new_1m());
    let db = catalog.db_schema(&database).ok_or(Error::MissingDb)?;

    let plugin_return_state = influxdb3_py_api::system_py::execute_python_with_batch(
        &code,
        &data.valid_data,
        db,
        query_executor,
        None,
        &request.input_arguments,
    )?;

    let log_lines = plugin_return_state.log();

    let mut database_writes = plugin_return_state.write_db_lines;
    database_writes.insert(database, plugin_return_state.write_back_lines);

    let test_write_handler = TestWriteHandler::new(Arc::clone(&catalog), now_time);
    let errors = test_write_handler.validate_all_writes(&database_writes);

    Ok(WalPluginTestResponse {
        log_lines,
        database_writes,
        errors,
    })
}

#[derive(Debug)]
pub struct TestWriteHandler {
    catalog: Arc<Catalog>,
    now_time: iox_time::Time,
}

use data_types::NamespaceName;
use hashbrown::HashMap;
use influxdb3_wal::Gen1Duration;
use influxdb3_write::write_buffer::validator::WriteValidator;
use influxdb3_write::Precision;

impl TestWriteHandler {
    pub fn new(catalog: Arc<Catalog>, now_time: iox_time::Time) -> Self {
        Self { catalog, now_time }
    }

    /// Validates a vec of lines for a namespace, returning any errors that arise as strings
    fn validate_write_lines(
        &self,
        namespace: NamespaceName<'static>,
        lines: &[String],
    ) -> Vec<String> {
        let mut errors = Vec::new();

        let db_name = namespace.as_str();

        let validator = match WriteValidator::initialize(
            namespace.clone(),
            Arc::clone(&self.catalog),
            self.now_time.timestamp_nanos(),
        ) {
            Ok(v) => v,
            Err(e) => {
                errors.push(format!(
                    "Failed to initialize validator for db {}: {}",
                    db_name, e
                ));
                return errors;
            }
        };

        let lp = lines.join("\n");
        match validator.v1_parse_lines_and_update_schema(
            &lp,
            false,
            self.now_time,
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
        errors
    }
    pub fn validate_all_writes(&self, writes: &HashMap<String, Vec<String>>) -> Vec<String> {
        let mut all_errors = Vec::new();
        for (db_name, lines) in writes {
            let namespace = match NamespaceName::new(db_name.to_string()) {
                Ok(namespace) => namespace,
                Err(e) => {
                    all_errors.push(format!("database name {} is invalid: {}", db_name, e));
                    continue;
                }
            };

            let db_errors = self.validate_write_lines(namespace, lines);
            all_errors.extend(db_errors);
        }

        all_errors
    }
}

#[cfg(feature = "system-py")]
pub(crate) fn run_test_cron_plugin(
    now_time: iox_time::Time,
    catalog: Arc<influxdb3_catalog::catalog::Catalog>,
    query_executor: Arc<dyn QueryExecutor>,
    code: String,
    request: influxdb3_client::plugin_development::CronPluginTestRequest,
) -> Result<influxdb3_client::plugin_development::CronPluginTestResponse, Error> {
    let database = request.database;
    let db = catalog.db_schema(&database).ok_or(Error::MissingDb)?;

    let cron_schedule = request.schedule.as_deref().unwrap_or("* * * * * *");

    let schedule = cron::Schedule::from_str(cron_schedule)?;
    let Some(schedule_time) = schedule.after(&now_time.date_time()).next() else {
        return Err(Error::CronScheduleNeverTriggers(cron_schedule.to_string()));
    };

    let plugin_return_state = influxdb3_py_api::system_py::execute_cron_trigger(
        &code,
        schedule_time,
        db,
        query_executor,
        &request.input_arguments,
    )?;

    let log_lines = plugin_return_state.log();

    let mut database_writes = plugin_return_state.write_db_lines;
    database_writes.insert(database, plugin_return_state.write_back_lines);

    let test_write_handler = TestWriteHandler::new(Arc::clone(&catalog), now_time);
    let errors = test_write_handler.validate_all_writes(&database_writes);
    let trigger_time = schedule_time.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true);

    Ok(
        influxdb3_client::plugin_development::CronPluginTestResponse {
            trigger_time: Some(trigger_time),
            log_lines,
            database_writes,
            errors,
        },
    )
}

#[cfg(feature = "system-py")]
#[cfg(test)]
mod tests {
    use super::*;
    use data_types::NamespaceName;
    use hashbrown::HashMap;
    use influxdb3_catalog::catalog::Catalog;
    use influxdb3_internal_api::query_executor::UnimplementedQueryExecutor;
    use influxdb3_write::write_buffer::validator::WriteValidator;
    use influxdb3_write::Precision;
    use iox_time::Time;

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
            filename: "test".into(),
            database: "_testdb".into(),
            input_lp: lp,
            input_arguments: Some(HashMap::from([(
                String::from("arg1"),
                String::from("val1"),
            )])),
        };

        let executor: Arc<dyn QueryExecutor> = Arc::new(UnimplementedQueryExecutor);

        let response =
            run_test_wal_plugin(now, Arc::new(catalog), executor, code.to_string(), request)
                .unwrap();

        let expected_log_lines = vec![
            "INFO: arg1: val1",
            "INFO: table: cpu",
            "INFO: row: {'host': 'A', 'region': 'west', 'usage': 1, 'system': 23.2, 'time': 100}",
            "INFO: table: mem",
            "INFO: row: {'host': 'B', 'user': 43.1, 'time': 120}",
            "INFO: done",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
        assert_eq!(response.log_lines, expected_log_lines);

        let expected_testdb_lines = vec![
            "some_table,tag1=tag1_value,tag2=tag2_value field1=1i,field2=2.0,field3=\"number three\""
                .to_string(),
        ];
        assert_eq!(
            response.database_writes.get("_testdb").unwrap(),
            &expected_testdb_lines
        );
        let expected_mytestdb_lines =
            vec!["other_table other_field=1i,other_field2=3.14 1302".to_string()];
        assert_eq!(
            response.database_writes.get("mytestdb").unwrap(),
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
        .int64_field("f1", 10)\
        .uint64_field("f2", 20)\
        .bool_field("f3", True)
    influxdb3_local.write_to_db("foodb", cpu_valid)

    cpu_invalid = LineBuilder("cpu")\
        .tag("host", "A")\
        .string_field("f1", "not_an_int")
    influxdb3_local.write_to_db("foodb", cpu_invalid)"#;

        let lp = ["mem,host=B user=43.1 120"].join("\n");

        let request = WalPluginTestRequest {
            filename: "test".into(),
            database: "_testdb".into(),
            input_lp: lp,
            input_arguments: None,
        };

        let executor: Arc<dyn QueryExecutor> = Arc::new(UnimplementedQueryExecutor);

        let reesponse = run_test_wal_plugin(
            now,
            Arc::clone(&catalog),
            executor,
            code.to_string(),
            request,
        )
        .unwrap();

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
            "cpu,host=A f1=10i,f2=20u,f3=t".to_string(),
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
