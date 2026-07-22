use super::*;
use arrow_array::TimestampNanosecondArray;
use datafusion::{assert_batches_eq, assert_batches_sorted_eq, datasource::MemTable};
use influxdb3_catalog::catalog::{
    ErrorBehavior, NodeSpec, TriggerDefinition, TriggerSettings, TriggerSpecificationDefinition,
};
use influxdb3_id::TriggerId;

#[tokio::test]
async fn test_trigger_table_includes_error_behavior() {
    let triggers = vec![
        Arc::new(TriggerDefinition {
            trigger_id: TriggerId::new(1),
            trigger_name: "log_trigger".into(),
            plugin_filename: "log.py".to_string(),
            database_name: "mydb".into(),
            node_spec: NodeSpec::All,
            trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
            trigger_settings: TriggerSettings {
                error_behavior: ErrorBehavior::Log,
                ..Default::default()
            },
            trigger_arguments: None,
            disabled: false,
        }),
        Arc::new(TriggerDefinition {
            trigger_id: TriggerId::new(2),
            trigger_name: "retry_trigger".into(),
            plugin_filename: "retry.py".to_string(),
            database_name: "mydb".into(),
            node_spec: NodeSpec::All,
            trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
            trigger_settings: TriggerSettings {
                error_behavior: ErrorBehavior::Retry,
                ..Default::default()
            },
            trigger_arguments: None,
            disabled: true,
        }),
        Arc::new(TriggerDefinition {
            trigger_id: TriggerId::new(3),
            trigger_name: "disable_trigger".into(),
            plugin_filename: "disable.py".to_string(),
            database_name: "mydb".into(),
            node_spec: NodeSpec::All,
            trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
            trigger_settings: TriggerSettings {
                error_behavior: ErrorBehavior::Disable,
                ..Default::default()
            },
            trigger_arguments: None,
            disabled: false,
        }),
    ];

    let table = ProcessingEngineTriggerTable::new(triggers);
    let batch = table.scan(None, None).await.unwrap();

    assert_batches_sorted_eq!(
        [
            "+-----------------+-----------------+------------------------+----------+----------------+",
            "| trigger_name    | plugin_filename | trigger_specification  | disabled | error_behavior |",
            "+-----------------+-----------------+------------------------+----------+----------------+",
            "| disable_trigger | disable.py      | \"all_tables_wal_write\" | false    | disable        |",
            "| log_trigger     | log.py          | \"all_tables_wal_write\" | false    | log            |",
            "| retry_trigger   | retry.py        | \"all_tables_wal_write\" | true     | retry          |",
            "+-----------------+-----------------+------------------------+----------+----------------+",
        ],
        &[batch]
    );
}

/// Build a `MemTable` with the `processing_engine_logs` schema and three rows
/// at 1s, 2s, and 3s, standing in for the storage-backed logs table.
fn logs_mem_table() -> Arc<dyn TableProvider> {
    let schema: SchemaRef = Arc::new(processing_engine_logs_schema());
    let string_col = |values: &[&str]| -> ArrayRef { Arc::new(StringArray::from(values.to_vec())) };
    let time = TimestampNanosecondArray::from(vec![1_000_000_000, 2_000_000_000, 3_000_000_000])
        .with_timezone("UTC");
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            string_col(&["db1", "db2", "db1"]),       // database_name
            string_col(&["", "", ""]),                // error_details
            string_col(&["INFO", "WARN", "ERROR"]),   // log_level
            string_col(&["one", "two", "three"]),     // log_text
            string_col(&["node1", "node1", "node1"]), // node_id
            string_col(&["p.py", "p.py", "p.py"]),    // plugin_filename
            string_col(&["r1", "r2", "r3"]),          // run_id
            Arc::new(time),                           // time
            string_col(&["t1", "t2", "t3"]),          // trigger_name
        ],
    )
    .unwrap();
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
}

#[test]
fn test_logs_view_schema_appends_event_time() {
    let base = logs_mem_table();
    let view = processing_engine_logs_view(Arc::clone(&base)).unwrap();

    let base_schema = base.schema();
    let view_schema = view.schema();
    assert_eq!(view_schema.fields().len(), base_schema.fields().len() + 1);
    for (base_field, view_field) in base_schema.fields().iter().zip(view_schema.fields()) {
        assert_eq!(base_field, view_field);
    }

    let event_time = view_schema.fields().last().unwrap();
    let time = base_schema.field_with_name("time").unwrap();
    assert_eq!(event_time.name(), "event_time");
    assert_eq!(event_time.data_type(), time.data_type());
    assert_eq!(event_time.is_nullable(), time.is_nullable());
}

#[tokio::test]
async fn test_logs_view_event_time_mirrors_time() {
    let view = processing_engine_logs_view(logs_mem_table()).unwrap();
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_table(PROCESSING_ENGINE_LOGS_TABLE_NAME, view)
        .unwrap();

    let batches = ctx
        .sql(
            "SELECT log_text, time, event_time FROM processing_engine_logs \
             WHERE event_time > '1970-01-01T00:00:01Z' ORDER BY event_time",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_batches_eq!(
        [
            "+----------+----------------------+----------------------+",
            "| log_text | time                 | event_time           |",
            "+----------+----------------------+----------------------+",
            "| two      | 1970-01-01T00:00:02Z | 1970-01-01T00:00:02Z |",
            "| three    | 1970-01-01T00:00:03Z | 1970-01-01T00:00:03Z |",
            "+----------+----------------------+----------------------+",
        ],
        &batches
    );
}

/// Wraps a provider and records the filters passed to `scan`, so tests can
/// assert what the optimizer actually pushes into the inner table.
#[derive(Debug)]
struct FilterRecordingProvider {
    inner: Arc<dyn TableProvider>,
    filters: Arc<std::sync::Mutex<Vec<Expr>>>,
}

#[async_trait]
impl TableProvider for FilterRecordingProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Match ProcessingEngineLogsTable, which reports Inexact for all filters.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.filters.lock().unwrap().extend(filters.iter().cloned());
        self.inner.scan(ctx, projection, filters, limit).await
    }
}

#[tokio::test]
async fn test_logs_view_pushes_event_time_filter_down_as_time() {
    let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
    let base = Arc::new(FilterRecordingProvider {
        inner: logs_mem_table(),
        filters: Arc::clone(&recorded),
    });
    let view = processing_engine_logs_view(base).unwrap();
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_table(PROCESSING_ENGINE_LOGS_TABLE_NAME, view)
        .unwrap();

    ctx.sql(
        "SELECT log_text FROM processing_engine_logs \
         WHERE event_time > '1970-01-01T00:00:01Z'",
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();

    // The inner scan must receive the filter rewritten to the physical time
    // column; a filter still referencing event_time would be invisible to
    // ChunkFilter's time-range pruning.
    let filters = recorded.lock().unwrap();
    assert!(
        filters
            .iter()
            .any(|f| f.column_refs().iter().any(|c| c.name == "time")),
        "no filter on time was pushed down to the inner scan: {filters:?}"
    );
    assert!(
        filters
            .iter()
            .all(|f| f.column_refs().iter().all(|c| c.name != "event_time")),
        "a filter on event_time leaked into the inner scan: {filters:?}"
    );
}

#[test]
fn test_logs_view_passthrough_when_event_time_exists() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new(
            "event_time",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("log_text", DataType::Utf8, true),
        Field::new(
            "time",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
    ]));
    let base: Arc<dyn TableProvider> =
        Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![]]).unwrap());

    let view = processing_engine_logs_view(base).unwrap();

    assert_eq!(view.schema(), schema);
}

#[tokio::test]
async fn test_logs_view_select_star_includes_event_time() {
    let view = processing_engine_logs_view(logs_mem_table()).unwrap();
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_table(PROCESSING_ENGINE_LOGS_TABLE_NAME, view)
        .unwrap();

    let df = ctx
        .sql("SELECT * FROM processing_engine_logs")
        .await
        .unwrap();
    let column_names = df
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        column_names,
        vec![
            "database_name",
            "error_details",
            "log_level",
            "log_text",
            "node_id",
            "plugin_filename",
            "run_id",
            "time",
            "trigger_name",
            "event_time",
        ]
    );
}

#[tokio::test]
async fn test_trigger_arguments_table_empty() {
    let table = ProcessingEngineTriggerArgumentsTable::new(vec![]);
    let batch = table.scan(None, None).await.unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[tokio::test]
async fn test_trigger_arguments_table_with_triggers_no_args() {
    let trigger = Arc::new(TriggerDefinition {
        trigger_id: TriggerId::new(1),
        trigger_name: "test_trigger".into(),
        plugin_filename: "test_plugin.py".to_string(),
        database_name: "mydb".into(),
        node_spec: NodeSpec::All,
        trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
        trigger_settings: TriggerSettings::default(),
        trigger_arguments: None,
        disabled: false,
    });

    let table = ProcessingEngineTriggerArgumentsTable::new(vec![trigger]);
    let batch = table.scan(None, None).await.unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[tokio::test]
async fn test_trigger_arguments_table_with_arguments() {
    let mut args = hashbrown::HashMap::new();
    args.insert("key1".to_string(), "value1".to_string());
    args.insert("key2".to_string(), "value2".to_string());

    let trigger = Arc::new(TriggerDefinition {
        trigger_id: TriggerId::new(1),
        trigger_name: "test_trigger".into(),
        plugin_filename: "test_plugin.py".to_string(),
        database_name: "mydb".into(),
        node_spec: NodeSpec::All,
        trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
        trigger_settings: TriggerSettings::default(),
        trigger_arguments: Some(args),
        disabled: false,
    });

    let table = ProcessingEngineTriggerArgumentsTable::new(vec![trigger]);
    let batch = table.scan(None, None).await.unwrap();

    // Use assert_batches_sorted_eq to check the output
    assert_batches_sorted_eq!(
        [
            "+--------------+--------------+----------------+",
            "| trigger_name | argument_key | argument_value |",
            "+--------------+--------------+----------------+",
            "| test_trigger | key1         | value1         |",
            "| test_trigger | key2         | value2         |",
            "+--------------+--------------+----------------+",
        ],
        &[batch]
    );
}

#[tokio::test]
async fn test_trigger_arguments_table_multiple_triggers() {
    let mut args1 = hashbrown::HashMap::new();
    args1.insert("arg1".to_string(), "val1".to_string());

    let mut args2 = hashbrown::HashMap::new();
    args2.insert("arg2".to_string(), "val2".to_string());
    args2.insert("arg3".to_string(), "val3".to_string());

    let trigger1 = Arc::new(TriggerDefinition {
        trigger_id: TriggerId::new(1),
        trigger_name: "trigger1".into(),
        plugin_filename: "plugin1.py".to_string(),
        database_name: "db1".into(),
        node_spec: NodeSpec::All,
        trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
        trigger_settings: TriggerSettings::default(),
        trigger_arguments: Some(args1),
        disabled: false,
    });

    let trigger2 = Arc::new(TriggerDefinition {
        trigger_id: TriggerId::new(2),
        trigger_name: "trigger2".into(),
        plugin_filename: "plugin2.py".to_string(),
        database_name: "db2".into(),
        node_spec: NodeSpec::All,
        trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
        trigger_settings: TriggerSettings::default(),
        trigger_arguments: Some(args2),
        disabled: false,
    });

    let table = ProcessingEngineTriggerArgumentsTable::new(vec![trigger1, trigger2]);
    let batch = table.scan(None, None).await.unwrap();

    // Use assert_batches_sorted_eq to check the output
    assert_batches_sorted_eq!(
        [
            "+--------------+--------------+----------------+",
            "| trigger_name | argument_key | argument_value |",
            "+--------------+--------------+----------------+",
            "| trigger1     | arg1         | val1           |",
            "| trigger2     | arg2         | val2           |",
            "| trigger2     | arg3         | val3           |",
            "+--------------+--------------+----------------+",
        ],
        &[batch]
    );
}
