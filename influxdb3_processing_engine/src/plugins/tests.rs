use super::*;
use crate::virtualenv::init_pyo3;
use chrono::{TimeZone, Utc};
use hashbrown::HashMap;
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
use influxdb3_id::DbId;
use influxdb3_internal_api::query_executor::UnimplementedQueryExecutor;
use influxdb3_write::Precision;
use influxdb3_write::write_buffer::validator::WriteValidator;
use iox_time::{MockProvider, Time};
use object_store::memory::InMemory;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn test_wal_plugin() {
    init_pyo3();
    let now = Time::from_timestamp_nanos(1);
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(now));
    let cache = Arc::new(Mutex::new(CacheStore::new(
        Arc::clone(&time_provider),
        Duration::from_secs(10),
    )));
    let catalog = Catalog::new(
        "foo",
        Arc::new(InMemory::new()),
        time_provider,
        Default::default(),
    )
    .await
    .unwrap();
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
        cache_name: None,
        input_arguments: Some(HashMap::from([(
            String::from("arg1"),
            String::from("val1"),
        )])),
    };

    let executor: Arc<dyn QueryExecutor> = Arc::new(UnimplementedQueryExecutor);
    let buffering_writer = Arc::new(DryRunBufferer::new());

    let response = run_dry_run_wal_plugin(
        now,
        Arc::new(catalog),
        executor,
        Arc::clone(&buffering_writer),
        code.to_string(),
        cache,
        request,
    )
    .unwrap();

    let plugin_log_lines: Vec<_> = response
        .log_lines
        .iter()
        .filter(|l| {
            !l.starts_with("INFO: starting execution") && !l.starts_with("INFO: finished execution")
        })
        .cloned()
        .collect();
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
    assert_eq!(plugin_log_lines, expected_log_lines);

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

#[tokio::test(flavor = "multi_thread")]
async fn test_wal_plugin_invalid_lines() {
    init_pyo3();
    // set up a catalog and write some data into it to create a schema
    let now = Time::from_timestamp_nanos(1);
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(now));
    let cache = Arc::new(Mutex::new(CacheStore::new(
        Arc::clone(&time_provider),
        Duration::from_secs(10),
    )));
    let catalog = Arc::new(
        Catalog::new(
            "foo",
            Arc::new(InMemory::new()),
            time_provider,
            Default::default(),
        )
        .await
        .unwrap(),
    );
    let database_name = DatabaseName::new("foodb").unwrap();
    let validator =
        WriteValidator::initialize(database_name.clone(), Arc::clone(&catalog)).unwrap();
    let _data = validator
        .v1_parse_lines_and_catalog_updates(
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
        cache_name: None,
        input_arguments: None,
    };

    let executor: Arc<dyn QueryExecutor> = Arc::new(UnimplementedQueryExecutor);
    let buffering_writer = Arc::new(DryRunBufferer::new());

    let response = run_dry_run_wal_plugin(
        now,
        Arc::clone(&catalog),
        executor,
        Arc::clone(&buffering_writer),
        code.to_string(),
        cache,
        request,
    )
    .unwrap();

    let expected_testdb_lines = vec![
        "some_table,tag1=tag1_value,tag2=tag2_value field1=1i,field2=2.0,field3=\"number three\""
            .to_string(),
    ];
    assert_eq!(
        response.database_writes.get("_testdb").unwrap(),
        &expected_testdb_lines
    );

    // Both lines to foodb should be captured
    let expected_foodb_lines = vec![
        "cpu,host=A f1=10i,f2=20u,f3=t".to_string(),
        "cpu,host=A f1=\"not_an_int\"".to_string(),
    ];
    assert_eq!(
        response.database_writes.get("foodb").unwrap(),
        &expected_foodb_lines
    );

    // Validator catches the schema mismatch: f1 was defined as int but second write uses string
    assert_eq!(response.errors.len(), 1);
    assert!(response.errors[0].contains("f1"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_schedule_plugin_py_api_surface_area() {
    init_pyo3();

    let code = r#"
def process_scheduled_call(influxdb3_local, call_time, args=None):
    allowed = {"info", "warn", "error", "query", "write", "cache", "write_to_db", "write_sync", "write_sync_to_db"}
    attrs = {name for name in dir(influxdb3_local) if not name.startswith("__")}
    extras = attrs - allowed
    missing = allowed - attrs
    if extras or missing:
        raise RuntimeError(f"unexpected attributes: extras={sorted(extras)}, missing={sorted(missing)}")
"#;

    let cache = Arc::new(Mutex::new(CacheStore::new(
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
        Duration::from_secs(10),
    )));
    let buffering_writer = Arc::new(DryRunBufferer::new());

    let result = influxdb3_py_api::system_py::execute_schedule_trigger(
        code,
        Utc.timestamp_opt(0, 0).unwrap(),
        Arc::new(DatabaseSchema::new(DbId::from(0), Arc::from("test_db"))),
        Arc::new(UnimplementedQueryExecutor),
        buffering_writer,
        influxdb3_py_api::system_py::PluginLogger::dry_run(),
        &None::<HashMap<String, String>>,
        PyCache::new_test_cache(cache, "_shared_test".to_string()),
        None,
    );

    assert!(
        result.is_ok(),
        "PyPluginCallApi exposes unexpected Python methods: {result:?}"
    );
}

/// Tests that single-file plugins execute in isolated namespaces.
///
/// Without namespace isolation, concurrent plugins could overwrite each other's
/// function definitions (e.g., both define `foo()` with different signatures).
/// This test runs two plugins sequentially and verifies the second doesn't see
/// the first's helper function leaked into `__main__`.
#[tokio::test(flavor = "multi_thread")]
async fn test_plugin_namespace_isolation() {
    init_pyo3();
    let now = Time::from_timestamp_nanos(1);
    let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(now));
    let cache = Arc::new(Mutex::new(CacheStore::new(
        Arc::clone(&time_provider),
        Duration::from_secs(10),
    )));
    let catalog = Arc::new(
        Catalog::new(
            "foo",
            Arc::new(InMemory::new()),
            Arc::clone(&time_provider),
            Default::default(),
        )
        .await
        .unwrap(),
    );

    // Plugin A defines helper_a
    let code_a = r#"
def helper_a():
    return 42

def process_writes(influxdb3_local, table_batches, args=None):
    influxdb3_local.info(f"plugin_a: helper_a() = {helper_a()}")"#;

    // Plugin B checks if helper_a leaked into __main__ (it shouldn't have)
    let code_b = r#"
def helper_b():
    return 99

def process_writes(influxdb3_local, table_batches, args=None):
    import __main__
    # If isolation is broken, helper_a from plugin A would be in __main__
    helper_a_leaked = 'helper_a' in dir(__main__)
    helper_b_leaked = 'helper_b' in dir(__main__)
    influxdb3_local.info(f"helper_a_leaked: {helper_a_leaked}")
    influxdb3_local.info(f"helper_b_leaked: {helper_b_leaked}")
    influxdb3_local.info(f"plugin_b: helper_b() = {helper_b()}")"#;

    let executor: Arc<dyn QueryExecutor> = Arc::new(UnimplementedQueryExecutor);
    let buffering_writer = Arc::new(DryRunBufferer::new());
    let lp = "cpu,host=A usage=1i 100";

    // Run plugin A first
    let request_a = WalPluginTestRequest {
        filename: "plugin_a".into(),
        database: "_testdb".into(),
        input_lp: lp.into(),
        cache_name: None,
        input_arguments: None,
    };
    let response_a = run_dry_run_wal_plugin(
        now,
        Arc::clone(&catalog),
        Arc::clone(&executor),
        Arc::clone(&buffering_writer),
        code_a.to_string(),
        Arc::clone(&cache),
        request_a,
    )
    .unwrap();
    assert!(
        response_a
            .log_lines
            .iter()
            .any(|l| l.contains("plugin_a: helper_a() = 42")),
        "Plugin A should have executed successfully"
    );

    // Run plugin B and check for leakage from plugin A
    let request_b = WalPluginTestRequest {
        filename: "plugin_b".into(),
        database: "_testdb".into(),
        input_lp: lp.into(),
        cache_name: None,
        input_arguments: None,
    };
    let response_b = run_dry_run_wal_plugin(
        now,
        Arc::clone(&catalog),
        executor,
        Arc::clone(&buffering_writer),
        code_b.to_string(),
        cache,
        request_b,
    )
    .unwrap();

    // Verify plugin B executed and neither helper leaked into __main__
    assert!(
        response_b
            .log_lines
            .iter()
            .any(|l| l.contains("helper_a_leaked: False")),
        "Plugin A's helper_a should NOT have leaked into __main__. Got: {:?}",
        response_b.log_lines
    );
    assert!(
        response_b
            .log_lines
            .iter()
            .any(|l| l.contains("helper_b_leaked: False")),
        "Plugin B's helper_b should NOT have leaked into __main__. Got: {:?}",
        response_b.log_lines
    );
    assert!(
        response_b
            .log_lines
            .iter()
            .any(|l| l.contains("plugin_b: helper_b() = 99")),
        "Plugin B's helper should still be callable from its own namespace"
    );
}
