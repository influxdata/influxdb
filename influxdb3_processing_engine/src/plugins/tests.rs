use super::*;
use crate::{query::UnimplementedQueryEndpoint, virtualenv::init_pyo3};
use chrono::TimeZone;
use hashbrown::HashMap;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_id::DbId;
use influxdb3_write::Precision;
use influxdb3_write::write_buffer::validator::WriteValidator;
use iox_time::{MockProvider, Time, TimeProvider};
use object_store::memory::InMemory;
use std::sync::Barrier;
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

    let response = run_dry_run_wal_plugin(
        now,
        catalog,
        Arc::new(UnimplementedQueryEndpoint),
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
    let db = DatabaseName::new("foodb").unwrap();
    let validator = WriteValidator::initialize(db.clone(), Arc::clone(&catalog)).unwrap();
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

    let response = run_dry_run_wal_plugin(
        now,
        Arc::clone(&catalog),
        Arc::new(UnimplementedQueryEndpoint),
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

    let result = influxdb3_py_api::system_py::execute_schedule_trigger(
        code,
        Utc.timestamp_opt(0, 0).unwrap(),
        Arc::new(DatabaseSchema::new(DbId::from(0), Arc::from("test_db"))),
        Arc::new(UnimplementedQueryEndpoint),
        Arc::new(WriteAccumulator::default()),
        influxdb3_py_api::logging::PluginLogger::dry_run(),
        &None::<HashMap<String, String>>,
        PyCache::new_test_cache(cache, "_shared_test".to_string()),
        None,
        tokio_util::sync::CancellationToken::new(),
    );

    assert!(
        result.is_ok(),
        "PyPluginCallApi exposes unexpected Python methods: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_schedule_plugin_cancelled_token_interrupts_host_api() {
    init_pyo3();

    let code = r#"
def process_scheduled_call(influxdb3_local, call_time, args=None):
    influxdb3_local.info("must be interrupted")
"#;

    let cache = Arc::new(Mutex::new(CacheStore::new(
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
        Duration::from_secs(10),
    )));
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();

    let result = influxdb3_py_api::system_py::execute_schedule_trigger(
        code,
        Utc.timestamp_opt(0, 0).unwrap(),
        Arc::new(DatabaseSchema::new(DbId::from(0), Arc::from("test_db"))),
        Arc::new(UnimplementedQueryEndpoint),
        Arc::new(WriteAccumulator::default()),
        influxdb3_py_api::logging::PluginLogger::dry_run(),
        &None::<HashMap<String, String>>,
        PyCache::new_test_cache(cache, "_shared_test".to_string()),
        None,
        cancel,
    );

    let err = result.expect_err("a cancelled plugin host call should be interrupted");
    assert!(
        err.to_string()
            .contains("influxdb3 is shutting down; aborting plugin execution"),
        "expected cancellation error, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_schedule_plugin_query_rejects_invalid_database_name() {
    init_pyo3();

    let code = r#"
def process_scheduled_call(influxdb3_local, call_time, args=None):
    influxdb3_local.query("SELECT 1", database="bad db")
"#;

    let cache = Arc::new(Mutex::new(CacheStore::new(
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
        Duration::from_secs(10),
    )));

    let result = influxdb3_py_api::system_py::execute_schedule_trigger(
        code,
        Utc.timestamp_opt(0, 0).unwrap(),
        Arc::new(DatabaseSchema::new(DbId::from(0), Arc::from("test_db"))),
        Arc::new(UnimplementedQueryEndpoint),
        Arc::new(WriteAccumulator::default()),
        influxdb3_py_api::logging::PluginLogger::dry_run(),
        &None::<HashMap<String, String>>,
        PyCache::new_test_cache(cache, "_shared_test".to_string()),
        None,
        tokio_util::sync::CancellationToken::new(),
    );

    let err = result.expect_err("query with an invalid database name should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("invalid database name"),
        "expected database name validation error, got: {msg}"
    );
    assert!(
        msg.contains("bad db"),
        "expected invalid db name, got: {msg}"
    );
    assert!(
        msg.contains("contains invalid character"),
        "expected invalid character detail, got: {msg}"
    );
}

/// A multi-file plugin whose import fails must surface the real Python error,
/// not the generic "function is not present" message.
#[tokio::test(flavor = "multi_thread")]
async fn test_schedule_plugin_multi_file_import_error_surfaced() {
    init_pyo3();

    let plugin_parent = tempfile::TempDir::new().unwrap();
    let plugin_dir = plugin_parent.path().join("badimport");
    std::fs::create_dir(&plugin_dir).unwrap();
    std::fs::write(
        plugin_dir.join("__init__.py"),
        "import totally_missing_dep_xyz\n\n\
         def process_scheduled_call(influxdb3_local, call_time, args=None):\n    pass\n",
    )
    .unwrap();

    let cache = Arc::new(Mutex::new(CacheStore::new(
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
        Duration::from_secs(10),
    )));

    let result = influxdb3_py_api::system_py::execute_schedule_trigger(
        "",
        Utc.timestamp_opt(0, 0).unwrap(),
        Arc::new(DatabaseSchema::new(DbId::from(0), Arc::from("test_db"))),
        Arc::new(UnimplementedQueryEndpoint),
        Arc::new(WriteAccumulator::default()),
        influxdb3_py_api::logging::PluginLogger::dry_run(),
        &None::<HashMap<String, String>>,
        PyCache::new_test_cache(cache, "_shared_test".to_string()),
        Some(plugin_dir.as_path()),
        tokio_util::sync::CancellationToken::new(),
    );

    let err = result.expect_err("plugin with missing import dependency should fail to load");
    let msg = err.to_string();
    assert!(
        msg.contains("totally_missing_dep_xyz"),
        "expected the real import error to be surfaced, got: {msg}"
    );
}

/// An AttributeError raised while a multi-file plugin's module is importing must
/// be surfaced as the real cause, not collapsed into the generic missing-function
/// message (the import never reaches the entry-point lookup).
#[tokio::test(flavor = "multi_thread")]
async fn test_schedule_plugin_multi_file_import_time_attribute_error_surfaced() {
    init_pyo3();

    let plugin_parent = tempfile::TempDir::new().unwrap();
    let plugin_dir = plugin_parent.path().join("attrimport");
    std::fs::create_dir(&plugin_dir).unwrap();
    std::fs::write(
        plugin_dir.join("__init__.py"),
        "import sys\nsys.this_attr_xyz_missing\n\n\
         def process_scheduled_call(influxdb3_local, call_time, args=None):\n    pass\n",
    )
    .unwrap();

    let cache = Arc::new(Mutex::new(CacheStore::new(
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
        Duration::from_secs(10),
    )));

    let result = influxdb3_py_api::system_py::execute_schedule_trigger(
        "",
        Utc.timestamp_opt(0, 0).unwrap(),
        Arc::new(DatabaseSchema::new(DbId::from(0), Arc::from("test_db"))),
        Arc::new(UnimplementedQueryEndpoint),
        Arc::new(WriteAccumulator::default()),
        influxdb3_py_api::logging::PluginLogger::dry_run(),
        &None::<HashMap<String, String>>,
        PyCache::new_test_cache(cache, "_shared_test".to_string()),
        Some(plugin_dir.as_path()),
        tokio_util::sync::CancellationToken::new(),
    );

    let err = result.expect_err("plugin raising AttributeError on import should fail to load");
    let msg = err.to_string();
    assert!(
        msg.contains("this_attr_xyz_missing"),
        "expected the real import-time error to be surfaced, got: {msg}"
    );
}

/// A multi-file plugin that imports cleanly but lacks the entry-point function
/// must still report the generic "function is not present" message.
#[tokio::test(flavor = "multi_thread")]
async fn test_schedule_plugin_multi_file_missing_function_reports_generic_error() {
    init_pyo3();

    let plugin_parent = tempfile::TempDir::new().unwrap();
    let plugin_dir = plugin_parent.path().join("nofunc");
    std::fs::create_dir(&plugin_dir).unwrap();
    std::fs::write(
        plugin_dir.join("__init__.py"),
        "def some_other_function():\n    pass\n",
    )
    .unwrap();

    let cache = Arc::new(Mutex::new(CacheStore::new(
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
        Duration::from_secs(10),
    )));

    let result = influxdb3_py_api::system_py::execute_schedule_trigger(
        "",
        Utc.timestamp_opt(0, 0).unwrap(),
        Arc::new(DatabaseSchema::new(DbId::from(0), Arc::from("test_db"))),
        Arc::new(UnimplementedQueryEndpoint),
        Arc::new(WriteAccumulator::default()),
        influxdb3_py_api::logging::PluginLogger::dry_run(),
        &None::<HashMap<String, String>>,
        PyCache::new_test_cache(cache, "_shared_test".to_string()),
        Some(plugin_dir.as_path()),
        tokio_util::sync::CancellationToken::new(),
    );

    let err = result.expect_err("plugin missing the entry-point function should fail to load");
    let msg = err.to_string();
    assert!(
        msg.contains("process_scheduled_call function is not present"),
        "expected the generic missing-function error, got: {msg}"
    );
}

/// A multi-file plugin whose `__init__.py` has a syntax error must surface the
/// real `SyntaxError`, not be masked as a missing entry-point.
#[tokio::test(flavor = "multi_thread")]
async fn test_schedule_plugin_multi_file_syntax_error_surfaced() {
    init_pyo3();

    let plugin_parent = tempfile::TempDir::new().unwrap();
    let plugin_dir = plugin_parent.path().join("syntaxerr");
    std::fs::create_dir(&plugin_dir).unwrap();
    // Missing trailing colon -> SyntaxError at import time.
    std::fs::write(
        plugin_dir.join("__init__.py"),
        "def process_scheduled_call(influxdb3_local, call_time, args=None)\n    pass\n",
    )
    .unwrap();

    let cache = Arc::new(Mutex::new(CacheStore::new(
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
        Duration::from_secs(10),
    )));

    let result = influxdb3_py_api::system_py::execute_schedule_trigger(
        "",
        Utc.timestamp_opt(0, 0).unwrap(),
        Arc::new(DatabaseSchema::new(DbId::from(0), Arc::from("test_db"))),
        Arc::new(UnimplementedQueryEndpoint),
        Arc::new(WriteAccumulator::default()),
        influxdb3_py_api::logging::PluginLogger::dry_run(),
        &None::<HashMap<String, String>>,
        PyCache::new_test_cache(cache, "_shared_test".to_string()),
        Some(plugin_dir.as_path()),
        tokio_util::sync::CancellationToken::new(),
    );

    let err = result.expect_err("plugin with a syntax error should fail to load");
    let msg = err.to_string();
    assert!(
        !msg.contains("is not present in the plugin"),
        "syntax error was masked as a missing entry-point, got: {msg}"
    );
    assert!(
        msg.contains("SyntaxError"),
        "expected the real SyntaxError to be surfaced, got: {msg}"
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
    let catalog = Catalog::new(
        "foo",
        Arc::new(InMemory::new()),
        Arc::clone(&time_provider),
        Default::default(),
    )
    .await
    .unwrap();

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
        Arc::new(UnimplementedQueryEndpoint),
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
        catalog,
        Arc::new(UnimplementedQueryEndpoint),
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

fn write_syspath_race_plugin(root: &std::path::Path, i: usize) -> std::path::PathBuf {
    let parent = root.join(format!("parent_syspath_race_{i}"));
    let plugin = parent.join(format!("pkg_syspath_race_{i}"));
    std::fs::create_dir_all(&plugin).unwrap();

    std::fs::write(
        parent.join(format!("helper_syspath_race_{i}.py")),
        format!("VALUE = {i}\n"),
    )
    .unwrap();

    std::fs::write(
        plugin.join("__init__.py"),
        format!(
            "import time\n\
             time.sleep(0.02)\n\
             import helper_syspath_race_{i}\n\
             \n\
             def process_scheduled_call(influxdb3_local, call_time, args=None):\n    \
                 influxdb3_local.info(f\"loaded helper value {{helper_syspath_race_{i}.VALUE}}\")\n"
        ),
    )
    .unwrap();

    plugin
}

/// Concurrent imports must not interfere with temporary `sys.path` entries.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_multifile_loads_with_distinct_parents_all_succeed() {
    init_pyo3();

    const N: usize = 12;
    let temp_dir = tempfile::tempdir().unwrap();
    let plugin_roots: Vec<_> = (0..N)
        .map(|i| write_syspath_race_plugin(temp_dir.path(), i))
        .collect();
    let cache = Arc::new(Mutex::new(CacheStore::new(
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
        Duration::from_secs(10),
    )));
    let barrier = Arc::new(Barrier::new(N));
    let mut handles = Vec::with_capacity(N);

    for (i, plugin_root) in plugin_roots.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        let cache = Arc::clone(&cache);
        handles.push(tokio::task::spawn_blocking(move || {
            barrier.wait();
            let result = influxdb3_py_api::system_py::execute_schedule_trigger(
                "",
                Utc.timestamp_opt(0, 0).unwrap(),
                Arc::new(DatabaseSchema::new(DbId::from(0), Arc::from("test_db"))),
                Arc::new(UnimplementedQueryEndpoint),
                Arc::new(WriteAccumulator::default()),
                influxdb3_py_api::logging::PluginLogger::dry_run(),
                &None::<HashMap<String, String>>,
                PyCache::new_test_cache(cache, format!("_shared_syspath_test_{i}")),
                Some(plugin_root.as_path()),
                tokio_util::sync::CancellationToken::new(),
            );

            match result {
                Ok(return_state) => {
                    let expected = format!("INFO: loaded helper value {i}");
                    if return_state
                        .log_lines
                        .iter()
                        .any(|line| line.to_string() == expected)
                    {
                        Ok(())
                    } else {
                        Err(format!(
                            "plugin {i}: expected log {expected:?}, got {:?}",
                            return_state.log_lines
                        ))
                    }
                }
                Err(error) => Err(format!("plugin {i}: {error}")),
            }
        }));
    }

    let mut failures = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(error),
            Err(error) => failures.push(format!("task join error: {error}")),
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {N} concurrent multi-file plugin loads failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// A multi-file plugin created after the first import loads without a restart.
/// FileFinder caches the parent dir by mtime, so a dir added later is invisible
/// until the cache is invalidated.
#[tokio::test(flavor = "multi_thread")]
async fn test_multifile_plugin_added_after_first_import_loads() {
    init_pyo3();

    // Unique names: a name already in sys.modules skips the FileFinder scan.
    let first_pkg = "pkg_first_3814";
    let second_pkg = "pkg_second_3814";

    // Each package raises unless loaded under its own name, proving the second
    // package's own code ran.
    let code = |name: &str| {
        format!(
            "def process_scheduled_call(influxdb3_local, call_time, args=None):\n    if __name__ != \"{name}\":\n        raise RuntimeError(\"wrong module: \" + __name__)\n"
        )
    };

    let temp_dir = tempfile::tempdir().unwrap();
    let parent = temp_dir.path();

    // First plugin: importing it populates the FileFinder cache for `parent`.
    let plugin_a = parent.join(first_pkg);
    std::fs::create_dir(&plugin_a).unwrap();
    std::fs::write(plugin_a.join("__init__.py"), code(first_pkg)).unwrap();

    let cache = Arc::new(Mutex::new(CacheStore::new(
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
        Duration::from_secs(10),
    )));

    let exec = |root: &std::path::Path| {
        influxdb3_py_api::system_py::execute_schedule_trigger(
            "",
            Utc.timestamp_opt(0, 0).unwrap(),
            Arc::new(DatabaseSchema::new(DbId::from(0), Arc::from("test_db"))),
            Arc::new(UnimplementedQueryEndpoint),
            Arc::new(WriteAccumulator::default()),
            influxdb3_py_api::logging::PluginLogger::dry_run(),
            &None::<HashMap<String, String>>,
            PyCache::new_test_cache(Arc::clone(&cache), "_shared_test".to_string()),
            Some(root),
            tokio_util::sync::CancellationToken::new(),
        )
    };

    exec(&plugin_a).expect("first multi-file plugin should load");

    // Parent mtime after the first import equals what FileFinder cached.
    let t_scan = std::fs::metadata(parent).unwrap().modified().unwrap();

    // Second plugin created at runtime in the same parent dir.
    let plugin_b = parent.join(second_pkg);
    std::fs::create_dir(&plugin_b).unwrap();
    std::fs::write(plugin_b.join("__init__.py"), code(second_pkg)).unwrap();

    // Force the parent mtime stale so FileFinder will not auto-refresh.
    std::fs::File::open(parent)
        .unwrap()
        .set_modified(t_scan)
        .unwrap();

    let result = exec(&plugin_b);
    assert!(
        result.is_ok(),
        "second multi-file plugin added after the first import should load \
         without a restart, but failed: {result:?}"
    );
}

/// The import retry is scoped to the missing plugin package: a missing
/// dependency inside the package already ran __init__.py, so a blind retry would
/// run its top-level side effects twice.
#[tokio::test(flavor = "multi_thread")]
async fn test_multifile_plugin_missing_dependency_does_not_rerun_top_level() {
    init_pyo3();

    let temp_dir = tempfile::tempdir().unwrap();
    let parent = temp_dir.path();
    let marker = parent.join("top_level_runs.txt");

    // __init__.py records each top-level run, then imports a nonexistent dep.
    let plugin = parent.join("pkg_dep_3814");
    std::fs::create_dir(&plugin).unwrap();
    let init = format!(
        "with open(r\"{}\", \"a\") as _f:\n    _f.write(\"x\")\nimport missing_dependency_3814\n\ndef process_scheduled_call(influxdb3_local, call_time, args=None):\n    pass\n",
        marker.display()
    );
    std::fs::write(plugin.join("__init__.py"), init).unwrap();

    let cache = Arc::new(Mutex::new(CacheStore::new(
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
        Duration::from_secs(10),
    )));

    let result = influxdb3_py_api::system_py::execute_schedule_trigger(
        "",
        Utc.timestamp_opt(0, 0).unwrap(),
        Arc::new(DatabaseSchema::new(DbId::from(0), Arc::from("test_db"))),
        Arc::new(UnimplementedQueryEndpoint),
        Arc::new(WriteAccumulator::default()),
        influxdb3_py_api::logging::PluginLogger::dry_run(),
        &None::<HashMap<String, String>>,
        PyCache::new_test_cache(Arc::clone(&cache), "_shared_test".to_string()),
        Some(plugin.as_path()),
        tokio_util::sync::CancellationToken::new(),
    );

    // The missing-dependency error must still surface, not be masked by the retry.
    assert!(
        result.is_err(),
        "import of a missing dependency should fail, got: {result:?}"
    );
    // The plugin's top-level code must run exactly once, not be re-run by a retry.
    let runs = std::fs::read_to_string(&marker).unwrap_or_default();
    assert_eq!(
        runs.len(),
        1,
        "plugin top-level code ran {} time(s); the import retry must not re-run it",
        runs.len()
    );
}
