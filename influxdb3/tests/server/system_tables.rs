use arrow_util::assert_batches_sorted_eq;
use influxdb3_client::Precision;
use serde_json::json;
use std::fs;
use tempfile::TempDir;

use crate::server::{ConfigProvider, TestServer, collect_stream};

#[tokio::test]
async fn queries_table() {
    let server = TestServer::spawn().await;

    server
        .write_lp_to_db(
            "foo",
            "cpu,host=s1,region=us-east usage=0.9 2998574931\n\
             cpu,host=s1,region=us-east usage=0.89 2998574932\n\
             cpu,host=s1,region=us-east usage=0.85 2998574933",
            Precision::Second,
        )
        .await
        .expect("write some lp");

    let mut client = server.flight_sql_client("foo").await;

    // Check queries table for completed queries, will be empty:
    {
        let response = client
            .query("SELECT COUNT(*) FROM system.queries WHERE running = false")
            .await
            .unwrap();

        let batches = collect_stream(response).await;
        assert_batches_sorted_eq!(
            [
                "+----------+",
                "| count(*) |",
                "+----------+",
                "| 0        |",
                "+----------+",
            ],
            &batches
        );
    }

    // Do some queries, to produce some query logs:
    {
        let queries = [
            "SELECT * FROM cpu",           // valid
            "SELECT * FROM mem",           // not valid table, will fail, and not be logged
            "SELECT usage, time FROM cpu", // specific columns
        ];
        for q in queries {
            let response = client.query(q).await;
            // collect the stream to make sure the query completes:
            if let Ok(stream) = response {
                let _batches = collect_stream(stream).await;
            }
        }
    }

    // Now check the log:
    {
        // A sub-set of columns is selected in this query, because the queries
        // table contains may columns whose values change in susequent test runs
        let response = client
            .query(
                "SELECT \
                    phase, \
                    query_type, \
                    query_text, \
                    success, \
                    running, \
                    cancelled \
                FROM system.queries \
                WHERE success = true",
            )
            .await
            .unwrap();

        let batches = collect_stream(response).await;
        assert_batches_sorted_eq!(
            [
                "+---------+------------+--------------------------------------------------------------------------------+---------+---------+-----------+",
                "| phase   | query_type | query_text                                                                     | success | running | cancelled |",
                "+---------+------------+--------------------------------------------------------------------------------+---------+---------+-----------+",
                "| success | flightsql  | CommandStatementQuerySELECT * FROM cpu                                         | true    | false   | false     |",
                "| success | flightsql  | CommandStatementQuerySELECT COUNT(*) FROM system.queries WHERE running = false | true    | false   | false     |",
                "| success | flightsql  | CommandStatementQuerySELECT usage, time FROM cpu                               | true    | false   | false     |",
                "+---------+------------+--------------------------------------------------------------------------------+---------+---------+-----------+",
            ],
            &batches
        );
    }
}

#[test_log::test(tokio::test)]
async fn last_caches_table() {
    let server = TestServer::spawn().await;
    let db1_name = "foo";
    let db2_name = "bar";
    // Write some LP to initialize the catalog
    server
        .write_lp_to_db(
            db1_name,
            "\
        cpu,region=us,host=a,cpu=1 usage=90\n\
        mem,region=us,host=a usage=500\n\
        ",
            Precision::Second,
        )
        .await
        .expect("write to db");
    server
        .write_lp_to_db(
            db2_name,
            "\
        cpu,region=us,host=a,cpu=1 usage=90\n\
        mem,region=us,host=a usage=500\n\
        ",
            Precision::Second,
        )
        .await
        .expect("write to db");

    // Check that there are no last caches:
    {
        let resp = server
            .flight_sql_client(db1_name)
            .await
            .query("SELECT * FROM system.last_caches")
            .await
            .unwrap();
        let batches = collect_stream(resp).await;
        assert_batches_sorted_eq!(["++", "++",], &batches);
    }
    {
        let resp = server
            .flight_sql_client(db2_name)
            .await
            .query("SELECT * FROM system.last_caches")
            .await
            .unwrap();
        let batches = collect_stream(resp).await;
        assert_batches_sorted_eq!(["++", "++",], &batches);
    }

    // Create some last caches, two on DB1 and one on DB2:
    assert!(
        server
            .api_v3_configure_last_cache_create(&json!({
                "db": db1_name,
                "table": "cpu",
                "key_columns": ["host"],
            }))
            .await
            .status()
            .is_success()
    );
    assert!(
        server
            .api_v3_configure_last_cache_create(&json!({
                "db": db1_name,
                "table": "mem",
                "name": "mem_last_cache",
                "value_columns": ["usage"],
                "ttl": 60
            }))
            .await
            .status()
            .is_success()
    );
    assert!(
        server
            .api_v3_configure_last_cache_create(&json!({
                "db": db2_name,
                "table": "cpu",
                "count": 5
            }))
            .await
            .status()
            .is_success()
    );

    // Check the system table for each DB:
    {
        let resp = server
            .flight_sql_client(db1_name)
            .await
            .query("SELECT * FROM system.last_caches")
            .await
            .unwrap();
        let batches = collect_stream(resp).await;
        assert_batches_sorted_eq!(
            [
                "+-------+---------------------+----------------+------------------+------------------+--------------------+-------+-------+",
                "| table | name                | key_column_ids | key_column_names | value_column_ids | value_column_names | count | ttl   |",
                "+-------+---------------------+----------------+------------------+------------------+--------------------+-------+-------+",
                "| cpu   | cpu_host_last_cache | [1]            | [host]           |                  |                    | 1     | 14400 |",
                "| mem   | mem_last_cache      | [0, 1]         | [region, host]   | [2]              | [usage]            | 1     | 60    |",
                "+-------+---------------------+----------------+------------------+------------------+--------------------+-------+-------+",
            ],
            &batches
        );
    }
    {
        let resp = server
            .flight_sql_client(db2_name)
            .await
            .query("SELECT * FROM system.last_caches")
            .await
            .unwrap();
        let batches = collect_stream(resp).await;
        assert_batches_sorted_eq!(
            [
                "+-------+--------------------------------+----------------+---------------------+------------------+--------------------+-------+-------+",
                "| table | name                           | key_column_ids | key_column_names    | value_column_ids | value_column_names | count | ttl   |",
                "+-------+--------------------------------+----------------+---------------------+------------------+--------------------+-------+-------+",
                "| cpu   | cpu_region_host_cpu_last_cache | [0, 1, 2]      | [region, host, cpu] |                  |                    | 5     | 14400 |",
                "+-------+--------------------------------+----------------+---------------------+------------------+--------------------+-------+-------+",
            ],
            &batches
        );
    }

    // Make some changes to the caches and check the system table

    // Delete one of the caches:
    {
        assert!(
            server
                .api_v3_configure_last_cache_delete(&json!({
                    "db": db1_name,
                    "table": "cpu",
                    "name": "cpu_host_last_cache",
                }))
                .await
                .status()
                .is_success()
        );

        let resp = server
            .flight_sql_client(db1_name)
            .await
            .query("SELECT * FROM system.last_caches")
            .await
            .unwrap();
        let batches = collect_stream(resp).await;
        assert_batches_sorted_eq!(
            [
                "+-------+----------------+----------------+------------------+------------------+--------------------+-------+-----+",
                "| table | name           | key_column_ids | key_column_names | value_column_ids | value_column_names | count | ttl |",
                "+-------+----------------+----------------+------------------+------------------+--------------------+-------+-----+",
                "| mem   | mem_last_cache | [0, 1]         | [region, host]   | [2]              | [usage]            | 1     | 60  |",
                "+-------+----------------+----------------+------------------+------------------+--------------------+-------+-----+",
            ],
            &batches
        );
    }

    // Add fields to one of the caches, in this case, the `temp` field will get added to the
    // value columns for the respective cache, but since this cache accepts new fields, the value
    // columns are not shown in the system table result:
    {
        server
            .write_lp_to_db(
                db2_name,
                "cpu,region=us,host=a,cpu=2 usage=40,temp=95",
                Precision::Second,
            )
            .await
            .unwrap();

        let resp = server
            .flight_sql_client(db2_name)
            .await
            .query("SELECT * FROM system.last_caches")
            .await
            .unwrap();
        let batches = collect_stream(resp).await;
        assert_batches_sorted_eq!(
            [
                "+-------+--------------------------------+----------------+---------------------+------------------+--------------------+-------+-------+",
                "| table | name                           | key_column_ids | key_column_names    | value_column_ids | value_column_names | count | ttl   |",
                "+-------+--------------------------------+----------------+---------------------+------------------+--------------------+-------+-------+",
                "| cpu   | cpu_region_host_cpu_last_cache | [0, 1, 2]      | [region, host, cpu] |                  |                    | 5     | 14400 |",
                "+-------+--------------------------------+----------------+---------------------+------------------+--------------------+-------+-------+",
            ],
            &batches
        );
    }
}

#[tokio::test]
async fn distinct_caches_table() {
    let server = TestServer::spawn().await;
    let db_1_name = "foo";
    let db_2_name = "bar";
    // write some lp to both db's to initialize the catalog:
    server
        .write_lp_to_db(
            db_1_name,
            "\
        cpu,region=us-east,host=a usage=90\n\
        mem,region=us-east,host=a usage=90\n\
        ",
            Precision::Second,
        )
        .await
        .unwrap();
    server
        .write_lp_to_db(
            db_2_name,
            "\
        cpu,region=us-east,host=a usage=90\n\
        mem,region=us-east,host=a usage=90\n\
        ",
            Precision::Second,
        )
        .await
        .unwrap();

    // check that there are no distinct caches:
    for db_name in [db_1_name, db_2_name] {
        let response_stream = server
            .flight_sql_client(db_name)
            .await
            .query("SELECT * FROM system.distinct_caches")
            .await
            .unwrap();
        let batches = collect_stream(response_stream).await;
        assert_batches_sorted_eq!(["++", "++",], &batches);
    }

    // create some distinct caches on the two databases:
    assert!(
        server
            .api_v3_configure_distinct_cache_create(&json!({
                "db": db_1_name,
                "table": "cpu",
                "columns": ["region", "host"],
            }))
            .await
            .status()
            .is_success()
    );
    assert!(
        server
            .api_v3_configure_distinct_cache_create(&json!({
                "db": db_1_name,
                "table": "mem",
                "columns": ["region", "host"],
                "max_cardinality": 1_000,
            }))
            .await
            .status()
            .is_success()
    );
    assert!(
        server
            .api_v3_configure_distinct_cache_create(&json!({
                "db": db_2_name,
                "table": "cpu",
                "columns": ["host"],
                "max_age": 1_000,
            }))
            .await
            .status()
            .is_success()
    );

    // check the system table query for each db:
    {
        let response_stream = server
            .flight_sql_client(db_1_name)
            .await
            .query("SELECT * FROM system.distinct_caches")
            .await
            .unwrap();
        let batches = collect_stream(response_stream).await;
        assert_batches_sorted_eq!(
            [
                "+-------+--------------------------------+------------+----------------+-----------------+-----------------+",
                "| table | name                           | column_ids | column_names   | max_cardinality | max_age_seconds |",
                "+-------+--------------------------------+------------+----------------+-----------------+-----------------+",
                "| cpu   | cpu_region_host_distinct_cache | [0, 1]     | [region, host] | 100000          | 86400           |",
                "| mem   | mem_region_host_distinct_cache | [0, 1]     | [region, host] | 1000            | 86400           |",
                "+-------+--------------------------------+------------+----------------+-----------------+-----------------+",
            ],
            &batches
        );
    }
    {
        let response_stream = server
            .flight_sql_client(db_2_name)
            .await
            .query("SELECT * FROM system.distinct_caches")
            .await
            .unwrap();
        let batches = collect_stream(response_stream).await;
        assert_batches_sorted_eq!(
            [
                "+-------+-------------------------+------------+--------------+-----------------+-----------------+",
                "| table | name                    | column_ids | column_names | max_cardinality | max_age_seconds |",
                "+-------+-------------------------+------------+--------------+-----------------+-----------------+",
                "| cpu   | cpu_host_distinct_cache | [1]        | [host]       | 100000          | 1000            |",
                "+-------+-------------------------+------------+--------------+-----------------+-----------------+",
            ],
            &batches
        );
    }

    // delete caches and check that the system tables reflect those changes:
    assert!(
        server
            .api_v3_configure_distinct_cache_delete(&json!({
                "db": db_1_name,
                "table": "cpu",
                "name": "cpu_region_host_distinct_cache",
            }))
            .await
            .status()
            .is_success()
    );
    assert!(
        server
            .api_v3_configure_distinct_cache_delete(&json!({
                "db": db_2_name,
                "table": "cpu",
                "name": "cpu_host_distinct_cache",
            }))
            .await
            .status()
            .is_success()
    );

    // check the system tables again:
    {
        let response_stream = server
            .flight_sql_client(db_1_name)
            .await
            .query("SELECT * FROM system.distinct_caches")
            .await
            .unwrap();
        let batches = collect_stream(response_stream).await;
        assert_batches_sorted_eq!(
            [
                "+-------+--------------------------------+------------+----------------+-----------------+-----------------+",
                "| table | name                           | column_ids | column_names   | max_cardinality | max_age_seconds |",
                "+-------+--------------------------------+------------+----------------+-----------------+-----------------+",
                "| mem   | mem_region_host_distinct_cache | [0, 1]     | [region, host] | 1000            | 86400           |",
                "+-------+--------------------------------+------------+----------------+-----------------+-----------------+",
            ],
            &batches
        );
    }
    {
        let response_stream = server
            .flight_sql_client(db_2_name)
            .await
            .query("SELECT * FROM system.distinct_caches")
            .await
            .unwrap();
        let batches = collect_stream(response_stream).await;
        assert_batches_sorted_eq!(["++", "++",], &batches);
    }
}

#[tokio::test]
async fn test_generation_durations_system_table_with_defaults() {
    let server = TestServer::spawn().await;
    let response_stream = server
        .flight_sql_client("_internal")
        .await
        .query("select * from system.generation_durations")
        .await
        .unwrap();
    let batches = collect_stream(response_stream).await;
    assert_batches_sorted_eq!(
        [
            "+-------+------------------+",
            "| level | duration_seconds |",
            "+-------+------------------+",
            "| 1     | 600              |",
            "+-------+------------------+",
        ],
        &batches
    );
}

#[tokio::test]
async fn test_generation_durations_system_table_with_non_defaults() {
    let tmp_dir = TempDir::new().unwrap();
    let server = TestServer::configure()
        .with_gen1_duration("1m")
        .with_object_store_dir(tmp_dir.path().to_str().unwrap())
        .spawn()
        .await;
    let response_stream = server
        .flight_sql_client("_internal")
        .await
        .query("select * from system.generation_durations")
        .await
        .unwrap();
    let batches = collect_stream(response_stream).await;
    assert_batches_sorted_eq!(
        [
            "+-------+------------------+",
            "| level | duration_seconds |",
            "+-------+------------------+",
            "| 1     | 60               |",
            "+-------+------------------+",
        ],
        &batches
    );

    drop(server);

    // spawn again using different gen1 duration, so we can check
    // that the server starts, but the gen1 duration doesn't change:
    let server = TestServer::configure()
        .with_gen1_duration("5m")
        .with_object_store_dir(tmp_dir.path().to_str().unwrap())
        .spawn()
        .await;
    let response_stream = server
        .flight_sql_client("_internal")
        .await
        .query("select * from system.generation_durations")
        .await
        .unwrap();
    let batches = collect_stream(response_stream).await;
    assert_batches_sorted_eq!(
        [
            "+-------+------------------+",
            "| level | duration_seconds |",
            "+-------+------------------+",
            "| 1     | 60               |",
            "+-------+------------------+",
        ],
        &batches
    );
}

#[tokio::test]
async fn test_nodes_table_with_cli_params() {
    let server = TestServer::spawn().await;

    // Query the system.nodes table for cli_params
    let mut client = server.flight_sql_client("_internal").await;
    let response_stream = client
        .query("SELECT node_id, cli_params FROM system.nodes")
        .await
        .unwrap();
    let batches = collect_stream(response_stream).await;

    // Should have one row
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    // Check that node_id matches what we expect
    let node_id_col = batch.column_by_name("node_id").unwrap();
    let node_id_array = node_id_col
        .as_any()
        .downcast_ref::<arrow::array::StringViewArray>()
        .unwrap();
    assert_eq!(node_id_array.value(0), "test-server");

    // Check that cli_params exists and is valid JSON
    let cli_params_col = batch.column_by_name("cli_params").unwrap();
    let cli_params_array = cli_params_col
        .as_any()
        .downcast_ref::<arrow::array::StringViewArray>()
        .unwrap();
    let cli_params_json = cli_params_array.value(0);

    // Parse it as JSON to verify it's valid
    let parsed: serde_json::Value =
        serde_json::from_str(cli_params_json).expect("cli_params should be valid JSON");
    assert!(parsed.is_object(), "cli_params should be a JSON object");

    // Check that at least some expected parameters are present
    let params_obj = parsed.as_object().unwrap();
    assert!(
        params_obj.contains_key("node-id"),
        "Should contain node-id parameter"
    );
    assert_eq!(
        params_obj.get("node-id").unwrap().as_str(),
        Some("test-server")
    );

    // Check that sensitive parameters are masked
    if params_obj.contains_key("without-auth") {
        assert_eq!(
            params_obj.get("without-auth").unwrap().as_str(),
            Some("*******"),
            "without-auth parameter should be masked"
        );
    }
}

#[tokio::test]
async fn test_nodes_table_filters_sensitive_params() {
    let server = TestServer::spawn().await;

    // Query the system.nodes table for cli_params
    let mut client = server.flight_sql_client("_internal").await;
    let response_stream = client
        .query("SELECT cli_params FROM system.nodes")
        .await
        .unwrap();
    let batches = collect_stream(response_stream).await;

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    // Get the cli_params value
    let cli_params_col = batch.column_by_name("cli_params").unwrap();
    let cli_params_array = cli_params_col
        .as_any()
        .downcast_ref::<arrow::array::StringViewArray>()
        .unwrap();
    let cli_params_json = cli_params_array.value(0);

    // Parse JSON and check that sensitive values are masked
    let parsed: serde_json::Value =
        serde_json::from_str(cli_params_json).expect("cli_params should be valid JSON");
    let params_obj = parsed.as_object().unwrap();

    // Check known sensitive parameters that should be masked
    for (key, value) in params_obj.iter() {
        let key_lower = key.to_lowercase();

        // Check for any parameters that should be masked
        if key_lower.contains("key")
            || key_lower.contains("secret")
            || key_lower.contains("password")
            || key_lower.contains("token")
            || key == "without-auth"
            || key == "tls-cert"
            || key == "tls-key"
        {
            assert_eq!(
                value.as_str(),
                Some("*******"),
                "Parameter '{}' should be masked but has value: {:?}",
                key,
                value
            );
        }
    }

    // Verify that non-sensitive parameters are NOT masked
    if let Some(node_id_value) = params_obj.get("node-id") {
        assert_ne!(
            node_id_value.as_str(),
            Some("*******"),
            "node-id should not be masked"
        );
    }
}

#[tokio::test]
async fn test_plugin_files_system_table() {
    let plugin_dir = TempDir::new().unwrap();
    let plugin_dir_path = plugin_dir.path();

    // Create a single-file plugin
    let single_plugin_content = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    for table_name, batches in table_batches.items():
        influxdb3_local.info(f"Processing {table_name}")
"#;
    fs::write(
        plugin_dir_path.join("simple_plugin.py"),
        single_plugin_content,
    )
    .unwrap();

    // Create a multi-file plugin directory
    let multi_plugin_dir = plugin_dir_path.join("multi_plugin");
    fs::create_dir(&multi_plugin_dir).unwrap();

    let main_content = r#"
from helper import process_data

def process_writes(influxdb3_local, table_batches, args=None):
    process_data(influxdb3_local, table_batches)
"#;
    fs::write(multi_plugin_dir.join("__main__.py"), main_content).unwrap();

    let helper_content = r#"
def process_data(influxdb3_local, table_batches):
    influxdb3_local.info("Helper processing data")
"#;
    fs::write(multi_plugin_dir.join("helper.py"), helper_content).unwrap();

    // Start server with plugin directory
    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir_path.to_str().unwrap())
        .spawn()
        .await;

    // Create database and table
    server
        .write_lp_to_db("test_db", "cpu usage=1", Precision::Second)
        .await
        .unwrap();

    // Check initially empty plugin_files table
    {
        let mut client = server.flight_sql_client("_internal").await;
        let response = client
            .query("SELECT COUNT(*) as count FROM system.plugin_files")
            .await
            .unwrap();
        let batches = collect_stream(response).await;
        assert_batches_sorted_eq!(
            [
                "+-------+",
                "| count |",
                "+-------+",
                "| 0     |",
                "+-------+",
            ],
            &batches
        );
    }

    // Create triggers for both plugins
    let resp = server
        .api_v3_configure_processing_engine_trigger(&json!({
            "db": "test_db",
            "trigger_name": "simple_trigger",
            "path": "simple_plugin.py",
            "trigger_specification": "all_tables",
            "trigger_settings": {
                "run_async": false,
                "error_behavior": "log"
            },
            "disabled": false
        }))
        .await;
    assert!(resp.status().is_success());

    let resp = server
        .api_v3_configure_processing_engine_trigger(&json!({
            "db": "test_db",
            "trigger_name": "multi_trigger",
            "path": "multi_plugin",
            "trigger_specification": "table:cpu",
            "trigger_settings": {
                "run_async": false,
                "error_behavior": "log"
            },
            "disabled": false
        }))
        .await;
    assert!(resp.status().is_success());

    // Now query plugin_files and verify all files appear
    {
        let mut client = server.flight_sql_client("_internal").await;
        let response = client
            .query("SELECT plugin_name, file_name, size_bytes > 0 as has_size FROM system.plugin_files ORDER BY plugin_name, file_name")
            .await
            .unwrap();
        let batches = collect_stream(response).await;
        assert_batches_sorted_eq!(
            [
                "+----------------+------------------+----------+",
                "| plugin_name    | file_name        | has_size |",
                "+----------------+------------------+----------+",
                "| multi_trigger  | __main__.py      | true     |",
                "| multi_trigger  | helper.py        | true     |",
                "| simple_trigger | simple_plugin.py | true     |",
                "+----------------+------------------+----------+",
            ],
            &batches
        );
    }
}

#[tokio::test]
async fn test_processing_engine_triggers_table() {
    let plugin_dir = TempDir::new().unwrap();
    let plugin_dir_path = plugin_dir.path();

    // Create test plugins
    let wal_plugin = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    pass
"#;
    fs::write(plugin_dir_path.join("wal_plugin.py"), wal_plugin).unwrap();

    let schedule_plugin = r#"
def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    pass
"#;
    fs::write(plugin_dir_path.join("schedule_plugin.py"), schedule_plugin).unwrap();

    // Start server with plugin directory
    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir_path.to_str().unwrap())
        .spawn()
        .await;

    // Create database and table
    server
        .write_lp_to_db("test_db", "cpu usage=1", Precision::Second)
        .await
        .unwrap();

    // Initially empty triggers table
    {
        let mut client = server.flight_sql_client("test_db").await;
        let response = client
            .query("SELECT COUNT(*) as count FROM system.processing_engine_triggers")
            .await
            .unwrap();
        let batches = collect_stream(response).await;
        assert_batches_sorted_eq!(
            [
                "+-------+",
                "| count |",
                "+-------+",
                "| 0     |",
                "+-------+",
            ],
            &batches
        );
    }

    // Create various triggers
    let resp = server
        .api_v3_configure_processing_engine_trigger(&json!({
            "db": "test_db",
            "trigger_name": "wal_trigger",
            "path": "wal_plugin.py",
            "trigger_specification": "table:cpu",
            "trigger_settings": {
                "run_async": false,
                "error_behavior": "log"
            },
            "disabled": false
        }))
        .await;
    assert!(resp.status().is_success());

    let resp = server
        .api_v3_configure_processing_engine_trigger(&json!({
            "db": "test_db",
            "trigger_name": "schedule_trigger",
            "path": "schedule_plugin.py",
            "trigger_specification": "every:10s",
            "trigger_settings": {
                "run_async": false,
                "error_behavior": "log"
            },
            "disabled": false
        }))
        .await;
    assert!(resp.status().is_success());

    let resp = server
        .api_v3_configure_processing_engine_trigger(&json!({
            "db": "test_db",
            "trigger_name": "disabled_trigger",
            "path": "wal_plugin.py",
            "trigger_specification": "all_tables",
            "trigger_settings": {
                "run_async": false,
                "error_behavior": "log"
            },
            "disabled": true
        }))
        .await;
    assert!(resp.status().is_success());

    // Query the triggers table
    {
        let mut client = server.flight_sql_client("test_db").await;
        let response = client
            .query("SELECT trigger_name, plugin_filename, disabled FROM system.processing_engine_triggers ORDER BY trigger_name")
            .await
            .unwrap();
        let batches = collect_stream(response).await;
        assert_batches_sorted_eq!(
            [
                "+------------------+--------------------+----------+",
                "| trigger_name     | plugin_filename    | disabled |",
                "+------------------+--------------------+----------+",
                "| disabled_trigger | wal_plugin.py      | true     |",
                "| schedule_trigger | schedule_plugin.py | false    |",
                "| wal_trigger      | wal_plugin.py      | false    |",
                "+------------------+--------------------+----------+",
            ],
            &batches
        );
    }
}

#[tokio::test]
async fn test_processing_engine_trigger_arguments_table() {
    let plugin_dir = TempDir::new().unwrap();
    let plugin_dir_path = plugin_dir.path();

    // Create a test plugin
    let plugin_content = r#"
def process_writes(influxdb3_local, table_batches, args=None):
    if args:
        for key, value in args.items():
            influxdb3_local.info(f"{key}={value}")
"#;
    fs::write(plugin_dir_path.join("test_plugin.py"), plugin_content).unwrap();

    // Start server with plugin directory
    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir_path.to_str().unwrap())
        .spawn()
        .await;

    // Create database
    server
        .write_lp_to_db("test_db", "cpu usage=1", Precision::Second)
        .await
        .unwrap();

    // Initially empty trigger arguments table
    {
        let mut client = server.flight_sql_client("test_db").await;
        let response = client
            .query("SELECT COUNT(*) as count FROM system.processing_engine_trigger_arguments")
            .await
            .unwrap();
        let batches = collect_stream(response).await;
        assert_batches_sorted_eq!(
            [
                "+-------+",
                "| count |",
                "+-------+",
                "| 0     |",
                "+-------+",
            ],
            &batches
        );
    }

    // Create trigger with arguments
    let resp = server
        .api_v3_configure_processing_engine_trigger(&json!({
            "db": "test_db",
            "trigger_name": "trigger_with_args",
            "path": "test_plugin.py",
            "trigger_specification": "all_tables",
            "trigger_settings": {
                "run_async": false,
                "error_behavior": "log"
            },
            "trigger_arguments": {
                "key1": "value1",
                "key2": "value2",
                "threshold": "100"
            },
            "disabled": false
        }))
        .await;
    assert!(resp.status().is_success());

    // Create trigger without arguments
    let resp = server
        .api_v3_configure_processing_engine_trigger(&json!({
            "db": "test_db",
            "trigger_name": "trigger_no_args",
            "path": "test_plugin.py",
            "trigger_specification": "table:cpu",
            "trigger_settings": {
                "run_async": false,
                "error_behavior": "log"
            },
            "disabled": false
        }))
        .await;
    assert!(resp.status().is_success());

    // Query the trigger arguments table
    {
        let mut client = server.flight_sql_client("test_db").await;
        let response = client
            .query("SELECT trigger_name, argument_key, argument_value FROM system.processing_engine_trigger_arguments ORDER BY trigger_name, argument_key")
            .await
            .unwrap();
        let batches = collect_stream(response).await;
        assert_batches_sorted_eq!(
            [
                "+-------------------+--------------+----------------+",
                "| trigger_name      | argument_key | argument_value |",
                "+-------------------+--------------+----------------+",
                "| trigger_with_args | key1         | value1         |",
                "| trigger_with_args | key2         | value2         |",
                "| trigger_with_args | threshold    | 100            |",
                "+-------------------+--------------+----------------+",
            ],
            &batches
        );
    }
}
