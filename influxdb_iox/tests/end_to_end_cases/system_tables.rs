use crate::common::server_fixture::TestConfig;
use crate::{
    common::server_fixture::{ServerFixture, ServerType},
    end_to_end_cases::scenario::Scenario,
};
use arrow_util::{assert_batches_eq, test_util::normalize_batches};
use std::num::NonZeroU32;

use super::scenario::{create_readable_database, list_chunks, rand_name};

#[tokio::test]
async fn test_operations() {
    let fixture = ServerFixture::create_shared(ServerType::Database).await;

    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();
    let mut operations_client = fixture.operations_client();

    let db_name1 = rand_name();
    let db_name2 = rand_name();
    create_readable_database(&db_name1, fixture.grpc_channel()).await;
    create_readable_database(&db_name2, fixture.grpc_channel()).await;

    // write only into db_name1
    let partition_key = "cpu";
    let table_name = "cpu";
    let lp_lines = vec!["cpu,region=west user=23.2 100"];

    write_client
        .write_lp(&db_name1, lp_lines.join("\n"), 0)
        .await
        .expect("write succeeded");

    let chunks = list_chunks(&fixture, &db_name1).await;
    let chunk_id = chunks[0].id;

    // Move the chunk to read buffer
    let iox_operation = management_client
        .close_partition_chunk(&db_name1, table_name, partition_key, chunk_id.into())
        .await
        .expect("new partition chunk");

    let operation_id = iox_operation.operation.id();
    operations_client
        .wait_operation(operation_id, Some(std::time::Duration::from_secs(1)))
        .await
        .expect("failed to wait operation");

    let mut client = fixture.flight_client();
    let sql_query = "select status, description from system.operations";

    let batches = client
        .perform_query(&db_name1, sql_query)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // parameterize on db_name1

    let expected_read_data = vec![
        "+---------+---------------------------------+",
        "| status  | description                     |",
        "+---------+---------------------------------+",
        "| Success | Compacting chunks to ReadBuffer |",
        "+---------+---------------------------------+",
    ];

    assert_batches_eq!(expected_read_data, &batches);

    // Should not see jobs from db1 when querying db2
    let batches = client
        .perform_query(&db_name2, sql_query)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let expected_read_data = vec![
        "+--------+-------------+",
        "| status | description |",
        "+--------+-------------+",
        "+--------+-------------+",
    ];

    assert_batches_eq!(expected_read_data, &batches);
}

#[tokio::test]
async fn test_queries() {
    // Need to enable tracing to capture traces
    let config = TestConfig::new(ServerType::Database)
        .with_server_id(NonZeroU32::new(2).unwrap())
        .with_env("TRACES_EXPORTER", "jaeger");

    let fixture = ServerFixture::create_single_use_with_config(config).await;

    let scenario = Scenario::new();
    let (db_name, _db_uuid) = scenario
        .create_database(&mut fixture.management_client())
        .await;

    // issue a storage gRPC query as well (likewise will error, but we
    // are just checking that things are hooked up here).
    let read_source = scenario.read_source();
    let range = Some(generated_types::TimestampRange {
        start: 111111,
        end: 222222,
    });

    // run a valid query (success = true)
    let read_filter_request = tonic::Request::new(generated_types::ReadFilterRequest {
        read_source,
        range,
        ..Default::default()
    });
    fixture
        .storage_client()
        .read_filter(read_filter_request)
        .await
        .unwrap();

    // run an invalid query (success = false)
    let read_source = scenario.read_source();
    let range = Some(generated_types::TimestampRange {
        // start is after end, so error
        start: 22222,
        end: 11111,
    });

    let mut read_filter_request = tonic::Request::new(generated_types::ReadFilterRequest {
        read_source,
        range,
        ..Default::default()
    });
    read_filter_request
        .metadata_mut()
        .insert("uber-trace-id", "45ded43:1:0:1".parse().unwrap());

    fixture
        .storage_client()
        .read_filter(read_filter_request)
        .await
        .unwrap_err();

    // Note: don't select issue_time as that changes from run to run
    //
    // Note 2: it is possible for another test to issue queries
    // against this database concurrently and appear in the queries
    // list (sql observer mode does it) so only select for what we
    // expect
    //
    // See https://github.com/influxdata/influxdb_iox/issues/3396
    let query =
        "select query_type, query_text, success, trace_id from system.queries where query_type = 'read_filter'";

    // Query system.queries and should have an entry for the storage rpc
    let batches = fixture
        .flight_client()
        .perform_query(&db_name, query)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batches = normalize_batches(batches, scenario.normalizer());

    let expected_read_data = vec![
        "+-------------+---------------------------------+---------+----------+",
        "| query_type  | query_text                      | success | trace_id |",
        "+-------------+---------------------------------+---------+----------+",
        "| read_filter | {                               | true    |          |",
        "|             |   \"ReadSource\": {               |         |          |",
        "|             |     \"typeUrl\": \"/TODO\",         |         |          |",
        "|             |     \"value\": \"ZZZZZZZZZZZZZZZZ\" |         |          |",
        "|             |   },                            |         |          |",
        "|             |   \"range\": {                    |         |          |",
        "|             |     \"start\": \"111111\",          |         |          |",
        "|             |     \"end\": \"222222\"             |         |          |",
        "|             |   }                             |         |          |",
        "|             | }                               |         |          |",
        "| read_filter | {                               | false   | 45ded43  |",
        "|             |   \"ReadSource\": {               |         |          |",
        "|             |     \"typeUrl\": \"/TODO\",         |         |          |",
        "|             |     \"value\": \"ZZZZZZZZZZZZZZZZ\" |         |          |",
        "|             |   },                            |         |          |",
        "|             |   \"range\": {                    |         |          |",
        "|             |     \"start\": \"22222\",           |         |          |",
        "|             |     \"end\": \"11111\"              |         |          |",
        "|             |   }                             |         |          |",
        "|             | }                               |         |          |",
        "+-------------+---------------------------------+---------+----------+",
    ];
    assert_batches_eq!(expected_read_data, &batches);

    // send in an invalid SQL query (fails during planning). It would
    // be nice to also test failing during execution but I am not
    // quite sure how to trigger that
    let query = "select foo from blarg read_filter";
    fixture
        .flight_client()
        .perform_query(&db_name, query)
        .await
        .unwrap_err();

    // Query system.queries and should also have an entry for the sql
    // we just wrote (and what we are about to write)
    let query = "select query_type, query_text, completed_duration IS NOT NULL as is_complete, success from system.queries where query_type = 'sql' and query_text like '%read_filter%'";
    let batches = fixture
        .flight_client()
        .perform_query(&db_name, query)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batches = normalize_batches(batches, scenario.normalizer());

    let expected_read_data = vec![
        "+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+---------+",
        "| query_type | query_text                                                                                                                                                             | is_complete | success |",
        "+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+---------+",
        "| sql        | select query_type, query_text, success, trace_id from system.queries where query_type = 'read_filter'                                                                  | true        | true    |",
        "| sql        | select foo from blarg read_filter                                                                                                                                      | true        | false   |",
        "| sql        | select query_type, query_text, completed_duration IS NOT NULL as is_complete, success from system.queries where query_type = 'sql' and query_text like '%read_filter%' | false       | false   |",
        "+------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+---------+",
    ];
    assert_batches_eq!(expected_read_data, &batches);
}
