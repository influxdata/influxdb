use arrow_util::assert_batches_sorted_eq;
use influxdb_iox_client::management::generated_types::DatabaseRules;

use super::scenario::rand_name;
use crate::common::server_fixture::ServerFixture;

#[tokio::test]
async fn test_delete() {
    test_helpers::maybe_start_logging();
    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();
    let mut management_client = fixture.management_client();
    let mut delete_client = fixture.delete_client();
    let mut flight_client = fixture.flight_client();

    // DB name and rules
    let db_name = rand_name();
    let rules = DatabaseRules {
        name: db_name.clone(),
        ..Default::default()
    };

    // create that db
    management_client
        .create_database(rules.clone())
        .await
        .expect("create database failed");

    // Load a few rows of data
    let lp_lines = vec![
        "cpu,region=west user=23.2 100",
        "cpu,region=west user=21.0 150",
        "disk,region=east bytes=99i 200",
    ];

    let num_lines_written = write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    assert_eq!(num_lines_written, 3);

    // Query cpu
    let mut query_results = flight_client
        .perform_query(db_name.clone(), "select * from cpu")
        .await
        .unwrap();
    let batches = query_results.to_batches().await.unwrap();
    let expected = [
        "+--------+--------------------------------+------+",
        "| region | time                           | user |",
        "+--------+--------------------------------+------+",
        "| west   | 1970-01-01T00:00:00.000000100Z | 23.2 |",
        "| west   | 1970-01-01T00:00:00.000000150Z | 21   |",
        "+--------+--------------------------------+------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    // Delete some data
    let table = "cpu";
    let start = "100";
    let stop = "120";
    let pred = "region = west";
    let _del = delete_client
        .delete(db_name.clone(), table, start, stop, pred)
        .await
        .unwrap();

    // query to verify data deleted
    let mut query_results = flight_client
        .perform_query(db_name.clone(), "select * from cpu")
        .await
        .unwrap();
    let batches = query_results.to_batches().await.unwrap();
    let expected = [
        "+--------+--------------------------------+------+",
        "| region | time                           | user |",
        "+--------+--------------------------------+------+",
        "| west   | 1970-01-01T00:00:00.000000150Z | 21   |",
        "+--------+--------------------------------+------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    // Query cpu again with a selection predicate
    let mut query_results = flight_client
        .perform_query(
            db_name.clone(),
            r#"select * from cpu where cpu.region='west';"#,
        )
        .await
        .unwrap();
    let batches = query_results.to_batches().await.unwrap();
    // result should be as above
    assert_batches_sorted_eq!(&expected, &batches);

    // Query cpu again with a differentselection predicate
    let mut query_results = flight_client
        .perform_query(db_name.clone(), "select * from cpu where user!=21")
        .await
        .unwrap();
    let batches = query_results.to_batches().await.unwrap();
    // result should be nothing
    let expected = ["++", "++"];
    assert_batches_sorted_eq!(&expected, &batches);

    // ------------------------------------------
    // Negative Delete test to get error messages

    // Delete from non-existing table
    let table = "notable";
    let start = "100";
    let stop = "120";
    let pred = "region = west";
    let del = delete_client
        .delete(db_name.clone(), table, start, stop, pred)
        .await
        .unwrap_err()
        .to_string();
    assert!(del.contains("Cannot delete data from table"));

    // Verify both existing tables still have the same data
    // query to verify data deleted
    // cpu
    let mut query_results = flight_client
        .perform_query(db_name.clone(), "select * from cpu")
        .await
        .unwrap();
    let batches = query_results.to_batches().await.unwrap();
    let cpu_expected = [
        "+--------+--------------------------------+------+",
        "| region | time                           | user |",
        "+--------+--------------------------------+------+",
        "| west   | 1970-01-01T00:00:00.000000150Z | 21   |",
        "+--------+--------------------------------+------+",
    ];
    assert_batches_sorted_eq!(&cpu_expected, &batches);
    // disk
    let mut query_results = flight_client
        .perform_query(db_name.clone(), "select * from disk")
        .await
        .unwrap();
    let batches = query_results.to_batches().await.unwrap();
    let disk_expected = [
        "+-------+--------+--------------------------------+",
        "| bytes | region | time                           |",
        "+-------+--------+--------------------------------+",
        "| 99    | east   | 1970-01-01T00:00:00.000000200Z |",
        "+-------+--------+--------------------------------+",
    ];
    assert_batches_sorted_eq!(&disk_expected, &batches);
}
