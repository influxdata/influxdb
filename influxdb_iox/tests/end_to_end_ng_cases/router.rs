use influxdb_iox_client::write::generated_types::{column, Column, TableBatch};
use test_helpers_end_to_end_ng::{maybe_skip_integration, MiniCluster, Step, StepTest};

#[tokio::test]
async fn write_via_grpc() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let table_name = "the_table";
    let table_batches = vec![TableBatch {
        table_name: table_name.to_string(),
        columns: vec![
            Column {
                column_name: "time".to_string(),
                semantic_type: column::SemanticType::Time as i32,
                values: Some(column::Values {
                    i64_values: vec![123456],
                    f64_values: vec![],
                    u64_values: vec![],
                    string_values: vec![],
                    bool_values: vec![],
                    bytes_values: vec![],
                    packed_string_values: None,
                    interned_string_values: None,
                }),
                null_mask: vec![],
            },
            Column {
                column_name: "val".to_string(),
                semantic_type: column::SemanticType::Field as i32,
                values: Some(column::Values {
                    i64_values: vec![42],
                    f64_values: vec![],
                    u64_values: vec![],
                    string_values: vec![],
                    bool_values: vec![],
                    bytes_values: vec![],
                    packed_string_values: None,
                    interned_string_values: None,
                }),
                null_mask: vec![],
            },
            Column {
                column_name: "tag1".to_string(),
                semantic_type: column::SemanticType::Tag as i32,
                values: Some(column::Values {
                    i64_values: vec![],
                    f64_values: vec![],
                    u64_values: vec![],
                    string_values: vec!["A".into()],
                    bool_values: vec![],
                    bytes_values: vec![],
                    packed_string_values: None,
                    interned_string_values: None,
                }),
                null_mask: vec![],
            },
            Column {
                column_name: "tag2".to_string(),
                semantic_type: column::SemanticType::Tag as i32,
                values: Some(column::Values {
                    i64_values: vec![],
                    f64_values: vec![],
                    u64_values: vec![],
                    string_values: vec!["B".into()],
                    bool_values: vec![],
                    bytes_values: vec![],
                    packed_string_values: None,
                    interned_string_values: None,
                }),
                null_mask: vec![],
            },
        ],
        row_count: 1,
    }];

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteTableBatches(table_batches),
            // format!("{},tag1=A,tag2=B val=42i 123456", table_name)),
            // Wait for data to be persisted to parquet
            Step::WaitForPersisted,
            Step::Query {
                sql: format!("select * from {}", table_name),
                expected: vec![
                    "+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | time                           | val |",
                    "+------+------+--------------------------------+-----+",
                    "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                    "+------+------+--------------------------------+-----+",
                ],
            },
        ],
    )
    .run()
    .await
}
