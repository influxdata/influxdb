use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest, TestConfig};

#[tokio::test]
/// Test with multiple ingesters
async fn basic_multi_ingesters() {
    let database_url = maybe_skip_integration!();
    test_helpers::maybe_start_logging();

    let ingester1_config = TestConfig::new_ingester2_never_persist(&database_url);
    let ingester2_config = TestConfig::another_ingester(&ingester1_config);
    let ingester_configs = [ingester1_config, ingester2_config];

    let ingester_addresses: Vec<_> = ingester_configs
        .iter()
        .map(|ingester_config| ingester_config.ingester_base())
        .collect();
    let router_config =
        TestConfig::new_router2(&ingester_configs[0]).with_ingester_addresses(&ingester_addresses);
    let querier_config =
        TestConfig::new_querier2(&ingester_configs[0]).with_ingester_addresses(&ingester_addresses);

    let mut cluster = MiniCluster::new();
    for ingester_config in ingester_configs {
        cluster = cluster.with_ingester(ingester_config).await;
    }

    cluster = cluster
        .with_router(router_config)
        .await
        .with_querier(querier_config)
        .await;

    let mut test_steps = vec![Step::RecordNumParquetFiles];
    // Make 10 writes to hopefully round-robin over both ingesters
    for i in 1..=10 {
        test_steps.push(Step::WriteLineProtocol(format!(
            "some_table,tag1=A,tag2=B val={i}i {i}"
        )));
    }
    // Persist those writes
    test_steps.push(Step::Persist);
    test_steps.push(Step::WaitForPersisted2 {
        // One file from each ingester
        expected_increase: 2,
    });
    // Make 10 more writes to stay in the ingesters' memory
    for i in 11..=20 {
        test_steps.push(Step::WriteLineProtocol(format!(
            "some_table,tag1=A,tag2=B val={i}i {i}"
        )));
    }

    test_steps.push(Step::Query {
        sql: "select * from some_table".into(),
        expected: vec![
            "+------+------+--------------------------------+-----+",
            "| tag1 | tag2 | time                           | val |",
            "+------+------+--------------------------------+-----+",
            "| A    | B    | 1970-01-01T00:00:00.000000001Z | 1   |",
            "| A    | B    | 1970-01-01T00:00:00.000000002Z | 2   |",
            "| A    | B    | 1970-01-01T00:00:00.000000003Z | 3   |",
            "| A    | B    | 1970-01-01T00:00:00.000000004Z | 4   |",
            "| A    | B    | 1970-01-01T00:00:00.000000005Z | 5   |",
            "| A    | B    | 1970-01-01T00:00:00.000000006Z | 6   |",
            "| A    | B    | 1970-01-01T00:00:00.000000007Z | 7   |",
            "| A    | B    | 1970-01-01T00:00:00.000000008Z | 8   |",
            "| A    | B    | 1970-01-01T00:00:00.000000009Z | 9   |",
            "| A    | B    | 1970-01-01T00:00:00.000000010Z | 10  |",
            "| A    | B    | 1970-01-01T00:00:00.000000011Z | 11  |",
            "| A    | B    | 1970-01-01T00:00:00.000000012Z | 12  |",
            "| A    | B    | 1970-01-01T00:00:00.000000013Z | 13  |",
            "| A    | B    | 1970-01-01T00:00:00.000000014Z | 14  |",
            "| A    | B    | 1970-01-01T00:00:00.000000015Z | 15  |",
            "| A    | B    | 1970-01-01T00:00:00.000000016Z | 16  |",
            "| A    | B    | 1970-01-01T00:00:00.000000017Z | 17  |",
            "| A    | B    | 1970-01-01T00:00:00.000000018Z | 18  |",
            "| A    | B    | 1970-01-01T00:00:00.000000019Z | 19  |",
            "| A    | B    | 1970-01-01T00:00:00.000000020Z | 20  |",
            "+------+------+--------------------------------+-----+",
        ],
    });
    StepTest::new(&mut cluster, test_steps).run().await;
}
