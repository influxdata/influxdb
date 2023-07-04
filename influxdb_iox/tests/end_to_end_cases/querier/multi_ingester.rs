use arrow_util::assert_batches_sorted_eq;
use futures::FutureExt;
use ingester_query_grpc::{influxdata::iox::ingester::v1 as proto, IngesterQueryRequest};
use std::num::NonZeroUsize;
use test_helpers_end_to_end::{
    maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState, TestConfig,
};

#[tokio::test]
/// Test with multiple ingesters
async fn basic_multi_ingesters() {
    let database_url = maybe_skip_integration!();
    test_helpers::maybe_start_logging();

    let ingester1_config = TestConfig::new_ingester_never_persist(&database_url);
    let ingester2_config = TestConfig::another_ingester(&ingester1_config);
    let ingester_configs = [ingester1_config, ingester2_config];

    let ingester_addresses: Vec<_> = ingester_configs
        .iter()
        .map(|ingester_config| ingester_config.ingester_base())
        .collect();
    let router_config =
        TestConfig::new_router(&ingester_configs[0]).with_ingester_addresses(&ingester_addresses);
    let querier_config =
        TestConfig::new_querier(&ingester_configs[0]).with_ingester_addresses(&ingester_addresses);

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
    test_steps.push(Step::WaitForPersisted {
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

#[tokio::test]
async fn write_replication() {
    let database_url = maybe_skip_integration!();
    test_helpers::maybe_start_logging();

    let table_name = "some_table";

    let ingester1_config = TestConfig::new_ingester_never_persist(&database_url);
    let ingester2_config = TestConfig::another_ingester(&ingester1_config);
    let ingester_configs = [ingester1_config, ingester2_config];

    let ingester_addresses: Vec<_> = ingester_configs
        .iter()
        .map(|ingester_config| ingester_config.ingester_base())
        .collect();
    let router_config = TestConfig::new_router(&ingester_configs[0])
        .with_ingester_addresses(&ingester_addresses)
        // Require both ingesters to get this write to be counted as a full write
        .with_rpc_write_replicas(NonZeroUsize::new(2).unwrap());
    let querier_config =
        TestConfig::new_querier(&ingester_configs[0]).with_ingester_addresses(&ingester_addresses);

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
    for i in 1..=10 {
        test_steps.push(Step::WriteLineProtocol(format!(
            "{table_name},tag1=A,tag2=B val={i}i {i}"
        )));
    }
    // Persist those writes
    test_steps.push(Step::Persist);
    test_steps.push(Step::WaitForPersisted {
        // One file from each ingester
        expected_increase: 2,
    });
    // Make 10 more writes to stay in the ingesters' memory
    for i in 11..=20 {
        test_steps.push(Step::WriteLineProtocol(format!(
            "{table_name},tag1=A,tag2=B val={i}i {i}"
        )));
    }

    test_steps.push(Step::Query {
        sql: format!("select * from {table_name}"),
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

    // Each ingester should have the same data (the data that hasn't been persisted yet)
    for ingester_connection in cluster
        .ingesters()
        .iter()
        .map(|ingester| ingester.ingester_grpc_connection())
    {
        test_steps.push(Step::Custom(Box::new(move |state: &mut StepTestState| {
            let ingester_connection = ingester_connection.clone();
            async move {
                // query the ingester
                let query = IngesterQueryRequest::new(
                    state.cluster().namespace_id().await,
                    state.cluster().table_id(table_name).await,
                    vec![],
                    Some(::predicate::EMPTY_PREDICATE),
                );
                let query: proto::IngesterQueryRequest = query.try_into().unwrap();
                let ingester_response = state
                    .cluster()
                    .query_ingester(query, ingester_connection)
                    .await
                    .unwrap();

                assert_eq!(ingester_response.partitions.len(), 1);
                let ingester_partition = ingester_response
                    .partitions
                    .into_iter()
                    .next()
                    .expect("just checked len");

                let ingester_uuid = ingester_partition.app_metadata.ingester_uuid;
                assert!(!ingester_uuid.is_empty());

                let expected = [
                    "+------+------+--------------------------------+-----+",
                    "| tag1 | tag2 | time                           | val |",
                    "+------+------+--------------------------------+-----+",
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
                ];
                assert_batches_sorted_eq!(&expected, &ingester_partition.record_batches);
            }
            .boxed()
        })));
    }

    StepTest::new(&mut cluster, test_steps).run().await;
}
