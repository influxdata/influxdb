use std::time::Duration;

use arrow::{array::as_primitive_array, datatypes::Int64Type, record_batch::RecordBatch};
use futures::FutureExt;
use influxdb_iox_client::write_info::generated_types::{
    GetWriteInfoResponse, KafkaPartitionStatus,
};
use test_helpers::timeout::FutureTimeout;
use test_helpers_end_to_end_ng::{
    all_readable, combined_token_info, maybe_skip_integration, MiniCluster, Step, StepTest,
    StepTestState, TestConfig,
};

#[tokio::test]
/// Test with multiple ingesters
async fn basic_multi_ingesters() {
    let database_url = maybe_skip_integration!();

    // write into two different kafka partitions: 0 and 1
    let router2_config =
        TestConfig::new_router2(&database_url).with_new_write_buffer_kafka_partitions(2);

    // ingester gets partition 0
    let ingester_config = TestConfig::new_ingester(&router2_config).with_kafka_partition(0);
    let ingester2_config = TestConfig::new_ingester(&router2_config).with_kafka_partition(1);

    let querier_config = TestConfig::new_querier_without_ingester(&ingester_config)
        // Configure to talk with both the ingesters
        .with_ingester_addresses(&[
            ingester_config.ingester_base().as_ref(),
            ingester2_config.ingester_base().as_ref(),
        ]);

    // Set up the cluster  ====================================
    let mut cluster = MiniCluster::new()
        .with_router2(router2_config)
        .await
        .with_ingester(ingester_config)
        .await
        // second ingester
        .with_other(ingester2_config)
        .await
        .with_querier(querier_config)
        .await;

    // pick 100 table names to spread across both ingesters
    let lp_data = (0..100)
        .map(|i| format!("table_{},tag1=A,tag2=B val={}i 123456", i, i))
        .collect::<Vec<_>>()
        .join("\n");

    let test_steps = vec![
        Step::WriteLineProtocol(lp_data),
        // wait for data to be readable in ingester2
        Step::Custom(Box::new(move |state: &mut StepTestState| {
            async {
                let combined_response = get_multi_ingester_readable_combined_response(state).await;

                // make sure data is spread across all kafka
                // partitions by ensuring all partition is readable
                // (and there is none that is unknown)
                assert!(
                    combined_response
                        .kafka_partition_infos
                        .iter()
                        .all(|info| info.status() == KafkaPartitionStatus::Readable),
                    "combined responses: {:?}",
                    combined_response
                );
            }
            .boxed()
        })),
        // spot check results (full validation is in verification_steps)
        Step::Query {
            sql: "select * from table_5".into(),
            expected: vec![
                "+------+------+--------------------------------+-----+",
                "| tag1 | tag2 | time                           | val |",
                "+------+------+--------------------------------+-----+",
                "| A    | B    | 1970-01-01T00:00:00.000123456Z | 5   |",
                "+------+------+--------------------------------+-----+",
            ],
        },
        Step::Query {
            sql: "select * from table_42".into(),
            expected: vec![
                "+------+------+--------------------------------+-----+",
                "| tag1 | tag2 | time                           | val |",
                "+------+------+--------------------------------+-----+",
                "| A    | B    | 1970-01-01T00:00:00.000123456Z | 42  |",
                "+------+------+--------------------------------+-----+",
            ],
        },
    ]
    .into_iter()
    // read all the data back out
    .chain((0..100).map(|i| Step::VerifiedQuery {
        sql: format!("select * from table_{}", i),
        verify: Box::new(move |batches: Vec<RecordBatch>| {
            // results look like this:
            // "+------+------+--------------------------------+-----+",
            // "| tag1 | tag2 | time                           | val |",
            // "+------+------+--------------------------------+-----+",
            // "| A    | B    | 1970-01-01T00:00:00.000123456Z | val |",
            // "+------+------+--------------------------------+-----+",
            assert_eq!(batches.len(), 1, "{:?}", batches);
            assert_eq!(
                batches[0].schema().fields()[3].name(),
                "val",
                "{:?}",
                batches
            );
            let array = as_primitive_array::<Int64Type>(batches[0].column(3));
            assert_eq!(array.len(), 1);
            assert_eq!(array.value(0), i);
        }),
    }))
    .collect();

    // Run the tests
    StepTest::new(&mut cluster, test_steps).run().await
}

/// Return a combined readable response from the ingesters that is
/// readable for all partitions. Retries forever
async fn get_multi_ingester_readable_combined_response(
    state: &mut StepTestState<'_>,
) -> GetWriteInfoResponse {
    async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        let cluster = state.cluster();

        let ingester_connections = vec![
            cluster.ingester().ingester_grpc_connection(),
            cluster.other_servers()[0].ingester_grpc_connection(),
        ];

        loop {
            let combined_response =
                combined_token_info(state.write_tokens().to_vec(), ingester_connections.clone())
                    .await;

            match combined_response {
                Ok(combined_response) => {
                    if all_readable(&combined_response) {
                        println!("All data is readable: {:?}", combined_response);
                        return combined_response;
                    } else {
                        println!("retrying, not yet readable: {:?}", combined_response);
                    }
                }
                Err(e) => {
                    println!("retrying, error getting token status: {}", e);
                }
            }
            interval.tick().await;
        }
    }
    // run for at most 10 seconds
    .with_timeout_panic(Duration::from_secs(10))
    .await
}
