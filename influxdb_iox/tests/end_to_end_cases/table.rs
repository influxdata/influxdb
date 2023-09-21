use futures::FutureExt;
use influxdb_iox_client::table::generated_types::{Part, PartitionTemplate, TemplatePart};
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState};

#[tokio::test]
async fn create_tables() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    // Set up the cluster
    let mut cluster = MiniCluster::create_shared(database_url).await;

    // Step to create namespace and its tables
    let mut steps: Vec<_> = vec![Step::Custom(Box::new(move |state: &mut StepTestState| {
        async move {
            let namespace_name = state.cluster().namespace();

            let mut namespace_client = influxdb_iox_client::namespace::Client::new(
                state.cluster().router().router_grpc_connection(),
            );

            // create namespace without partition template
            namespace_client
                .create_namespace(namespace_name, None, None, None)
                .await
                .unwrap();

            let mut table_client = influxdb_iox_client::table::Client::new(
                state.cluster().router().router_grpc_connection(),
            );

            // table1: create implicitly by writing to it

            // table2: create explicitly without partition template
            table_client
                .create_table(namespace_name, "table2", None)
                .await
                .unwrap();

            // table3: create explicitly with partition template
            table_client
                .create_table(
                    namespace_name,
                    "table3",
                    Some(PartitionTemplate {
                        parts: vec![
                            TemplatePart {
                                part: Some(Part::TagValue("tag2".into())),
                            },
                            TemplatePart {
                                part: Some(Part::TimeFormat("%Y.%j".into())),
                            },
                        ],
                    }),
                )
                .await
                .unwrap();

            // table4: create explicitly with time format `whatever` which is a valid strftime format
            table_client
                .create_table(
                    namespace_name,
                    "table4",
                    Some(PartitionTemplate {
                        parts: vec![TemplatePart {
                            part: Some(Part::TimeFormat("whatever".into())),
                        }],
                    }),
                )
                .await
                .unwrap();

            // table5: create time format %Y-bananas
            table_client
                .create_table(
                    namespace_name,
                    "table5",
                    Some(PartitionTemplate {
                        parts: vec![TemplatePart {
                            part: Some(Part::TimeFormat("%Y-bananas".into())),
                        }],
                    }),
                )
                .await
                .unwrap();
        }
        .boxed()
    }))];

    // Steps to write data to tables
    steps.extend((1..=5).flat_map(|tid| {
        [Step::WriteLineProtocol(
            [
                format!("table{tid},tag1=v1a,tag2=v2a,tag3=v3a f=1 11"),
                format!("table{tid},tag1=v1b,tag2=v2a,tag3=v3a f=1 11"),
                format!("table{tid},tag1=v1a,tag2=v2b,tag3=v3a f=1 11"),
                format!("table{tid},tag1=v1b,tag2=v2b,tag3=v3a f=1 11"),
                format!("table{tid},tag1=v1a,tag2=v2a,tag3=v3b f=1 11"),
                format!("table{tid},tag1=v1b,tag2=v2a,tag3=v3b f=1 11"),
                format!("table{tid},tag1=v1a,tag2=v2b,tag3=v3b f=1 11"),
                format!("table{tid},tag1=v1b,tag2=v2b,tag3=v3b f=1 11"),
            ]
            .join("\n"),
        )]
        .into_iter()
    }));

    // Steps to query tables
    steps.extend((1..=5).flat_map(|tid| {
        [Step::Query {
            sql: format!("select * from table{tid}"),
            expected: vec![
                "+-----+------+------+------+--------------------------------+",
                "| f   | tag1 | tag2 | tag3 | time                           |",
                "+-----+------+------+------+--------------------------------+",
                "| 1.0 | v1a  | v2a  | v3a  | 1970-01-01T00:00:00.000000011Z |",
                "| 1.0 | v1a  | v2a  | v3b  | 1970-01-01T00:00:00.000000011Z |",
                "| 1.0 | v1a  | v2b  | v3a  | 1970-01-01T00:00:00.000000011Z |",
                "| 1.0 | v1a  | v2b  | v3b  | 1970-01-01T00:00:00.000000011Z |",
                "| 1.0 | v1b  | v2a  | v3a  | 1970-01-01T00:00:00.000000011Z |",
                "| 1.0 | v1b  | v2a  | v3b  | 1970-01-01T00:00:00.000000011Z |",
                "| 1.0 | v1b  | v2b  | v3a  | 1970-01-01T00:00:00.000000011Z |",
                "| 1.0 | v1b  | v2b  | v3b  | 1970-01-01T00:00:00.000000011Z |",
                "+-----+------+------+------+--------------------------------+",
            ],
        }]
    }));

    // Step get partition keys for table1, table2, table3, table4
    steps.push(Step::PartitionKeys {
        table_name: "table1".into(),
        namespace_name: None,
        // default DAY partition key because table1 was created implicitly
        expected: vec!["1970-01-01"],
    });
    steps.push(Step::PartitionKeys {
        table_name: "table2".into(),
        namespace_name: None,
        // default DAY partition key because table2 was created without partition template
        expected: vec!["1970-01-01"],
    });
    steps.push(Step::PartitionKeys {
        table_name: "table3".into(),
        namespace_name: None,
        // partition key from partition template
        expected: vec!["v2a|1970.001", "v2b|1970.001"],
    });
    steps.push(Step::PartitionKeys {
        table_name: "table4".into(),
        namespace_name: None,
        expected: vec!["whatever"],
    });
    steps.push(Step::PartitionKeys {
        table_name: "table5".into(),
        namespace_name: None,
        expected: vec!["1970-bananas"],
    });

    // run the steps
    StepTest::new(&mut cluster, steps).run().await
}
