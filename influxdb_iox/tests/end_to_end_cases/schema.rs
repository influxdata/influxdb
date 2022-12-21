use futures::FutureExt;
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState};

/// Test the schema client
#[tokio::test]
async fn ingester_schema_client() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(String::from(
                "my_awesome_table,tag1=A,tag2=B val=42i 123456",
            )),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let mut client = influxdb_iox_client::schema::Client::new(
                        state.cluster().router().router_grpc_connection(),
                    );
                    let response = client
                        .get_schema(state.cluster().namespace())
                        .await
                        .expect("successful response");

                    let response = dbg!(response);
                    let table = response
                        .tables
                        .get("my_awesome_table")
                        .expect("table not found");

                    let mut column_names: Vec<_> =
                        table.columns.keys().map(ToString::to_string).collect();
                    column_names.sort_unstable();

                    assert_eq!(column_names, &["tag1", "tag2", "time", "val"]);
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}
