use futures::FutureExt;
use test_helpers_end_to_end_ng::{
    maybe_skip_integration, MiniCluster, Step, StepTest, StepTestState,
};

use assert_cmd::Command;
use predicates::prelude::*;

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
                        state.cluster().router2().router_grpc_connection(),
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

                    let mut column_names: Vec<_> = table
                        .columns
                        .iter()
                        .map(|(name, _col)| name.to_string())
                        .collect();
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

/// Test the schema cli command
#[tokio::test]
async fn ingester_schema_cli() {
    test_helpers::maybe_start_logging();
    let database_url = maybe_skip_integration!();

    let mut cluster = MiniCluster::create_shared(database_url).await;

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(String::from(
                "my_awesome_table2,tag1=A,tag2=B val=42i 123456",
            )),
            Step::Custom(Box::new(|state: &mut StepTestState| {
                async {
                    let router_addr = state.cluster().router2().router_grpc_base().to_string();

                    // Validate the output of the schema CLI command
                    Command::cargo_bin("influxdb_iox")
                        .unwrap()
                        .arg("-h")
                        .arg(&router_addr)
                        .arg("schema")
                        .arg("get")
                        .arg(state.cluster().namespace())
                        .assert()
                        .success()
                        .stdout(
                            predicate::str::contains("my_awesome_table2")
                                .and(predicate::str::contains("tag1"))
                                .and(predicate::str::contains("val")),
                        );
                }
                .boxed()
            })),
        ],
    )
    .run()
    .await
}
