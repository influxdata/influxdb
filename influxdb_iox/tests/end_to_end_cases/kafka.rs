use crate::common::server_fixture::{ServerFixture, ServerType};
use influxdb_iox_client::management::generated_types::*;
use std::time::Instant;
use test_helpers::assert_contains;

#[tokio::test]
async fn test_create_database_invalid_kafka() {
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut client = server_fixture.management_client();

    let rules = DatabaseRules {
        name: "db_with_bad_kafka_address".into(),
        write_buffer_connection: Some(WriteBufferConnection {
            r#type: "kafka".into(),
            connection: "i_am_not_a_kafka_server:1234".into(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let start = Instant::now();
    let err = client
        .create_database(rules)
        .await
        .expect_err("expected request to fail");

    println!("Failed after {:?}", Instant::now() - start);

    // expect that this error has a useful error related to kafka (not "timeout")
    assert_contains!(
        err.to_string(),
        "error creating write buffer: Meta data fetch error: BrokerTransportFailure"
    );
}
