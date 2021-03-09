use std::num::NonZeroU32;

use influxdb_iox_client::management::{self, generated_types::DatabaseRules};
use influxdb_iox_client::write::{self, WriteError};
use test_helpers::assert_contains;

use super::util::rand_name;

use crate::common::server_fixture::ServerFixture;

#[tokio::test]
async fn test_write() {
    // TODO sort out changing the test ID
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = management::Client::new(fixture.grpc_channel());
    let mut write_client = write::Client::new(fixture.grpc_channel());

    const TEST_ID: u32 = 42;

    let db_name = rand_name();

    management_client
        .create_database(DatabaseRules {
            name: db_name.clone(),
            ..Default::default()
        })
        .await
        .expect("create database failed");

    management_client
        .update_writer_id(NonZeroU32::new(TEST_ID).unwrap())
        .await
        .expect("set ID failed");

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

    // ---- test bad data ----
    let err = write_client
        .write(&db_name, "XXX")
        .await
        .expect_err("expected write to fail");

    assert_contains!(
        err.to_string(),
        r#"Client specified an invalid argument: Violation for field "lp_data": Invalid Line Protocol: A generic parsing error occurred"#
    );
    assert!(matches!(dbg!(err), WriteError::ServerError(_)));

    // ---- test non existent database ----
    let err = write_client
        .write("Non_existent_database", lp_lines.join("\n"))
        .await
        .expect_err("expected write to fail");

    assert_contains!(
        err.to_string(),
        r#"Unexpected server error: Some requested entity was not found: Resource database/Non_existent_database not found"#
    );
    assert!(matches!(dbg!(err), WriteError::ServerError(_)));
}
