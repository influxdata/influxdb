use std::num::NonZeroU32;

use influxdb_iox_client::management::{self, generated_types::DatabaseRules};
use influxdb_iox_client::write::{self, WriteError};
use test_helpers::assert_contains;

use super::util::rand_name;

/// Tests the basics of the write API
pub async fn test(channel: tonic::transport::Channel) {
    let mut management_client = management::Client::new(channel.clone());
    let mut write_client = write::Client::new(channel);

    test_write(&mut management_client, &mut write_client).await
}

async fn test_write(management_client: &mut management::Client, write_client: &mut write::Client) {
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
