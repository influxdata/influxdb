use influxdb_iox_client::write::WriteError;
use test_helpers::assert_contains;

use crate::common::server_fixture::ServerFixture;

use super::scenario::{create_readable_database, rand_name};

#[tokio::test]
async fn test_write() {
    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

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
