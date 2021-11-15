use crate::common::server_fixture::{ServerFixture, ServerType};

#[tokio::test]
async fn test_http_error_messages() {
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;
    let client = server_fixture.influxdb2_client();

    // send malformed request (bucket id is invalid)
    let result = client
        .write_line_protocol("Bar", "Foo", "arbitrary")
        .await
        .expect_err("Should have errored");

    let expected_error = r#"HTTP request returned an error: 400 Bad Request, `{"code":"invalid","message":"Error parsing line protocol: error parsing line 1: A generic parsing error occurred: TakeWhile1"}`"#;
    assert_eq!(result.to_string(), expected_error);
}
