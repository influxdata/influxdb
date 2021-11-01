use crate::common::server_fixture::ServerFixture;

#[tokio::test]
async fn test_http_error_messages() {
    let server_fixture = ServerFixture::create_shared().await;
    let client = server_fixture.influxdb2_client();

    // send malformed request (bucket id is invalid)
    let result = client
        .write_line_protocol("Bar", "Foo", "arbitrary")
        .await
        .expect_err("Should have errored");

    let expected_error = "HTTP request returned an error: 400 Bad Request, `{\"error\":\"Error parsing line protocol: A generic parsing error occurred: TakeWhile1\",\"error_code\":100}`";
    assert_eq!(result.to_string(), expected_error);
}
