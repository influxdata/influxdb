use crate::common::server_fixture::{ServerFixture, ServerType};

#[tokio::test]
async fn test_http_error_messages() {
    let server_fixture = ServerFixture::create_shared(ServerType::Database).await;
    let mut client = server_fixture.write_client();

    // send malformed request (bucket id is invalid)
    let result = client
        .write_lp("Bar", "Foo", 0)
        .await
        .expect_err("Should have errored");

    let expected_error = r#"An unexpected error occurred in the client library: error parsing line 1: A generic parsing error occurred: TakeWhile1"#;
    assert_eq!(result.to_string(), expected_error);
}
