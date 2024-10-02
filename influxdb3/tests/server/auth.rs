use arrow_flight::error::FlightError;
use arrow_util::assert_batches_sorted_eq;
use influxdb3_client::Precision;
use reqwest::StatusCode;

use crate::{collect_stream, ConfigProvider, TestServer};

#[tokio::test]
async fn auth() {
    const HASHED_TOKEN: &str = "5315f0c4714537843face80cca8c18e27ce88e31e9be7a5232dc4dc8444f27c0227a9bd64831d3ab58f652bd0262dd8558dd08870ac9e5c650972ce9e4259439";
    const TOKEN: &str = "apiv3_mp75KQAhbqv0GeQXk8MPuZ3ztaLEaR5JzS8iifk1FwuroSVyXXyrJK1c4gEr1kHkmbgzDV-j3MvQpaIMVJBAiA";

    let server = TestServer::configure()
        .with_auth_token(HASHED_TOKEN, TOKEN)
        .spawn()
        .await;

    let client = reqwest::Client::new();
    let base = server.client_addr();
    let write_lp_url = format!("{base}/api/v3/write_lp");
    let write_lp_params = [("db", "foo")];
    let query_sql_url = format!("{base}/api/v3/query_sql");
    let query_sql_params = [("db", "foo"), ("q", "select * from cpu")];

    assert_eq!(
        client
            .post(&write_lp_url)
            .query(&write_lp_params)
            .body("cpu,host=a val=1i 123")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get(&query_sql_url)
            .query(&query_sql_params)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(&write_lp_url)
            .query(&write_lp_params)
            .body("cpu,host=a val=1i 123")
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        client
            .post(&write_lp_url)
            .query(&write_lp_params)
            .body("cpu,host=a val=1i 123")
            // support both Bearer and Token auth schemes
            .header("Authorization", format!("Token {TOKEN}"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        client
            .get(&query_sql_url)
            .query(&query_sql_params)
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    // Malformed Header Tests
    // Test that there is an extra string after the token foo
    assert_eq!(
        client
            .get(&query_sql_url)
            .query(&query_sql_params)
            .header("Authorization", format!("Bearer {TOKEN} whee"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get(&query_sql_url)
            .query(&query_sql_params)
            .header("Authorization", format!("bearer {TOKEN}"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get(&query_sql_url)
            .query(&query_sql_params)
            .header("Authorization", "Bearer")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get(&query_sql_url)
            .query(&query_sql_params)
            .header("auth", format!("Bearer {TOKEN}"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[test_log::test(tokio::test)]
async fn auth_grpc() {
    const HASHED_TOKEN: &str = "5315f0c4714537843face80cca8c18e27ce88e31e9be7a5232dc4dc8444f27c0227a9bd64831d3ab58f652bd0262dd8558dd08870ac9e5c650972ce9e4259439";
    const TOKEN: &str = "apiv3_mp75KQAhbqv0GeQXk8MPuZ3ztaLEaR5JzS8iifk1FwuroSVyXXyrJK1c4gEr1kHkmbgzDV-j3MvQpaIMVJBAiA";

    let server = TestServer::configure()
        .with_auth_token(HASHED_TOKEN, TOKEN)
        .spawn()
        .await;

    // Write some data to the server, this will be authorized through the HTTP API
    server
        .write_lp_to_db(
            "foo",
            "cpu,host=s1,region=us-east usage=0.9 1\n\
            cpu,host=s1,region=us-east usage=0.89 2\n\
            cpu,host=s1,region=us-east usage=0.85 3",
            Precision::Nanosecond,
        )
        .await
        .unwrap();

    // Check that with a valid authorization header, it succeeds:
    for header in ["authorization", "Authorization"] {
        // Spin up a FlightSQL client
        let mut client = server.flight_sql_client("foo").await;

        // Set the authorization header on the client:
        client
            .add_header(header, &format!("Bearer {TOKEN}"))
            .unwrap();

        // Make the query again, this time it should work:
        let response = client
            .query("SELECT host, region, time, usage FROM cpu")
            .await
            .unwrap();
        let batches = collect_stream(response).await;
        assert_batches_sorted_eq!(
            [
                "+------+---------+--------------------------------+-------+",
                "| host | region  | time                           | usage |",
                "+------+---------+--------------------------------+-------+",
                "| s1   | us-east | 1970-01-01T00:00:00.000000001Z | 0.9   |",
                "| s1   | us-east | 1970-01-01T00:00:00.000000002Z | 0.89  |",
                "| s1   | us-east | 1970-01-01T00:00:00.000000003Z | 0.85  |",
                "+------+---------+--------------------------------+-------+",
            ],
            &batches
        );
    }

    // Check that without providing an Authentication header, it gives back
    // an Unauthenticated error:
    {
        let mut client = server.flight_sql_client("foo").await;
        let error = client.query("SELECT * FROM cpu").await.unwrap_err();
        assert!(matches!(error, FlightError::Tonic(s) if s.code() == tonic::Code::Unauthenticated));
    }

    // Create some new clients that set the authorization header incorrectly to
    // ensure errors are returned:

    // Misspelled "Bearer"
    {
        let mut client = server.flight_sql_client("foo").await;
        client
            .add_header("authorization", &format!("bearer {TOKEN}"))
            .unwrap();
        let error = client.query("SELECT * FROM cpu").await.unwrap_err();
        assert!(matches!(error, FlightError::Tonic(s) if s.code() == tonic::Code::Unauthenticated));
    }

    // Invalid token, this actually gives Permission denied
    {
        let mut client = server.flight_sql_client("foo").await;
        client
            .add_header("authorization", "Bearer invalid-token")
            .unwrap();
        let error = client.query("SELECT * FROM cpu").await.unwrap_err();
        assert!(
            matches!(error, FlightError::Tonic(s) if s.code() == tonic::Code::PermissionDenied)
        );
    }

    // Misspelled header key
    {
        let mut client = server.flight_sql_client("foo").await;
        client
            .add_header("auth", &format!("Bearer {TOKEN}"))
            .unwrap();
        let error = client.query("SELECT * FROM cpu").await.unwrap_err();
        assert!(matches!(error, FlightError::Tonic(s) if s.code() == tonic::Code::Unauthenticated));
    }
}

#[tokio::test]
async fn v1_password_parameter() {
    const HASHED_TOKEN: &str = "5315f0c4714537843face80cca8c18e27ce88e31e9be7a5232dc4dc8444f27c0227a9bd64831d3ab58f652bd0262dd8558dd08870ac9e5c650972ce9e4259439";
    const TOKEN: &str = "apiv3_mp75KQAhbqv0GeQXk8MPuZ3ztaLEaR5JzS8iifk1FwuroSVyXXyrJK1c4gEr1kHkmbgzDV-j3MvQpaIMVJBAiA";

    let server = TestServer::configure()
        .with_auth_token(HASHED_TOKEN, TOKEN)
        .spawn()
        .await;

    let client = reqwest::Client::new();
    let query_url = format!("{base}/query", base = server.client_addr());
    let write_url = format!("{base}/write", base = server.client_addr());
    // Send requests without any authentication:
    assert_eq!(
        client
            .get(&query_url)
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::UNAUTHORIZED,
    );
    assert_eq!(
        client
            .get(&write_url)
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::UNAUTHORIZED,
    );

    // Ensure that an invalid token passed in the `p` parameter is still unauthorized:
    assert_eq!(
        client
            .get(&query_url)
            .query(&[("p", "not-the-token-you-were-looking-for")])
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::UNAUTHORIZED,
    );
    assert_eq!(
        client
            .get(&write_url)
            .query(&[("p", "not-the-token-you-were-looking-for")])
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::UNAUTHORIZED,
    );

    // make some writes so that the query API will work below:
    server
        .write_lp_to_db("foo", "cpu,host=a usage=0.9", Precision::Second)
        .await
        .unwrap();

    // Send request to query API with the token in the v1 `p` parameter:
    assert_eq!(
        client
            .get(&query_url)
            .query(&[("p", TOKEN), ("q", "SELECT * FROM cpu"), ("db", "foo")])
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::OK,
    );
    // Send request to query API with the token in auth header:
    assert_eq!(
        client
            .get(&query_url)
            .query(&[("q", "SELECT * FROM cpu"), ("db", "foo")])
            .bearer_auth(TOKEN)
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::OK,
    );

    let valid_write_body = "cpu,host=val usage=0.5";

    // Send request to write API with the token in the v1 `p` parameter:
    assert_eq!(
        client
            .post(&write_url)
            .query(&[("p", TOKEN), ("db", "foo")])
            .body(valid_write_body)
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::OK,
    );
    // Send request to write API with the token in auth header:
    assert_eq!(
        client
            .post(&write_url)
            .bearer_auth(TOKEN)
            .query(&[("db", "foo")])
            .body(valid_write_body)
            .send()
            .await
            .expect("send request")
            .status(),
        StatusCode::OK,
    );
}
