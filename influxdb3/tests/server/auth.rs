use arrow_flight::error::FlightError;
use arrow_util::assert_batches_sorted_eq;
use influxdb3_client::Precision;
use reqwest::StatusCode;

use crate::{collect_stream, TestServer};

#[tokio::test]
async fn auth() {
    const HASHED_TOKEN: &str = "5315f0c4714537843face80cca8c18e27ce88e31e9be7a5232dc4dc8444f27c0227a9bd64831d3ab58f652bd0262dd8558dd08870ac9e5c650972ce9e4259439";
    const TOKEN: &str = "apiv3_mp75KQAhbqv0GeQXk8MPuZ3ztaLEaR5JzS8iifk1FwuroSVyXXyrJK1c4gEr1kHkmbgzDV-j3MvQpaIMVJBAiA";

    let server = TestServer::configure()
        .auth_token(HASHED_TOKEN, TOKEN)
        .spawn()
        .await;

    let client = reqwest::Client::new();
    let base = server.client_addr();

    assert_eq!(
        client
            .post(format!("{base}/api/v3/write_lp"))
            .query(&[("db", "foo")])
            .body("cpu,host=a val=1i 123")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get(format!("{base}/api/v3/query_sql"))
            .query(&[("db", "foo"), ("q", "select * from cpu")])
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(format!("{base}/api/v3/write_lp"))
            .query(&[("db", "foo")])
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
            .get(format!("{base}/api/v3/query_sql"))
            .query(&[("db", "foo"), ("q", "select * from cpu")])
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
            .get(format!("{base}/api/v3/query_sql"))
            .query(&[("db", "foo"), ("q", "select * from cpu")])
            .header("Authorization", format!("Bearer {TOKEN} whee"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get(format!("{base}/api/v3/query_sql"))
            .query(&[("db", "foo"), ("q", "select * from cpu")])
            .header("Authorization", format!("bearer {TOKEN}"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get(format!("{base}/api/v3/query_sql"))
            .query(&[("db", "foo"), ("q", "select * from cpu")])
            .header("Authorization", "Bearer")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get(format!("{base}/api/v3/query_sql"))
            .query(&[("db", "foo"), ("q", "select * from cpu")])
            .header("auth", format!("Bearer {TOKEN}"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn auth_grpc() {
    const HASHED_TOKEN: &str = "5315f0c4714537843face80cca8c18e27ce88e31e9be7a5232dc4dc8444f27c0227a9bd64831d3ab58f652bd0262dd8558dd08870ac9e5c650972ce9e4259439";
    const TOKEN: &str = "apiv3_mp75KQAhbqv0GeQXk8MPuZ3ztaLEaR5JzS8iifk1FwuroSVyXXyrJK1c4gEr1kHkmbgzDV-j3MvQpaIMVJBAiA";

    let server = TestServer::configure()
        .auth_token(HASHED_TOKEN, TOKEN)
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

    {
        // Spin up a FlightSQL client
        let mut client = server.flight_sql_client("foo").await;

        // Make a query using the client, without providing any authorization header:
        let error = client.query("SELECT * FROM cpu").await.unwrap_err();
        assert!(matches!(error, FlightError::Tonic(s) if s.code() == tonic::Code::Unauthenticated));

        // Set the authorization header on the client:
        client
            .add_header("authorization", &format!("Bearer {TOKEN}"))
            .unwrap();

        // Make the query again, this time it should work:
        let response = client.query("SELECT * FROM cpu").await.unwrap();
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

    // Create some new clients that set the authorization header incorrectly to
    // ensure errors are returned:

    // Mispelled "Bearer"
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

    // Mispelled header key
    {
        let mut client = server.flight_sql_client("foo").await;
        client
            .add_header("authorizon", &format!("Bearer {TOKEN}"))
            .unwrap();
        let error = client.query("SELECT * FROM cpu").await.unwrap_err();
        assert!(matches!(error, FlightError::Tonic(s) if s.code() == tonic::Code::Unauthenticated));
    }
}
