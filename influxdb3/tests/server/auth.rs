use reqwest::StatusCode;

use crate::TestServer;

#[tokio::test]
async fn auth() {
    const HASHED_TOKEN: &str = "5315f0c4714537843face80cca8c18e27ce88e31e9be7a5232dc4dc8444f27c0227a9bd64831d3ab58f652bd0262dd8558dd08870ac9e5c650972ce9e4259439";
    const TOKEN: &str = "apiv3_mp75KQAhbqv0GeQXk8MPuZ3ztaLEaR5JzS8iifk1FwuroSVyXXyrJK1c4gEr1kHkmbgzDV-j3MvQpaIMVJBAiA";

    let server = TestServer::configure()
        .auth_token(HASHED_TOKEN)
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
