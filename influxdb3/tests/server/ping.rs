use hyper::Method;
use serde_json::Value;

use crate::server::TestServer;

#[tokio::test]
async fn test_ping() {
    let server = TestServer::spawn().await;
    let client = server.http_client();

    struct TestCase<'a> {
        url: &'a str,
        method: Method,
    }

    let ping_url = format!("{base}/ping", base = server.client_addr());

    let test_cases = [
        TestCase {
            url: &ping_url,
            method: Method::GET,
        },
        TestCase {
            url: &ping_url,
            method: Method::POST,
        },
    ];

    for t in test_cases {
        let resp = client
            .request(t.method.clone(), t.url)
            .send()
            .await
            .unwrap();
        let json = resp.json::<Value>().await.unwrap();
        println!("Method: {}, URL: {}", t.method, t.url);
        println!("{json:#}");
        let map = json.as_object().unwrap();
        assert!(map.contains_key("version"));
        assert!(map.contains_key("revision"));
    }
}
