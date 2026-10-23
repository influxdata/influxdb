use crate::server::TestServer;

#[tokio::test]
async fn metrics_endpoint_includes_node_id_label() {
    let server = TestServer::spawn().await;
    let body = server
        .http_client()
        .get(format!("{}/metrics", server.client_addr()))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    let series_lines: Vec<&str> = body
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect();

    assert!(
        !series_lines.is_empty(),
        "expected /metrics to expose at least one series, got:\n{body}"
    );

    for line in &series_lines {
        assert!(
            line.contains(r#"node_id="test-server""#),
            "expected every series to carry node_id label, missing on:\n  {line}\nfull body:\n{body}"
        );
    }
}
