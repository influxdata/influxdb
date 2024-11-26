use hyper::StatusCode;
use influxdb3_pro_clap_blocks::serve::BufferMode;
use pretty_assertions::assert_eq;

use crate::server::{pro::tmp_dir, ConfigProvider, TestServer};

#[tokio::test]
async fn query_compactor_only_node_should_return_method_not_allowed() {
    let obj_store_path = tmp_dir();
    let compactor_node = TestServer::configure_pro()
        .with_host_id("compactor")
        .with_mode(BufferMode::Compactor)
        .with_object_store(obj_store_path)
        .spawn()
        .await;

    let resp = compactor_node
        .api_v3_query_sql(&[
            ("db", "foo"),
            ("q", "SELECT * FROM boo"),
            ("format", "pretty"),
        ])
        .await;

    assert_eq!(StatusCode::METHOD_NOT_ALLOWED, resp.status());
}
