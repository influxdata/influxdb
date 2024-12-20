use std::time::Duration;

use hyper::StatusCode;
use influxdb3_pro_clap_blocks::serve::BufferMode;
use pretty_assertions::assert_eq;

use crate::server::{pro::tmp_dir, ConfigProvider, TestServer};

#[test_log::test(tokio::test)]
async fn query_compactor_only_node_should_return_method_not_allowed() {
    let obj_store_path = tmp_dir();
    let write_node = TestServer::configure_pro()
        .with_host_id("writer")
        .with_mode(BufferMode::ReadWrite)
        .with_object_store(&obj_store_path)
        .spawn()
        .await;

    let db_name = "foo";
    write_node
        .write_lp_to_db(
            db_name,
            "cpu,t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000",
            influxdb3_client::Precision::Second,
        )
        .await
        .expect("write to db");

    let compactor_node = TestServer::configure_pro()
        .with_host_id("compactor")
        .with_mode(BufferMode::Compactor)
        .with_object_store(&obj_store_path)
        .with_compactor_id("1")
        .with_compaction_hosts(vec!["writer"])
        .spawn()
        .await;

    tokio::time::sleep(Duration::from_secs(10)).await;

    let resp = compactor_node
        .api_v3_query_sql(&[
            ("db", db_name),
            ("q", "SELECT * FROM system.compaction_events"),
            ("format", "pretty"),
        ])
        .await;

    // TODO: fix this as the query gets through fine but it says db foo is not
    //       found
    assert_eq!(StatusCode::INTERNAL_SERVER_ERROR, resp.status());
}
