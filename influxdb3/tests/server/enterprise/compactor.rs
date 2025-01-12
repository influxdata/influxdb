use std::time::Duration;

use hyper::StatusCode;
use influxdb3_enterprise_clap_blocks::serve::BufferMode;
use observability_deps::tracing::debug;
use pretty_assertions::assert_eq;

use crate::server::{enterprise::tmp_dir, ConfigProvider, TestServer};

#[test_log::test(tokio::test)]
async fn compactor_only_node_should_respond_to_compaction_events_query() {
    let obj_store_path = tmp_dir();
    debug!(?obj_store_path, ">>> Path");
    let db_name = "foo";
    let write_node = TestServer::configure_pro()
        .with_host_id("writer")
        .with_mode(BufferMode::ReadWrite)
        .with_object_store(&obj_store_path)
        .spawn()
        .await;

    let compactor_node = TestServer::configure_pro()
        .with_host_id("compactor")
        .with_mode(BufferMode::Compactor)
        .with_object_store(&obj_store_path)
        .with_compactor_id("1")
        .with_compaction_hosts(vec!["writer"])
        .spawn()
        .await;

    // Just 1 write doesn't snapshot, wal_flush_interval is 10ms and
    // wal_snapshot_size is 1. Need to write at least 3 times for snapshot
    // to kick off (look into SnapshotTracker::snapshot method)
    for _ in 1..=4 {
        write_node
            .write_lp_to_db(
                db_name,
                "cpu,t1=a,t2=b,t3=c f1=true,f2=\"hello\",f3=4i,f4=4u,f5=5 1000",
                influxdb3_client::Precision::Second,
            )
            .await
            .expect("write to db");
    }
    let resp = write_node
        .api_v3_query_sql(&[
            ("db", db_name),
            ("q", "SELECT * FROM cpu"),
            ("format", "pretty"),
        ])
        .await;
    assert_eq!(StatusCode::OK, resp.status());

    // compaction loop runs at 10s interval by default so we wait extra
    // second before asserting
    tokio::time::sleep(Duration::from_secs(11)).await;

    let resp = compactor_node
        .api_v3_query_sql(&[
            ("db", db_name),
            ("q", "SELECT * FROM system.compaction_events"),
            ("format", "pretty"),
        ])
        .await;

    assert_eq!(StatusCode::OK, resp.status());

    // compaction node cannot answer other queries
    let resp = compactor_node
        .api_v3_query_sql(&[
            ("db", db_name),
            ("q", "SELECT * FROM cpu"),
            ("format", "pretty"),
        ])
        .await;
    assert_eq!(StatusCode::INTERNAL_SERVER_ERROR, resp.status());
}
