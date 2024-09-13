use std::time::Duration;

use influxdb3_client::Precision;
use influxdb3_pro_clap_blocks::serve::BufferMode;

use crate::{pro::tmp_dir, ConfigProvider, TestServer};

#[tokio::test]
async fn two_primaries_one_replica() {
    let obj_store_path = tmp_dir();
    // create two primaries:
    let spock = TestServer::configure_pro()
        .with_host_id("spock")
        .with_object_store(&obj_store_path)
        .spawn()
        .await;
    let tuvok = TestServer::configure_pro()
        .with_host_id("tuvok")
        .with_object_store(&obj_store_path)
        .spawn()
        .await;

    // create a replica that replicates both:
    let replica = TestServer::configure_pro()
        .with_host_id("replica")
        .with_mode(BufferMode::Read)
        .with_replicas(["spock", "tuvok"])
        .with_replication_interval("10ms")
        .with_object_store(obj_store_path)
        .spawn()
        .await;

    // make some writes spock:
    spock
        .write_lp_to_db(
            "mines",
            "west,dev=belt,id=123 health=0.5 1",
            Precision::Millisecond,
        )
        .await
        .unwrap();

    // make some writes to tuvok:
    tuvok
        .write_lp_to_db(
            "mines",
            "west,dev=drill,id=3001 health=0.9 2",
            Precision::Millisecond,
        )
        .await
        .unwrap();

    // wait some time for replication etc. to take place:
    tokio::time::sleep(Duration::from_millis(500)).await;

    // query the replica:
    let resp = replica
        .api_v3_query_sql(&[
            ("db", "mines"),
            ("q", "SELECT * FROM west ORDER BY time DESC"),
            ("format", "pretty"),
        ])
        .await
        .text()
        .await
        .expect("do query");

    // should see writes made to both hosts:
    assert_eq!(
        "\
         +-------+--------+------+-------------------------+\n\
         | dev   | health | id   | time                    |\n\
         +-------+--------+------+-------------------------+\n\
         | drill | 0.9    | 3001 | 1970-01-01T00:00:00.002 |\n\
         | belt  | 0.5    | 123  | 1970-01-01T00:00:00.001 |\n\
         +-------+--------+------+-------------------------+",
        resp
    );
}
