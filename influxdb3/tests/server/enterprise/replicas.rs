use std::time::Duration;

use influxdb3_client::Precision;
use influxdb3_enterprise_clap_blocks::serve::BufferMode;
use serde_json::json;

use crate::server::{enterprise::tmp_dir, ConfigProvider, TestServer};

#[tokio::test]
async fn two_primaries_one_replica() {
    let obj_store_path = tmp_dir();
    // create two primaries:
    let spock = TestServer::configure_pro()
        .with_writer_id("spock")
        .with_object_store(&obj_store_path)
        .spawn()
        .await;
    let tuvok = TestServer::configure_pro()
        .with_writer_id("tuvok")
        .with_object_store(&obj_store_path)
        .spawn()
        .await;

    // create a replica that replicates both:
    let replica = TestServer::configure_pro()
        .with_writer_id("replica")
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
    tokio::time::sleep(Duration::from_millis(2_000)).await;

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

#[tokio::test]
async fn replicate_last_cache() {
    let obj_store_path = tmp_dir();
    // create two primaries:
    let spock = TestServer::configure_pro()
        .with_writer_id("spock")
        .with_object_store(&obj_store_path)
        .spawn()
        .await;
    let tuvok = TestServer::configure_pro()
        .with_writer_id("tuvok")
        .with_object_store(&obj_store_path)
        .spawn()
        .await;

    // make some writes spock to initialize the catalog:
    spock
        .write_lp_to_db(
            "starfleet",
            "ships,registry=ncc-1701,system=shields health=0.8 1",
            Precision::Millisecond,
        )
        .await
        .unwrap();

    // create a last cache on spock
    assert!(spock
        .api_v3_configure_last_cache_create(&json!({
                "db": "starfleet",
                "table": "ships",
        }))
        .await
        .status()
        .is_success());

    // create a replica that replicates both:
    let replica = TestServer::configure_pro()
        .with_writer_id("replica")
        .with_mode(BufferMode::Read)
        .with_replicas(["spock", "tuvok"])
        .with_replication_interval("10ms")
        .with_object_store(obj_store_path)
        .spawn()
        .await;

    // wait some time for replication etc. to take place, so that the last cache
    // is added to the replica before we do the below writes:
    tokio::time::sleep(Duration::from_millis(500)).await;

    // make some writes spock:
    spock
        .write_lp_to_db(
            "starfleet",
            "\
            ships,registry=ncc-1701,system=shields health=0.7 2\n\
            ships,registry=ncc-1701,system=warp-drive health=0.8 3\n\
            ships,registry=ncc-1701,system=shields health=0.8 4\n\
            ships,registry=ncc-1701,system=warp-drive health=0.9 5",
            Precision::Millisecond,
        )
        .await
        .unwrap();

    // make some writes tuvok:
    tuvok
        .write_lp_to_db(
            "starfleet",
            "\
            ships,registry=ncc-74656,system=shields health=0.3 6\n\
            ships,registry=ncc-74656,system=warp-drive health=0.5 7\n\
            ships,registry=ncc-74656,system=shields health=0.2 8\n\
            ships,registry=ncc-74656,system=warp-drive health=0.4 9",
            Precision::Millisecond,
        )
        .await
        .unwrap();

    // wait some time for replication etc. to take place:
    tokio::time::sleep(Duration::from_millis(500)).await;

    // query the last cache in the replica:
    let resp = replica
        .api_v3_query_sql(&[
            ("db", "starfleet"),
            ("q", "SELECT * FROM last_cache('ships') ORDER BY time DESC"),
            ("format", "pretty"),
        ])
        .await
        .text()
        .await
        .expect("do query");

    // should see the cache populated with records from both primary hosts, but since the
    // last cache is only caching the default count of 1, then it will only have the most
    // recent entry for each time series, i.e., not all above writes will appear here:
    assert_eq!(
        "\
        +-----------+------------+--------+-------------------------+\n\
        | registry  | system     | health | time                    |\n\
        +-----------+------------+--------+-------------------------+\n\
        | ncc-74656 | warp-drive | 0.4    | 1970-01-01T00:00:00.009 |\n\
        | ncc-74656 | shields    | 0.2    | 1970-01-01T00:00:00.008 |\n\
        | ncc-1701  | warp-drive | 0.9    | 1970-01-01T00:00:00.005 |\n\
        | ncc-1701  | shields    | 0.8    | 1970-01-01T00:00:00.004 |\n\
        +-----------+------------+--------+-------------------------+",
        resp
    );
}
