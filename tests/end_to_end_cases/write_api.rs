use influxdb_iox_client::write::WriteError;
use test_helpers::assert_contains;

use crate::common::server_fixture::ServerFixture;

use super::scenario::{create_readable_database, rand_name};
use arrow_util::assert_batches_sorted_eq;
use entry::{
    lines_to_sharded_entries,
    test_helpers::{partitioner, sharder},
};
use generated_types::influxdata::iox::management::v1::database_rules::RoutingRules;
use generated_types::influxdata::iox::management::v1::{
    node_group::Node, shard, HashRing, Matcher, MatcherToShard, NodeGroup, RoutingConfig, Shard,
    ShardConfig,
};
use influxdb_line_protocol::parse_lines;
use std::collections::HashMap;

#[tokio::test]
async fn test_write() {
    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    // ---- test successful writes ----
    let lp_lines = vec![
        "cpu,region=west user=23.2 100",
        "cpu,region=west user=21.0 150",
        "disk,region=east bytes=99i 200",
    ];

    let num_lines_written = write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("cannot write");

    assert_eq!(num_lines_written, 3);

    // ---- test bad data ----
    let err = write_client
        .write(&db_name, "XXX")
        .await
        .expect_err("expected write to fail");

    assert_contains!(
        err.to_string(),
        r#"Client specified an invalid argument: Violation for field "lp_data": Invalid Line Protocol: A generic parsing error occurred"#
    );
    assert!(matches!(dbg!(err), WriteError::ServerError(_)));

    // ---- test non existent database ----
    let err = write_client
        .write("Non_existent_database", lp_lines.join("\n"))
        .await
        .expect_err("expected write to fail");

    assert_contains!(
        err.to_string(),
        r#"Unexpected server error: Some requested entity was not found: Resource database/Non_existent_database not found"#
    );
    assert!(matches!(dbg!(err), WriteError::ServerError(_)));

    // ---- test hard limit ----
    // stream data until limit is reached
    let mut maybe_err = None;
    for i in 0..1_000 {
        let lp_lines: Vec<_> = (0..1_000)
            .map(|j| format!("flood,tag1={},tag2={} x={},y={} 0", i, j, i, j))
            .collect();
        if let Err(err) = write_client.write(&db_name, lp_lines.join("\n")).await {
            maybe_err = Some(err);
            break;
        }
    }
    assert!(maybe_err.is_some());
    let err = maybe_err.unwrap();
    let WriteError::ServerError(status) = dbg!(err);
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    // IMPORTANT: At this point, the database is flooded and pretty much
    // useless. Don't append any tests after the "hard limit" test!
}

#[tokio::test]
async fn test_write_entry() {
    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    let lp_data = vec!["cpu bar=1 10", "cpu bar=2 20"].join("\n");

    let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();
    let default_time = 456;
    let sharded_entries =
        lines_to_sharded_entries(&lines, default_time, sharder(1).as_ref(), &partitioner(1))
            .unwrap();

    let entry: Vec<u8> = sharded_entries.into_iter().next().unwrap().entry.into();

    write_client.write_entry(&db_name, entry).await.unwrap();

    let mut query_results = fixture
        .flight_client()
        .perform_query(&db_name, "select * from cpu")
        .await
        .unwrap();

    let mut batches = Vec::new();
    while let Some(data) = query_results.next().await.unwrap() {
        batches.push(data);
    }

    let expected = vec![
        "+-----+-------------------------------+",
        "| bar | time                          |",
        "+-----+-------------------------------+",
        "| 1   | 1970-01-01 00:00:00.000000010 |",
        "| 2   | 1970-01-01 00:00:00.000000020 |",
        "+-----+-------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);
}

#[tokio::test]
async fn test_write_routed() {
    const TEST_ROUTER_ID: u32 = 1;

    const TEST_TARGET_ID_1: u32 = 2;
    const TEST_TARGET_ID_2: u32 = 3;
    const TEST_TARGET_ID_3: u32 = 4;

    const TEST_REMOTE_ID_1: u32 = 2;
    const TEST_REMOTE_ID_2: u32 = 3;
    const TEST_REMOTE_ID_3: u32 = 4;

    const TEST_SHARD_ID_1: u32 = 42;
    const TEST_SHARD_ID_2: u32 = 43;
    const TEST_SHARD_ID_3: u32 = 44;

    let router = ServerFixture::create_single_use().await;
    let mut router_mgmt = router.management_client();
    router_mgmt
        .update_server_id(TEST_ROUTER_ID)
        .await
        .expect("set ID failed");
    router.wait_server_initialized().await;

    let target_1 = ServerFixture::create_single_use().await;
    let mut target_1_mgmt = target_1.management_client();
    target_1_mgmt
        .update_server_id(TEST_TARGET_ID_1)
        .await
        .expect("set ID failed");
    target_1.wait_server_initialized().await;

    router_mgmt
        .update_remote(TEST_REMOTE_ID_1, target_1.grpc_base())
        .await
        .expect("set remote failed");

    let target_2 = ServerFixture::create_single_use().await;
    let mut target_2_mgmt = target_2.management_client();
    target_2_mgmt
        .update_server_id(TEST_TARGET_ID_2)
        .await
        .expect("set ID failed");
    target_2.wait_server_initialized().await;

    router_mgmt
        .update_remote(TEST_REMOTE_ID_2, target_2.grpc_base())
        .await
        .expect("set remote failed");

    let target_3 = ServerFixture::create_single_use().await;
    let mut target_3_mgmt = target_3.management_client();
    target_3_mgmt
        .update_server_id(TEST_TARGET_ID_3)
        .await
        .expect("set ID failed");
    target_3.wait_server_initialized().await;

    router_mgmt
        .update_remote(TEST_REMOTE_ID_3, target_3.grpc_base())
        .await
        .expect("set remote failed");

    let db_name = rand_name();
    create_readable_database(&db_name, router.grpc_channel()).await;
    create_readable_database(&db_name, target_1.grpc_channel()).await;
    create_readable_database(&db_name, target_2.grpc_channel()).await;
    create_readable_database(&db_name, target_3.grpc_channel()).await;

    // Set sharding rules on the router:
    let mut router_db_rules = router_mgmt
        .get_database(&db_name)
        .await
        .expect("cannot get database on router");
    let shard_config = ShardConfig {
        specific_targets: vec![
            MatcherToShard {
                matcher: Some(Matcher {
                    table_name_regex: "^cpu$".to_string(),
                    ..Default::default()
                }),
                shard: TEST_SHARD_ID_1,
            },
            MatcherToShard {
                matcher: Some(Matcher {
                    table_name_regex: "^mem$".to_string(),
                    ..Default::default()
                }),
                shard: TEST_SHARD_ID_3,
            },
        ],
        hash_ring: Some(HashRing {
            table_name: true,
            shards: vec![TEST_SHARD_ID_2],
            ..Default::default()
        }),
        shards: vec![
            (
                TEST_SHARD_ID_1,
                Shard {
                    sink: Some(shard::Sink::Iox(NodeGroup {
                        nodes: vec![Node {
                            id: TEST_REMOTE_ID_1,
                        }],
                    })),
                },
            ),
            (
                TEST_SHARD_ID_2,
                Shard {
                    sink: Some(shard::Sink::Iox(NodeGroup {
                        nodes: vec![Node {
                            id: TEST_REMOTE_ID_2,
                        }],
                    })),
                },
            ),
            (
                TEST_SHARD_ID_3,
                Shard {
                    sink: Some(shard::Sink::Iox(NodeGroup {
                        nodes: vec![Node {
                            id: TEST_REMOTE_ID_3,
                        }],
                    })),
                },
            ),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>(),
        ..Default::default()
    };
    router_db_rules.routing_rules = Some(RoutingRules::ShardConfig(shard_config));

    router_mgmt
        .update_database(router_db_rules)
        .await
        .expect("cannot update router db rules");

    // Write some data
    let mut write_client = router.write_client();

    let lp_lines = vec![
        "cpu bar=1 100",
        "cpu bar=2 200",
        "disk bar=3 300",
        "mem baz=4 400",
    ];

    let num_lines_written = write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("cannot write");

    assert_eq!(num_lines_written, 4);

    // The router will have split the write request by table name into two shards.
    // Target 1 will have received only the "cpu" table.
    // Target 2 will have received only the "disk" table.

    let mut query_results = target_1
        .flight_client()
        .perform_query(&db_name, "select * from cpu")
        .await
        .expect("failed to query target 1");

    let mut batches = Vec::new();
    while let Some(data) = query_results.next().await.unwrap() {
        batches.push(data);
    }

    let expected = vec![
        "+-----+-------------------------------+",
        "| bar | time                          |",
        "+-----+-------------------------------+",
        "| 1   | 1970-01-01 00:00:00.000000100 |",
        "| 2   | 1970-01-01 00:00:00.000000200 |",
        "+-----+-------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    assert!(target_1
        .flight_client()
        .perform_query(&db_name, "select * from disk")
        .await
        .unwrap_err()
        .to_string()
        .contains("Table or CTE with name 'disk' not found\""));

    let mut query_results = target_2
        .flight_client()
        .perform_query(&db_name, "select * from disk")
        .await
        .expect("failed to query target 2");

    let mut batches = Vec::new();
    while let Some(data) = query_results.next().await.unwrap() {
        batches.push(data);
    }

    let expected = vec![
        "+-----+-------------------------------+",
        "| bar | time                          |",
        "+-----+-------------------------------+",
        "| 3   | 1970-01-01 00:00:00.000000300 |",
        "+-----+-------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    assert!(target_2
        .flight_client()
        .perform_query(&db_name, "select * from cpu")
        .await
        .unwrap_err()
        .to_string()
        .contains("Table or CTE with name 'cpu' not found\""));

    ////

    let mut query_results = target_3
        .flight_client()
        .perform_query(&db_name, "select * from mem")
        .await
        .expect("failed to query target 3");

    let mut batches = Vec::new();
    while let Some(data) = query_results.next().await.unwrap() {
        batches.push(data);
    }

    let expected = vec![
        "+-----+-------------------------------+",
        "| baz | time                          |",
        "+-----+-------------------------------+",
        "| 4   | 1970-01-01 00:00:00.000000400 |",
        "+-----+-------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);
}

#[tokio::test]
async fn test_write_routed_errors() {
    const TEST_ROUTER_ID: u32 = 1;
    const TEST_REMOTE_ID: u32 = 2;
    const TEST_SHARD_ID: u32 = 42;

    let router = ServerFixture::create_single_use().await;
    let mut router_mgmt = router.management_client();
    router_mgmt
        .update_server_id(TEST_ROUTER_ID)
        .await
        .expect("set ID failed");
    router.wait_server_initialized().await;

    let db_name = rand_name();
    create_readable_database(&db_name, router.grpc_channel()).await;

    // Set sharding rules on the router:
    let mut router_db_rules = router_mgmt
        .get_database(&db_name)
        .await
        .expect("cannot get database on router");
    let shard_config = ShardConfig {
        specific_targets: vec![MatcherToShard {
            matcher: Some(Matcher {
                table_name_regex: "^cpu$".to_string(),
                ..Default::default()
            }),
            shard: TEST_SHARD_ID,
        }],
        shards: vec![(
            TEST_SHARD_ID,
            Shard {
                sink: Some(shard::Sink::Iox(NodeGroup {
                    nodes: vec![Node { id: TEST_REMOTE_ID }],
                })),
            },
        )]
        .into_iter()
        .collect::<HashMap<_, _>>(),
        ..Default::default()
    };
    router_db_rules.routing_rules = Some(RoutingRules::ShardConfig(shard_config));
    router_mgmt
        .update_database(router_db_rules)
        .await
        .expect("cannot update router db rules");

    // We intentionally omit to configure the "remotes" name resolution on the
    // router server.
    let mut write_client = router.write_client();
    let lp_lines = vec!["cpu bar=1 100", "cpu bar=2 200"];
    let err = write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        format!(
            "Unexpected server error: Some requested entity was not found: Resource \
             remote/[ServerId({})] not found",
            TEST_REMOTE_ID
        )
    );

    // TODO(mkm): check connection error and successful communication with a
    // target that replies with an error...
}

#[tokio::test]
async fn test_write_routed_no_shard() {
    const TEST_ROUTER_ID: u32 = 1;

    const TEST_TARGET_ID_1: u32 = 2;
    const TEST_TARGET_ID_2: u32 = 3;
    const TEST_TARGET_ID_3: u32 = 4;

    const TEST_REMOTE_ID_1: u32 = 2;
    const TEST_REMOTE_ID_2: u32 = 3;
    const TEST_REMOTE_ID_3: u32 = 4;

    let router = ServerFixture::create_single_use().await;
    let mut router_mgmt = router.management_client();
    router_mgmt
        .update_server_id(TEST_ROUTER_ID)
        .await
        .expect("set ID failed");
    router.wait_server_initialized().await;

    let target_1 = ServerFixture::create_single_use().await;
    let mut target_1_mgmt = target_1.management_client();
    target_1_mgmt
        .update_server_id(TEST_TARGET_ID_1)
        .await
        .expect("set ID failed");
    target_1.wait_server_initialized().await;

    router_mgmt
        .update_remote(TEST_REMOTE_ID_1, target_1.grpc_base())
        .await
        .expect("set remote failed");

    let target_2 = ServerFixture::create_single_use().await;
    let mut target_2_mgmt = target_2.management_client();
    target_2_mgmt
        .update_server_id(TEST_TARGET_ID_2)
        .await
        .expect("set ID failed");
    target_2.wait_server_initialized().await;

    router_mgmt
        .update_remote(TEST_REMOTE_ID_2, target_2.grpc_base())
        .await
        .expect("set remote failed");

    let target_3 = ServerFixture::create_single_use().await;
    let mut target_3_mgmt = target_3.management_client();
    target_3_mgmt
        .update_server_id(TEST_TARGET_ID_3)
        .await
        .expect("set ID failed");
    target_3.wait_server_initialized().await;

    router_mgmt
        .update_remote(TEST_REMOTE_ID_3, target_3.grpc_base())
        .await
        .expect("set remote failed");

    let db_name_1 = rand_name();
    let db_name_2 = rand_name();
    for &db_name in &[&db_name_1, &db_name_2] {
        create_readable_database(db_name, router.grpc_channel()).await;
        create_readable_database(db_name, target_1.grpc_channel()).await;
        create_readable_database(db_name, target_2.grpc_channel()).await;
        create_readable_database(db_name, target_3.grpc_channel()).await;
    }

    // Set routing rules on the router:
    for (db_name, remote_id) in &[
        (db_name_1.clone(), TEST_REMOTE_ID_1),
        (db_name_2.clone(), TEST_REMOTE_ID_2),
    ] {
        let mut router_db_rules = router_mgmt
            .get_database(db_name)
            .await
            .expect("cannot get database on router");
        let routing_config = RoutingConfig {
            target: Some(NodeGroup {
                nodes: vec![Node { id: *remote_id }],
            }),
        };
        router_db_rules.routing_rules = Some(RoutingRules::RoutingConfig(routing_config));

        router_mgmt
            .update_database(router_db_rules)
            .await
            .expect("cannot update router db rules");
    }

    // Write some data
    let line_1 = "cpu bar=1 100";
    let line_2 = "disk bar=2 100";

    let mut write_client = router.write_client();

    for (&ref db_name, &ref line) in &[(&db_name_1, line_1), (&db_name_2, line_2)] {
        let num_lines_written = write_client
            .write(db_name, line)
            .await
            .expect("cannot write");
        assert_eq!(num_lines_written, 1);
    }

    // The router will have split the write request by database name.
    // Target 1 will have received only the "cpu" table.
    // Target 2 will have received only the "disk" table.
    // Target 3 won't get any writes.

    let mut query_results = target_1
        .flight_client()
        .perform_query(&db_name_1, "select * from cpu")
        .await
        .expect("failed to query target 1");

    let mut batches = Vec::new();
    while let Some(data) = query_results.next().await.unwrap() {
        batches.push(data);
    }

    let expected = vec![
        "+-----+-------------------------------+",
        "| bar | time                          |",
        "+-----+-------------------------------+",
        "| 1   | 1970-01-01 00:00:00.000000100 |",
        "+-----+-------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    assert!(target_1
        .flight_client()
        .perform_query(&db_name_1, "select * from disk")
        .await
        .unwrap_err()
        .to_string()
        .contains("Table or CTE with name 'disk' not found\""));

    let mut query_results = target_2
        .flight_client()
        .perform_query(&db_name_2, "select * from disk")
        .await
        .expect("failed to query target 2");

    let mut batches = Vec::new();
    while let Some(data) = query_results.next().await.unwrap() {
        batches.push(data);
    }

    let expected = vec![
        "+-----+-------------------------------+",
        "| bar | time                          |",
        "+-----+-------------------------------+",
        "| 2   | 1970-01-01 00:00:00.000000100 |",
        "+-----+-------------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    assert!(target_2
        .flight_client()
        .perform_query(&db_name_1, "select * from cpu")
        .await
        .unwrap_err()
        .to_string()
        .contains("Table or CTE with name 'cpu' not found\""));

    // Ensure that target_3 didn't get any writes.

    assert!(target_3
        .flight_client()
        .perform_query(&db_name_1, "select * from cpu")
        .await
        .unwrap_err()
        .to_string()
        .contains("Table or CTE with name 'cpu' not found\""));

    assert!(target_3
        .flight_client()
        .perform_query(&db_name_2, "select * from disk")
        .await
        .unwrap_err()
        .to_string()
        .contains("Table or CTE with name 'disk' not found\""));
}
