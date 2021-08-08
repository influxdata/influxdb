use itertools::Itertools;

use arrow_util::assert_batches_eq;
use data_types::chunk_metadata::ChunkStorage;
use influxdb_iox_client::operations;

use crate::{
    common::server_fixture::ServerFixture,
    end_to_end_cases::scenario::{list_chunks, wait_for_exact_chunk_states},
};

use super::scenario::{collect_query, create_readable_database, rand_name, DatabaseBuilder};
use crate::common::server_fixture::DEFAULT_SERVER_ID;
use generated_types::influxdata::iox::management::v1::{operation_metadata::Job, CompactChunks};

#[tokio::test]
async fn test_chunk_is_persisted_automatically() {
    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    DatabaseBuilder::new(db_name.clone())
        .persist(true)
        .persist_age_threshold_seconds(1)
        .late_arrive_window_seconds(1)
        .build(fixture.grpc_channel())
        .await;

    let lp_lines: Vec<_> = (0..1_000)
        .map(|i| format!("data,tag1=val{} x={} {}", i, i * 10, i))
        .collect();

    let num_lines_written = write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("successful write");
    assert_eq!(num_lines_written, 1000);

    wait_for_exact_chunk_states(
        &fixture,
        &db_name,
        vec![ChunkStorage::ReadBufferAndObjectStore],
        std::time::Duration::from_secs(5),
    )
    .await;

    // Should have compacted into a single chunk
    let chunks = list_chunks(&fixture, &db_name).await;
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].row_count, 1_000);
}

async fn write_data(
    write_client: &mut influxdb_iox_client::write::Client,
    db_name: &str,
    num_payloads: u64,
    num_duplicates: u64,
    payload_size: u64,
) {
    let payloads: Vec<_> = (0..num_payloads)
        .map(|x| {
            (0..payload_size)
                .map(|i| format!("data,tag{}=val{} x={} {}", x, i, i * 10, i))
                .join("\n")
        })
        .collect();

    for payload in &payloads {
        // Writing the same data multiple times should be compacted away
        for _ in 0..=num_duplicates {
            let num_lines_written = write_client
                .write(db_name, payload)
                .await
                .expect("successful write");
            assert_eq!(num_lines_written, payload_size as usize);
        }
    }
}

#[tokio::test]
async fn test_full_lifecycle() {
    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();

    let num_payloads = 10;
    let num_duplicates = 1;
    let payload_size = 1_000;

    let total_rows = num_payloads * (1 + num_duplicates) * payload_size;

    let db_name = rand_name();
    DatabaseBuilder::new(db_name.clone())
        .persist(true)
        // Each write should go into a separate chunk to test compaction
        .mub_row_threshold(payload_size)
        // Only trigger persistence once we've finished writing
        .persist_row_threshold(total_rows)
        .persist_age_threshold_seconds(1000)
        // A low late arrival time to speed up the test
        .late_arrive_window_seconds(1)
        .build(fixture.grpc_channel())
        .await;

    write_data(
        &mut write_client,
        &db_name,
        num_payloads,
        num_duplicates,
        payload_size,
    )
    .await;

    wait_for_exact_chunk_states(
        &fixture,
        &db_name,
        vec![ChunkStorage::ObjectStoreOnly],
        std::time::Duration::from_secs(10),
    )
    .await;

    // Expect compaction to have occurred
    let performed_compaction = fixture
        .operations_client()
        .list_operations()
        .await
        .unwrap()
        .iter()
        .any(|operation| match operation.metadata().job {
            Some(Job::CompactChunks(CompactChunks {
                db_name: operation_db_name,
                ..
            })) => operation_db_name == db_name,
            _ => false,
        });
    assert!(performed_compaction);

    // Expect them to have been compacted into a single read buffer
    // with the duplicates eliminated
    let chunks = list_chunks(&fixture, &db_name).await;
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].row_count, (num_payloads * payload_size) as usize)
}

#[tokio::test]
async fn test_update_late_arrival() {
    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();

    let payload_size = 100;

    let db_name = rand_name();
    DatabaseBuilder::new(db_name.clone())
        .persist(true)
        // Don't close MUB automatically
        .mub_row_threshold(payload_size * 2)
        .persist_row_threshold(payload_size)
        .persist_age_threshold_seconds(1000)
        // Initially set to be a large value
        .late_arrive_window_seconds(1000)
        .build(fixture.grpc_channel())
        .await;

    write_data(&mut write_client, &db_name, 1, 0, payload_size).await;

    let mut management = fixture.management_client();

    let chunks = management.list_chunks(&db_name).await.unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(
        chunks[0].storage,
        influxdb_iox_client::management::generated_types::ChunkStorage::OpenMutableBuffer as i32
    );

    let mut rules = management.get_database(&db_name).await.unwrap();
    rules
        .lifecycle_rules
        .as_mut()
        .unwrap()
        .late_arrive_window_seconds = 1;

    fixture
        .management_client()
        .update_database(rules)
        .await
        .unwrap();

    wait_for_exact_chunk_states(
        &fixture,
        &db_name,
        vec![ChunkStorage::ReadBufferAndObjectStore],
        std::time::Duration::from_secs(5),
    )
    .await;
}

#[tokio::test]
async fn test_query_chunk_after_restart() {
    // fixtures
    let fixture = ServerFixture::create_single_use().await;
    let db_name = rand_name();

    // set server ID
    let mut management_client = fixture.management_client();
    management_client
        .update_server_id(DEFAULT_SERVER_ID)
        .await
        .expect("set ID failed");
    fixture.wait_server_initialized().await;

    // create DB and a RB chunk
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    // enable persistence prior to write
    let mut rules = management_client.get_database(&db_name).await.unwrap();
    rules.lifecycle_rules = Some({
        let mut lifecycle_rules = rules.lifecycle_rules.unwrap();
        lifecycle_rules.persist = true;
        lifecycle_rules.late_arrive_window_seconds = 1;
        lifecycle_rules.persist_row_threshold = 1;
        lifecycle_rules.persist_age_threshold_seconds = 1;
        lifecycle_rules
    });
    management_client.update_database(rules).await.unwrap();

    create_readbuffer_chunk(&fixture, &db_name).await;

    // wait the chunk to be persisted
    wait_for_exact_chunk_states(
        &fixture,
        &db_name,
        vec![ChunkStorage::ReadBufferAndObjectStore],
        std::time::Duration::from_secs(10),
    )
    .await;

    // check before restart
    assert_chunk_query_works(&fixture, &db_name).await;

    // restart server
    let fixture = fixture.restart_server().await;
    fixture.wait_server_initialized().await;

    // query data after restart
    assert_chunk_query_works(&fixture, &db_name).await;
}

/// Create a closed read buffer chunk and return its id
async fn create_readbuffer_chunk(fixture: &ServerFixture, db_name: &str) -> u32 {
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();
    let mut operations_client = fixture.operations_client();

    let partition_key = "cpu";
    let table_name = "cpu";
    let lp_lines = vec!["cpu,region=west user=23.2 100"];

    write_client
        .write(db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    let chunks = list_chunks(fixture, db_name).await;

    assert_eq!(chunks.len(), 1, "Chunks: {:#?}", chunks);
    let chunk_id = chunks[0].id;
    assert_eq!(chunks[0].storage, ChunkStorage::OpenMutableBuffer);

    // Move the chunk to read buffer
    let operation = management_client
        .close_partition_chunk(db_name, table_name, partition_key, 0)
        .await
        .expect("new partition chunk");

    println!("Operation response is {:?}", operation);
    let operation_id = operation.id();

    let meta = operations::ClientOperation::try_new(operation)
        .unwrap()
        .metadata();

    // ensure we got a legit job description back
    if let Some(Job::CloseChunk(close_chunk)) = meta.job {
        assert_eq!(close_chunk.db_name, db_name);
        assert_eq!(close_chunk.partition_key, partition_key);
        assert_eq!(close_chunk.chunk_id, 0);
    } else {
        panic!("unexpected job returned")
    };

    // wait for the job to be done
    operations_client
        .wait_operation(operation_id, Some(std::time::Duration::from_secs(1)))
        .await
        .expect("failed to wait operation");

    // And now the chunk should be good
    let mut chunks = list_chunks(fixture, db_name).await;
    chunks.sort_by(|c1, c2| c1.id.cmp(&c2.id));

    assert_eq!(chunks.len(), 1, "Chunks: {:#?}", chunks);
    assert_eq!(chunks[0].id, chunk_id);
    assert_eq!(chunks[0].storage, ChunkStorage::ReadBuffer);

    chunk_id
}

async fn assert_chunk_query_works(fixture: &ServerFixture, db_name: &str) {
    let mut client = fixture.flight_client();
    let sql_query = "select region, user, time from cpu";

    let query_results = client.perform_query(db_name, sql_query).await.unwrap();

    let batches = collect_query(query_results).await;
    let expected_read_data = vec![
        "+--------+------+--------------------------------+",
        "| region | user | time                           |",
        "+--------+------+--------------------------------+",
        "| west   | 23.2 | 1970-01-01T00:00:00.000000100Z |",
        "+--------+------+--------------------------------+",
    ];

    assert_batches_eq!(expected_read_data, &batches);
}
