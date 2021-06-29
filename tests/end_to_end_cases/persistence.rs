use std::convert::TryInto;

use itertools::Itertools;

use arrow_util::assert_batches_eq;
use data_types::chunk_metadata::{ChunkStorage, ChunkSummary};
use influxdb_iox_client::operations;

use crate::common::server_fixture::ServerFixture;

use super::scenario::{
    collect_query, create_quickly_persisting_database, create_readable_database, rand_name,
};

#[tokio::test]
async fn test_chunk_is_persisted_automatically() {
    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_quickly_persisting_database(&db_name, fixture.grpc_channel(), 1).await;

    let lp_lines: Vec<_> = (0..1_000)
        .map(|i| format!("data,tag1=val{} x={} {}", i, i * 10, i))
        .collect();

    let num_lines_written = write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("successful write");
    assert_eq!(num_lines_written, 1000);

    wait_for_chunk(
        &fixture,
        &db_name,
        ChunkStorage::ReadBufferAndObjectStore,
        std::time::Duration::from_secs(5),
    )
    .await;

    // Should have compacted into a single chunk
    let chunks = list_chunks(&fixture, &db_name).await;
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].row_count, 1_000);
}

#[tokio::test]
async fn test_full_lifecycle() {
    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_quickly_persisting_database(&db_name, fixture.grpc_channel(), 100).await;

    // write in enough data to exceed the soft limit (512K) and
    // expect that it compacts, persists and then unloads the data from memory
    let num_payloads = 10;
    let num_duplicates = 2;
    let payload_size = 1_000;

    let payloads: Vec<_> = (0..num_payloads)
        .map(|x| {
            (0..payload_size)
                .map(|i| format!("data,tag{}=val{} x={} {}", x, i, i * 10, i))
                .join("\n")
        })
        .collect();

    for payload in payloads.iter().take(num_payloads - 1) {
        // Writing the same data multiple times should be compacted away
        for _ in 0..num_duplicates {
            let num_lines_written = write_client
                .write(&db_name, payload)
                .await
                .expect("successful write");
            assert_eq!(num_lines_written, payload_size);
        }
    }

    // Don't duplicate last write as it is what crosses the persist row threshold
    let num_lines_written = write_client
        .write(&db_name, payloads.last().unwrap())
        .await
        .expect("successful write");
    assert_eq!(num_lines_written, payload_size);

    wait_for_chunk(
        &fixture,
        &db_name,
        ChunkStorage::ObjectStoreOnly,
        std::time::Duration::from_secs(10),
    )
    .await;

    // Expect them to have been compacted into a single read buffer
    // with the duplicates eliminated
    let chunks = list_chunks(&fixture, &db_name).await;
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].row_count, num_payloads * payload_size)
}

#[tokio::test]
async fn test_query_chunk_after_restart() {
    // fixtures
    let fixture = ServerFixture::create_single_use().await;
    let server_id = 42;
    let db_name = rand_name();

    // set server ID
    let mut management_client = fixture.management_client();
    management_client
        .update_server_id(server_id)
        .await
        .expect("set ID failed");
    fixture.wait_server_initialized().await;

    // create DB and a RB chunk
    create_readable_database(&db_name, fixture.grpc_channel()).await;
    let chunk_id = create_readbuffer_chunk(&fixture, &db_name).await;

    // enable persistence
    let mut rules = management_client.get_database(&db_name).await.unwrap();
    rules.lifecycle_rules = Some({
        let mut lifecycle_rules = rules.lifecycle_rules.unwrap();
        lifecycle_rules.persist = true;
        lifecycle_rules.late_arrive_window_seconds = 1;
        lifecycle_rules
    });
    management_client.update_database(rules).await.unwrap();

    // wait for persistence
    wait_for_persisted_chunk(
        &fixture,
        &db_name,
        chunk_id,
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
    use influxdb_iox_client::management::generated_types::operation_metadata::Job;

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

    // And now the chunk  should be good
    let mut chunks = list_chunks(fixture, db_name).await;
    chunks.sort_by(|c1, c2| c1.id.cmp(&c2.id));

    assert_eq!(chunks.len(), 1, "Chunks: {:#?}", chunks);
    assert_eq!(chunks[0].id, chunk_id);
    assert_eq!(chunks[0].storage, ChunkStorage::ReadBuffer);

    chunk_id
}

// Wait for the specified chunk to be persisted to object store
async fn wait_for_persisted_chunk(
    fixture: &ServerFixture,
    db_name: &str,
    chunk_id: u32,
    wait_time: std::time::Duration,
) {
    let t_start = std::time::Instant::now();

    loop {
        let chunks = list_chunks(fixture, db_name).await;

        let chunk = chunks.iter().find(|chunk| chunk.id == chunk_id).unwrap();
        if (chunk.storage == ChunkStorage::ReadBufferAndObjectStore)
            || (chunk.storage == ChunkStorage::ObjectStoreOnly)
        {
            return;
        }

        assert!(t_start.elapsed() < wait_time);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

// Wait for at least one chunk to be in the specified storage state
async fn wait_for_chunk(
    fixture: &ServerFixture,
    db_name: &str,
    desired_storage: ChunkStorage,
    wait_time: std::time::Duration,
) {
    let t_start = std::time::Instant::now();

    loop {
        let chunks = list_chunks(fixture, db_name).await;

        if chunks.iter().any(|chunk| chunk.storage == desired_storage) {
            return;
        }

        // Log the current status of the chunks
        for chunk in &chunks {
            println!(
                "{:?}: chunk {} partition {} storage: {:?} row_count: {} time_of_last_write: {:?}",
                (t_start.elapsed()),
                chunk.id,
                chunk.partition_key,
                chunk.storage,
                chunk.row_count,
                chunk.time_of_last_write
            );
        }

        if t_start.elapsed() >= wait_time {
            let operations = fixture.operations_client().list_operations().await.unwrap();
            let mut operations: Vec<_> = operations
                .into_iter()
                .map(|x| (x.name().parse::<usize>().unwrap(), x.metadata()))
                .collect();
            operations.sort_by_key(|x| x.0);

            panic!("Could not find chunk in desired state {:?} within {:?}.\nChunks were: {:#?}\nOperations were: {:#?}", desired_storage, wait_time, chunks, operations)
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn assert_chunk_query_works(fixture: &ServerFixture, db_name: &str) {
    let mut client = fixture.flight_client();
    let sql_query = "select region, user, time from cpu";

    let query_results = client.perform_query(db_name, sql_query).await.unwrap();

    let batches = collect_query(query_results).await;
    let expected_read_data = vec![
        "+--------+------+-------------------------------+",
        "| region | user | time                          |",
        "+--------+------+-------------------------------+",
        "| west   | 23.2 | 1970-01-01 00:00:00.000000100 |",
        "+--------+------+-------------------------------+",
    ];

    assert_batches_eq!(expected_read_data, &batches);
}

/// Gets the list of ChunkSummaries from the server
async fn list_chunks(fixture: &ServerFixture, db_name: &str) -> Vec<ChunkSummary> {
    let mut management_client = fixture.management_client();
    let chunks = management_client.list_chunks(db_name).await.unwrap();

    chunks.into_iter().map(|c| c.try_into().unwrap()).collect()
}
