use arrow_util::assert_batches_eq;
use generated_types::influxdata::iox::management::v1::*;
use influxdb_iox_client::operations;

use super::scenario::{collect_query, create_readable_database, rand_name};
use crate::common::server_fixture::ServerFixture;

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

    // enable persistance
    let mut rules = management_client.get_database(&db_name).await.unwrap();
    rules.lifecycle_rules = Some({
        let mut lifecycle_rules = rules.lifecycle_rules.unwrap();
        lifecycle_rules.persist = true;
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

    let chunks = management_client
        .list_chunks(db_name)
        .await
        .expect("listing chunks");

    assert_eq!(chunks.len(), 1, "Chunks: {:#?}", chunks);
    let chunk_id = chunks[0].id;
    assert_eq!(chunks[0].storage, ChunkStorage::OpenMutableBuffer as i32);

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
    let mut chunks = management_client
        .list_chunks(db_name)
        .await
        .expect("listing chunks");
    chunks.sort_by(|c1, c2| c1.id.cmp(&c2.id));

    assert_eq!(chunks.len(), 1, "Chunks: {:#?}", chunks);
    assert_eq!(chunks[0].id, chunk_id);
    assert_eq!(chunks[0].storage, ChunkStorage::ReadBuffer as i32);

    chunk_id
}

async fn wait_for_persisted_chunk(
    fixture: &ServerFixture,
    db_name: &str,
    chunk_id: u32,
    wait_time: std::time::Duration,
) {
    let t_start = std::time::Instant::now();

    loop {
        let mut management_client = fixture.management_client();
        let chunks = management_client.list_chunks(db_name).await.unwrap();
        let chunk = chunks.iter().find(|chunk| chunk.id == chunk_id).unwrap();
        if (chunk.storage == ChunkStorage::ReadBufferAndObjectStore as i32)
            || (chunk.storage == ChunkStorage::ObjectStoreOnly as i32)
        {
            return;
        }

        assert!(t_start.elapsed() < wait_time);
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
