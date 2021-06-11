use std::{
    convert::TryInto,
    time::{Duration, Instant},
};

use data_types::chunk_metadata::ChunkSummary;

use crate::common::server_fixture::ServerFixture;

use super::scenario::{create_quickly_persisting_database, rand_name};

#[tokio::test]
async fn test_persistence() {
    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();
    let mut management_client = fixture.management_client();

    let db_name = rand_name();
    create_quickly_persisting_database(&db_name, fixture.grpc_channel()).await;

    // Stream in a write that should exceed the limit
    let lp_lines: Vec<_> = (0..1_000)
        .map(|i| format!("data,tag1=val{} x={} {}", i, i * 10, i))
        .collect();

    let num_lines_written = write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("successful write");
    assert_eq!(num_lines_written, 1000);

    // wait for the chunk to be written to object store
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut chunks = vec![];
    loop {
        assert!(
            Instant::now() < deadline,
            "Chunk did not persist in time. Chunks were: {:#?}",
            chunks
        );

        chunks = management_client
            .list_chunks(&db_name)
            .await
            .expect("listing chunks");

        let storage_string = chunks
            .iter()
            .map(|c| {
                let c: ChunkSummary = c.clone().try_into().unwrap();
                format!("{:?}", c.storage)
            })
            .collect::<Vec<_>>()
            .join(",");

        // Found a persisted chunk, all good
        if storage_string == "ReadBufferAndObjectStore" {
            return;
        }

        // keep looking
        println!("Current chunk storage: {:#?}", storage_string);
        tokio::time::sleep(Duration::from_millis(200)).await
    }
}
