use itertools::Itertools;

use data_types::chunk_metadata::ChunkStorage;

use crate::{common::server_fixture::ServerFixture, end_to_end_cases::scenario::list_chunks};

use super::scenario::{rand_name, DatabaseBuilder};

#[tokio::test]
async fn test_mub_freeze() {
    let fixture = ServerFixture::create_shared().await;
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    DatabaseBuilder::new(db_name.clone())
        // Disable background worker to prevent compaction and ensure
        // freeze is called by the write path
        .worker_backoff_millis(100_000)
        .mub_row_threshold(20)
        .build(fixture.grpc_channel())
        .await;

    let lp_lines: Vec<_> = (0..20)
        .map(|i| format!("data,tag1=val{} x={} {}", i, i * 10, i))
        .collect();

    let num_lines_written = write_client
        .write(&db_name, lp_lines.iter().join("\n"))
        .await
        .expect("successful write");
    assert_eq!(num_lines_written, 20);

    let chunks = list_chunks(&fixture, &db_name).await;
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].storage, ChunkStorage::ClosedMutableBuffer);

    let num_lines_written = write_client
        .write(&db_name, lp_lines.iter().take(10).join("\n"))
        .await
        .expect("successful write");
    assert_eq!(num_lines_written, 10);

    // Not exceeded row threshold - shouldn't freeze
    let mut chunks = list_chunks(&fixture, &db_name).await;
    chunks.sort_by_key(|chunk| chunk.storage);
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].storage, ChunkStorage::OpenMutableBuffer);
    assert_eq!(chunks[1].storage, ChunkStorage::ClosedMutableBuffer);

    let num_lines_written = write_client
        .write(&db_name, lp_lines.iter().take(10).join("\n"))
        .await
        .expect("successful write");
    assert_eq!(num_lines_written, 10);

    // Exceeded row threshold - should freeze
    let mut chunks = list_chunks(&fixture, &db_name).await;
    chunks.sort_by_key(|chunk| chunk.storage);
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].storage, ChunkStorage::ClosedMutableBuffer);
    assert_eq!(chunks[1].storage, ChunkStorage::ClosedMutableBuffer);
}
