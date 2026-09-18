use crate::{Precision, write_buffer::validator::WriteValidator};

use super::*;
use arrow_util::{assert_batches_eq, assert_batches_sorted_eq};
use datafusion::prelude::{Expr, col, lit_timestamp_nano};
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
use influxdb3_types::DatabaseName;
use iox_time::{MockProvider, Time};
use object_store::memory::InMemory;

struct TestWriter {
    catalog: Arc<Catalog>,
}

impl TestWriter {
    const DB_NAME: &str = "test-db";

    async fn new() -> Self {
        let obj_store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let catalog = Catalog::new("test-node", obj_store, time_provider, Default::default())
            .await
            .expect("should initialize catalog");
        Self { catalog }
    }

    async fn write_to_rows(&self, lp: impl AsRef<str>, ingest_time_sec: i64) -> Vec<Row> {
        let db = DatabaseName::try_from(Self::DB_NAME).unwrap();
        let ingest_time_ns = ingest_time_sec * 1_000_000_000;
        let validator = WriteValidator::initialize(db, Arc::clone(&self.catalog)).unwrap();
        validator
            .v1_parse_lines_and_catalog_updates(
                lp.as_ref(),
                false,
                Time::from_timestamp_nanos(ingest_time_ns),
                Precision::Nanosecond,
            )
            .unwrap()
            .commit_catalog_changes()
            .await
            .map(|r| r.unwrap_success())
            .unwrap()
            .into_inner()
            .to_rows()
    }

    fn db_schema(&self) -> Arc<DatabaseSchema> {
        self.catalog.db_schema(Self::DB_NAME).unwrap()
    }
}

#[tokio::test]
async fn test_partitioned_table_buffer_batches() {
    let writer = TestWriter::new().await;

    let mut row_batches = Vec::new();
    for t in 0..10 {
        let offset = t * 10;
        let rows = writer
            .write_to_rows(
                format!(
                    "\
        tbl,tag=a val=\"thing {t}-1\" {o1}\n\
        tbl,tag=b val=\"thing {t}-2\" {o2}\n\
        ",
                    o1 = offset + 1,
                    o2 = offset + 2,
                ),
                offset,
            )
            .await;
        row_batches.push((rows, offset));
    }

    let table_def = writer.db_schema().table_definition("tbl").unwrap();

    let mut table_buffer = TableBuffer::new();
    for (rows, offset) in row_batches {
        table_buffer.buffer_chunk(offset, &rows);
    }

    let partitioned_batches = table_buffer
        .partitioned_record_batches(Arc::clone(&table_def), &ChunkFilter::default())
        .unwrap();

    assert_eq!(10, partitioned_batches.len());

    for t in 0..10 {
        let offset = t * 10;
        let buffered = partitioned_batches.get(&offset).unwrap();
        let batches = &buffered.combined();
        assert_eq!(
            Some(TimestampMinMax::new(offset + 1, offset + 2)),
            buffered.timestamp_min_max()
        );
        assert_batches_sorted_eq!(
            [
                "+-----+--------------------------------+-----------+",
                "| tag | time                           | val       |",
                "+-----+--------------------------------+-----------+",
                format!(
                    "| a   | 1970-01-01T00:00:00.{:0>9}Z | thing {t}-1 |",
                    offset + 1
                )
                .as_str(),
                format!(
                    "| b   | 1970-01-01T00:00:00.{:0>9}Z | thing {t}-2 |",
                    offset + 2
                )
                .as_str(),
                "+-----+--------------------------------+-----------+",
            ],
            batches
        );
    }
}

#[tokio::test]
async fn test_computed_size_of_buffer() {
    let writer = TestWriter::new().await;

    let rows = writer
        .write_to_rows(
            "\
        tbl,tag=a value=1i 1\n\
        tbl,tag=b value=2i 2\n\
        tbl,tag=this\\ is\\ a\\ long\\ tag\\ value\\ to\\ store value=3i 3\n\
        ",
            0,
        )
        .await;

    let mut table_buffer = TableBuffer::new();
    table_buffer.buffer_chunk(0, &rows);

    let size = table_buffer.computed_size();
    assert_eq!(size, 1427);
}

#[test]
fn timestamp_min_max_works_when_empty() {
    let table_buffer = TableBuffer::new();
    let timestamp_min_max = table_buffer.timestamp_min_max();
    assert_eq!(timestamp_min_max.min, 0);
    assert_eq!(timestamp_min_max.max, 0);
}

#[test_log::test(tokio::test)]
async fn test_time_filters() {
    let writer = TestWriter::new().await;

    let mut row_batches = Vec::new();
    for offset in 0..100 {
        let rows = writer
            .write_to_rows(
                format!(
                    "\
            tbl,tag=a val={}\n\
            tbl,tag=b val={}\n\
            ",
                    offset + 1,
                    offset + 2
                ),
                offset,
            )
            .await;
        row_batches.push((offset, rows));
    }
    let table_def = writer.db_schema().table_definition("tbl").unwrap();
    let mut table_buffer = TableBuffer::new();

    for (offset, rows) in row_batches {
        table_buffer.buffer_chunk(offset, &rows);
    }

    struct TestCase<'a> {
        filter: &'a [Expr],
        expected_output: &'a [&'a str],
    }

    let test_cases = [
        TestCase {
            filter: &[col("time").gt(lit_timestamp_nano(97_000_000_000i64))],
            expected_output: &[
                "+-----+----------------------+-------+",
                "| tag | time                 | val   |",
                "+-----+----------------------+-------+",
                "| a   | 1970-01-01T00:01:38Z | 99.0  |",
                "| a   | 1970-01-01T00:01:39Z | 100.0 |",
                "| b   | 1970-01-01T00:01:38Z | 100.0 |",
                "| b   | 1970-01-01T00:01:39Z | 101.0 |",
                "+-----+----------------------+-------+",
            ],
        },
        TestCase {
            filter: &[col("time").lt(lit_timestamp_nano(3_000_000_000i64))],
            expected_output: &[
                "+-----+----------------------+-----+",
                "| tag | time                 | val |",
                "+-----+----------------------+-----+",
                "| a   | 1970-01-01T00:00:00Z | 1.0 |",
                "| a   | 1970-01-01T00:00:01Z | 2.0 |",
                "| a   | 1970-01-01T00:00:02Z | 3.0 |",
                "| b   | 1970-01-01T00:00:00Z | 2.0 |",
                "| b   | 1970-01-01T00:00:01Z | 3.0 |",
                "| b   | 1970-01-01T00:00:02Z | 4.0 |",
                "+-----+----------------------+-----+",
            ],
        },
        TestCase {
            filter: &[col("time")
                .gt(lit_timestamp_nano(3_000_000_000i64))
                .and(col("time").lt(lit_timestamp_nano(6_000_000_000i64)))],
            expected_output: &[
                "+-----+----------------------+-----+",
                "| tag | time                 | val |",
                "+-----+----------------------+-----+",
                "| a   | 1970-01-01T00:00:04Z | 5.0 |",
                "| a   | 1970-01-01T00:00:05Z | 6.0 |",
                "| b   | 1970-01-01T00:00:04Z | 6.0 |",
                "| b   | 1970-01-01T00:00:05Z | 7.0 |",
                "+-----+----------------------+-----+",
            ],
        },
    ];

    for t in test_cases {
        let filter = ChunkFilter::new(&table_def, t.filter).unwrap();
        let batches = table_buffer
            .partitioned_record_batches(Arc::clone(&table_def), &filter)
            .unwrap()
            .into_values()
            .flat_map(|buffered| buffered.combined())
            .collect::<Vec<RecordBatch>>();
        assert_batches_sorted_eq!(t.expected_output, &batches);
    }
}

#[tokio::test]
async fn test_chunk_splits_on_large_string_payload() {
    // Set a low limit so we can test chunk splitting without using huge amounts of memory.
    // Each string is 50 bytes, so with a limit of 99 bytes:
    // - rows1 (50 bytes): 0 + 50 <= 99, fits in chunk1
    // - rows2 (50 bytes): 50 + 50 = 100 > 99, predictive check triggers new chunk2
    // - rows3 (50 bytes): 50 + 50 = 100 > 99, predictive check triggers new chunk3
    let _guard = VarColMaxGuard::new(99);

    let writer = TestWriter::new().await;

    // Each string is 50 bytes
    let rows1 = writer
        .write_to_rows(
            "tbl,tag=a val=\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\" 1",
            0,
        )
        .await;

    let rows2 = writer
        .write_to_rows(
            "tbl,tag=b val=\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\" 2",
            0,
        )
        .await;

    let rows3 = writer
        .write_to_rows(
            "tbl,tag=c val=\"cccccccccccccccccccccccccccccccccccccccccccccccccc\" 3",
            0,
        )
        .await;

    let table_def = writer.db_schema().table_definition("tbl").unwrap();

    let mut table_buffer = TableBuffer::new();

    // Buffer first batch - chunk is empty, 0 + 50 <= 99, goes to chunk1
    // After: chunk1.string_bytes_per_column[val] = 50
    table_buffer.buffer_chunk(0, &rows1);
    assert_eq!(
        table_buffer
            .chunk_time_to_chunks
            .get(&0)
            .unwrap()
            .chunks
            .len(),
        1
    );

    // Buffer second batch - predictive check: 50 + 50 = 100 > 99, triggers new chunk
    // After: chunk2.string_bytes_per_column[val] = 50
    table_buffer.buffer_chunk(0, &rows2);
    assert_eq!(
        table_buffer
            .chunk_time_to_chunks
            .get(&0)
            .unwrap()
            .chunks
            .len(),
        2
    );

    // Buffer third batch - predictive check: 50 + 50 = 100 > 99, triggers new chunk
    // After: chunk3.string_bytes_per_column[val] = 50
    table_buffer.buffer_chunk(0, &rows3);
    assert_eq!(
        table_buffer
            .chunk_time_to_chunks
            .get(&0)
            .unwrap()
            .chunks
            .len(),
        3
    );

    // Verify all data can be retrieved
    let batches = table_buffer
        .partitioned_record_batches(Arc::clone(&table_def), &ChunkFilter::default())
        .unwrap();

    assert_eq!(batches.len(), 1); // 1 time partition
    let buffered = batches.get(&0).unwrap();
    assert_eq!(buffered.timestamp_min_max().unwrap().min, 1);
    assert_eq!(buffered.timestamp_min_max().unwrap().max, 3);
    assert_eq!(buffered.live.len(), 3); // 3 chunks -> 3 record batches

    // Check total row count across all batches
    let total_rows: usize = buffered.live.iter().map(|rb| rb.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_chunk_accumulates_when_under_limit() {
    // Test that multiple writes accumulate in the same chunk when under the limit.
    // Each string is 30 bytes, limit is 100 bytes:
    // - rows1 (30 bytes): 0 + 30 <= 100, fits in chunk1
    // - rows2 (30 bytes): 30 + 30 = 60 <= 100, still fits in chunk1
    // - rows3 (30 bytes): 60 + 30 = 90 <= 100, still fits in chunk1
    // - rows4 (30 bytes): 90 + 30 = 120 > 100, triggers new chunk2
    let _guard = VarColMaxGuard::new(100);

    let writer = TestWriter::new().await;

    // Each string is 30 bytes
    let rows1 = writer
        .write_to_rows("tbl,tag=a val=\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\" 1", 0)
        .await;

    let rows2 = writer
        .write_to_rows("tbl,tag=b val=\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\" 2", 0)
        .await;

    let rows3 = writer
        .write_to_rows("tbl,tag=c val=\"cccccccccccccccccccccccccccccc\" 3", 0)
        .await;

    let rows4 = writer
        .write_to_rows("tbl,tag=d val=\"dddddddddddddddddddddddddddddd\" 4", 0)
        .await;

    let table_def = writer.db_schema().table_definition("tbl").unwrap();

    let mut table_buffer = TableBuffer::new();

    // First three writes should all go to the same chunk
    table_buffer.buffer_chunk(0, &rows1);
    assert_eq!(
        table_buffer
            .chunk_time_to_chunks
            .get(&0)
            .unwrap()
            .chunks
            .len(),
        1
    );

    table_buffer.buffer_chunk(0, &rows2);
    assert_eq!(
        table_buffer
            .chunk_time_to_chunks
            .get(&0)
            .unwrap()
            .chunks
            .len(),
        1
    );

    table_buffer.buffer_chunk(0, &rows3);
    assert_eq!(
        table_buffer
            .chunk_time_to_chunks
            .get(&0)
            .unwrap()
            .chunks
            .len(),
        1
    );

    // Fourth write should trigger a new chunk
    table_buffer.buffer_chunk(0, &rows4);
    assert_eq!(
        table_buffer
            .chunk_time_to_chunks
            .get(&0)
            .unwrap()
            .chunks
            .len(),
        2
    );

    // Verify chunk sizes: first chunk should have 3 rows, second chunk should have 1 row
    let chunks = &table_buffer.chunk_time_to_chunks.get(&0).unwrap().chunks;
    assert_eq!(chunks[0].row_count, 3);
    assert_eq!(chunks[1].row_count, 1);

    // Verify all data can be retrieved
    let batches = table_buffer
        .partitioned_record_batches(Arc::clone(&table_def), &ChunkFilter::default())
        .unwrap();

    let buffered = batches.get(&0).unwrap();
    assert_eq!(buffered.timestamp_min_max().unwrap().min, 1);
    assert_eq!(buffered.timestamp_min_max().unwrap().max, 4);

    let total_rows: usize = buffered.live.iter().map(|rb| rb.num_rows()).sum();
    assert_eq!(total_rows, 4);
}

#[tokio::test]
async fn test_chunk_splits_on_large_tag_payload() {
    // Each tag value is 50 bytes, limit is 99 bytes:
    // - rows1 (50 byte tag): 0 + 50 <= 99, fits in chunk1
    // - rows2 (50 byte tag): 50 + 50 = 100 > 99, triggers new chunk2
    // - rows3 (50 byte tag): 50 + 50 = 100 > 99, triggers new chunk3
    let _guard = VarColMaxGuard::new(99);
    let writer = TestWriter::new().await;
    let tag_a = "a".repeat(50);
    let tag_b = "b".repeat(50);
    let tag_c = "c".repeat(50);

    let rows1 = writer
        .write_to_rows(format!("tbl,tag={tag_a} val=1.0 1"), 0)
        .await;

    let rows2 = writer
        .write_to_rows(format!("tbl,tag={tag_b} val=2.0 2"), 0)
        .await;

    let rows3 = writer
        .write_to_rows(format!("tbl,tag={tag_c} val=3.0 3"), 0)
        .await;

    let table_def = writer.db_schema().table_definition("tbl").unwrap();
    let mut table_buffer = TableBuffer::new();
    table_buffer.buffer_chunk(0, &rows1);
    assert_eq!(
        table_buffer
            .chunk_time_to_chunks
            .get(&0)
            .unwrap()
            .chunks
            .len(),
        1
    );
    table_buffer.buffer_chunk(0, &rows2);
    assert_eq!(
        table_buffer
            .chunk_time_to_chunks
            .get(&0)
            .unwrap()
            .chunks
            .len(),
        2
    );
    table_buffer.buffer_chunk(0, &rows3);
    assert_eq!(
        table_buffer
            .chunk_time_to_chunks
            .get(&0)
            .unwrap()
            .chunks
            .len(),
        3
    );

    // Verify all data can be retrieved
    let batches = table_buffer
        .partitioned_record_batches(Arc::clone(&table_def), &ChunkFilter::default())
        .unwrap();

    assert_eq!(batches.len(), 1);
    let buffered = batches.get(&0).unwrap();
    assert_eq!(buffered.timestamp_min_max().unwrap().min, 1);
    assert_eq!(buffered.timestamp_min_max().unwrap().max, 3);
    assert_eq!(buffered.live.len(), 3);

    let total_rows: usize = buffered.live.iter().map(|rb| rb.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

/// A snapshot's chunks are persisted concurrently, and each job hands its own
/// chunk over to its parquet file. Dropping a chunk is therefore per chunk, not
/// per table: a chunk is served from `snapshotting_chunks` until its own file
/// exists and from that file afterwards, so evicting the whole table when the
/// first job finishes makes every sibling chunk — whole gen1 chunk_times —
/// briefly invisible to queries.
#[tokio::test]
async fn test_remove_snapshotting_chunk_leaves_the_other_chunks_queryable() {
    // 30-byte strings against a 100-byte cap, so chunk_time 0 splits in two and
    // the ordinal has to distinguish them.
    let _guard = VarColMaxGuard::new(100);

    let writer = TestWriter::new().await;
    let rows1 = writer
        .write_to_rows("tbl,tag=a val=\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\" 1", 0)
        .await;
    let rows2 = writer
        .write_to_rows("tbl,tag=b val=\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\" 2", 0)
        .await;
    let rows3 = writer
        .write_to_rows("tbl,tag=c val=\"cccccccccccccccccccccccccccccc\" 3", 0)
        .await;
    let rows4 = writer
        .write_to_rows("tbl,tag=d val=\"dddddddddddddddddddddddddddddd\" 4", 0)
        .await;
    let table_def = writer.db_schema().table_definition("tbl").unwrap();

    let mut table_buffer = TableBuffer::new();
    // chunk_time 0: rows1-3 fill the first chunk, rows4 opens a second one.
    table_buffer.buffer_chunk(0, &rows1);
    table_buffer.buffer_chunk(0, &rows2);
    table_buffer.buffer_chunk(0, &rows3);
    table_buffer.buffer_chunk(0, &rows4);
    assert_eq!(
        table_buffer
            .chunk_time_to_chunks
            .get(&0)
            .unwrap()
            .chunks
            .len(),
        2
    );
    // chunk_time 10: one chunk, the sibling gen1 block.
    table_buffer.buffer_chunk(10, &rows1);

    let snapshot_chunks = table_buffer.snapshot(Arc::clone(&table_def), i64::MAX);
    assert_eq!(
        vec![(0, 0), (0, 1), (10, 0)],
        snapshot_chunks
            .iter()
            .map(|c| (c.chunk_time, c.chunk_ordinal))
            .collect::<Vec<_>>(),
        "snapshot assigns each chunk an ordinal within its chunk_time"
    );

    let queryable_rows = |buffer: &TableBuffer| -> Vec<(i64, usize)> {
        let batches = buffer
            .partitioned_record_batches(Arc::clone(&table_def), &ChunkFilter::default())
            .unwrap();
        let mut counts: Vec<(i64, usize)> = batches
            .into_iter()
            .map(|(chunk_time, buffered)| {
                (
                    chunk_time,
                    buffered
                        .combined()
                        .iter()
                        .map(|rb| rb.num_rows())
                        .sum::<usize>(),
                )
            })
            .collect();
        counts.sort();
        counts
    };

    assert_eq!(vec![(0, 4), (10, 1)], queryable_rows(&table_buffer));

    // The first job finishes: only its chunk goes, the rest stay queryable.
    table_buffer.remove_snapshotting_chunk(0, 0);
    assert_eq!(
        vec![(0, 1), (10, 1)],
        queryable_rows(&table_buffer),
        "the split sibling at chunk_time 0 and the chunk at chunk_time 10 are \
         still being persisted and must remain queryable"
    );

    table_buffer.remove_snapshotting_chunk(10, 0);
    assert_eq!(vec![(0, 1)], queryable_rows(&table_buffer));

    // An ordinal that no longer matches anything is a no-op, not a table wipe.
    table_buffer.remove_snapshotting_chunk(0, 7);
    assert_eq!(vec![(0, 1)], queryable_rows(&table_buffer));

    table_buffer.remove_snapshotting_chunk(0, 1);
    assert!(queryable_rows(&table_buffer).is_empty());
}

/// Overwrites of the same (tag set, timestamp) collapse at buffer time
/// (<https://github.com/influxdata/influxdb_pro/issues/5352>): the latest
/// write wins, fields the overwrite does not provide are carried forward
/// (field-set union), and superseded versions reach neither the query path
/// nor the snapshot path.
#[tokio::test]
async fn test_overwrite_collapses_to_latest_write() {
    let writer = TestWriter::new().await;

    // Interleave versions across series in single and separate batches, and
    // include a partial overwrite (v2 of u1 omits `w`, which v1 provided).
    let rows_a = writer
        .write_to_rows(
            "tbl,t=u1 v=1i,w=10i 1000\n\
             tbl,t=u2 v=1i 1000\n\
             tbl,t=u1 v=2i 1000",
            0,
        )
        .await;
    let rows_b = writer
        .write_to_rows("tbl,t=u2 v=2i 1000\ntbl,t=u1 v=3i 1000", 1)
        .await;

    let table_def = writer.db_schema().table_definition("tbl").unwrap();
    let mut table_buffer = TableBuffer::new();
    table_buffer.buffer_chunk(0, &rows_a);
    table_buffer.buffer_chunk(0, &rows_b);

    let expected = [
        "+----+-----------------------------+---+----+",
        "| t  | time                        | v | w  |",
        "+----+-----------------------------+---+----+",
        "| u1 | 1970-01-01T00:00:00.000001Z | 3 | 10 |",
        "| u2 | 1970-01-01T00:00:00.000001Z | 2 |    |",
        "+----+-----------------------------+---+----+",
    ];

    // Query path: one row per series, latest values, unioned fields.
    let partitioned_batches = table_buffer
        .partitioned_record_batches(Arc::clone(&table_def), &ChunkFilter::default())
        .unwrap();
    let batches = partitioned_batches.get(&0).unwrap().combined();
    assert_batches_sorted_eq!(expected, &batches);

    // Snapshot (persist) path: the batch handed to persistence is already
    // collapsed, so a superseded version can never be written to parquet.
    // (Snapshot batches order columns by column id, so time comes last.)
    let snapshot_chunks = table_buffer.snapshot(Arc::clone(&table_def), i64::MAX);
    assert_eq!(1, snapshot_chunks.len());
    assert_batches_sorted_eq!(
        [
            "+----+---+----+-----------------------------+",
            "| t  | v | w  | time                        |",
            "+----+---+----+-----------------------------+",
            "| u1 | 3 | 10 | 1970-01-01T00:00:00.000001Z |",
            "| u2 | 2 |    | 1970-01-01T00:00:00.000001Z |",
            "+----+---+----+-----------------------------+",
        ],
        &[snapshot_chunks[0].record_batch.clone()]
    );
}

/// Overwrite collapsing spans var-column chunk splits
/// (<https://github.com/influxdata/influxdb_pro/issues/5352>): the overwrite
/// index is per chunk_time, so a row whose earlier version lives in a
/// previous split chunk still supersedes it, and fields the overwrite does
/// not provide are carried across the split — including columns the new
/// chunk has never seen.
#[tokio::test]
async fn test_overwrite_collapses_across_chunk_split() {
    let _guard = VarColMaxGuard::new(100);
    let writer = TestWriter::new().await;

    // 60 bytes of string payload per batch: batch 1 fits (60 <= 100), batch
    // 2 would exceed (60 + 60 > 100) and opens a second chunk.
    let s60 = "x".repeat(60);
    let rows1 = writer
        .write_to_rows(format!("tbl,t=u1 v=1i,w=10i,s=\"{s60}\" 1000"), 0)
        .await;
    // Overwrite of the same (tag set, time) lands in the split chunk and
    // omits `w`, which must be carried over from the superseded row in the
    // earlier chunk.
    let rows2 = writer
        .write_to_rows(format!("tbl,t=u1 v=2i,s=\"{s60}\" 1000"), 1)
        .await;

    let table_def = writer.db_schema().table_definition("tbl").unwrap();
    let mut table_buffer = TableBuffer::new();
    table_buffer.buffer_chunk(0, &rows1);
    table_buffer.buffer_chunk(0, &rows2);
    assert_eq!(
        table_buffer
            .chunk_time_to_chunks
            .get(&0)
            .unwrap()
            .chunks
            .len(),
        2,
        "payload should have split the chunk"
    );

    let partitioned_batches = table_buffer
        .partitioned_record_batches(Arc::clone(&table_def), &ChunkFilter::default())
        .unwrap();
    let batches = partitioned_batches.get(&0).unwrap().combined();
    let total_rows: usize = batches.iter().map(|rb| rb.num_rows()).sum();
    assert_eq!(total_rows, 1, "superseded version must be masked out");
    assert_batches_sorted_eq!(
        [
            "+--------------------------------------------------------------+----+-----------------------------+---+----+",
            "| s                                                            | t  | time                        | v | w  |",
            "+--------------------------------------------------------------+----+-----------------------------+---+----+",
            &format!("| {s60} | u1 | 1970-01-01T00:00:00.000001Z | 2 | 10 |"),
            "+--------------------------------------------------------------+----+-----------------------------+---+----+",
        ],
        &batches
    );
}

/// A split chunk whose every row was superseded by overwrites in a later
/// split has nothing left to persist: snapshot must skip it rather than emit
/// an empty parquet file, while the surviving chunk keeps its ordinal.
#[tokio::test]
async fn test_snapshot_skips_fully_superseded_split_chunk() {
    let _guard = VarColMaxGuard::new(100);
    let writer = TestWriter::new().await;

    let s60 = "x".repeat(60);
    let rows1 = writer
        .write_to_rows(format!("tbl,t=u1 v=1i,s=\"{s60}\" 1000"), 0)
        .await;
    let rows2 = writer
        .write_to_rows(format!("tbl,t=u1 v=2i,s=\"{s60}\" 1000"), 1)
        .await;

    let table_def = writer.db_schema().table_definition("tbl").unwrap();
    let mut table_buffer = TableBuffer::new();
    table_buffer.buffer_chunk(0, &rows1);
    table_buffer.buffer_chunk(0, &rows2);
    assert_eq!(
        table_buffer
            .chunk_time_to_chunks
            .get(&0)
            .unwrap()
            .chunks
            .len(),
        2,
        "payload should have split the chunk"
    );

    let snapshot_chunks = table_buffer.snapshot(Arc::clone(&table_def), i64::MAX);
    assert_eq!(
        snapshot_chunks.len(),
        1,
        "the fully superseded first split must not become a snapshot chunk"
    );
    assert_eq!(snapshot_chunks[0].chunk_ordinal, 1);
    assert_eq!(snapshot_chunks[0].record_batch.num_rows(), 1);
    // Persist batches carry columns in column-id order.
    assert_batches_eq!(
        [
            "+----+---+--------------------------------------------------------------+-----------------------------+",
            "| t  | v | s                                                            | time                        |",
            "+----+---+--------------------------------------------------------------+-----------------------------+",
            &format!("| u1 | 2 | {s60} | 1970-01-01T00:00:00.000001Z |"),
            "+----+---+--------------------------------------------------------------+-----------------------------+",
        ],
        &[snapshot_chunks[0].record_batch.clone()]
    );
}

/// An overwrite arriving while a snapshot's parquet files are being written
/// lands in a live chunk and is reported separately from the frozen
/// snapshotting batches, so the consumer can rank the frozen state below it
/// and dedup deterministically (live wins).
#[tokio::test]
async fn test_snapshotting_and_live_batches_are_separated() {
    let writer = TestWriter::new().await;
    let rows_v1 = writer.write_to_rows("tbl,t=u1 v=1i 1000", 0).await;
    let rows_v2 = writer.write_to_rows("tbl,t=u1 v=2i 1000", 1).await;

    let table_def = writer.db_schema().table_definition("tbl").unwrap();
    let mut table_buffer = TableBuffer::new();
    table_buffer.buffer_chunk(0, &rows_v1);
    let snapshot_chunks = table_buffer.snapshot(Arc::clone(&table_def), i64::MAX);
    assert_eq!(1, snapshot_chunks.len());

    // v2 arrives mid-persist: it goes to a fresh live chunk.
    table_buffer.buffer_chunk(0, &rows_v2);

    let partitioned_batches = table_buffer
        .partitioned_record_batches(Arc::clone(&table_def), &ChunkFilter::default())
        .unwrap();
    let buffered = partitioned_batches.get(&0).unwrap();
    assert_eq!(buffered.snapshotting.len(), 1);
    assert_eq!(buffered.live.len(), 1);
    assert_eq!(buffered.snapshotting[0].num_rows(), 1);
    assert_eq!(buffered.live[0].num_rows(), 1);
}

/// Each provenance set reports its own time range, so the chunk built from
/// the frozen snapshotting batches does not advertise times only present in
/// live batches (and vice versa).
#[tokio::test]
async fn test_buffered_batches_track_per_provenance_time_ranges() {
    let writer = TestWriter::new().await;
    let rows_v1 = writer.write_to_rows("tbl,t=u1 v=1i 1000", 0).await;
    let rows_v2 = writer.write_to_rows("tbl,t=u1 v=2i 2000", 1).await;

    let table_def = writer.db_schema().table_definition("tbl").unwrap();
    let mut table_buffer = TableBuffer::new();
    table_buffer.buffer_chunk(0, &rows_v1);
    let snapshot_chunks = table_buffer.snapshot(Arc::clone(&table_def), i64::MAX);
    assert_eq!(1, snapshot_chunks.len());
    table_buffer.buffer_chunk(0, &rows_v2);

    let partitioned_batches = table_buffer
        .partitioned_record_batches(Arc::clone(&table_def), &ChunkFilter::default())
        .unwrap();
    let buffered = partitioned_batches.get(&0).unwrap();
    assert_eq!(
        buffered.snapshotting_min_max,
        Some(TimestampMinMax::new(1000, 1000))
    );
    assert_eq!(
        buffered.live_min_max,
        Some(TimestampMinMax::new(2000, 2000))
    );
    assert_eq!(
        buffered.timestamp_min_max(),
        Some(TimestampMinMax::new(1000, 2000))
    );
}

// --- fused superseded-row masking -------------------------------------------

/// Overwrites across every column type: the produced batch carries exactly the
/// live rows, with carried-forward fields and nulls intact, for booleans,
/// integers, unsigned, floats, strings, tags and time alike.
#[tokio::test]
async fn test_record_batch_masks_superseded_rows_for_every_column_type() {
    let writer = TestWriter::new().await;
    let rows = writer
        .write_to_rows(
            "tbl,t=a b=true,i=1i,u=2u,f=1.5,s=\"one\" 1000\n\
             tbl,t=b i=3i 1000\n\
             tbl,t=a s=\"three\" 2000",
            0,
        )
        .await;
    // Overwrite the first two points: new values for some fields, the rest
    // carried forward from the superseded rows.
    let overwrites = writer
        .write_to_rows(
            "tbl,t=a i=9i,b=false 1000\n\
             tbl,t=b u=7u 1000",
            1,
        )
        .await;

    let table_def = writer.db_schema().table_definition("tbl").unwrap();
    let mut table_buffer = TableBuffer::new();
    table_buffer.buffer_chunk(0, &rows);
    table_buffer.buffer_chunk(0, &overwrites);
    let chunk = &table_buffer.chunk_time_to_chunks.get(&0).unwrap().chunks[0];
    assert_eq!(chunk.row_count, 5);
    assert_eq!(chunk.superseded_count, 2);

    let batch = chunk.record_batch(Arc::clone(&table_def)).unwrap();
    assert_batches_sorted_eq!(
        [
            "+-------+-----+---+-------+---+-----------------------------+---+",
            "| b     | f   | i | s     | t | time                        | u |",
            "+-------+-----+---+-------+---+-----------------------------+---+",
            "|       |     |   | three | a | 1970-01-01T00:00:00.000002Z |   |",
            "|       |     | 3 |       | b | 1970-01-01T00:00:00.000001Z | 7 |",
            "| false | 1.5 | 9 | one   | a | 1970-01-01T00:00:00.000001Z | 2 |",
            "+-------+-----+---+-------+---+-----------------------------+---+",
        ],
        &[batch]
    );
}

/// `materialize` takes a row range: rows outside it are not produced, and
/// superseded rows inside it are still masked out.
#[tokio::test]
async fn test_materialize_range_selects_rows_and_masks_superseded() {
    let writer = TestWriter::new().await;
    let rows = writer
        .write_to_rows(
            "tbl,t=a v=1i 1000\ntbl,t=b v=2i 1000\ntbl,t=c v=3i 1000\ntbl,t=d v=4i 1000",
            0,
        )
        .await;
    let overwrite = writer.write_to_rows("tbl,t=b v=20i 1000", 1).await;

    let table_def = writer.db_schema().table_definition("tbl").unwrap();
    let mut table_buffer = TableBuffer::new();
    table_buffer.buffer_chunk(0, &rows);
    table_buffer.buffer_chunk(0, &overwrite);
    let chunk = &table_buffer.chunk_time_to_chunks.get(&0).unwrap().chunks[0];

    // Rows 1..4 are b (superseded), c, d.
    let batch = chunk.materialize(Arc::clone(&table_def), 1..4).unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_batches_sorted_eq!(
        [
            "+---+-----------------------------+---+",
            "| t | time                        | v |",
            "+---+-----------------------------+---+",
            "| c | 1970-01-01T00:00:00.000001Z | 3 |",
            "| d | 1970-01-01T00:00:00.000001Z | 4 |",
            "+---+-----------------------------+---+",
        ],
        &[batch]
    );
    // The tail alone: the replacement row.
    let tail = chunk.materialize(Arc::clone(&table_def), 4..5).unwrap();
    assert_eq!(tail.num_rows(), 1);
    // The whole chunk equals `record_batch`.
    let whole = chunk.materialize(Arc::clone(&table_def), 0..5).unwrap();
    assert_eq!(whole.num_rows(), 4);
}

#[tokio::test]
async fn test_materialize_rejects_out_of_bounds_range() {
    let writer = TestWriter::new().await;
    let rows = writer.write_to_rows("tbl,t=a v=1i 1000", 0).await;
    let table_def = writer.db_schema().table_definition("tbl").unwrap();
    let mut table_buffer = TableBuffer::new();
    table_buffer.buffer_chunk(0, &rows);
    let chunk = &table_buffer.chunk_time_to_chunks.get(&0).unwrap().chunks[0];

    let err = chunk
        .materialize(Arc::clone(&table_def), 0..2)
        .expect_err("end past row_count must be rejected");
    assert!(
        matches!(
            err,
            Error::RowRangeOutOfBounds {
                start: 0,
                end: 2,
                row_count: 1
            }
        ),
        "{err:?}"
    );
    let err = chunk
        .materialize(Arc::clone(&table_def), std::ops::Range { start: 1, end: 0 })
        .expect_err("inverted range must be rejected");
    assert!(matches!(err, Error::RowRangeOutOfBounds { .. }), "{err:?}");
}

/// A range that covers only superseded rows yields an empty batch that still
/// carries the table schema.
#[tokio::test]
async fn test_materialize_fully_superseded_range_is_empty_with_schema() {
    let writer = TestWriter::new().await;
    let rows = writer.write_to_rows("tbl,t=a v=1i 1000", 0).await;
    let overwrite = writer.write_to_rows("tbl,t=a v=2i 1000", 1).await;
    let table_def = writer.db_schema().table_definition("tbl").unwrap();
    let mut table_buffer = TableBuffer::new();
    table_buffer.buffer_chunk(0, &rows);
    table_buffer.buffer_chunk(0, &overwrite);
    let chunk = &table_buffer.chunk_time_to_chunks.get(&0).unwrap().chunks[0];

    let batch = chunk.materialize(Arc::clone(&table_def), 0..1).unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.schema(), table_def.schema.as_arrow());
}

/// The watermark is the lowest row index superseded since it was last reset,
/// for a future materialization cache to tell whether a cached prefix is stale.
#[tokio::test]
async fn test_superseded_watermark_tracks_lowest_row_since_reset() {
    let writer = TestWriter::new().await;
    let rows = writer
        .write_to_rows(
            "tbl,t=a v=1i 1000\ntbl,t=b v=2i 1000\ntbl,t=c v=3i 1000\ntbl,t=d v=4i 1000",
            0,
        )
        .await;
    let ow_c = writer.write_to_rows("tbl,t=c v=30i 1000", 1).await;
    let ow_a = writer.write_to_rows("tbl,t=a v=10i 1000", 2).await;
    let ow_d = writer.write_to_rows("tbl,t=d v=40i 1000", 3).await;

    let mut table_buffer = TableBuffer::new();
    table_buffer.buffer_chunk(0, &rows);
    let chunk = |tb: &TableBuffer| {
        tb.chunk_time_to_chunks.get(&0).unwrap().chunks[0].superseded_watermark()
    };
    assert_eq!(chunk(&table_buffer), None, "nothing superseded yet");

    table_buffer.buffer_chunk(0, &ow_c);
    assert_eq!(chunk(&table_buffer), Some(2));
    table_buffer.buffer_chunk(0, &ow_a);
    assert_eq!(chunk(&table_buffer), Some(0), "lower row wins");

    table_buffer
        .chunk_time_to_chunks
        .get_mut(&0)
        .unwrap()
        .chunks[0]
        .reset_superseded_watermark();
    assert_eq!(chunk(&table_buffer), None);
    table_buffer.buffer_chunk(0, &ow_d);
    assert_eq!(
        chunk(&table_buffer),
        Some(3),
        "only supersedes since the reset count"
    );
}

/// The persist path and the query path agree on which rows survive: the
/// snapshot batch of a chunk with superseded rows holds the same rows as its
/// query batch.
#[tokio::test]
async fn test_snapshot_batch_matches_query_batch_with_superseded_rows() {
    let writer = TestWriter::new().await;
    let rows = writer
        .write_to_rows(
            "tbl,t=a v=1i,s=\"x\" 1000\ntbl,t=b v=2i 1000\ntbl,t=c v=3i,s=\"z\" 1000",
            0,
        )
        .await;
    let overwrites = writer
        .write_to_rows("tbl,t=a v=10i 1000\ntbl,t=c s=\"zz\" 1000", 1)
        .await;
    let table_def = writer.db_schema().table_definition("tbl").unwrap();
    let mut table_buffer = TableBuffer::new();
    table_buffer.buffer_chunk(0, &rows);
    table_buffer.buffer_chunk(0, &overwrites);

    let query_batch = table_buffer.chunk_time_to_chunks.get(&0).unwrap().chunks[0]
        .record_batch(Arc::clone(&table_def))
        .unwrap();
    let snapshot = table_buffer.snapshot(Arc::clone(&table_def), i64::MAX);
    assert_eq!(snapshot.len(), 1);
    let persist_batch = snapshot[0].record_batch.clone();

    // Same columns (persist is column-id ordered, query is schema ordered).
    let project = |b: &RecordBatch| {
        let idx: Vec<usize> = ["s", "t", "time", "v"]
            .iter()
            .map(|n| b.schema().index_of(n).unwrap())
            .collect();
        b.project(&idx).unwrap()
    };
    let expected = [
        "+----+---+-----------------------------+----+",
        "| s  | t | time                        | v  |",
        "+----+---+-----------------------------+----+",
        "| x  | a | 1970-01-01T00:00:00.000001Z | 10 |",
        "|    | b | 1970-01-01T00:00:00.000001Z | 2  |",
        "| zz | c | 1970-01-01T00:00:00.000001Z | 3  |",
        "+----+---+-----------------------------+----+",
    ];
    assert_batches_sorted_eq!(expected, &[project(&query_batch)]);
    assert_batches_sorted_eq!(expected, &[project(&persist_batch)]);
}
