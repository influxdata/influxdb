use crate::{Precision, write_buffer::validator::WriteValidator};

use super::*;
use arrow_util::assert_batches_sorted_eq;
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
        let catalog = Arc::new(
            Catalog::new("test-node", obj_store, time_provider, Default::default())
                .await
                .expect("should initialize catalog"),
        );
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
        let (ts_min_max, batches) = partitioned_batches.get(&offset).unwrap();
        assert_eq!(TimestampMinMax::new(offset + 1, offset + 2), *ts_min_max);
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
    assert_eq!(size, 1251);
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
            .flat_map(|(_, batches)| batches)
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
    assert_eq!(table_buffer.chunk_time_to_chunks.get(&0).unwrap().len(), 1);

    // Buffer second batch - predictive check: 50 + 50 = 100 > 99, triggers new chunk
    // After: chunk2.string_bytes_per_column[val] = 50
    table_buffer.buffer_chunk(0, &rows2);
    assert_eq!(table_buffer.chunk_time_to_chunks.get(&0).unwrap().len(), 2);

    // Buffer third batch - predictive check: 50 + 50 = 100 > 99, triggers new chunk
    // After: chunk3.string_bytes_per_column[val] = 50
    table_buffer.buffer_chunk(0, &rows3);
    assert_eq!(table_buffer.chunk_time_to_chunks.get(&0).unwrap().len(), 3);

    // Verify all data can be retrieved
    let batches = table_buffer
        .partitioned_record_batches(Arc::clone(&table_def), &ChunkFilter::default())
        .unwrap();

    assert_eq!(batches.len(), 1); // 1 time partition
    let (ts_min_max, record_batches) = batches.get(&0).unwrap();
    assert_eq!(ts_min_max.min, 1);
    assert_eq!(ts_min_max.max, 3);
    assert_eq!(record_batches.len(), 3); // 3 chunks -> 3 record batches

    // Check total row count across all batches
    let total_rows: usize = record_batches.iter().map(|rb| rb.num_rows()).sum();
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
    assert_eq!(table_buffer.chunk_time_to_chunks.get(&0).unwrap().len(), 1);

    table_buffer.buffer_chunk(0, &rows2);
    assert_eq!(table_buffer.chunk_time_to_chunks.get(&0).unwrap().len(), 1);

    table_buffer.buffer_chunk(0, &rows3);
    assert_eq!(table_buffer.chunk_time_to_chunks.get(&0).unwrap().len(), 1);

    // Fourth write should trigger a new chunk
    table_buffer.buffer_chunk(0, &rows4);
    assert_eq!(table_buffer.chunk_time_to_chunks.get(&0).unwrap().len(), 2);

    // Verify chunk sizes: first chunk should have 3 rows, second chunk should have 1 row
    let chunks = table_buffer.chunk_time_to_chunks.get(&0).unwrap();
    assert_eq!(chunks[0].row_count, 3);
    assert_eq!(chunks[1].row_count, 1);

    // Verify all data can be retrieved
    let batches = table_buffer
        .partitioned_record_batches(Arc::clone(&table_def), &ChunkFilter::default())
        .unwrap();

    let (ts_min_max, record_batches) = batches.get(&0).unwrap();
    assert_eq!(ts_min_max.min, 1);
    assert_eq!(ts_min_max.max, 4);

    let total_rows: usize = record_batches.iter().map(|rb| rb.num_rows()).sum();
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
    assert_eq!(table_buffer.chunk_time_to_chunks.get(&0).unwrap().len(), 1);
    table_buffer.buffer_chunk(0, &rows2);
    assert_eq!(table_buffer.chunk_time_to_chunks.get(&0).unwrap().len(), 2);
    table_buffer.buffer_chunk(0, &rows3);
    assert_eq!(table_buffer.chunk_time_to_chunks.get(&0).unwrap().len(), 3);

    // Verify all data can be retrieved
    let batches = table_buffer
        .partitioned_record_batches(Arc::clone(&table_def), &ChunkFilter::default())
        .unwrap();

    assert_eq!(batches.len(), 1);
    let (ts_min_max, record_batches) = batches.get(&0).unwrap();
    assert_eq!(ts_min_max.min, 1);
    assert_eq!(ts_min_max.max, 3);
    assert_eq!(record_batches.len(), 3);

    let total_rows: usize = record_batches.iter().map(|rb| rb.num_rows()).sum();
    assert_eq!(total_rows, 3);
}
