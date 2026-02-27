use crate::{Precision, write_buffer::validator::WriteValidator};

use super::*;
use arrow_util::assert_batches_sorted_eq;
use data_types::NamespaceName;
use datafusion::prelude::{Expr, col, lit_timestamp_nano};
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
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
        let db = NamespaceName::try_from(Self::DB_NAME).unwrap();
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
