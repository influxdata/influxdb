use std::{collections::BTreeSet, sync::Arc};

use arrow::record_batch::RecordBatch;
use parking_lot::Mutex;

use data_types::partition_metadata::{ColumnSummary, InfluxDbType, TableSummary};
use entry::TableBatch;
use mutable_batch::MutableBatch;
pub use mutable_batch::{Error, Result};
use schema::selection::Selection;
use schema::{InfluxColumnType, Schema};
use snapshot::ChunkSnapshot;

pub mod snapshot;

/// Metrics for a [`MBChunk`]
#[derive(Debug)]
#[allow(missing_copy_implementations)]
pub struct ChunkMetrics {
    // Placeholder
}

impl ChunkMetrics {
    /// Creates an instance of ChunkMetrics that isn't registered with a central
    /// metric registry. Observations made to instruments on this ChunkMetrics instance
    /// will therefore not be visible to other ChunkMetrics instances or metric instruments
    /// created on a metrics domain, and vice versa
    pub fn new_unregistered() -> Self {
        Self {}
    }

    /// Creates a new instance of [`ChunkMetrics`] with the provided metrics registry
    pub fn new(_metrics: &metric::Registry) -> Self {
        Self {}
    }
}

/// Represents a Chunk of data (a horizontal subset of a table) in
/// the mutable store.
#[derive(Debug)]
pub struct MBChunk {
    /// The name of this table
    table_name: Arc<str>,

    /// Metrics tracked by this chunk
    metrics: ChunkMetrics,

    /// Map of column id from the chunk dictionary to the column
    mutable_batch: MutableBatch,

    /// Cached chunk snapshot
    ///
    /// Note: This is a mutex to allow mutation within
    /// `Chunk::snapshot()` which only takes an immutable borrow
    snapshot: Mutex<Option<Arc<ChunkSnapshot>>>,
}

impl MBChunk {
    /// Create a new batch and write the contents of the [`TableBatch`] into it. Chunks
    /// shouldn't exist without some data.
    ///
    /// If `mask` is provided, only entries that are marked w/ `true` are written.
    pub fn new(
        metrics: ChunkMetrics,
        batch: TableBatch<'_>,
        mask: Option<&[bool]>,
    ) -> Result<Self> {
        let table_name = Arc::from(batch.name());

        let mut mutable_batch = MutableBatch::new();
        mutable_batch.write_table_batch(batch, mask)?;

        Ok(Self {
            table_name,
            mutable_batch,
            metrics,
            snapshot: Mutex::new(None),
        })
    }

    /// Write the contents of a [`TableBatch`] into this Chunk.
    ///
    /// If `mask` is provided, only entries that are marked w/ `true` are written.
    ///
    /// Panics if the batch specifies a different name for the table in this Chunk
    pub fn write_table_batch(
        &mut self,
        batch: TableBatch<'_>,
        mask: Option<&[bool]>,
    ) -> Result<()> {
        let table_name = batch.name();
        assert_eq!(
            table_name,
            self.table_name.as_ref(),
            "can only insert table batch for a single table to chunk"
        );

        self.mutable_batch.write_table_batch(batch, mask)?;

        // Invalidate chunk snapshot
        *self
            .snapshot
            .try_lock()
            .expect("concurrent readers/writers to MBChunk") = None;

        Ok(())
    }

    /// Returns a queryable snapshot of this chunk and an indicator if the snapshot was just cached.
    #[cfg(not(feature = "nocache"))]
    pub fn snapshot(&self) -> (Arc<ChunkSnapshot>, bool) {
        let mut guard = self.snapshot.lock();
        if let Some(snapshot) = &*guard {
            return (Arc::clone(snapshot), false);
        }

        let snapshot = Arc::new(ChunkSnapshot::new(self));
        *guard = Some(Arc::clone(&snapshot));
        (snapshot, true)
    }

    /// Returns a queryable snapshot of this chunk and an indicator if the snapshot was just cached.
    #[cfg(feature = "nocache")]
    pub fn snapshot(&self) -> (Arc<ChunkSnapshot>, bool) {
        (Arc::new(ChunkSnapshot::new(self)), false)
    }

    /// Return the name of the table in this chunk
    pub fn table_name(&self) -> &Arc<str> {
        &self.table_name
    }

    /// Returns the schema for a given selection
    ///
    /// If Selection::All the returned columns are sorted by name
    pub fn schema(&self, selection: Selection<'_>) -> Result<Schema> {
        self.mutable_batch.schema(selection)
    }

    /// Convert the table specified in this chunk into some number of
    /// record batches, appended to dst
    pub fn to_arrow(&self, selection: Selection<'_>) -> Result<RecordBatch> {
        self.mutable_batch.to_arrow(selection)
    }

    /// Returns a table summary for this chunk
    pub fn table_summary(&self) -> TableSummary {
        let mut columns: Vec<_> = self
            .mutable_batch
            .columns()
            .map(|(column_name, c)| ColumnSummary {
                name: column_name.clone(),
                stats: c.stats(),
                influxdb_type: Some(match c.influx_type() {
                    InfluxColumnType::IOx(_) => InfluxDbType::IOx,
                    InfluxColumnType::Tag => InfluxDbType::Tag,
                    InfluxColumnType::Field(_) => InfluxDbType::Field,
                    InfluxColumnType::Timestamp => InfluxDbType::Timestamp,
                }),
            })
            .collect();

        columns.sort_by(|a, b| a.name.cmp(&b.name));

        TableSummary {
            name: self.table_name.to_string(),
            columns,
        }
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    ///
    /// This includes the size of `self`.
    ///
    /// Note: This does not include the size of any cached ChunkSnapshot
    pub fn size(&self) -> usize {
        let size_self = std::mem::size_of::<Self>();

        let size_columns = self
            .mutable_batch
            .columns()
            .map(|(k, v)| k.capacity() + v.size())
            .sum::<usize>();

        let size_table_name = self.table_name.len();

        let snapshot_size = {
            let guard = self.snapshot.lock();
            guard.as_ref().map(|snapshot| snapshot.size()).unwrap_or(0)
        };

        size_self + size_columns + size_table_name + snapshot_size
    }

    /// Returns an iterator over (column_name, estimated_size) for all
    /// columns in this chunk.
    pub fn column_sizes(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        self.mutable_batch
            .columns()
            .map(|(column_name, c)| (column_name.as_str(), c.size()))
    }

    /// Return the number of rows in this chunk
    pub fn rows(&self) -> usize {
        self.mutable_batch.rows()
    }
}

/// Test helper utilities
pub mod test_helpers {
    use entry::test_helpers::lp_to_entry;

    use super::*;

    /// A helper that will write line protocol string to the passed in Chunk.
    /// All data will be under a single partition with a clock value and
    /// server id of 1.
    pub fn write_lp_to_chunk(lp: &str, chunk: &mut MBChunk) -> Result<()> {
        let entry = lp_to_entry(lp);

        for w in entry.partition_writes().unwrap() {
            let table_batches = w.table_batches();
            // ensure they are all to the same table
            let table_names: BTreeSet<String> =
                table_batches.iter().map(|b| b.name().to_string()).collect();

            assert!(
                table_names.len() <= 1,
                "Can only write 0 or one tables to chunk. Found {:?}",
                table_names
            );

            for batch in table_batches {
                chunk.write_table_batch(batch, None)?;
            }
        }

        Ok(())
    }

    /// Create a new [`MBChunk`] with the given write
    pub fn write_lp_to_new_chunk(lp: &str) -> Result<MBChunk> {
        let entry = lp_to_entry(lp);
        let mut chunk: Option<MBChunk> = None;

        for w in entry.partition_writes().unwrap() {
            let table_batches = w.table_batches();
            // ensure they are all to the same table
            let table_names: BTreeSet<String> =
                table_batches.iter().map(|b| b.name().to_string()).collect();

            assert!(
                table_names.len() <= 1,
                "Can only write 0 or one tables to chunk. Found {:?}",
                table_names
            );

            for batch in table_batches {
                match chunk {
                    Some(ref mut c) => c.write_table_batch(batch, None)?,
                    None => {
                        chunk = Some(MBChunk::new(ChunkMetrics::new_unregistered(), batch, None)?);
                    }
                }
            }
        }

        Ok(chunk.expect("Must write at least one table batch to create a chunk"))
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::TryFrom, num::NonZeroU64, vec};

    use arrow::datatypes::DataType as ArrowDataType;

    use arrow_util::assert_batches_eq;
    use data_types::partition_metadata::{
        ColumnSummary, InfluxDbType, StatValues, Statistics, TableSummary,
    };
    use entry::test_helpers::lp_to_entry;
    use schema::builder::SchemaBuilder;

    use super::{
        test_helpers::{write_lp_to_chunk, write_lp_to_new_chunk},
        *,
    };

    #[test]
    fn writes_table_batches() {
        let lp = vec!["cpu,host=a val=23 1", "cpu,host=b val=2 1"].join("\n");
        let chunk = write_lp_to_new_chunk(&lp).unwrap();

        let expected = vec![
            "+------+--------------------------------+-----+",
            "| host | time                           | val |",
            "+------+--------------------------------+-----+",
            "| a    | 1970-01-01T00:00:00.000000001Z | 23  |",
            "| b    | 1970-01-01T00:00:00.000000001Z | 2   |",
            "+------+--------------------------------+-----+",
        ];

        assert_batches_eq!(expected, &chunk_to_batches(&chunk));
    }

    #[test]
    fn writes_table_3_batches() {
        let lp = vec!["cpu,host=a val=23 1", "cpu,host=b val=2 1"].join("\n");
        let mut chunk = write_lp_to_new_chunk(&lp).unwrap();

        let lp = vec!["cpu,host=c val=11 1"].join("\n");
        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        let lp = vec!["cpu,host=a val=14 2"].join("\n");
        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        let expected = vec![
            "+------+--------------------------------+-----+",
            "| host | time                           | val |",
            "+------+--------------------------------+-----+",
            "| a    | 1970-01-01T00:00:00.000000001Z | 23  |",
            "| b    | 1970-01-01T00:00:00.000000001Z | 2   |",
            "| c    | 1970-01-01T00:00:00.000000001Z | 11  |",
            "| a    | 1970-01-01T00:00:00.000000002Z | 14  |",
            "+------+--------------------------------+-----+",
        ];

        assert_batches_eq!(expected, &chunk_to_batches(&chunk));
    }

    #[test]
    fn test_summary() {
        let lp = r#"
            cpu,host=a val=23 1
            cpu,host=b,env=prod val=2 1
            cpu,host=c,env=stage val=11 1
            cpu,host=a,env=prod val=14 2
        "#;
        let chunk = write_lp_to_new_chunk(lp).unwrap();

        let summary = chunk.table_summary();
        assert_eq!(summary.name, "cpu");
        let expected_column_summaries = vec![
            ColumnSummary {
                name: "env".to_string(),
                influxdb_type: Some(InfluxDbType::Tag),
                stats: Statistics::String(StatValues {
                    min: Some("prod".to_string()),
                    max: Some("stage".to_string()),
                    total_count: 4,
                    null_count: 1,
                    distinct_count: Some(NonZeroU64::new(3).unwrap()),
                }),
            },
            ColumnSummary {
                name: "host".to_string(),
                influxdb_type: Some(InfluxDbType::Tag),
                stats: Statistics::String(StatValues {
                    min: Some("a".to_string()),
                    max: Some("c".to_string()),
                    total_count: 4,
                    null_count: 0,
                    distinct_count: Some(NonZeroU64::new(3).unwrap()),
                }),
            },
            ColumnSummary {
                name: "time".to_string(),
                influxdb_type: Some(InfluxDbType::Timestamp),
                stats: Statistics::I64(StatValues {
                    min: Some(1),
                    max: Some(2),
                    total_count: 4,
                    null_count: 0,
                    distinct_count: None,
                }),
            },
            ColumnSummary {
                name: "val".to_string(),
                influxdb_type: Some(InfluxDbType::Field),
                stats: Statistics::F64(StatValues {
                    min: Some(2.),
                    max: Some(23.),
                    total_count: 4,
                    null_count: 0,
                    distinct_count: None,
                }),
            },
        ];
        assert_eq!(
            summary.columns, expected_column_summaries,
            "\n\nactual:\n{:#?}\n\nexpected:\n{:#?}\n\n",
            summary.columns, expected_column_summaries
        );
    }

    #[test]
    fn test_summary_null_columns() {
        // Tests that inserting multiple writes, each with a subset of the columns,
        // results in the correct counts in all columns.

        // first write has no tags
        let lp1 = "cpu field=23 1";

        // second write has host tag
        let lp2 = "cpu,host=a field=34 2";

        // third write has host tag and a new field
        let lp3 = "cpu,host=a field=45,new_field=100 3";

        // final write has only new field
        let lp4 = "cpu new_field=200 4";

        let mut chunk = write_lp_to_new_chunk(lp1).unwrap();
        write_lp_to_chunk(lp2, &mut chunk).unwrap();
        write_lp_to_chunk(lp3, &mut chunk).unwrap();
        write_lp_to_chunk(lp4, &mut chunk).unwrap();

        let summary = chunk.table_summary();

        let expected_column_summaries = vec![
            ColumnSummary {
                name: "field".into(),
                influxdb_type: Some(InfluxDbType::Field),
                stats: Statistics::F64(StatValues {
                    min: Some(23.0),
                    max: Some(45.0),
                    total_count: 4,
                    null_count: 1,
                    distinct_count: None,
                }),
            },
            ColumnSummary {
                name: "host".into(),
                influxdb_type: Some(InfluxDbType::Tag),
                stats: Statistics::String(StatValues {
                    min: Some("a".into()),
                    max: Some("a".into()),
                    total_count: 4,
                    null_count: 2,
                    distinct_count: NonZeroU64::new(2),
                }),
            },
            ColumnSummary {
                name: "new_field".into(),
                influxdb_type: Some(InfluxDbType::Field),
                stats: Statistics::F64(StatValues {
                    min: Some(100.0),
                    max: Some(200.0),
                    total_count: 4,
                    null_count: 2,
                    distinct_count: None,
                }),
            },
            ColumnSummary {
                name: "time".into(),
                influxdb_type: Some(InfluxDbType::Timestamp),
                stats: Statistics::I64(StatValues {
                    min: Some(1),
                    max: Some(4),
                    total_count: 4,
                    null_count: 0,
                    distinct_count: None,
                }),
            },
        ];
        assert_eq!(
            summary.columns, expected_column_summaries,
            "\n\nactual:\n{:#?}\n\nexpected:\n{:#?}\n\n",
            summary.columns, expected_column_summaries
        );

        // total counts for all columns should be the same
        assert!(summary.columns.iter().all(|s| s.stats.total_count() == 4));
    }

    // test statistics generation for each type as at time of writing the codepaths were slightly different
    macro_rules! assert_summary_eq {
        ($EXPECTED:expr, $CHUNK:expr, $COL_NAME:expr) => {
            let table_summary: TableSummary = $CHUNK.table_summary().into();
            let col_summary = table_summary.column($COL_NAME).expect("cound find column");

            assert_eq!(
                col_summary, &$EXPECTED,
                "\n\nactual:\n{:#?}\n\nexpected:\n{:#?}\n\n",
                col_summary, $EXPECTED
            );
        };
    }

    #[test]
    fn test_tag_stats() {
        let lp = r#"
            cpu,host=a  v=1 10
            cpu,host=b  v=1 20
            cpu,host=a  v=1 30
            cpu,host2=a v=1 40
            cpu,host=c  v=1 50
        "#;
        let chunk = write_lp_to_new_chunk(lp).unwrap();
        let expected = ColumnSummary {
            name: "host".into(),
            influxdb_type: Some(InfluxDbType::Tag),
            stats: Statistics::String(StatValues {
                min: Some("a".into()),
                max: Some("c".into()),
                total_count: 5,
                null_count: 1,
                // 4 distinct values, including null
                distinct_count: Some(NonZeroU64::new(4).unwrap()),
            }),
        };
        assert_summary_eq!(expected, chunk, "host");
    }

    #[test]
    fn test_bool_field_stats() {
        let lp = r#"
            cpu,host=a val=true 10
            cpu,host=b val=false 20
            cpu,host=c other_val=2 30
            cpu,host=a val=false 40
            cpu,host=c other_val=2 50
        "#;
        let chunk = write_lp_to_new_chunk(lp).unwrap();
        let expected = ColumnSummary {
            name: "val".into(),
            influxdb_type: Some(InfluxDbType::Field),
            stats: Statistics::Bool(StatValues {
                min: Some(false),
                max: Some(true),
                total_count: 5,
                null_count: 2,
                distinct_count: None,
            }),
        };
        assert_summary_eq!(expected, chunk, "val");
    }

    #[test]
    fn test_u64_field_stats() {
        let lp = r#"
            cpu,host=a val=5u 10
            cpu,host=b val=2u 20
            cpu,host=c other_val=2 30
            cpu,host=a val=1u 40
            cpu,host=c other_val=2 50
        "#;
        let chunk = write_lp_to_new_chunk(lp).unwrap();
        let expected = ColumnSummary {
            name: "val".into(),
            influxdb_type: Some(InfluxDbType::Field),
            stats: Statistics::U64(StatValues {
                min: Some(1),
                max: Some(5),
                total_count: 5,
                null_count: 2,
                distinct_count: None,
            }),
        };
        assert_summary_eq!(expected, chunk, "val");
    }

    #[test]
    fn test_f64_field_stats() {
        let lp = r#"
            cpu,host=a val=5.0 10
            cpu,host=b val=2.0 20
            cpu,host=c other_val=2 30
            cpu,host=a val=1.0 40
            cpu,host=c other_val=2.0 50
        "#;
        let chunk = write_lp_to_new_chunk(lp).unwrap();
        let expected = ColumnSummary {
            name: "val".into(),
            influxdb_type: Some(InfluxDbType::Field),
            stats: Statistics::F64(StatValues {
                min: Some(1.0),
                max: Some(5.0),
                total_count: 5,
                null_count: 2,
                distinct_count: None,
            }),
        };
        assert_summary_eq!(expected, chunk, "val");
    }

    #[test]
    fn test_i64_field_stats() {
        let lp = r#"
            cpu,host=a val=5i 10
            cpu,host=b val=2i 20
            cpu,host=c other_val=2 30
            cpu,host=a val=1i 40
            cpu,host=c other_val=2.0 50
        "#;
        let chunk = write_lp_to_new_chunk(lp).unwrap();
        let expected = ColumnSummary {
            name: "val".into(),
            influxdb_type: Some(InfluxDbType::Field),
            stats: Statistics::I64(StatValues {
                min: Some(1),
                max: Some(5),
                total_count: 5,
                null_count: 2,
                distinct_count: None,
            }),
        };
        assert_summary_eq!(expected, chunk, "val");
    }

    #[test]
    fn test_string_field_stats() {
        let lp = r#"
            cpu,host=a val="v1" 10
            cpu,host=b val="v2" 20
            cpu,host=c other_val=2 30
            cpu,host=a val="v3" 40
            cpu,host=c other_val=2.0 50
        "#;
        let chunk = write_lp_to_new_chunk(lp).unwrap();
        let expected = ColumnSummary {
            name: "val".into(),
            influxdb_type: Some(InfluxDbType::Field),
            stats: Statistics::String(StatValues {
                min: Some("v1".into()),
                max: Some("v3".into()),
                total_count: 5,
                null_count: 2,
                distinct_count: None,
            }),
        };
        assert_summary_eq!(expected, chunk, "val");
    }

    #[test]
    fn test_time_stats() {
        let lp = r#"
            cpu,host=a val=1 10
            cpu,host=b val=2 20
            cpu,host=c other_val=3 11
            cpu,host=a val=4 2
            cpu,host=c val=25 12
        "#;
        let chunk = write_lp_to_new_chunk(lp).unwrap();
        let expected = ColumnSummary {
            name: "time".into(),
            influxdb_type: Some(InfluxDbType::Timestamp),
            stats: Statistics::I64(StatValues {
                min: Some(2),
                max: Some(20),
                total_count: 5,
                null_count: 0,
                distinct_count: None,
            }),
        };
        assert_summary_eq!(expected, chunk, "time");
    }

    #[test]
    fn test_tag_stats_multi_write() {
        // write in two chunks, second write creates new host2 tag so existing stats need to be backfilled
        let lp1 = r#"
            cpu,host=a  v=1 10
            cpu,host=b  v=1 20
            cpu,host=a  v=1 30
        "#;
        let mut chunk = write_lp_to_new_chunk(lp1).unwrap();

        let lp2 = r#"
            cpu,host2=z v=1 40
            cpu,host=c  v=1 5
        "#;
        write_lp_to_chunk(lp2, &mut chunk).unwrap();

        let expected = ColumnSummary {
            name: "host".into(),
            influxdb_type: Some(InfluxDbType::Tag),
            stats: Statistics::String(StatValues {
                min: Some("a".into()),
                max: Some("c".into()),
                total_count: 5,
                null_count: 1,
                // 4 distinct values, including null
                distinct_count: Some(NonZeroU64::new(4).unwrap()),
            }),
        };
        assert_summary_eq!(expected, chunk, "host");

        let expected = ColumnSummary {
            name: "host2".into(),
            influxdb_type: Some(InfluxDbType::Tag),
            stats: Statistics::String(StatValues {
                min: Some("z".into()),
                max: Some("z".into()),
                total_count: 5, // same total_count as above
                null_count: 4,
                // 2 distinct values ("z" and null)
                distinct_count: Some(NonZeroU64::new(2).unwrap()),
            }),
        };
        assert_summary_eq!(expected, chunk, "host2");
    }

    #[test]
    #[cfg(not(feature = "nocache"))]
    fn test_snapshot() {
        let lp = vec!["cpu,host=a val=23 1", "cpu,host=b val=2 1"].join("\n");
        let mut chunk = write_lp_to_new_chunk(&lp).unwrap();

        let (s1, c1) = chunk.snapshot();
        assert!(c1);
        let (s2, c2) = chunk.snapshot();
        assert!(!c2);

        write_lp_to_chunk(&lp, &mut chunk).unwrap();
        let (s3, c3) = chunk.snapshot();
        assert!(c3);
        let (s4, c4) = chunk.snapshot();
        assert!(!c4);

        assert_eq!(Arc::as_ptr(&s1), Arc::as_ptr(&s2));
        assert_ne!(Arc::as_ptr(&s1), Arc::as_ptr(&s3));
        assert_eq!(Arc::as_ptr(&s3), Arc::as_ptr(&s4));
    }

    fn chunk_to_batches(chunk: &MBChunk) -> Vec<RecordBatch> {
        vec![chunk.to_arrow(Selection::All).unwrap()]
    }

    #[test]
    fn table_size() {
        let lp = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ]
        .join("\n");
        let mut chunk = write_lp_to_new_chunk(&lp).unwrap();
        let s1 = chunk.size();

        write_lp_to_chunk(&lp, &mut chunk).unwrap();
        let s2 = chunk.size();

        write_lp_to_chunk(&lp, &mut chunk).unwrap();
        let s3 = chunk.size();

        // Should increase or stay identical (if array capacities are sufficient) each time
        assert!(s2 >= s1);
        assert!(s3 >= s2);

        // also assume that we wrote enough data to bump the capacity at least once
        assert!(s3 > s1);
    }

    #[test]
    fn test_to_arrow_schema_all() {
        let lp = "h2o,state=MA,city=Boston float_field=70.4,int_field=8i,uint_field=42u,bool_field=t,string_field=\"foo\" 100";
        let chunk = write_lp_to_new_chunk(lp).unwrap();

        let selection = Selection::All;
        let actual_schema = chunk.schema(selection).unwrap();
        let expected_schema = SchemaBuilder::new()
            .field("bool_field", ArrowDataType::Boolean)
            .tag("city")
            .field("float_field", ArrowDataType::Float64)
            .field("int_field", ArrowDataType::Int64)
            .tag("state")
            .field("string_field", ArrowDataType::Utf8)
            .timestamp()
            .field("uint_field", ArrowDataType::UInt64)
            .build()
            .unwrap();

        assert_eq!(
            expected_schema, actual_schema,
            "Expected:\n{:#?}\nActual:\n{:#?}\n",
            expected_schema, actual_schema
        );
    }

    #[test]
    fn test_to_arrow_schema_subset() {
        let lp = "h2o,state=MA,city=Boston float_field=70.4 100";
        let chunk = write_lp_to_new_chunk(lp).unwrap();

        let selection = Selection::Some(&["float_field"]);
        let actual_schema = chunk.schema(selection).unwrap();
        let expected_schema = SchemaBuilder::new()
            .field("float_field", ArrowDataType::Float64)
            .build()
            .unwrap();

        assert_eq!(
            expected_schema, actual_schema,
            "Expected:\n{:#?}\nActual:\n{:#?}\n",
            expected_schema, actual_schema
        );
    }

    #[test]
    fn test_mask() {
        let mut entries = vec![];
        let mut masks = vec![];

        let lp = [
            "table,tag=a float_field=1.1,int_field=11i,uint_field=111u,bool_field=t,string_field=\"axx\" 100",
            "table,tag=b float_field=2.2,int_field=22i,uint_field=222u,bool_field=f,string_field=\"bxx\" 200",
            "table,tag=c float_field=3.3,int_field=33i,uint_field=333u,bool_field=f,string_field=\"cxx\" 300",
            "table,tag=d float_field=4.4,int_field=44i,uint_field=444u,bool_field=t,string_field=\"dxx\" 400",
        ].join("\n");
        masks.push(vec![false, true, true, false]);
        entries.push(lp_to_entry(&lp));

        let lp = [
            "table,tag=e float_field=5.5,int_field=55i,uint_field=555u,bool_field=f,string_field=\"exx\" 500",
            "table,tag=f float_field=6.6,int_field=66i,uint_field=666u,bool_field=t,string_field=\"fxx\" 600",
            "table foo=1 700",
            "table foo=2 800",
            "table foo=3 900",
        ].join("\n");
        masks.push(vec![true, false, true, false, false]);
        entries.push(lp_to_entry(&lp));

        let mut chunk: Option<MBChunk> = None;
        for (entry, mask) in entries.into_iter().zip(masks.into_iter()) {
            for w in entry.partition_writes().unwrap() {
                for batch in w.table_batches() {
                    match chunk {
                        Some(ref mut c) => c.write_table_batch(batch, Some(mask.as_ref())).unwrap(),
                        None => {
                            chunk = Some(
                                MBChunk::new(
                                    ChunkMetrics::new_unregistered(),
                                    batch,
                                    Some(mask.as_ref()),
                                )
                                .unwrap(),
                            );
                        }
                    }
                }
            }
        }
        let chunk = chunk.unwrap();

        let expected = ColumnSummary {
            name: "float_field".into(),
            influxdb_type: Some(InfluxDbType::Field),
            stats: Statistics::F64(StatValues {
                min: Some(2.2),
                max: Some(5.5),
                total_count: 4,
                null_count: 1,
                distinct_count: None,
            }),
        };
        assert_summary_eq!(expected, chunk, "float_field");

        let expected = ColumnSummary {
            name: "int_field".into(),
            influxdb_type: Some(InfluxDbType::Field),
            stats: Statistics::I64(StatValues {
                min: Some(22),
                max: Some(55),
                total_count: 4,
                null_count: 1,
                distinct_count: None,
            }),
        };
        assert_summary_eq!(expected, chunk, "int_field");

        let expected = ColumnSummary {
            name: "uint_field".into(),
            influxdb_type: Some(InfluxDbType::Field),
            stats: Statistics::U64(StatValues {
                min: Some(222),
                max: Some(555),
                total_count: 4,
                null_count: 1,
                distinct_count: None,
            }),
        };
        assert_summary_eq!(expected, chunk, "uint_field");

        let expected = ColumnSummary {
            name: "bool_field".into(),
            influxdb_type: Some(InfluxDbType::Field),
            stats: Statistics::Bool(StatValues {
                min: Some(false),
                max: Some(false),
                total_count: 4,
                null_count: 1,
                distinct_count: None,
            }),
        };
        assert_summary_eq!(expected, chunk, "bool_field");

        let expected = ColumnSummary {
            name: "string_field".into(),
            influxdb_type: Some(InfluxDbType::Field),
            stats: Statistics::String(StatValues {
                min: Some("bxx".into()),
                max: Some("exx".into()),
                total_count: 4,
                null_count: 1,
                distinct_count: None,
            }),
        };
        assert_summary_eq!(expected, chunk, "string_field");

        let expected = ColumnSummary {
            name: "time".into(),
            influxdb_type: Some(InfluxDbType::Timestamp),
            stats: Statistics::I64(StatValues {
                min: Some(200),
                max: Some(700),
                total_count: 4,
                null_count: 0,
                distinct_count: None,
            }),
        };
        assert_summary_eq!(expected, chunk, "time");

        let expected = ColumnSummary {
            name: "tag".into(),
            influxdb_type: Some(InfluxDbType::Tag),
            stats: Statistics::String(StatValues {
                min: Some("b".into()),
                max: Some("e".into()),
                total_count: 4,
                null_count: 1,
                distinct_count: Some(NonZeroU64::try_from(4).unwrap()),
            }),
        };
        assert_summary_eq!(expected, chunk, "tag");
    }

    #[test]
    fn test_mask_wrong_length() {
        let lp = [
            "table,tag=a float_field=1.1,int_field=11i,uint_field=111u,bool_field=t,string_field=\"axx\" 100",
            "table,tag=b float_field=2.2,int_field=22i,uint_field=222u,bool_field=f,string_field=\"bxx\" 200",
        ].join("\n");
        let entry = lp_to_entry(&lp);
        let partition_write = entry.partition_writes().unwrap().pop().unwrap();
        let mask = vec![false, true, true, false];

        let batch = partition_write.table_batches().pop().unwrap();
        let err =
            MBChunk::new(ChunkMetrics::new_unregistered(), batch, Some(mask.as_ref())).unwrap_err();
        assert!(matches!(err, Error::IncorrectMaskLength { .. }));

        let batch = partition_write.table_batches().pop().unwrap();
        let mut chunk = MBChunk::new(ChunkMetrics::new_unregistered(), batch, None).unwrap();

        let batch = partition_write.table_batches().pop().unwrap();
        let err = chunk
            .write_table_batch(batch, Some(mask.as_ref()))
            .unwrap_err();
        assert!(matches!(err, Error::IncorrectMaskLength { .. }));
    }
}
