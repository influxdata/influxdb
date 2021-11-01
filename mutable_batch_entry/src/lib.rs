//! Code to convert entry to [`MutableBatch`]

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use entry::{Column as EntryColumn, Entry, SequencedEntry, TableBatch};
use hashbrown::{HashMap, HashSet};
use mutable_batch::{
    payload::{DbWrite, WriteMeta},
    writer::Writer,
    MutableBatch,
};
use schema::{InfluxColumnType, InfluxFieldType, TIME_COLUMN_NAME};
use snafu::{ensure, ResultExt, Snafu};

/// Error type for entry conversion
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display(
        "Invalid null mask, expected to be {} bytes but was {}",
        expected_bytes,
        actual_bytes
    ))]
    InvalidNullMask {
        expected_bytes: usize,
        actual_bytes: usize,
    },

    #[snafu(display("duplicate column name: {}", column))]
    DuplicateColumnName { column: String },

    #[snafu(display("table batch must contain time column"))]
    MissingTime,

    #[snafu(display("time column must not contain nulls"))]
    NullTime,

    #[snafu(display("entry contained empty table batch"))]
    EmptyTableBatch,

    #[snafu(display("error writing column {}: {}", column, source))]
    Write {
        source: mutable_batch::writer::Error,
        column: String,
    },
}

/// Result type for entry conversion
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Converts a [`SequencedEntry`] to a [`DbWrite`]
pub fn sequenced_entry_to_write(entry: &SequencedEntry) -> Result<DbWrite> {
    let meta = WriteMeta::new(
        entry.sequence().cloned(),
        entry.producer_wallclock_timestamp(),
        entry.span_context().cloned(),
    );

    let tables = entry_to_batches(entry.entry())?;

    Ok(DbWrite::new(tables, meta))
}

/// Converts an [`Entry`] to a collection of [`MutableBatch`] keyed by table name
///
/// Note: this flattens partitioning
pub fn entry_to_batches(entry: &Entry) -> Result<HashMap<String, MutableBatch>> {
    let mut batches = HashMap::new();
    for partition_write in entry.partition_writes_iter() {
        for table_batch in partition_write.table_batches_iter() {
            let table_name = table_batch.name();

            let (_, batch) = batches
                .raw_entry_mut()
                .from_key(table_name)
                .or_insert_with(|| (table_name.to_string(), MutableBatch::new()));
            write_table_batch(batch, table_batch)?;
        }
    }
    Ok(batches)
}

/// Writes the provided [`TableBatch`] to a [`MutableBatch`] on error any changes made
/// to `batch` are reverted
fn write_table_batch(batch: &mut MutableBatch, table_batch: TableBatch<'_>) -> Result<()> {
    let row_count = table_batch.row_count();
    ensure!(row_count != 0, EmptyTableBatch);

    let columns = table_batch.columns();
    let mut column_names = HashSet::with_capacity(columns.len());
    for column in &columns {
        ensure!(
            column_names.insert(column.name()),
            DuplicateColumnName {
                column: column.name()
            }
        );
    }

    // Batch must contain a time column
    ensure!(column_names.contains(TIME_COLUMN_NAME), MissingTime);

    let mut writer = Writer::new(batch, row_count);
    for column in &columns {
        let valid_mask = construct_valid_mask(column)?;
        let valid_mask = valid_mask.as_deref();

        let inner = column.inner();

        match column.influx_type() {
            InfluxColumnType::Field(InfluxFieldType::Float) => {
                let values = inner
                    .values_as_f64values()
                    .unwrap()
                    .values()
                    .into_iter()
                    .flatten();
                writer.write_f64(column.name(), valid_mask, values)
            }
            InfluxColumnType::Field(InfluxFieldType::Integer) => {
                let values = inner
                    .values_as_i64values()
                    .unwrap()
                    .values()
                    .into_iter()
                    .flatten();
                writer.write_i64(column.name(), valid_mask, values)
            }
            InfluxColumnType::Field(InfluxFieldType::UInteger) => {
                let values = inner
                    .values_as_u64values()
                    .unwrap()
                    .values()
                    .into_iter()
                    .flatten();
                writer.write_u64(column.name(), valid_mask, values)
            }
            InfluxColumnType::Tag => {
                let values = inner
                    .values_as_string_values()
                    .unwrap()
                    .values()
                    .into_iter()
                    .flatten();
                writer.write_tag(column.name(), valid_mask, values)
            }
            InfluxColumnType::Field(InfluxFieldType::String) => {
                let values = inner
                    .values_as_string_values()
                    .unwrap()
                    .values()
                    .into_iter()
                    .flatten();
                writer.write_string(column.name(), valid_mask, values)
            }
            InfluxColumnType::Field(InfluxFieldType::Boolean) => {
                let values = inner
                    .values_as_bool_values()
                    .unwrap()
                    .values()
                    .into_iter()
                    .flatten()
                    .cloned();
                writer.write_bool(column.name(), valid_mask, values)
            }
            InfluxColumnType::Timestamp => {
                if valid_mask.is_some() {
                    return Err(Error::NullTime);
                }

                let values = inner
                    .values_as_i64values()
                    .unwrap()
                    .values()
                    .into_iter()
                    .flatten();
                writer.write_time(column.name(), values)
            }
            InfluxColumnType::IOx(_) => unimplemented!(),
        }
        .context(Write {
            column: column.name(),
        })?;
    }
    writer.commit();
    Ok(())
}

/// Construct a validity mask from the given column's null mask
fn construct_valid_mask(column: &EntryColumn<'_>) -> Result<Option<Vec<u8>>> {
    let buf_len = (column.row_count + 7) >> 3;
    let data = match column.inner().null_mask() {
        Some(data) => data,
        None => return Ok(None),
    };

    if data.iter().all(|x| *x == 0) {
        return Ok(None);
    }

    ensure!(
        data.len() == buf_len,
        InvalidNullMask {
            expected_bytes: buf_len,
            actual_bytes: data.len()
        }
    );

    Ok(Some(
        data.iter()
            .map(|x| {
                // Currently the bit mask is backwards
                !x.reverse_bits()
            })
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_util::assert_batches_eq;
    use data_types::database_rules::{PartitionTemplate, TemplatePart};
    use entry::test_helpers::{lp_to_entries, lp_to_entry};
    use entry::Entry;
    use schema::selection::Selection;

    fn first_batch(entry: &Entry) -> TableBatch<'_> {
        entry.table_batches().next().unwrap().1
    }

    #[test]
    fn test_basic() {
        let mut batch = MutableBatch::new();
        let lp = "foo,t1=asdf iv=1i,uv=1u,fv=1.0,bv=true,sv=\"hi\" 1";
        let entry = lp_to_entry(lp);

        write_table_batch(&mut batch, first_batch(&entry)).unwrap();

        let expected = &[
            "+------+----+----+----+------+--------------------------------+----+",
            "| bv   | fv | iv | sv | t1   | time                           | uv |",
            "+------+----+----+----+------+--------------------------------+----+",
            "| true | 1  | 1  | hi | asdf | 1970-01-01T00:00:00.000000001Z | 1  |",
            "+------+----+----+----+------+--------------------------------+----+",
        ];

        assert_batches_eq!(expected, &[batch.to_arrow(Selection::All).unwrap()]);

        let lp = "foo t1=\"string\" 1";
        let entry = lp_to_entry(lp);
        let err = write_table_batch(&mut batch, first_batch(&entry)).unwrap_err();
        assert_eq!(err.to_string(), "error writing column t1: Unable to insert iox::column_type::field::string type into a column of iox::column_type::tag");

        let lp = "foo iv=1u 1";
        let entry = lp_to_entry(lp);
        let err = write_table_batch(&mut batch, first_batch(&entry)).unwrap_err();
        assert_eq!(err.to_string(), "error writing column iv: Unable to insert iox::column_type::field::uinteger type into a column of iox::column_type::field::integer");

        let lp = "foo fv=1i 1";
        let entry = lp_to_entry(lp);
        let err = write_table_batch(&mut batch, first_batch(&entry)).unwrap_err();
        assert_eq!(err.to_string(), "error writing column fv: Unable to insert iox::column_type::field::integer type into a column of iox::column_type::field::float");

        let lp = "foo bv=1 1";
        let entry = lp_to_entry(lp);
        let err = write_table_batch(&mut batch, first_batch(&entry)).unwrap_err();
        assert_eq!(err.to_string(), "error writing column bv: Unable to insert iox::column_type::field::float type into a column of iox::column_type::field::boolean");

        let lp = "foo sv=true 1";
        let entry = lp_to_entry(lp);
        let err = write_table_batch(&mut batch, first_batch(&entry)).unwrap_err();
        assert_eq!(err.to_string(), "error writing column sv: Unable to insert iox::column_type::field::boolean type into a column of iox::column_type::field::string");

        let lp = "foo,sv=\"bar\" f=3i 1";
        let entry = lp_to_entry(lp);
        let err = write_table_batch(&mut batch, first_batch(&entry)).unwrap_err();
        assert_eq!(err.to_string(), "error writing column sv: Unable to insert iox::column_type::tag type into a column of iox::column_type::field::string");

        assert_batches_eq!(expected, &[batch.to_arrow(Selection::All).unwrap()]);

        let lp = r#"
            foo,t1=v1 fv=3.0,bv=false 2
            foo,t1=v3 fv=3.0,bv=false 2
            foo,t2=v6 bv=true,iv=3i 3
        "#;
        let entry = lp_to_entry(lp);

        write_table_batch(&mut batch, first_batch(&entry)).unwrap();

        assert_batches_eq!(
            &[
                "+-------+----+----+----+------+----+--------------------------------+----+",
                "| bv    | fv | iv | sv | t1   | t2 | time                           | uv |",
                "+-------+----+----+----+------+----+--------------------------------+----+",
                "| true  | 1  | 1  | hi | asdf |    | 1970-01-01T00:00:00.000000001Z | 1  |",
                "| false | 3  |    |    | v1   |    | 1970-01-01T00:00:00.000000002Z |    |",
                "| false | 3  |    |    | v3   |    | 1970-01-01T00:00:00.000000002Z |    |",
                "| true  |    | 3  |    |      | v6 | 1970-01-01T00:00:00.000000003Z |    |",
                "+-------+----+----+----+------+----+--------------------------------+----+",
            ],
            &[batch.to_arrow(Selection::All).unwrap()]
        );
    }

    #[test]
    fn test_entry_to_batches() {
        let lp = r#"
            cpu,part=a,tag1=v1 fv=3.0,sv="32" 1
            cpu,part=a,tag1=v1 fv=3.0 2
            cpu,part=a,tag2=v1 iv=3 1
            cpu,part=a,tag2=v1 iv=3 2
            mem,part=a,tag1=v2 v=2 1
            mem,part=a,tag1=v2 v=2,b=true 2
            mem,part=b,tag1=v2 v=2 2
        "#;

        let entries = lp_to_entries(
            lp,
            &PartitionTemplate {
                parts: vec![TemplatePart::Column("part".to_string())],
            },
        );
        assert_eq!(entries.len(), 1);
        let entry = entries.into_iter().next().unwrap();

        assert_eq!(entry.table_batches().count(), 3);

        // Should flatten partitioning
        let batches = entry_to_batches(&entry).unwrap();
        assert_eq!(batches.len(), 2);

        assert_batches_eq!(
            &[
                "+----+----+------+----+------+------+--------------------------------+",
                "| fv | iv | part | sv | tag1 | tag2 | time                           |",
                "+----+----+------+----+------+------+--------------------------------+",
                "| 3  |    | a    | 32 | v1   |      | 1970-01-01T00:00:00.000000001Z |",
                "| 3  |    | a    |    | v1   |      | 1970-01-01T00:00:00.000000002Z |",
                "|    | 3  | a    |    |      | v1   | 1970-01-01T00:00:00.000000001Z |",
                "|    | 3  | a    |    |      | v1   | 1970-01-01T00:00:00.000000002Z |",
                "+----+----+------+----+------+------+--------------------------------+",
            ],
            &[batches["cpu"].to_arrow(Selection::All).unwrap()]
        );

        assert_batches_eq!(
            &[
                "+------+------+------+--------------------------------+---+",
                "| b    | part | tag1 | time                           | v |",
                "+------+------+------+--------------------------------+---+",
                "|      | a    | v2   | 1970-01-01T00:00:00.000000001Z | 2 |",
                "| true | a    | v2   | 1970-01-01T00:00:00.000000002Z | 2 |",
                "|      | b    | v2   | 1970-01-01T00:00:00.000000002Z | 2 |",
                "+------+------+------+--------------------------------+---+",
            ],
            &[batches["mem"].to_arrow(Selection::All).unwrap()]
        );
    }
}
