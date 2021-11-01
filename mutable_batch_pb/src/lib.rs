//! Code to convert between binary write format and [`MutableBatch`]

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

use hashbrown::HashSet;
use snafu::{ensure, ResultExt, Snafu};

use generated_types::influxdata::pbdata::v1::{
    column::{SemanticType, Values as PbValues},
    Column as PbColumn, TableBatch,
};
use mutable_batch::{writer::Writer, MutableBatch};
use schema::{InfluxColumnType, InfluxFieldType, TIME_COLUMN_NAME};

/// Error type for line protocol conversion
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("error writing column {}: {}", column, source))]
    Write {
        source: mutable_batch::writer::Error,
        column: String,
    },

    #[snafu(display("duplicate column name: {}", column))]
    DuplicateColumnName { column: String },

    #[snafu(display("table batch must contain time column"))]
    MissingTime,

    #[snafu(display("time column must not contain nulls"))]
    NullTime,

    #[snafu(display("column with no values: {}", column))]
    EmptyColumn { column: String },

    #[snafu(display("cannot infer type for column: {}", column))]
    InvalidType { column: String },
}

/// Result type for pbdata conversion
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Writes the provided [`TableBatch`] to a [`MutableBatch`] on error any changes made
/// to `batch` are reverted
pub fn write_table_batch(batch: &mut MutableBatch, table_batch: &TableBatch) -> Result<()> {
    let to_insert = table_batch.row_count as usize;
    if to_insert == 0 {
        return Ok(());
    }

    // Verify columns are unique
    let mut columns = HashSet::with_capacity(table_batch.columns.len());
    for col in &table_batch.columns {
        ensure!(
            columns.insert(col.column_name.as_str()),
            DuplicateColumnName {
                column: &col.column_name
            }
        );
    }

    // Batch must contain a time column
    ensure!(columns.contains(TIME_COLUMN_NAME), MissingTime);

    let mut writer = Writer::new(batch, to_insert);
    for column in &table_batch.columns {
        let influx_type = pb_column_type(column)?;
        let valid_mask = compute_valid_mask(&column.null_mask, to_insert);
        let valid_mask = valid_mask.as_deref();

        // Already verified has values
        let values = column.values.as_ref().unwrap();

        match influx_type {
            InfluxColumnType::Field(InfluxFieldType::Float) => writer.write_f64(
                &column.column_name,
                valid_mask,
                values.f64_values.iter().cloned(),
            ),
            InfluxColumnType::Field(InfluxFieldType::Integer) => writer.write_i64(
                &column.column_name,
                valid_mask,
                values.i64_values.iter().cloned(),
            ),
            InfluxColumnType::Field(InfluxFieldType::UInteger) => writer.write_u64(
                &column.column_name,
                valid_mask,
                values.u64_values.iter().cloned(),
            ),
            InfluxColumnType::Tag => writer.write_tag(
                &column.column_name,
                valid_mask,
                values.string_values.iter().map(|x| x.as_str()),
            ),
            InfluxColumnType::Field(InfluxFieldType::String) => writer.write_string(
                &column.column_name,
                valid_mask,
                values.string_values.iter().map(|x| x.as_str()),
            ),
            InfluxColumnType::Field(InfluxFieldType::Boolean) => writer.write_bool(
                &column.column_name,
                valid_mask,
                values.bool_values.iter().cloned(),
            ),
            InfluxColumnType::Timestamp => {
                ensure!(valid_mask.is_none(), NullTime);
                writer.write_time(&column.column_name, values.i64_values.iter().cloned())
            }
            InfluxColumnType::IOx(_) => unimplemented!(),
        }
        .context(Write {
            column: &column.column_name,
        })?;
    }

    writer.commit();
    Ok(())
}

/// Converts a potentially truncated null mask to a valid mask
fn compute_valid_mask(null_mask: &[u8], to_insert: usize) -> Option<Vec<u8>> {
    if null_mask.is_empty() || null_mask.iter().all(|x| *x == 0) {
        return None;
    }

    // The expected length of the validity mask
    let expected_len = (to_insert + 7) >> 3;

    // The number of bits over the byte boundary
    let overrun = to_insert & 7;

    let mut mask: Vec<_> = (0..expected_len)
        .map(|x| match null_mask.get(x) {
            Some(v) => !*v,
            None => 0xFF,
        })
        .collect();

    if overrun != 0 {
        *mask.last_mut().unwrap() &= (1 << overrun) - 1;
    }

    Some(mask)
}

fn pb_column_type(col: &PbColumn) -> Result<InfluxColumnType> {
    let value_type =
        col.values
            .as_ref()
            .and_then(pb_value_type)
            .ok_or_else(|| Error::EmptyColumn {
                column: col.column_name.clone(),
            })?;

    let semantic_type = SemanticType::from_i32(col.semantic_type);

    match (semantic_type, value_type) {
        (Some(SemanticType::Tag), InfluxFieldType::String) => Ok(InfluxColumnType::Tag),
        (Some(SemanticType::Field), field) => Ok(InfluxColumnType::Field(field)),
        (Some(SemanticType::Time), InfluxFieldType::Integer)
            if col.column_name.as_str() == TIME_COLUMN_NAME =>
        {
            Ok(InfluxColumnType::Timestamp)
        }
        _ => InvalidType {
            column: &col.column_name,
        }
        .fail(),
    }
}

fn pb_value_type(values: &PbValues) -> Option<InfluxFieldType> {
    if !values.string_values.is_empty() {
        return Some(InfluxFieldType::String);
    }
    if !values.i64_values.is_empty() {
        return Some(InfluxFieldType::Integer);
    }
    if !values.u64_values.is_empty() {
        return Some(InfluxFieldType::UInteger);
    }
    if !values.f64_values.is_empty() {
        return Some(InfluxFieldType::Float);
    }
    if !values.bool_values.is_empty() {
        return Some(InfluxFieldType::Boolean);
    }
    None
}

#[cfg(test)]
mod tests {
    use arrow_util::assert_batches_eq;
    use schema::selection::Selection;

    use super::*;

    #[test]
    fn test_basic() {
        let mut table_batch = TableBatch {
            table_name: "table".to_string(),
            columns: vec![
                PbColumn {
                    column_name: "tag1".to_string(),
                    semantic_type: SemanticType::Tag as _,
                    values: Some(PbValues {
                        i64_values: vec![],
                        f64_values: vec![],
                        u64_values: vec![],
                        string_values: vec![
                            "v1".to_string(),
                            "v1".to_string(),
                            "v2".to_string(),
                            "v2".to_string(),
                            "v1".to_string(),
                        ],
                        bool_values: vec![],
                        bytes_values: vec![],
                    }),
                    null_mask: vec![],
                },
                PbColumn {
                    column_name: "tag2".to_string(),
                    semantic_type: SemanticType::Tag as _,
                    values: Some(PbValues {
                        i64_values: vec![],
                        f64_values: vec![],
                        u64_values: vec![],
                        string_values: vec!["v2".to_string(), "v3".to_string()],
                        bool_values: vec![],
                        bytes_values: vec![],
                    }),
                    null_mask: vec![0b00010101],
                },
                PbColumn {
                    column_name: "f64".to_string(),
                    semantic_type: SemanticType::Field as _,
                    values: Some(PbValues {
                        i64_values: vec![],
                        f64_values: vec![3., 5.],
                        u64_values: vec![],
                        string_values: vec![],
                        bool_values: vec![],
                        bytes_values: vec![],
                    }),
                    null_mask: vec![0b00001101],
                },
                PbColumn {
                    column_name: "i64".to_string(),
                    semantic_type: SemanticType::Field as _,
                    values: Some(PbValues {
                        i64_values: vec![56, 2],
                        f64_values: vec![],
                        u64_values: vec![],
                        string_values: vec![],
                        bool_values: vec![],
                        bytes_values: vec![],
                    }),
                    null_mask: vec![0b00001110],
                },
                PbColumn {
                    column_name: "time".to_string(),
                    semantic_type: SemanticType::Time as _,
                    values: Some(PbValues {
                        i64_values: vec![1, 2, 3, 4, 5],
                        f64_values: vec![],
                        u64_values: vec![],
                        string_values: vec![],
                        bool_values: vec![],
                        bytes_values: vec![],
                    }),
                    null_mask: vec![0b00000000],
                },
            ],
            row_count: 5,
        };

        let mut batch = MutableBatch::new();

        write_table_batch(&mut batch, &table_batch).unwrap();

        let expected = &[
            "+-----+-----+------+------+--------------------------------+",
            "| f64 | i64 | tag1 | tag2 | time                           |",
            "+-----+-----+------+------+--------------------------------+",
            "|     | 56  | v1   |      | 1970-01-01T00:00:00.000000001Z |",
            "| 3   |     | v1   | v2   | 1970-01-01T00:00:00.000000002Z |",
            "|     |     | v2   |      | 1970-01-01T00:00:00.000000003Z |",
            "|     |     | v2   | v3   | 1970-01-01T00:00:00.000000004Z |",
            "| 5   | 2   | v1   |      | 1970-01-01T00:00:00.000000005Z |",
            "+-----+-----+------+------+--------------------------------+",
        ];

        assert_batches_eq!(expected, &[batch.to_arrow(Selection::All).unwrap()]);

        table_batch.columns.push(table_batch.columns[0].clone());

        let err = write_table_batch(&mut batch, &table_batch)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "duplicate column name: tag1");

        table_batch.columns.pop();

        // Missing time column -> error
        let mut time = table_batch.columns.remove(4);
        assert_eq!(time.column_name.as_str(), "time");

        let err = write_table_batch(&mut batch, &table_batch)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "table batch must contain time column");

        assert_batches_eq!(expected, &[batch.to_arrow(Selection::All).unwrap()]);

        // Nulls in time column -> error
        time.null_mask = vec![1];
        table_batch.columns.push(time);

        let err = write_table_batch(&mut batch, &table_batch)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "time column must not contain nulls");

        assert_batches_eq!(expected, &[batch.to_arrow(Selection::All).unwrap()]);

        // Missing values -> error
        table_batch.columns[0].values.take().unwrap();

        let err = write_table_batch(&mut batch, &table_batch)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "column with no values: tag1");

        assert_batches_eq!(expected, &[batch.to_arrow(Selection::All).unwrap()]);

        // No data -> error
        table_batch.columns[0].values = Some(PbValues {
            i64_values: vec![],
            f64_values: vec![],
            u64_values: vec![],
            string_values: vec![],
            bool_values: vec![],
            bytes_values: vec![],
        });

        let err = write_table_batch(&mut batch, &table_batch)
            .unwrap_err()
            .to_string();
        assert_eq!(err, "column with no values: tag1");

        assert_batches_eq!(expected, &[batch.to_arrow(Selection::All).unwrap()]);
    }
}
