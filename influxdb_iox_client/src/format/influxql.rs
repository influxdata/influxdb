use arrow::array::{Array, ArrayData, StringArray};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow::util::display::ArrayFormatter;
use comfy_table::{Cell, Table};
use generated_types::influxdata::iox::querier::v1::InfluxQlMetadata;
use std::io::Write;
use std::iter;
use thiserror::Error;

/// Error type for results formatting
#[derive(Debug, Error)]
pub enum Error {
    /// Arrow error.
    #[error("Arrow error: {}", .0)]
    Arrow(ArrowError),

    /// [`InfluxQlMetadata`] not found in Arrow schema metadata.
    #[error("Missing InfluxQL metadata")]
    MissingMetadata,

    /// Error deserializing [`InfluxQlMetadata`] from Arrow schema metadata.
    #[error("Invalid InfluxQL metadata: {0}")]
    InvalidMetadata(#[from] serde_json::Error),

    /// Error writing formatted output.
    #[error("Error writing output: {0}")]
    Write(#[from] std::io::Error),
}
type Result<T, E = Error> = std::result::Result<T, E>;

/// Options for controlling how table borders are rendered.
#[derive(Debug, Default, Clone, Copy)]
pub enum TableBorders {
    /// Use ASCII characters.
    #[default]
    Ascii,
    /// Use UNICODE box-drawing characters.
    Unicode,
    /// Do not render borders.
    None,
}

/// Options for the [`write_columnar`] function.
#[derive(Debug, Default)]
pub struct Options {
    /// Specify how borders should be rendered.
    pub borders: TableBorders,
}

impl Options {
    fn table_preset(&self) -> &'static str {
        match self.borders {
            TableBorders::Ascii => "||--+-++|    ++++++",
            TableBorders::Unicode => comfy_table::presets::UTF8_FULL,
            TableBorders::None => comfy_table::presets::NOTHING,
        }
    }
}

/// Write the record batches in a columnar format.
pub fn write_columnar(mut w: impl Write, batches: &[RecordBatch], options: Options) -> Result<()> {
    let arrow_opts = arrow::util::display::FormatOptions::default().with_display_error(true);

    let Some(schema) = batches.first().map(|b|b.schema()) else { return Ok(()) };
    let md = schema
        .metadata()
        .get(schema::INFLUXQL_METADATA_KEY)
        .ok_or(Error::MissingMetadata)?;

    let v: InfluxQlMetadata = serde_json::from_str(md)?;

    let measurement_idx = v.measurement_column_index as usize;
    let (tag_keys, tag_key_indexes): (Vec<_>, Vec<_>) = v
        .tag_key_columns
        .iter()
        .map(|tk| (tk.tag_key.as_str(), tk.column_index as usize))
        .unzip();

    // Find the column indices that should be displayed for the columnar output,
    // excluding the measurement name column and any tag key columns that only
    // appear in the `GROUP BY` clause.
    let col_indexes = (0..schema.fields().len())
        .filter(|i| {
            !v.tag_key_columns
                .iter()
                .any(|tk| tk.column_index as usize == *i && !tk.is_projected)
                && measurement_idx != *i
        })
        .collect::<Vec<_>>();

    // Collect the header names for the columnar output
    let header = col_indexes
        .iter()
        .map(|idx| Cell::new(schema.field(*idx).name()))
        .collect::<Vec<_>>();

    let new_table = || {
        let mut table = Table::new();
        table.load_preset(options.table_preset());
        table.set_header(header.clone());
        table
    };

    let mut table = new_table();

    for batch in batches {
        let cols = col_indexes
            .iter()
            .map(|idx| {
                ArrayFormatter::try_new(batch.column(*idx), &arrow_opts).map_err(Error::Arrow)
            })
            .collect::<Result<Vec<_>>>()?;

        let measurement = batch
            .column(measurement_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected measurement column to be a StringArray");

        // create an empty string array for any tag columns that are NULL
        let empty: StringArray =
            StringArray::from(ArrayData::new_null(&DataType::Utf8, measurement.len()));

        let tag_vals = tag_key_indexes
            .iter()
            .map(|idx| {
                batch
                    .column(*idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap_or(&empty)
            })
            .collect::<Vec<_>>();

        let mut curr_measurement = "";
        let mut curr_tag_values = iter::repeat("").take(tag_keys.len()).collect::<Vec<_>>();

        for row in 0..batch.num_rows() {
            let m = measurement.value(row);

            let meas_changed = if curr_measurement != m {
                curr_measurement = m;
                true
            } else {
                false
            };

            let tags_changed = if tag_vals
                .iter()
                .map(|col| col.value(row))
                .zip(&curr_tag_values)
                .all(|(next, prev)| next == *prev)
            {
                false
            } else {
                tag_vals
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (i, c.value(row)))
                    .for_each(|(i, v)| curr_tag_values[i] = v);
                true
            };

            if meas_changed || tags_changed {
                if table.row(0).is_some() {
                    writeln!(w, "{table}")?;
                }
                table = new_table();
                writeln!(w, "name: {curr_measurement}")?;

                // Only print the `tags:` label if there is a group key
                if let (Some(key), Some(val)) = (tag_keys.first(), curr_tag_values.first()) {
                    write!(w, "tags: {key}={val}")?;

                    for (key, val) in tag_keys[1..].iter().zip(&curr_tag_values[1..]) {
                        write!(w, ", {key}={val}")?;
                    }
                    writeln!(w)?;
                }
            }

            let mut cells = Vec::new();
            for col in &cols {
                cells.push(Cell::new(col.value(row).to_string()));
            }
            table.add_row(cells);
        }
    }

    writeln!(w, "{table}")?;

    Ok(())
}

#[cfg(test)]
mod test {
    use crate::format::influxql::{write_columnar, Options};
    use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use generated_types::influxdata::iox::querier::v1::influx_ql_metadata::TagKeyColumn;
    use generated_types::influxdata::iox::querier::v1::InfluxQlMetadata;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn times(vals: &[i64]) -> ArrayRef {
        Arc::new(TimestampNanosecondArray::from_iter_values(
            vals.iter().cloned(),
        ))
    }

    fn strs<T: AsRef<str>>(vals: &[Option<T>]) -> ArrayRef {
        Arc::new(StringArray::from_iter(vals))
    }

    fn f64s(vals: &[Option<f64>]) -> ArrayRef {
        Arc::new(Float64Array::from_iter(vals.iter()))
    }

    fn i64s(vals: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from_iter(vals.iter().cloned()))
    }

    fn batches(meta: InfluxQlMetadata) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("iox::measurement", DataType::Utf8, false),
                Field::new(
                    "time",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("cpu", DataType::Utf8, true),
                Field::new("device", DataType::Utf8, true),
                Field::new("usage_idle", DataType::Float64, true),
                Field::new("free", DataType::Int64, true),
            ],
            HashMap::from([(
                "iox::influxql::group_key::metadata".to_owned(),
                serde_json::to_string(&meta).unwrap(),
            )]),
        ));

        vec![RecordBatch::try_new(
            schema,
            vec![
                strs(&[
                    Some("cpu"),
                    Some("cpu"),
                    Some("cpu"),
                    Some("disk"),
                    Some("disk"),
                ]),
                times(&[
                    1157082300000000000,
                    1157082310000000000,
                    1157082300000000000,
                    1157082300000000000,
                    1157082300000000000,
                ]),
                strs(&[Some("cpu0"), Some("cpu0"), Some("cpu1"), None, None]),
                strs(&[None, None, None, Some("disk1s1"), Some("disk1s2")]),
                f64s(&[Some(99.1), Some(99.8), Some(99.2), None, None]),
                i64s(&[None, None, None, Some(2133), Some(4110)]),
            ],
        )
        .unwrap()]
    }

    #[test]
    fn test_write_columnar() {
        // No group key defined in the metadata
        let rb = batches(InfluxQlMetadata {
            measurement_column_index: 0,
            tag_key_columns: vec![],
        });
        let mut s = Vec::<u8>::new();
        write_columnar(&mut s, &rb, Options::default()).unwrap();
        let res = String::from_utf8(s).unwrap();
        insta::assert_snapshot!(res, @r###"
        name: cpu
        +---------------------+------+--------+------------+------+
        | time                | cpu  | device | usage_idle | free |
        +---------------------+------+--------+------------+------+
        | 2006-09-01T03:45:00 | cpu0 |        | 99.1       |      |
        | 2006-09-01T03:45:10 | cpu0 |        | 99.8       |      |
        | 2006-09-01T03:45:00 | cpu1 |        | 99.2       |      |
        +---------------------+------+--------+------------+------+
        name: disk
        +---------------------+-----+---------+------------+------+
        | time                | cpu | device  | usage_idle | free |
        +---------------------+-----+---------+------------+------+
        | 2006-09-01T03:45:00 |     | disk1s1 |            | 2133 |
        | 2006-09-01T03:45:00 |     | disk1s2 |            | 4110 |
        +---------------------+-----+---------+------------+------+
        "###);

        // Group by cpu tag
        let rb = batches(InfluxQlMetadata {
            measurement_column_index: 0,
            tag_key_columns: vec![TagKeyColumn {
                tag_key: "cpu".to_owned(),
                column_index: 2,
                is_projected: false,
            }],
        });
        let mut s = Vec::<u8>::new();
        write_columnar(&mut s, &rb, Options::default()).unwrap();
        let res = String::from_utf8(s).unwrap();
        insta::assert_snapshot!(res, @r###"
        name: cpu
        tags: cpu=cpu0
        +---------------------+--------+------------+------+
        | time                | device | usage_idle | free |
        +---------------------+--------+------------+------+
        | 2006-09-01T03:45:00 |        | 99.1       |      |
        | 2006-09-01T03:45:10 |        | 99.8       |      |
        +---------------------+--------+------------+------+
        name: cpu
        tags: cpu=cpu1
        +---------------------+--------+------------+------+
        | time                | device | usage_idle | free |
        +---------------------+--------+------------+------+
        | 2006-09-01T03:45:00 |        | 99.2       |      |
        +---------------------+--------+------------+------+
        name: disk
        tags: cpu=
        +---------------------+---------+------------+------+
        | time                | device  | usage_idle | free |
        +---------------------+---------+------------+------+
        | 2006-09-01T03:45:00 | disk1s1 |            | 2133 |
        | 2006-09-01T03:45:00 | disk1s2 |            | 4110 |
        +---------------------+---------+------------+------+
        "###);

        // group by cpu tag, and cpu tag is included in projection
        let rb = batches(InfluxQlMetadata {
            measurement_column_index: 0,
            tag_key_columns: vec![TagKeyColumn {
                tag_key: "cpu".to_owned(),
                column_index: 2,
                is_projected: true,
            }],
        });
        let mut s = Vec::<u8>::new();
        write_columnar(&mut s, &rb, Options::default()).unwrap();
        let res = String::from_utf8(s).unwrap();
        insta::assert_snapshot!(res, @r###"
        name: cpu
        tags: cpu=cpu0
        +---------------------+------+--------+------------+------+
        | time                | cpu  | device | usage_idle | free |
        +---------------------+------+--------+------------+------+
        | 2006-09-01T03:45:00 | cpu0 |        | 99.1       |      |
        | 2006-09-01T03:45:10 | cpu0 |        | 99.8       |      |
        +---------------------+------+--------+------------+------+
        name: cpu
        tags: cpu=cpu1
        +---------------------+------+--------+------------+------+
        | time                | cpu  | device | usage_idle | free |
        +---------------------+------+--------+------------+------+
        | 2006-09-01T03:45:00 | cpu1 |        | 99.2       |      |
        +---------------------+------+--------+------------+------+
        name: disk
        tags: cpu=
        +---------------------+-----+---------+------------+------+
        | time                | cpu | device  | usage_idle | free |
        +---------------------+-----+---------+------------+------+
        | 2006-09-01T03:45:00 |     | disk1s1 |            | 2133 |
        | 2006-09-01T03:45:00 |     | disk1s2 |            | 4110 |
        +---------------------+-----+---------+------------+------+
        "###);

        // group by cpu, device tags
        let rb = batches(InfluxQlMetadata {
            measurement_column_index: 0,
            tag_key_columns: vec![
                TagKeyColumn {
                    tag_key: "cpu".to_owned(),
                    column_index: 2,
                    is_projected: false,
                },
                TagKeyColumn {
                    tag_key: "device".to_owned(),
                    column_index: 3,
                    is_projected: false,
                },
            ],
        });
        let mut s = Vec::<u8>::new();
        write_columnar(&mut s, &rb, Options::default()).unwrap();
        let res = String::from_utf8(s).unwrap();
        insta::assert_snapshot!(res, @r###"
        name: cpu
        tags: cpu=cpu0, device=
        +---------------------+------------+------+
        | time                | usage_idle | free |
        +---------------------+------------+------+
        | 2006-09-01T03:45:00 | 99.1       |      |
        | 2006-09-01T03:45:10 | 99.8       |      |
        +---------------------+------------+------+
        name: cpu
        tags: cpu=cpu1, device=
        +---------------------+------------+------+
        | time                | usage_idle | free |
        +---------------------+------------+------+
        | 2006-09-01T03:45:00 | 99.2       |      |
        +---------------------+------------+------+
        name: disk
        tags: cpu=, device=disk1s1
        +---------------------+------------+------+
        | time                | usage_idle | free |
        +---------------------+------------+------+
        | 2006-09-01T03:45:00 |            | 2133 |
        +---------------------+------------+------+
        name: disk
        tags: cpu=, device=disk1s2
        +---------------------+------------+------+
        | time                | usage_idle | free |
        +---------------------+------------+------+
        | 2006-09-01T03:45:00 |            | 4110 |
        +---------------------+------------+------+
        "###);
    }
}
