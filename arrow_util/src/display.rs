use arrow::array::{ArrayRef, DurationNanosecondArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::error::{ArrowError, Result};
use arrow::record_batch::RecordBatch;

use comfy_table::{Cell, Table};

use chrono::prelude::*;

/// custom version of
/// [pretty_format_batches](arrow::util::pretty::pretty_format_batches)
/// that displays timestamps using RFC3339 format (e.g. `2021-07-20T23:28:50Z`)
///
/// Should be removed if/when the capability is added upstream to arrow:
/// <https://github.com/apache/arrow-rs/issues/599>
pub fn pretty_format_batches(results: &[RecordBatch]) -> Result<String> {
    Ok(create_table(results)?.to_string())
}

/// Convert the value at `column[row]` to a String
///
/// Special cases printing Timestamps in RFC3339 for IOx, otherwise
/// falls back to Arrow's implementation
///
fn array_value_to_string(column: &ArrayRef, row: usize) -> Result<String> {
    match column.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, None) if column.is_valid(row) => {
            let ts_column = column
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();

            let ts_value = ts_column.value(row);
            const NANOS_IN_SEC: i64 = 1_000_000_000;
            let secs = ts_value / NANOS_IN_SEC;
            let nanos = (ts_value - (secs * NANOS_IN_SEC)) as u32;
            let ts = NaiveDateTime::from_timestamp_opt(secs, nanos).ok_or_else(|| {
                ArrowError::ExternalError(
                    format!("Cannot process timestamp (secs={secs}, nanos={nanos})").into(),
                )
            })?;
            // treat as UTC
            let ts = DateTime::<Utc>::from_naive_utc_and_offset(ts, Utc);
            // convert to string in preferred influx format
            let use_z = true;
            Ok(ts.to_rfc3339_opts(SecondsFormat::AutoSi, use_z))
        }
        // TODO(edd): see https://github.com/apache/arrow-rs/issues/1168
        DataType::Duration(TimeUnit::Nanosecond) if column.is_valid(row) => {
            let dur_column = column
                .as_any()
                .downcast_ref::<DurationNanosecondArray>()
                .unwrap();

            let duration = std::time::Duration::from_nanos(
                dur_column
                    .value(row)
                    .try_into()
                    .map_err(|e| ArrowError::InvalidArgumentError(format!("{e:?}")))?,
            );
            Ok(format!("{duration:?}"))
        }
        _ => {
            // fallback to arrow's default printing for other types
            arrow::util::display::array_value_to_string(column, row)
        }
    }
}

/// Convert a series of record batches into a table
///
/// NB: COPIED FROM ARROW
fn create_table(results: &[RecordBatch]) -> Result<Table> {
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");

    if results.is_empty() {
        return Ok(table);
    }

    let schema = results[0].schema();

    let mut header = Vec::new();
    for field in schema.fields() {
        header.push(Cell::new(field.name()));
    }
    table.set_header(header);

    for (i, batch) in results.iter().enumerate() {
        if batch.schema() != schema {
            return Err(ArrowError::SchemaError(format!(
                "Batches have different schemas:\n\nFirst:\n{}\n\nBatch {}:\n{}",
                schema,
                i + 1,
                batch.schema()
            )));
        }

        for row in 0..batch.num_rows() {
            let mut cells = Vec::new();
            for col in 0..batch.num_columns() {
                let column = batch.column(col);
                cells.push(Cell::new(array_value_to_string(column, row)?));
            }
            table.add_row(cells);
        }
    }

    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::{
        array::{
            ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int64Array, StringArray,
            UInt64Array,
        },
        datatypes::Int32Type,
    };
    use datafusion::common::assert_contains;

    #[test]
    fn test_formatting() {
        // tests formatting all of the Arrow array types used in IOx

        // tags use string dictionary
        let dict_array: ArrayRef = Arc::new(
            vec![Some("a"), None, Some("b")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        );

        // field types
        let int64_array: ArrayRef =
            Arc::new([Some(-1), None, Some(2)].iter().collect::<Int64Array>());
        let uint64_array: ArrayRef =
            Arc::new([Some(1), None, Some(2)].iter().collect::<UInt64Array>());
        let float64_array: ArrayRef = Arc::new(
            [Some(1.0), None, Some(2.0)]
                .iter()
                .collect::<Float64Array>(),
        );
        let bool_array: ArrayRef = Arc::new(
            [Some(true), None, Some(false)]
                .iter()
                .collect::<BooleanArray>(),
        );
        let string_array: ArrayRef = Arc::new(
            vec![Some("foo"), None, Some("bar")]
                .into_iter()
                .collect::<StringArray>(),
        );

        // timestamp type
        let ts_array: ArrayRef = Arc::new(
            [None, Some(100), Some(1626823730000000000)]
                .iter()
                .collect::<TimestampNanosecondArray>(),
        );

        let batch = RecordBatch::try_from_iter(vec![
            ("dict", dict_array),
            ("int64", int64_array),
            ("uint64", uint64_array),
            ("float64", float64_array),
            ("bool", bool_array),
            ("string", string_array),
            ("time", ts_array),
        ])
        .unwrap();

        let table = pretty_format_batches(&[batch]).unwrap();

        let expected = vec![
            "+------+-------+--------+---------+-------+--------+--------------------------------+",
            "| dict | int64 | uint64 | float64 | bool  | string | time                           |",
            "+------+-------+--------+---------+-------+--------+--------------------------------+",
            "| a    | -1    | 1      | 1.0     | true  | foo    |                                |",
            "|      |       |        |         |       |        | 1970-01-01T00:00:00.000000100Z |",
            "| b    | 2     | 2      | 2.0     | false | bar    | 2021-07-20T23:28:50Z           |",
            "+------+-------+--------+---------+-------+--------+--------------------------------+",
        ];

        let actual: Vec<&str> = table.lines().collect();
        assert_eq!(
            expected, actual,
            "Expected:\n\n{expected:#?}\nActual:\n\n{actual:#?}\n"
        );
    }

    #[test]
    fn test_pretty_format_batches_checks_schemas() {
        let int64_array: ArrayRef = Arc::new([Some(2)].iter().collect::<Int64Array>());
        let uint64_array: ArrayRef = Arc::new([Some(2)].iter().collect::<UInt64Array>());

        let batch1 = RecordBatch::try_from_iter(vec![("col", int64_array)]).unwrap();
        let batch2 = RecordBatch::try_from_iter(vec![("col", uint64_array)]).unwrap();

        let err = pretty_format_batches(&[batch1, batch2]).unwrap_err();
        assert_contains!(err.to_string(), "Batches have different schemas:");
    }
}
