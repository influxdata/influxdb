use datafusion::arrow::{
    array::{
        as_boolean_array, as_dictionary_array, as_primitive_array, as_string_array, Array,
        ArrayAccessor, StringArray,
    },
    datatypes::{Float64Type, Int32Type, Int64Type, TimestampNanosecondType, UInt64Type},
    record_batch::RecordBatch,
};
use influxdb_line_protocol::{builder::FieldValue, FieldValue as LPFieldValue};
use schema::{InfluxColumnType, InfluxFieldType, Schema};

/// Converts a [`RecordBatch`] into line protocol lines.
pub fn convert_to_lines(
    measurement_name: &str,
    iox_schema: &Schema,
    batch: &RecordBatch,
) -> Result<Vec<u8>, String> {
    let mut lp_builder = influxdb_line_protocol::LineProtocolBuilder::new();

    for index in 0..batch.num_rows() {
        let lp_tags = lp_builder.measurement(measurement_name);

        // Add all tags
        let lp_tags = tags_values_iter(iox_schema, index, batch)
            .into_iter()
            .fold(lp_tags, |lp_tags, tag_column| {
                lp_tags.tag(tag_column.name, tag_column.value)
            });

        // add fields
        let mut fields = field_values_iter(iox_schema, index, batch).into_iter();

        // need at least one field (to put builder into "AfterTag" mode
        let first_field = fields
            .next()
            .ok_or_else(|| format!("Need at least one field, schema had none: {iox_schema:?}"))?;

        let lp_fields = lp_tags.field(first_field.name, first_field);

        // add rest of fileds
        let lp_fields = fields.fold(lp_fields, |lp_fields, field| {
            lp_fields.field(field.name, field)
        });

        let ts = timestamp_value(iox_schema, index, batch)?;
        lp_builder = lp_fields.timestamp(ts).close_line();
    }

    Ok(lp_builder.build())
}

/// Return an iterator over all non null tags in a batch
fn tags_values_iter<'a>(
    iox_schema: &'a Schema,
    row_index: usize,
    batch: &'a RecordBatch,
) -> impl IntoIterator<Item = TagColumn<'a>> {
    iox_schema
        .iter()
        .enumerate()
        .filter_map(move |(column_index, (influx_column_type, field))| {
            if influx_column_type == InfluxColumnType::Tag {
                // tags are always dictionaries
                let arr = as_dictionary_array::<Int32Type>(batch.column(column_index))
                    .downcast_dict::<StringArray>()
                    .expect("Tag was not a string dictionary array");

                // If the value of this column is not null, return it.
                if arr.is_valid(row_index) {
                    return Some(TagColumn {
                        name: field.name(),
                        value: arr.value(row_index),
                    });
                }
            }
            None
        })
}

/// Represents a particular column along with code that knows how to add that value to a builder.
struct TagColumn<'a> {
    name: &'a str,
    value: &'a str,
}

/// Return an iterator over all non null fields in a batch
fn field_values_iter<'a>(
    iox_schema: &'a Schema,
    row_index: usize,
    batch: &'a RecordBatch,
) -> impl IntoIterator<Item = FieldColumn<'a>> {
    iox_schema
        .iter()
        .enumerate()
        .filter_map(move |(column_index, (influx_column_type, field))| {
            // Skip any value that is NULL
            let arr = batch.column(column_index);
            if !arr.is_valid(row_index) {
                return None;
            }

            let name = field.name();

            // Extract the value from the relevant array and convert it
            let value = match influx_column_type {
                InfluxColumnType::Field(InfluxFieldType::Float) => {
                    LPFieldValue::F64(as_primitive_array::<Float64Type>(arr).value(row_index))
                }
                InfluxColumnType::Field(InfluxFieldType::Integer) => {
                    LPFieldValue::I64(as_primitive_array::<Int64Type>(arr).value(row_index))
                }
                InfluxColumnType::Field(InfluxFieldType::UInteger) => {
                    LPFieldValue::U64(as_primitive_array::<UInt64Type>(arr).value(row_index))
                }
                InfluxColumnType::Field(InfluxFieldType::String) => {
                    LPFieldValue::String(as_string_array(arr).value(row_index).into())
                }
                InfluxColumnType::Field(InfluxFieldType::Boolean) => {
                    LPFieldValue::Boolean(as_boolean_array(arr).value(row_index))
                }
                // not a field
                InfluxColumnType::Tag | InfluxColumnType::Timestamp => return None,
            };

            Some(FieldColumn { name, value })
        })
}

/// Represents a particular Field column's value in a way that knows how to format
struct FieldColumn<'a> {
    name: &'a str,
    value: LPFieldValue<'a>,
}

impl<'a> FieldValue for FieldColumn<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.value {
            LPFieldValue::I64(v) => v.fmt(f),
            LPFieldValue::U64(v) => v.fmt(f),
            LPFieldValue::F64(v) => v.fmt(f),
            LPFieldValue::String(v) => v.as_str().fmt(f),
            LPFieldValue::Boolean(v) => v.fmt(f),
        }
    }
}

/// Find the timestamp value for the specified row
fn timestamp_value<'a>(
    iox_schema: &'a Schema,
    row_index: usize,
    batch: &'a RecordBatch,
) -> Result<i64, String> {
    let column_index = iox_schema
        .iter()
        .enumerate()
        .filter_map(move |(column_index, (influx_column_type, _))| {
            if influx_column_type == InfluxColumnType::Timestamp {
                Some(column_index)
            } else {
                None
            }
        })
        .next()
        .ok_or_else(|| "No timestamp column found in schema".to_string())?;

    // timestamps are always TimestampNanosecondArray's and should always have a timestamp value filled in
    let arr = as_primitive_array::<TimestampNanosecondType>(batch.column(column_index));

    if !arr.is_valid(row_index) {
        Err(format!(
            "TimestampValue was unexpectedly null at row {row_index}"
        ))
    } else {
        Ok(arr.value(row_index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mutable_batch_lp::lines_to_batches;
    use schema::Projection;

    #[test]
    fn basic() {
        round_trip("my_measurement_name,tag=foo value=4 1000");
    }

    #[test]
    fn no_tags() {
        round_trip("my_no_tag_measurement_name value=4 1000");
    }

    #[test]
    #[should_panic = "Error parsing line protocol: LineProtocol { source: FieldSetMissing, line: 1 }"]
    fn no_fields() {
        round_trip("my_no_tag_measurement_name,tag=4 1000");
    }

    #[test]
    fn all_types() {
        // Note we use cannonical format (e.g. 'true' instead of 't')
        round_trip(
            r#"m,tag=row1 float_field=64 450
m,tag2=row2 float_field=65 550
m,tag2=row3 int_field=65i 560
m,tag2=row4 uint_field=5u 580
m,tag2=row5 bool_field=true 590
m,tag2=row6 str_field="blargh" 600
m,tag2=multi_field bool_field=false,str_field="blargh" 610
"#,
        );
    }

    /// ensures that parsing line protocol to record batches and then
    /// converting it back to line protocol results in the same output
    ///
    /// Note it must use cannonical format (e.g. 'true' instead of 't')
    fn round_trip(lp: &str) {
        let default_time = 0;
        let mutable_batches =
            lines_to_batches(lp, default_time).expect("Error parsing line protocol");
        assert_eq!(
            mutable_batches.len(),
            1,
            "round trip only supports one measurement"
        );
        let (table_name, mutable_batch) = mutable_batches.into_iter().next().unwrap();

        let selection = Projection::All;
        let record_batch = mutable_batch.to_arrow(selection).unwrap();
        let iox_schema = mutable_batch.schema(selection).unwrap();

        let output_lp = convert_to_lines(&table_name, &iox_schema, &record_batch)
            .expect("error converting lines");
        let output_lp = String::from_utf8_lossy(&output_lp);

        let lp = lp.trim();
        let output_lp = output_lp.trim();

        assert_eq!(
            lp, output_lp,
            "\n\nInput:\n\n{lp}\n\nOutput:\n\n{output_lp}\n"
        )
    }
}
