use arrow::{record_batch::RecordBatch, util::pretty::print_batches};
use hashbrown::HashMap;
use std::{
    collections::{BTreeMap, BTreeSet},
    iter,
    string::FromUtf8Error,
    sync::Arc,
};

use generated_types::{
    read_response::{frame::Data, DataType, SeriesFrame},
    Tag,
};
use schema::{builder::SchemaBuilder, InfluxColumnType, InfluxFieldType, Schema};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("arrow error: {:?}", source))]
    Arrow { source: arrow::error::ArrowError },

    #[snafu(display("frame type currently unsupported: {:?}", frame))]
    UnsupportedFrameType { frame: String },

    #[snafu(display("tag keys must be valid UTF-8: {:?}", source))]
    InvalidTagKey { source: FromUtf8Error },

    #[snafu(display("tag values must be valid UTF-8: {:?}", source))]
    InvalidTagValue { source: FromUtf8Error },

    #[snafu(display("measurement name must be valid UTF-8: {:?}", source))]
    InvalidMeasurementName { source: FromUtf8Error },

    #[snafu(display("unable to build schema: {:?}", source))]
    SchemaBuilding { source: schema::builder::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// Prints the provided data frames in a tabular format grouped into tables per
// distinct measurement.
pub fn pretty_print_frames(frames: &[Data]) -> Result<()> {
    let rbs = frames_to_record_batches(frames)?;
    for (k, rb) in rbs {
        println!("\n_measurement: {}", k);
        println!("rows: {:?}\n", &rb.num_rows());
        print_batches(&[rb]).context(ArrowSnafu)?;
    }
    Ok(())
}

// Prints the provided set of strings in a tabular format grouped.
pub fn pretty_print_strings(values: Vec<String>) -> Result<()> {
    let schema = SchemaBuilder::new()
        .influx_field("values", InfluxFieldType::String)
        .build()
        .context(SchemaBuildingSnafu)?;

    let arrow_schema: arrow::datatypes::SchemaRef = schema.into();
    let rb_columns: Vec<Arc<dyn arrow::array::Array>> =
        vec![Arc::new(arrow::array::StringArray::from(
            values.iter().map(|x| Some(x.as_str())).collect::<Vec<_>>(),
        ))];

    let rb = RecordBatch::try_new(arrow_schema, rb_columns).context(ArrowSnafu)?;

    println!("\ntag values: {:?}", &rb.num_rows());
    print_batches(&[rb]).context(ArrowSnafu)?;
    println!("\n");
    Ok(())
}

// This function takes a set of InfluxRPC data frames and converts them into an
// Arrow record batches, which are suitable for pretty printing.
fn frames_to_record_batches(frames: &[Data]) -> Result<BTreeMap<String, RecordBatch>> {
    // Run through all the frames once to build the schema of each table we need
    // to build as a record batch.
    let mut table_column_mapping = determine_tag_columns(frames);

    let mut all_tables = BTreeMap::new();
    let mut current_table_frame: Option<(IntermediateTable, SeriesFrame)> = None;

    if frames.is_empty() {
        return Ok(all_tables);
    }

    for frame in frames {
        match frame {
            generated_types::read_response::frame::Data::Group(_) => {
                return UnsupportedFrameTypeSnafu {
                    frame: "group_frame".to_owned(),
                }
                .fail();
            }
            generated_types::read_response::frame::Data::Series(sf) => {
                let cur_frame_measurement = &sf.tags[0].value;

                // First series frame in result set.
                if current_table_frame.is_none() {
                    let table = IntermediateTable::try_new(
                        table_column_mapping
                            .remove(cur_frame_measurement)
                            .expect("table column mappings exists for measurement"),
                    )?;

                    current_table_frame = Some((table, sf.clone()));
                    continue;
                }

                // Subsequent series frames in results.
                let (mut current_table, prev_series_frame) = current_table_frame.take().unwrap();

                // Series frame has moved on to a different measurement. Push
                // this table into a record batch and onto final results, then
                // create a new table.
                if measurement(&prev_series_frame) != cur_frame_measurement {
                    let rb: RecordBatch = current_table.try_into()?;
                    all_tables.insert(
                        String::from_utf8(measurement(&prev_series_frame).to_owned())
                            .context(InvalidMeasurementNameSnafu)?,
                        rb,
                    );

                    // Initialise next intermediate table to fill.
                    current_table = IntermediateTable::try_new(
                        table_column_mapping
                            .remove(cur_frame_measurement)
                            .expect("table column mappings exists for measurement"),
                    )?;
                }

                // Put current table (which may have been replaced with a new
                // table if _measurement has changed) and series frame back. The
                // field key can change on each series frame, so it's important
                // to update it each time we see a new series frame, so that the
                // value frames know where to push their data.
                current_table_frame = Some((current_table, sf.clone()));

                // no new column values written so no need to pad.
                continue;
            }
            generated_types::read_response::frame::Data::FloatPoints(f) => {
                // Get field key associated with previous series frame.
                let (current_table, prev_series_frame) = current_table_frame.as_mut().unwrap();
                let column = current_table.field_column(field_name(prev_series_frame));

                let values = f.values.iter().copied().map(Some).collect::<Vec<_>>();
                column.extend_f64(&values);

                let time_column = &mut current_table.time_column;
                time_column.extend_from_slice(&f.timestamps);
            }
            generated_types::read_response::frame::Data::IntegerPoints(f) => {
                // Get field key associated with previous series frame.
                let (current_table, prev_series_frame) = current_table_frame.as_mut().unwrap();
                let column = current_table.field_column(field_name(prev_series_frame));

                let values = f.values.iter().copied().map(Some).collect::<Vec<_>>();
                column.extend_i64(&values);

                let time_column = &mut current_table.time_column;
                time_column.extend_from_slice(&f.timestamps);
            }
            generated_types::read_response::frame::Data::UnsignedPoints(f) => {
                // Get field key associated with previous series frame.
                let (current_table, prev_series_frame) = current_table_frame.as_mut().unwrap();
                let column = current_table.field_column(field_name(prev_series_frame));

                let values = f.values.iter().copied().map(Some).collect::<Vec<_>>();
                column.extend_u64(&values);

                let time_column = &mut current_table.time_column;
                time_column.extend_from_slice(&f.timestamps);
            }
            generated_types::read_response::frame::Data::BooleanPoints(f) => {
                // Get field key associated with previous series frame.
                let (current_table, prev_series_frame) = current_table_frame.as_mut().unwrap();
                let column = current_table.field_column(field_name(prev_series_frame));

                let values = f.values.iter().copied().map(Some).collect::<Vec<_>>();
                column.extend_bool(&values);

                let time_column = &mut current_table.time_column;
                time_column.extend_from_slice(&f.timestamps);
            }
            generated_types::read_response::frame::Data::StringPoints(f) => {
                // Get field key associated with previous series frame.
                let (current_table, prev_series_frame) = current_table_frame.as_mut().unwrap();
                let column = current_table.field_column(field_name(prev_series_frame));

                let values = f
                    .values
                    .iter()
                    .map(|x| Some(x.to_owned()))
                    .collect::<Vec<_>>();
                column.extend_string(&values);

                let time_column = &mut current_table.time_column;
                time_column.extend_from_slice(&f.timestamps);
            }
        };

        // If the current frame contained field values/timestamps then we need
        // pad all the other columns with either values or NULL so that all
        // columns remain the same length.
        //
        let (current_table, prev_series_frame) = current_table_frame.as_mut().unwrap();
        let max_rows = current_table.max_rows();

        // Pad all tag columns with keys present in the previous series frame
        // with identical values.
        for Tag { key, value } in &prev_series_frame.tags {
            if tag_key_is_measurement(key) || tag_key_is_field(key) {
                continue;
            }

            let idx = current_table
                .tag_columns
                .get(key)
                .expect("tag column mapping to be present");

            let column = &mut current_table.column_data[*idx];
            let column_rows = column.len();
            assert!(max_rows >= column_rows);
            column.pad_tag(
                String::from_utf8(value.to_owned()).context(InvalidTagValueSnafu)?,
                max_rows - column_rows,
            );
        }

        // Pad all tag columns that were not present in the previous series
        // frame with NULL.
        for (_, &idx) in &current_table.tag_columns {
            let column = &mut current_table.column_data[idx];
            let column_rows = column.len();
            if column_rows < max_rows {
                column.pad_none(max_rows - column_rows);
            }
        }

        // Pad all field columns with NULL such that they're the same length as
        // the largest column.
        for (_, &idx) in &current_table.field_columns {
            let column = &mut current_table.column_data[idx];
            let column_rows = column.len();
            if column_rows < max_rows {
                column.pad_none(max_rows - column_rows);
            }
        }
    }

    // Convert and insert current table
    let (current_table, prev_series_frame) = current_table_frame.take().unwrap();
    let rb: RecordBatch = current_table.try_into()?;
    all_tables.insert(
        String::from_utf8(measurement(&prev_series_frame).to_owned())
            .context(InvalidMeasurementNameSnafu)?,
        rb,
    );

    Ok(all_tables)
}

#[derive(Debug)]
enum ColumnData {
    Float(Vec<Option<f64>>),
    Integer(Vec<Option<i64>>),
    Unsigned(Vec<Option<u64>>),
    Boolean(Vec<Option<bool>>),
    String(Vec<Option<String>>),
    Tag(Vec<Option<String>>),
}

impl ColumnData {
    fn pad_tag(&mut self, value: String, additional: usize) {
        if let Self::Tag(data) = self {
            data.extend(iter::repeat(Some(value)).take(additional));
        } else {
            unreachable!("can't pad strings into {:?} column", self)
        }
    }

    fn pad_none(&mut self, additional: usize) {
        match self {
            Self::Float(data) => data.extend(iter::repeat(None).take(additional)),
            Self::Integer(data) => data.extend(iter::repeat(None).take(additional)),
            Self::Unsigned(data) => data.extend(iter::repeat(None).take(additional)),
            Self::Boolean(data) => data.extend(iter::repeat(None).take(additional)),
            Self::String(data) => data.extend(iter::repeat(None).take(additional)),
            Self::Tag(data) => data.extend(iter::repeat(None).take(additional)),
        }
    }

    fn extend_f64(&mut self, arr: &[Option<f64>]) {
        if let Self::Float(data) = self {
            data.extend_from_slice(arr);
        } else {
            unreachable!("can't extend {:?} column with floats", self)
        }
    }

    fn extend_i64(&mut self, arr: &[Option<i64>]) {
        if let Self::Integer(data) = self {
            data.extend_from_slice(arr);
        } else {
            unreachable!("can't extend {:?} column with integers", self)
        }
    }

    fn extend_u64(&mut self, arr: &[Option<u64>]) {
        if let Self::Unsigned(data) = self {
            data.extend_from_slice(arr);
        } else {
            unreachable!("can't extend {:?} column with unsigned integers", self)
        }
    }

    fn extend_bool(&mut self, arr: &[Option<bool>]) {
        if let Self::Boolean(data) = self {
            data.extend_from_slice(arr);
        } else {
            unreachable!("can't extend {:?} column with bools", self)
        }
    }

    fn extend_string(&mut self, arr: &[Option<String>]) {
        if let Self::String(data) = self {
            data.extend_from_slice(arr);
        } else {
            unreachable!("can't extend {:?} column with strings", self)
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Float(arr) => arr.len(),
            Self::Integer(arr) => arr.len(),
            Self::Unsigned(arr) => arr.len(),
            Self::Boolean(arr) => arr.len(),
            Self::String(arr) => arr.len(),
            Self::Tag(arr) => arr.len(),
        }
    }
}

#[derive(Debug)]
struct IntermediateTable {
    schema: Schema,

    // constant-time access to the correct column from a tag or field key
    tag_columns: HashMap<Vec<u8>, usize>,
    field_columns: HashMap<Vec<u8>, usize>,

    column_data: Vec<ColumnData>,
    time_column: Vec<i64>,
}

impl IntermediateTable {
    fn try_new(table_columns: TableColumns) -> Result<Self, Error> {
        let mut schema_builder = SchemaBuilder::new();
        let mut tag_columns = HashMap::new();
        let mut field_columns = HashMap::new();
        let mut column_data = vec![];

        // First add the tag columns to the schema and column data.
        for tag_key in table_columns.tag_columns {
            let column_name = String::from_utf8(tag_key.clone()).context(InvalidTagKeySnafu)?;
            schema_builder.influx_column(&column_name, InfluxColumnType::Tag);

            // track position of column
            tag_columns.insert(tag_key, column_data.len());
            column_data.push(ColumnData::Tag(vec![]));
        }

        // Then add the field columns to the schema and column data.
        for (field_key, data_type) in table_columns.field_columns {
            let column_name = String::from_utf8(field_key.clone()).context(InvalidTagKeySnafu)?;
            schema_builder.influx_column(
                &column_name,
                InfluxColumnType::Field(match data_type {
                    DataType::Float => InfluxFieldType::Float,
                    DataType::Integer => InfluxFieldType::Integer,
                    DataType::Unsigned => InfluxFieldType::UInteger,
                    DataType::Boolean => InfluxFieldType::Boolean,
                    DataType::String => InfluxFieldType::String,
                }),
            );

            // track position of column
            field_columns.insert(field_key, column_data.len());
            column_data.push(match data_type {
                DataType::Float => ColumnData::Float(vec![]),
                DataType::Integer => ColumnData::Integer(vec![]),
                DataType::Unsigned => ColumnData::Unsigned(vec![]),
                DataType::Boolean => ColumnData::Boolean(vec![]),
                DataType::String => ColumnData::String(vec![]),
            });
        }

        // Finally add the timestamp column.
        schema_builder.influx_column("time", InfluxColumnType::Timestamp);
        let time_column = vec![];

        Ok(Self {
            schema: schema_builder.build().context(SchemaBuildingSnafu)?,
            tag_columns,
            field_columns,
            column_data,
            time_column,
        })
    }

    fn field_column(&mut self, field: &[u8]) -> &mut ColumnData {
        let idx = self
            .field_columns
            .get(field)
            .expect("field column mapping to be present");

        &mut self.column_data[*idx]
    }

    // Returns the number of rows in the largest column. Useful for padding the
    // rest of the columns out.
    fn max_rows(&self) -> usize {
        self.column_data
            .iter()
            .map(|c| c.len())
            .max()
            .unwrap_or_default()
    }
}

impl TryFrom<IntermediateTable> for RecordBatch {
    type Error = Error;

    fn try_from(table: IntermediateTable) -> Result<Self, Self::Error> {
        let arrow_schema: arrow::datatypes::SchemaRef = table.schema.into();

        let mut rb_columns: Vec<Arc<dyn arrow::array::Array>> =
            Vec::with_capacity(&table.column_data.len() + 1); // + time column

        for col in table.column_data {
            match col {
                ColumnData::Integer(v) => {
                    rb_columns.push(Arc::new(arrow::array::Int64Array::from(v)));
                }
                ColumnData::Unsigned(v) => {
                    rb_columns.push(Arc::new(arrow::array::UInt64Array::from(v)));
                }
                ColumnData::Float(v) => {
                    rb_columns.push(Arc::new(arrow::array::Float64Array::from(v)));
                }
                ColumnData::String(v) => {
                    rb_columns.push(Arc::new(arrow::array::StringArray::from(
                        v.iter().map(|s| s.as_deref()).collect::<Vec<_>>(),
                    )));
                }
                ColumnData::Boolean(v) => {
                    rb_columns.push(Arc::new(arrow::array::BooleanArray::from(v)));
                }
                ColumnData::Tag(v) => {
                    rb_columns.push(Arc::new(arrow::array::DictionaryArray::<
                        arrow::datatypes::Int32Type,
                    >::from_iter(
                        v.iter().map(|s| s.as_deref())
                    )));
                }
            }
        }

        // time column
        rb_columns.push(Arc::new(arrow::array::TimestampNanosecondArray::from(
            table.time_column,
        )));

        Self::try_new(arrow_schema, rb_columns).context(ArrowSnafu)
    }
}

// These constants describe known values for the keys associated with
// measurements and fields.
const MEASUREMENT_TAG_KEY_TEXT: [u8; 12] = [
    b'_', b'm', b'e', b'a', b's', b'u', b'r', b'e', b'm', b'e', b'n', b't',
];
pub(crate) const MEASUREMENT_TAG_KEY_BIN: [u8; 1] = [0_u8];
const FIELD_TAG_KEY_TEXT: [u8; 6] = [b'_', b'f', b'i', b'e', b'l', b'd'];
pub(crate) const FIELD_TAG_KEY_BIN: [u8; 1] = [255_u8];

// Store a collection of column names and types for a single table (measurement).
#[derive(Debug, Default, PartialEq, Eq)]
struct TableColumns {
    tag_columns: BTreeSet<Vec<u8>>,
    field_columns: BTreeMap<Vec<u8>, DataType>,
}

// Given a set of data frames determine from the series frames within the set
// of tag columns for each distinct table (measurement).
fn determine_tag_columns(frames: &[Data]) -> BTreeMap<Vec<u8>, TableColumns> {
    let mut schema: BTreeMap<Vec<u8>, TableColumns> = BTreeMap::new();
    for frame in frames {
        if let Data::Series(sf) = frame {
            assert!(!sf.tags.is_empty(), "expected _measurement and _field tags");

            // PERF: avoid clone of value
            let measurement_name = sf
                .tags
                .iter()
                .find(|t| tag_key_is_measurement(&t.key))
                .expect("measurement name not found")
                .value
                .clone();
            let table = schema.entry(measurement_name).or_default();

            for Tag { key, value } in sf.tags.iter().skip(1) {
                if tag_key_is_field(key) {
                    table.field_columns.insert(value.clone(), sf.data_type());
                    continue;
                }

                // PERF: avoid clone of key
                table.tag_columns.insert(key.clone()); // Add column to table schema
            }
        }
    }
    schema
}

// Extract a reference to the measurement name from a Series frame.
fn measurement(frame: &SeriesFrame) -> &Vec<u8> {
    assert!(tag_key_is_measurement(&frame.tags[0].key));
    &frame.tags[0].value
}

// Extract a reference to the field name from a Series frame.
fn field_name(frame: &SeriesFrame) -> &Vec<u8> {
    let idx = frame.tags.len() - 1;
    assert!(tag_key_is_field(&frame.tags[idx].key));
    &frame.tags[idx].value
}

pub(crate) fn tag_key_is_measurement(key: &[u8]) -> bool {
    (key == MEASUREMENT_TAG_KEY_TEXT) || (key == MEASUREMENT_TAG_KEY_BIN)
}

pub(crate) fn tag_key_is_field(key: &[u8]) -> bool {
    (key == FIELD_TAG_KEY_TEXT) || (key == FIELD_TAG_KEY_BIN)
}

#[cfg(test)]
mod test_super {
    use arrow::util::pretty::pretty_format_batches;
    use generated_types::read_response::{
        BooleanPointsFrame, FloatPointsFrame, IntegerPointsFrame, SeriesFrame, StringPointsFrame,
        UnsignedPointsFrame,
    };

    use super::*;

    // converts a vector of key/value pairs into a vector of `Tag`.
    fn make_tags(pairs: &[(&str, &str)]) -> Vec<Tag> {
        pairs
            .iter()
            .map(|(key, value)| Tag {
                key: key.as_bytes().to_vec(),
                value: value.as_bytes().to_vec(),
            })
            .collect::<Vec<_>>()
    }

    struct TableColumnInput<'a> {
        measurement: &'a str,
        tags: &'a [&'a str],
        fields: &'a [(&'a str, DataType)],
    }

    impl<'a> TableColumnInput<'a> {
        fn new(measurement: &'a str, tags: &'a [&str], fields: &'a [(&str, DataType)]) -> Self {
            Self {
                measurement,
                tags,
                fields,
            }
        }
    }

    // converts a vector of key/value tag pairs and a field datatype into a
    // collection of `TableColumns` objects.
    fn make_table_columns(input: &'_ [TableColumnInput<'_>]) -> BTreeMap<Vec<u8>, TableColumns> {
        let mut all_table_columns = BTreeMap::new();
        for TableColumnInput {
            measurement,
            tags,
            fields,
        } in input
        {
            let tag_columns = tags
                .iter()
                .map(|c| c.as_bytes().to_vec())
                .collect::<Vec<Vec<u8>>>();

            let mut tag_columns_set = BTreeSet::new();
            for c in tag_columns {
                tag_columns_set.insert(c);
            }

            let mut field_columns = BTreeMap::new();
            for (field, data_type) in *fields {
                field_columns.insert(field.as_bytes().to_vec(), *data_type);
            }

            let table_columns = TableColumns {
                tag_columns: tag_columns_set,
                field_columns,
            };

            all_table_columns.insert(measurement.as_bytes().to_vec(), table_columns);
        }
        all_table_columns
    }

    // generate a substantial set of frames across multiple tables.
    fn gen_frames() -> Vec<Data> {
        vec![
            Data::Series(SeriesFrame {
                tags: make_tags(&[
                    ("_measurement", "cpu"),
                    ("host", "foo"),
                    ("server", "a"),
                    ("_field", "temp"),
                ]),
                data_type: DataType::Float as i32,
            }),
            Data::FloatPoints(FloatPointsFrame {
                timestamps: vec![1, 2, 3, 4],
                values: vec![1.1, 2.2, 3.3, 4.4],
            }),
            Data::FloatPoints(FloatPointsFrame {
                timestamps: vec![5, 6, 7, 10],
                values: vec![5.1, 5.2, 5.3, 10.4],
            }),
            Data::Series(SeriesFrame {
                tags: make_tags(&[
                    ("_measurement", "cpu"),
                    ("host", "foo"),
                    ("server", "a"),
                    ("_field", "voltage"),
                ]),
                data_type: DataType::Integer as i32,
            }),
            Data::IntegerPoints(IntegerPointsFrame {
                timestamps: vec![1, 2],
                values: vec![22, 22],
            }),
            Data::Series(SeriesFrame {
                tags: make_tags(&[
                    ("_measurement", "cpu"),
                    ("host", "foo"),
                    ("new_column", "a"),
                    ("_field", "voltage"),
                ]),
                data_type: DataType::Integer as i32,
            }),
            Data::IntegerPoints(IntegerPointsFrame {
                timestamps: vec![100, 200],
                values: vec![1000, 2000],
            }),
            Data::Series(SeriesFrame {
                tags: make_tags(&[("_measurement", "another table"), ("_field", "voltage")]),
                data_type: DataType::String as i32,
            }),
            Data::StringPoints(StringPointsFrame {
                timestamps: vec![200, 201],
                values: vec!["hello".to_string(), "abc".to_string()],
            }),
            Data::Series(SeriesFrame {
                tags: make_tags(&[
                    ("_measurement", "another table"),
                    ("region", "west"),
                    ("_field", "voltage"),
                ]),
                data_type: DataType::String as i32,
            }),
            Data::StringPoints(StringPointsFrame {
                timestamps: vec![302, 304],
                values: vec!["foo".to_string(), "bar".to_string()],
            }),
            Data::Series(SeriesFrame {
                tags: make_tags(&[
                    ("_measurement", "another table"),
                    ("region", "north"),
                    ("_field", "bool_field"),
                ]),
                data_type: DataType::Boolean as i32,
            }),
            Data::BooleanPoints(BooleanPointsFrame {
                timestamps: vec![1000],
                values: vec![true],
            }),
            Data::Series(SeriesFrame {
                tags: make_tags(&[
                    ("_measurement", "another table"),
                    ("region", "south"),
                    ("_field", "unsigned_field"),
                ]),
                data_type: DataType::Unsigned as i32,
            }),
            Data::UnsignedPoints(UnsignedPointsFrame {
                timestamps: vec![2000],
                values: vec![600],
            }),
        ]
    }

    #[test]
    fn test_determine_tag_columns() {
        assert!(determine_tag_columns(&[]).is_empty());

        let frame = Data::Series(SeriesFrame {
            tags: make_tags(&[("_measurement", "cpu"), ("server", "a"), ("_field", "temp")]),
            data_type: DataType::Float as i32,
        });

        let exp = make_table_columns(&[TableColumnInput::new(
            "cpu",
            &["server"],
            &[("temp", DataType::Float)],
        )]);
        assert_eq!(determine_tag_columns(&[frame]), exp);

        // larger example
        let frames = gen_frames();

        let exp = make_table_columns(&[
            TableColumnInput::new(
                "cpu",
                &["host", "new_column", "server"],
                &[("temp", DataType::Float), ("voltage", DataType::Integer)],
            ),
            TableColumnInput::new(
                "another table",
                &["region"],
                &[
                    ("bool_field", DataType::Boolean),
                    ("unsigned_field", DataType::Unsigned),
                    ("voltage", DataType::String),
                ],
            ),
        ]);
        assert_eq!(determine_tag_columns(&frames), exp);
    }

    #[test]
    fn test_frames_to_into_record_batches() {
        let frames = gen_frames();

        let rbs = frames_to_record_batches(&frames);
        let exp = vec![
            (
                "another table",
                vec![
                    "+--------+------------+----------------+---------+-------------------------------+",
                    "| region | bool_field | unsigned_field | voltage | time                          |",
                    "+--------+------------+----------------+---------+-------------------------------+",
                    "|        |            |                | hello   | 1970-01-01 00:00:00.000000200 |",
                    "|        |            |                | abc     | 1970-01-01 00:00:00.000000201 |",
                    "| west   |            |                | foo     | 1970-01-01 00:00:00.000000302 |",
                    "| west   |            |                | bar     | 1970-01-01 00:00:00.000000304 |",
                    "| north  | true       |                |         | 1970-01-01 00:00:00.000001    |",
                    "| south  |            | 600            |         | 1970-01-01 00:00:00.000002    |",
                    "+--------+------------+----------------+---------+-------------------------------+",
                ],
            ),
            (
                "cpu",
                vec![
                "+------+------------+--------+------+---------+-------------------------------+",
                "| host | new_column | server | temp | voltage | time                          |",
                "+------+------------+--------+------+---------+-------------------------------+",
                "| foo  |            | a      | 1.1  |         | 1970-01-01 00:00:00.000000001 |",
                "| foo  |            | a      | 2.2  |         | 1970-01-01 00:00:00.000000002 |",
                "| foo  |            | a      | 3.3  |         | 1970-01-01 00:00:00.000000003 |",
                "| foo  |            | a      | 4.4  |         | 1970-01-01 00:00:00.000000004 |",
                "| foo  |            | a      | 5.1  |         | 1970-01-01 00:00:00.000000005 |",
                "| foo  |            | a      | 5.2  |         | 1970-01-01 00:00:00.000000006 |",
                "| foo  |            | a      | 5.3  |         | 1970-01-01 00:00:00.000000007 |",
                "| foo  |            | a      | 10.4 |         | 1970-01-01 00:00:00.000000010 |",
                "| foo  |            | a      |      | 22      | 1970-01-01 00:00:00.000000001 |",
                "| foo  |            | a      |      | 22      | 1970-01-01 00:00:00.000000002 |",
                "| foo  | a          |        |      | 1000    | 1970-01-01 00:00:00.000000100 |",
                "| foo  | a          |        |      | 2000    | 1970-01-01 00:00:00.000000200 |",
                "+------+------------+--------+------+---------+-------------------------------+",
            ],
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.join("\n")))
        .collect::<BTreeMap<String, String>>();

        let got = rbs
            .unwrap()
            .into_iter()
            .map(|(k, v)| {
                let table: String = pretty_format_batches(&[v]).unwrap().to_string();
                (k, table)
            })
            .collect::<BTreeMap<String, String>>();
        assert_eq!(got, exp);
    }
}
