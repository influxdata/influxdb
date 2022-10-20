//! This module contains code that "unpivots" annotated
//! [`RecordBatch`]es to [`Series`] and [`Group`]s for output by the
//! storage gRPC interface

use arrow::{
    self,
    array::{as_boolean_array, Array, ArrayRef, BooleanArray, DictionaryArray, StringArray},
    compute,
    datatypes::{DataType, Int32Type},
    record_batch::RecordBatch,
};
use datafusion::physical_plan::{common::collect, SendableRecordBatchStream};

use snafu::{OptionExt, ResultExt, Snafu};
use std::sync::Arc;
use tokio::sync::mpsc::error::SendError;

use crate::exec::{
    field::{self, FieldColumns, FieldIndexes},
    seriesset::series::Group,
};

use super::{
    series::{Either, Series},
    SeriesSet,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Plan Execution Error: {}", source))]
    Execution {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[snafu(display(
        "Error reading record batch while converting from SeriesSet: {:?}",
        source
    ))]
    Reading {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Error concatenating record batch while converting from SeriesSet: {:?}",
        source
    ))]
    Concatenating { source: arrow::error::ArrowError },

    #[snafu(display("Internal field error while converting series set: {}", source))]
    InternalField { source: field::Error },

    #[snafu(display("Internal error finding grouping colum: {}", column_name))]
    FindingGroupColumn { column_name: String },

    #[snafu(display("Sending series set results during conversion: {:?}", source))]
    SendingDuringConversion {
        source: Box<SendError<Result<SeriesSet>>>,
    },

    #[snafu(display("Sending grouped series set results during conversion: {:?}", source))]
    SendingDuringGroupedConversion {
        source: Box<SendError<Result<SeriesSet>>>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// Handles converting record batches into SeriesSets
#[derive(Debug, Default)]
pub struct SeriesSetConverter {}

impl SeriesSetConverter {
    /// Convert the results from running a DataFusion plan into the
    /// appropriate SeriesSetItems.
    ///
    /// The results must be in the logical format described in this
    /// module's documentation (i.e. ordered by tag keys)
    ///
    /// table_name: The name of the table
    ///
    /// tag_columns: The names of the columns that define tags
    ///
    /// field_columns: The names of the columns which are "fields"
    ///
    /// it: record batch iterator that produces data in the desired order
    pub async fn convert(
        &mut self,
        table_name: Arc<str>,
        tag_columns: Arc<Vec<Arc<str>>>,
        field_columns: FieldColumns,
        it: SendableRecordBatchStream,
    ) -> Result<Vec<SeriesSet>, Error> {
        // for now, this logic only handles a single `RecordBatch` so
        // concat data together.
        //
        // proper streaming support tracked by:
        // https://github.com/influxdata/influxdb_iox/issues/4445
        let batches = collect(it).await.context(ReadingSnafu)?;

        let batch = if !batches.is_empty() {
            compute::concat_batches(&batches[0].schema(), &batches).context(ConcatenatingSnafu)?
        } else {
            return Ok(vec![]);
        };

        let schema = batch.schema();
        // TODO: check that the tag columns are sorted by tag name...
        let tag_indexes =
            FieldIndexes::names_to_indexes(&schema, &tag_columns).context(InternalFieldSnafu)?;
        let field_indexes = FieldIndexes::from_field_columns(&schema, &field_columns)
            .context(InternalFieldSnafu)?;

        // Algorithm: compute, via bitsets, the rows at which each
        // tag column changes and thereby where the tagset
        // changes. Emit a new SeriesSet at each such transition
        let tag_transitions = tag_indexes
            .iter()
            .map(|&col| Self::compute_transitions(&batch, col))
            .collect::<Vec<_>>();

        // no tag columns, emit a single tagset
        let intersections: Vec<usize> = if tag_transitions.is_empty() {
            vec![batch.num_rows()]
        } else {
            // OR bitsets together to to find all rows where the
            // keyset (values of the tag keys) changes
            let mut tag_transitions_it = tag_transitions.into_iter();
            let init = tag_transitions_it.next().expect("not empty");
            let intersections = tag_transitions_it.fold(init, |a, b| {
                Arc::new(
                    compute::or(as_boolean_array(&a), as_boolean_array(&b)).expect("or operation"),
                )
            });

            as_boolean_array(&intersections)
                .iter()
                .enumerate()
                .filter(|(_idx, mask)| mask.unwrap_or(true))
                .map(|(idx, _mask)| idx)
                .chain(std::iter::once(batch.num_rows()))
                .collect()
        };

        let mut start_row: usize = 0;

        // create each series (since bitmap are not Send, we can't
        // call await during the loop)

        // emit each series
        let series_sets = intersections
            .into_iter()
            .map(|end_row| {
                let series_set = SeriesSet {
                    table_name: Arc::clone(&table_name),
                    tags: Self::get_tag_keys(&batch, start_row, &tag_columns, &tag_indexes),
                    field_indexes: field_indexes.clone(),
                    start_row,
                    num_rows: (end_row - start_row),
                    batch: batch.clone(),
                };

                start_row = end_row;
                series_set
            })
            .collect();

        Ok(series_sets)
    }

    /// returns a bitset with all row indexes where the value of the
    /// batch `col_idx` changes.  Does not include row 0, always includes
    /// the last row, `batch.num_rows() - 1`
    ///
    /// Note: This may return false positives in the presence of dictionaries
    /// containing duplicates
    fn compute_transitions(batch: &RecordBatch, col_idx: usize) -> ArrayRef {
        let num_rows = batch.num_rows();

        if num_rows == 0 {
            return Arc::new(BooleanArray::builder(0).finish());
        }

        let col = batch.column(col_idx);

        compute::concat(&[
            &{
                let mut b = BooleanArray::builder(1);
                b.append_value(false);
                b.finish()
            },
            &compute::neq_dyn(&col.slice(0, col.len() - 1), &col.slice(1, col.len() - 1))
                .expect("cmp"),
        ])
        .expect("concat")
    }

    /// Creates (column_name, column_value) pairs for each column
    /// named in `tag_column_name` at the corresponding index
    /// `tag_indexes`
    fn get_tag_keys(
        batch: &RecordBatch,
        row: usize,
        tag_column_names: &[Arc<str>],
        tag_indexes: &[usize],
    ) -> Vec<(Arc<str>, Arc<str>)> {
        assert_eq!(tag_column_names.len(), tag_indexes.len());

        tag_column_names
            .iter()
            .zip(tag_indexes)
            .filter_map(|(column_name, column_index)| {
                let col = batch.column(*column_index);
                let tag_value = match col.data_type() {
                    DataType::Utf8 => {
                        let col = col.as_any().downcast_ref::<StringArray>().unwrap();

                        if col.is_valid(row) {
                            Some(col.value(row).to_string())
                        } else {
                            None
                        }
                    }
                    DataType::Dictionary(key, value)
                        if key.as_ref() == &DataType::Int32
                            && value.as_ref() == &DataType::Utf8 =>
                    {
                        let col = col
                            .as_any()
                            .downcast_ref::<DictionaryArray<Int32Type>>()
                            .expect("Casting column");

                        if col.is_valid(row) {
                            let key = col.keys().value(row);
                            let value = col
                                .values()
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap()
                                .value(key as _)
                                .to_string();
                            Some(value)
                        } else {
                            None
                        }
                    }
                    _ => unimplemented!(
                        "Series get_tag_keys not supported for type {:?} in column {:?}",
                        col.data_type(),
                        batch.schema().fields()[*column_index]
                    ),
                };

                tag_value.map(|tag_value| (Arc::clone(column_name), Arc::from(tag_value.as_str())))
            })
            .collect()
    }
}

/// Reorders and groups a sequence of Series is grouped correctly
#[derive(Debug)]
pub struct GroupGenerator {
    group_columns: Vec<Arc<str>>,
}

impl GroupGenerator {
    pub fn new(group_columns: Vec<Arc<str>>) -> Self {
        Self { group_columns }
    }

    /// groups the set of `series` into SeriesOrGroups
    pub fn group(&self, series: Vec<Series>) -> Result<Vec<Either>> {
        let mut series = series
            .into_iter()
            .map(|series| SortableSeries::try_new(series, &self.group_columns))
            .collect::<Result<Vec<_>>>()?;

        // Potential optimization is to skip this sort if we are
        // grouping by a prefix of the tags for a single measurement
        //
        // Another potential optimization is if we are only grouping on
        // tag columns is to change the the actual plan output using
        // DataFusion to sort the data in the required group (likely
        // only possible with a single table)

        // Resort the data according to group key values
        series.sort();

        // now find the groups boundaries and emit the output
        let mut last_partition_key_vals: Option<Vec<Arc<str>>> = None;

        // Note that if there are no group columns, we still need to
        // sort by the tag keys, so that the output is sorted by tag
        // keys, and thus we can't bail out early here
        //
        // Interesting, it isn't clear flux requires this ordering, but
        // it is what TSM does so we preserve the behavior
        let mut output = vec![];

        // TODO make this more functional (issue is that sometimes the
        // loop inserts one item into `output` and sometimes it inserts 2)
        for SortableSeries {
            series,
            tag_vals,
            num_partition_keys,
        } in series.into_iter()
        {
            // keep only the values that form the group
            let mut partition_key_vals = tag_vals;
            partition_key_vals.truncate(num_partition_keys);

            // figure out if we are in a new group (partition key values have changed)
            let need_group_start = match &last_partition_key_vals {
                None => true,
                Some(last_partition_key_vals) => &partition_key_vals != last_partition_key_vals,
            };

            if need_group_start {
                last_partition_key_vals = Some(partition_key_vals.clone());

                let tag_keys = series.tags.iter().map(|tag| Arc::clone(&tag.key)).collect();

                let group = Group {
                    tag_keys,
                    partition_key_vals,
                };

                output.push(group.into());
            }

            output.push(series.into())
        }

        Ok(output)
    }
}

#[derive(Debug)]
/// Wrapper around a Series that has the values of the group_by columns extracted
struct SortableSeries {
    series: Series,

    /// All the tag values, reordered so that the group_columns are first
    tag_vals: Vec<Arc<str>>,

    /// How many of the first N tag_values are used for the partition key
    num_partition_keys: usize,
}

impl PartialEq for SortableSeries {
    fn eq(&self, other: &Self) -> bool {
        self.tag_vals.eq(&other.tag_vals)
    }
}

impl Eq for SortableSeries {}

impl PartialOrd for SortableSeries {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.tag_vals.partial_cmp(&other.tag_vals)
    }
}

impl Ord for SortableSeries {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.tag_vals.cmp(&other.tag_vals)
    }
}

impl SortableSeries {
    fn try_new(series: Series, group_columns: &[Arc<str>]) -> Result<Self> {
        // Compute the order of new tag values
        let tags = &series.tags;

        // tag_used_set[i] is true if we have used the value in tag_columns[i]
        let mut tag_used_set = vec![false; tags.len()];

        // put the group columns first
        //
        // Note that this is an O(N^2) algorithm. We are assuming the
        // number of tag columns is reasonably small
        let mut tag_vals: Vec<_> = group_columns
            .iter()
            .map(|col| {
                tags.iter()
                    .enumerate()
                    // Searching for columns linearly is likely to be pretty slow....
                    .find(|(_i, tag)| tag.key == *col)
                    .map(|(i, tag)| {
                        assert!(!tag_used_set[i], "repeated group column");
                        tag_used_set[i] = true;
                        Arc::clone(&tag.value)
                    })
                    .or_else(|| {
                        // treat these specially and use value "" to mirror what TSM does
                        // see https://github.com/influxdata/influxdb_iox/issues/2693#issuecomment-947695442
                        // for more details
                        if col.as_ref() == "_start" || col.as_ref() == "_stop" {
                            Some(Arc::from(""))
                        } else {
                            None
                        }
                    })
                    .context(FindingGroupColumnSnafu {
                        column_name: col.as_ref(),
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        // Fill in all remaining tags
        tag_vals.extend(tags.iter().enumerate().filter_map(|(i, tag)| {
            let use_tag = !tag_used_set[i];
            use_tag.then(|| Arc::clone(&tag.value))
        }));

        Ok(Self {
            series,
            tag_vals,
            num_partition_keys: group_columns.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{ArrayRef, Float64Array, Int64Array, TimestampNanosecondArray},
        csv,
        datatypes::DataType,
        datatypes::Field,
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
    };
    use arrow_util::assert_batches_eq;
    use datafusion_util::{stream_from_batch, stream_from_batches, stream_from_schema};
    use test_helpers::{str_pair_vec_to_vec, str_vec_to_arc_vec};

    use super::*;

    #[tokio::test]
    async fn test_convert_empty() {
        let schema = Arc::new(Schema::new(vec![]));
        let empty_iterator = stream_from_schema(schema);

        let table_name = "foo";
        let tag_columns = [];
        let field_columns = [];

        let results = convert(table_name, &tag_columns, &field_columns, empty_iterator).await;
        assert_eq!(results.len(), 0);
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("tag_a", DataType::Utf8, true),
            Field::new("tag_b", DataType::Utf8, true),
            Field::new("float_field", DataType::Float64, true),
            Field::new("int_field", DataType::Int64, true),
            Field::new("time", DataType::Int64, false),
        ]))
    }

    #[tokio::test]
    async fn test_convert_single_series_no_tags() {
        // single series
        let schema = test_schema();
        let inputs = parse_to_iterators(schema, &["one,ten,10.0,1,1000", "one,ten,10.1,2,2000"]);
        for (i, input) in inputs.into_iter().enumerate() {
            println!("Stream {}", i);

            let table_name = "foo";
            let tag_columns = [];
            let field_columns = ["float_field"];
            let results = convert(table_name, &tag_columns, &field_columns, input).await;

            assert_eq!(results.len(), 1);
            let series_set = &results[0];

            assert_eq!(series_set.table_name.as_ref(), "foo");
            assert!(series_set.tags.is_empty());
            assert_eq!(
                series_set.field_indexes,
                FieldIndexes::from_timestamp_and_value_indexes(4, &[2])
            );
            assert_eq!(series_set.start_row, 0);
            assert_eq!(series_set.num_rows, 2);

            // Test that the record batch made it through
            let expected_data = vec![
                "+-------+-------+-------------+-----------+------+",
                "| tag_a | tag_b | float_field | int_field | time |",
                "+-------+-------+-------------+-----------+------+",
                "| one   | ten   | 10          | 1         | 1000 |",
                "| one   | ten   | 10.1        | 2         | 2000 |",
                "+-------+-------+-------------+-----------+------+",
            ];

            assert_batches_eq!(expected_data, &[series_set.batch.clone()]);
        }
    }

    #[tokio::test]
    async fn test_convert_single_series_no_tags_nulls() {
        // single series
        let schema = test_schema();

        let inputs = parse_to_iterators(schema, &["one,ten,10.0,,1000", "one,ten,10.1,,2000"]);

        // send no values in the int_field colum
        for (i, input) in inputs.into_iter().enumerate() {
            println!("Stream {}", i);

            let table_name = "foo";
            let tag_columns = [];
            let field_columns = ["float_field"];
            let results = convert(table_name, &tag_columns, &field_columns, input).await;

            assert_eq!(results.len(), 1);
            let series_set = &results[0];

            assert_eq!(series_set.table_name.as_ref(), "foo");
            assert!(series_set.tags.is_empty());
            assert_eq!(
                series_set.field_indexes,
                FieldIndexes::from_timestamp_and_value_indexes(4, &[2])
            );
            assert_eq!(series_set.start_row, 0);
            assert_eq!(series_set.num_rows, 2);

            // Test that the record batch made it through
            let expected_data = vec![
                "+-------+-------+-------------+-----------+------+",
                "| tag_a | tag_b | float_field | int_field | time |",
                "+-------+-------+-------------+-----------+------+",
                "| one   | ten   | 10          |           | 1000 |",
                "| one   | ten   | 10.1        |           | 2000 |",
                "+-------+-------+-------------+-----------+------+",
            ];

            assert_batches_eq!(expected_data, &[series_set.batch.clone()]);
        }
    }

    #[tokio::test]
    async fn test_convert_single_series_one_tag() {
        // single series
        let schema = test_schema();
        let inputs = parse_to_iterators(schema, &["one,ten,10.0,1,1000", "one,ten,10.1,2,2000"]);

        for (i, input) in inputs.into_iter().enumerate() {
            println!("Stream {}", i);

            // test with one tag column, one series
            let table_name = "bar";
            let tag_columns = ["tag_a"];
            let field_columns = ["float_field"];
            let results = convert(table_name, &tag_columns, &field_columns, input).await;

            assert_eq!(results.len(), 1);
            let series_set = &results[0];

            assert_eq!(series_set.table_name.as_ref(), "bar");
            assert_eq!(series_set.tags, str_pair_vec_to_vec(&[("tag_a", "one")]));
            assert_eq!(
                series_set.field_indexes,
                FieldIndexes::from_timestamp_and_value_indexes(4, &[2])
            );
            assert_eq!(series_set.start_row, 0);
            assert_eq!(series_set.num_rows, 2);
        }
    }

    #[tokio::test]
    async fn test_convert_one_tag_multi_series() {
        let schema = test_schema();

        let inputs = parse_to_iterators(
            schema,
            &[
                "one,ten,10.0,1,1000",
                "one,ten,10.1,2,2000",
                "one,eleven,10.1,3,3000",
                "two,eleven,10.2,4,4000",
                "two,eleven,10.3,5,5000",
            ],
        );

        for (i, input) in inputs.into_iter().enumerate() {
            println!("Stream {}", i);

            let table_name = "foo";
            let tag_columns = ["tag_a"];
            let field_columns = ["int_field"];
            let results = convert(table_name, &tag_columns, &field_columns, input).await;

            assert_eq!(results.len(), 2);
            let series_set1 = &results[0];

            assert_eq!(series_set1.table_name.as_ref(), "foo");
            assert_eq!(series_set1.tags, str_pair_vec_to_vec(&[("tag_a", "one")]));
            assert_eq!(
                series_set1.field_indexes,
                FieldIndexes::from_timestamp_and_value_indexes(4, &[3])
            );
            assert_eq!(series_set1.start_row, 0);
            assert_eq!(series_set1.num_rows, 3);

            let series_set2 = &results[1];

            assert_eq!(series_set2.table_name.as_ref(), "foo");
            assert_eq!(series_set2.tags, str_pair_vec_to_vec(&[("tag_a", "two")]));
            assert_eq!(
                series_set2.field_indexes,
                FieldIndexes::from_timestamp_and_value_indexes(4, &[3])
            );
            assert_eq!(series_set2.start_row, 3);
            assert_eq!(series_set2.num_rows, 2);
        }
    }

    // two tag columns, three series
    #[tokio::test]
    async fn test_convert_two_tag_multi_series() {
        let schema = test_schema();

        let inputs = parse_to_iterators(
            schema,
            &[
                "one,ten,10.0,1,1000",
                "one,ten,10.1,2,2000",
                "one,eleven,10.1,3,3000",
                "two,eleven,10.2,4,4000",
                "two,eleven,10.3,5,5000",
            ],
        );

        for (i, input) in inputs.into_iter().enumerate() {
            println!("Stream {}", i);

            let table_name = "foo";
            let tag_columns = ["tag_a", "tag_b"];
            let field_columns = ["int_field"];
            let results = convert(table_name, &tag_columns, &field_columns, input).await;

            assert_eq!(results.len(), 3);
            let series_set1 = &results[0];

            assert_eq!(series_set1.table_name.as_ref(), "foo");
            assert_eq!(
                series_set1.tags,
                str_pair_vec_to_vec(&[("tag_a", "one"), ("tag_b", "ten")])
            );
            assert_eq!(series_set1.start_row, 0);
            assert_eq!(series_set1.num_rows, 2);

            let series_set2 = &results[1];

            assert_eq!(series_set2.table_name.as_ref(), "foo");
            assert_eq!(
                series_set2.tags,
                str_pair_vec_to_vec(&[("tag_a", "one"), ("tag_b", "eleven")])
            );
            assert_eq!(series_set2.start_row, 2);
            assert_eq!(series_set2.num_rows, 1);

            let series_set3 = &results[2];

            assert_eq!(series_set3.table_name.as_ref(), "foo");
            assert_eq!(
                series_set3.tags,
                str_pair_vec_to_vec(&[("tag_a", "two"), ("tag_b", "eleven")])
            );
            assert_eq!(series_set3.start_row, 3);
            assert_eq!(series_set3.num_rows, 2);
        }
    }

    #[tokio::test]
    async fn test_convert_two_tag_with_null_multi_series() {
        let tag_a = StringArray::from(vec!["one", "one", "one"]);
        let tag_b = StringArray::from(vec![Some("ten"), Some("ten"), None]);
        let float_field = Float64Array::from(vec![10.0, 10.1, 10.1]);
        let int_field = Int64Array::from(vec![1, 2, 3]);
        let time = TimestampNanosecondArray::from(vec![1000, 2000, 3000]);

        let batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("tag_a", Arc::new(tag_a) as ArrayRef, true),
            ("tag_b", Arc::new(tag_b), true),
            ("float_field", Arc::new(float_field), true),
            ("int_field", Arc::new(int_field), true),
            ("time", Arc::new(time), false),
        ])
        .unwrap();

        // Input has one row that has no value (NULL value) for tag_b, which is its own series
        let input = stream_from_batch(batch.schema(), batch);

        let table_name = "foo";
        let tag_columns = ["tag_a", "tag_b"];
        let field_columns = ["int_field"];
        let results = convert(table_name, &tag_columns, &field_columns, input).await;

        assert_eq!(results.len(), 2);
        let series_set1 = &results[0];

        assert_eq!(series_set1.table_name.as_ref(), "foo");
        assert_eq!(
            series_set1.tags,
            str_pair_vec_to_vec(&[("tag_a", "one"), ("tag_b", "ten")])
        );
        assert_eq!(series_set1.start_row, 0);
        assert_eq!(series_set1.num_rows, 2);

        let series_set2 = &results[1];

        assert_eq!(series_set2.table_name.as_ref(), "foo");
        assert_eq!(
            series_set2.tags,
            str_pair_vec_to_vec(&[("tag_a", "one")]) // note no value for tag_b, only one tag
        );
        assert_eq!(series_set2.start_row, 2);
        assert_eq!(series_set2.num_rows, 1);
    }

    /// Test helper: run conversion and return a Vec
    pub async fn convert<'a>(
        table_name: &'a str,
        tag_columns: &'a [&'a str],
        field_columns: &'a [&'a str],
        it: SendableRecordBatchStream,
    ) -> Vec<SeriesSet> {
        let mut converter = SeriesSetConverter::default();

        let table_name = Arc::from(table_name);
        let tag_columns = Arc::new(str_vec_to_arc_vec(tag_columns));
        let field_columns = FieldColumns::from(field_columns);

        converter
            .convert(table_name, tag_columns, field_columns, it)
            .await
            .expect("Conversion happened without error")
    }

    /// Test helper: parses the csv content into a single record batch arrow
    /// arrays columnar ArrayRef according to the schema
    fn parse_to_record_batch(schema: SchemaRef, data: &str) -> RecordBatch {
        let has_header = false;
        let delimiter = Some(b',');
        let batch_size = 1000;
        let bounds = None;
        let projection = None;
        let datetime_format = None;
        let mut reader = csv::Reader::new(
            data.as_bytes(),
            schema,
            has_header,
            delimiter,
            batch_size,
            bounds,
            projection,
            datetime_format,
        );

        let first_batch = reader.next().expect("Reading first batch");
        assert!(
            first_batch.is_ok(),
            "Can not parse record batch from csv: {:?}",
            first_batch
        );
        assert!(
            reader.next().is_none(),
            "Unexpected batch while parsing csv"
        );

        println!("batch: \n{:#?}", first_batch);

        first_batch.unwrap()
    }

    /// Parses a set of CSV lines into several `RecordBatchStream`s of varying sizes
    ///
    /// For example, with three input lines:
    /// line1
    /// line2
    /// line3
    ///
    /// This will produce two output streams:
    /// Stream1: (line1), (line2), (line3)
    /// Stream2: (line1, line2), (line3)
    fn parse_to_iterators(schema: SchemaRef, lines: &[&str]) -> Vec<SendableRecordBatchStream> {
        println!("** Input data:\n{:#?}\n\n", lines);
        (1..lines.len())
            .map(|chunk_size| {
                println!("Chunk size {}", chunk_size);
                // make record batches of each line
                let batches: Vec<_> = lines
                    .chunks(chunk_size)
                    .map(|chunk| {
                        let chunk = chunk.join("\n");
                        println!("  Chunk data\n{}", chunk);
                        parse_to_record_batch(Arc::clone(&schema), &chunk)
                    })
                    .map(Arc::new)
                    .collect();

                // stream from those batches
                assert!(!batches.is_empty());
                stream_from_batches(batches[0].schema(), batches)
            })
            .collect()
    }
}
