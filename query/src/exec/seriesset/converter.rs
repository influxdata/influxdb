//! This module contains code that "unpivots" annotated
//! [`RecordBatch`]es to [`Series`] and [`Group`]s for output by the
//! storage gRPC interface

use arrow::{
    self,
    array::{Array, DictionaryArray, StringArray},
    datatypes::{DataType, Int32Type},
    record_batch::RecordBatch,
};
use datafusion::physical_plan::SendableRecordBatchStream;

use observability_deps::tracing::trace;
use snafu::{OptionExt, ResultExt, Snafu};
use std::sync::Arc;
use tokio::sync::mpsc::error::SendError;
use tokio_stream::StreamExt;

use croaring::bitmap::Bitmap;

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
    ReadingRecordBatch { source: arrow::error::ArrowError },

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
        mut it: SendableRecordBatchStream,
    ) -> Result<Vec<SeriesSet>, Error> {
        let mut results = vec![];

        // for now, only handle a single record batch
        if let Some(batch) = it.next().await {
            let batch = batch.context(ReadingRecordBatch)?;

            if it.next().await.is_some() {
                // but not yet
                unimplemented!("Computing series across multiple record batches not yet supported");
            }

            let schema = batch.schema();
            // TODO: check that the tag columns are sorted by tag name...
            let tag_indexes =
                FieldIndexes::names_to_indexes(&schema, &tag_columns).context(InternalField)?;
            let field_indexes =
                FieldIndexes::from_field_columns(&schema, &field_columns).context(InternalField)?;

            // Algorithm: compute, via bitsets, the rows at which each
            // tag column changes and thereby where the tagset
            // changes. Emit a new SeriesSet at each such transition
            let mut tag_transitions = tag_indexes
                .iter()
                .map(|&col| Self::compute_transitions(&batch, col))
                .collect::<Vec<_>>();

            // no tag columns, emit a single tagset
            let intersections = if tag_transitions.is_empty() {
                let mut b = Bitmap::create_with_capacity(1);
                let end_row = batch.num_rows();
                b.add(end_row as u32);
                b
            } else {
                // OR bitsets together to to find all rows where the
                // keyset (values of the tag keys) changes
                let remaining = tag_transitions.split_off(1);

                remaining
                    .into_iter()
                    .for_each(|b| tag_transitions[0].or_inplace(&b));
                // take the first item
                tag_transitions.into_iter().next().unwrap()
            };

            let mut start_row: u32 = 0;

            // create each series (since bitmap are not Send, we can't
            // call await during the loop)

            // emit each series
            let series_sets = intersections.iter().map(|end_row| {
                let series_set = SeriesSet {
                    table_name: Arc::clone(&table_name),
                    tags: Self::get_tag_keys(
                        &batch,
                        start_row as usize,
                        &tag_columns,
                        &tag_indexes,
                    ),
                    field_indexes: field_indexes.clone(),
                    start_row: start_row as usize,
                    num_rows: (end_row - start_row) as usize,
                    batch: batch.clone(),
                };

                start_row = end_row;
                series_set
            });

            results.extend(series_sets);
        }
        Ok(results)
    }

    /// returns a bitset with all row indexes where the value of the
    /// batch `col_idx` changes.  Does not include row 0, always includes
    /// the last row, `batch.num_rows() - 1`
    ///
    /// Note: This may return false positives in the presence of dictionaries
    /// containing duplicates
    fn compute_transitions(batch: &RecordBatch, col_idx: usize) -> Bitmap {
        let num_rows = batch.num_rows();

        let mut bitmap = Bitmap::create_with_capacity(num_rows as u32);
        if num_rows < 1 {
            return bitmap;
        }

        // otherwise, scan the column for transitions
        let col = batch.column(col_idx);
        match col.data_type() {
            DataType::Utf8 => {
                let col = col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Casting column");
                let mut current_val = col.value(0);
                for row in 1..num_rows {
                    let next_val = col.value(row);
                    if next_val != current_val {
                        bitmap.add(row as u32);
                        current_val = next_val;
                    }
                }
            }
            DataType::Dictionary(key, value)
                if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Utf8 =>
            {
                let col = col
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("Casting column");
                let keys = col.keys();
                let get_key = |idx| {
                    if col.is_valid(idx) {
                        return Some(keys.value(idx));
                    }
                    None
                };

                let col_values = col.values();
                let values = col_values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Casting values column failed");

                let mut current_val = get_key(0);
                for row in 1..num_rows {
                    let next_val = get_key(row);
                    if next_val != current_val {
                        //
                        // N.B, concatenating two Arrow dictionary arrays can
                        // result in duplicate values with differing keys.
                        // Therefore, when keys differ we should verify they are
                        // encoding different values. See:
                        // https://github.com/apache/arrow-rs/pull/15
                        //
                        if let (Some(curr), Some(next)) = (current_val, next_val) {
                            if values.value(curr as usize) == values.value(next as usize) {
                                // these logical values are the same even though
                                // they have different encoded keys.
                                continue;
                            }
                        }

                        bitmap.add(row as u32);
                        current_val = next_val;
                    }
                }
            }
            _ => unimplemented!(
                "Series transition calculations not supported for tag type {:?} in column {:?}",
                col.data_type(),
                batch.schema().fields()[col_idx]
            ),
        }

        // for now, always treat the last row as ending a series
        bitmap.add(num_rows as u32);

        trace!(
            rows = ?bitmap.to_vec(),
            ?col_idx,
            "row transitions for results"
        );
        bitmap
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
                    .context(FindingGroupColumn {
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
    use datafusion::physical_plan::common::SizedRecordBatchStream;
    use test_helpers::{str_pair_vec_to_vec, str_vec_to_arc_vec};

    use super::*;

    #[tokio::test]
    async fn test_convert_empty() {
        let schema = Arc::new(Schema::new(vec![]));
        let empty_iterator = Box::pin(SizedRecordBatchStream::new(schema, vec![]));

        let table_name = "foo";
        let tag_columns = [];
        let field_columns = [];

        let results = convert(table_name, &tag_columns, &field_columns, empty_iterator).await;
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_convert_single_series_no_tags() {
        // single series
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_a", DataType::Utf8, true),
            Field::new("tag_b", DataType::Utf8, true),
            Field::new("float_field", DataType::Float64, true),
            Field::new("int_field", DataType::Int64, true),
            Field::new("time", DataType::Int64, false),
        ]));
        let input = parse_to_iterator(
            schema,
            "one,ten,10.0,1,1000\n\
             one,ten,10.1,2,2000\n",
        );

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

    #[tokio::test]
    async fn test_convert_single_series_no_tags_nulls() {
        // single series
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_a", DataType::Utf8, true),
            Field::new("tag_b", DataType::Utf8, true),
            Field::new("float_field", DataType::Float64, true),
            Field::new("int_field", DataType::Int64, true),
            Field::new("time", DataType::Int64, false),
        ]));
        // send no values in the int_field colum
        let input = parse_to_iterator(
            schema,
            "one,ten,10.0,,1000\n\
             one,ten,10.1,,2000\n",
        );

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

    #[tokio::test]
    async fn test_convert_single_series_one_tag() {
        // single series
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_a", DataType::Utf8, true),
            Field::new("tag_b", DataType::Utf8, true),
            Field::new("float_field", DataType::Float64, true),
            Field::new("int_field", DataType::Int64, true),
            Field::new("time", DataType::Int64, false),
        ]));
        let input = parse_to_iterator(
            schema,
            "one,ten,10.0,1,1000\n\
             one,ten,10.1,2,2000\n",
        );

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

    #[tokio::test]
    async fn test_convert_one_tag_multi_series() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_a", DataType::Utf8, true),
            Field::new("tag_b", DataType::Utf8, true),
            Field::new("float_field", DataType::Float64, true),
            Field::new("int_field", DataType::Int64, true),
            Field::new("time", DataType::Int64, false),
        ]));

        let input = parse_to_iterator(
            schema,
            "one,ten,10.0,1,1000\n\
             one,ten,10.1,2,2000\n\
             one,eleven,10.1,3,3000\n\
             two,eleven,10.2,4,4000\n\
             two,eleven,10.3,5,5000\n",
        );

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

    // two tag columns, three series
    #[tokio::test]
    async fn test_convert_two_tag_multi_series() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag_a", DataType::Utf8, true),
            Field::new("tag_b", DataType::Utf8, true),
            Field::new("float_field", DataType::Float64, true),
            Field::new("int_field", DataType::Int64, true),
            Field::new("time", DataType::Int64, false),
        ]));

        let input = parse_to_iterator(
            schema,
            "one,ten,10.0,1,1000\n\
             one,ten,10.1,2,2000\n\
             one,eleven,10.1,3,3000\n\
             two,eleven,10.2,4,4000\n\
             two,eleven,10.3,5,5000\n",
        );

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
        let input = batch_to_iterator(batch);

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
        let mut reader = csv::Reader::new(
            data.as_bytes(),
            schema,
            has_header,
            delimiter,
            batch_size,
            bounds,
            projection,
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

        println!("First batch: \n{:#?}", first_batch);

        first_batch.unwrap()
    }

    fn parse_to_iterator(schema: SchemaRef, data: &str) -> SendableRecordBatchStream {
        let batch = parse_to_record_batch(schema, data);
        batch_to_iterator(batch)
    }

    fn batch_to_iterator(batch: RecordBatch) -> SendableRecordBatchStream {
        Box::pin(SizedRecordBatchStream::new(
            batch.schema(),
            vec![Arc::new(batch)],
        ))
    }
}
