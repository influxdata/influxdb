//! This module contains the definition of a "SeriesSet" a plan that when run produces
//! rows that can be logically divided into "Series"
//!
//! Specifically, this thing can produce represents a set of "tables",
//! and each table is sorted on a set of "tag" columns, meaning the
//! groups / series will be contiguous.
//!
//! For example, the output columns of such a plan would be:
//! (tag col0) (tag col1) ... (tag colN) (field val1) (field val2) ... (field valN) .. (timestamps)
//!
//! Note that the data will come out ordered by the tag keys (ORDER BY
//! (tag col0) (tag col1) ... (tag colN))
//!
//! NOTE: We think the influx storage engine returns series sorted by
//! the tag values, but the order of the columns is also sorted. So
//! for example, if you have `region`, `host`, and `service` as tags,
//! the columns would be ordered `host`, `region`, and `service` as
//! well.

use std::sync::Arc;

use arrow::{
    array::StringArray,
    datatypes::DataType,
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchReader},
};
use delorean_arrow::arrow::{self};
use delorean_data_types::TIME_COLUMN_NAME;
//use snafu::{ensure, OptionExt, Snafu};
use snafu::{ResultExt, Snafu};
use tokio::sync::mpsc;

use croaring::bitmap::Bitmap;

#[derive(Debug, Snafu)]
/// Opaque error type
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

    #[snafu(display("Finding column: {:?} in schema: '{}'", column_name, source))]
    ColumnNotFound {
        column_name: String,
        source: arrow::error::ArrowError,
    },

    #[snafu(display("Sending results: {:?}", source))]
    Sending {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

#[allow(dead_code)]
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
/// Represents several logical timeseries that share the same
/// timestamps and name=value tag keys.
pub struct SeriesSet {
    /// The table name this series came from
    table_name: Arc<String>,

    /// key = value pairs that define this series
    tag_keys: Vec<(Arc<String>, Arc<String>)>,

    /// timestamp column index
    timestamp_index: usize,

    /// the column index each data field
    field_indices: Arc<Vec<usize>>,

    // The row in the record batch where the data starts (inclusive)
    start_row: usize,

    // The number of rows in the record batch that the data goes to
    num_rows: usize,

    // The underlying record batch data
    batch: RecordBatch,
}
pub type SeriesSetRef = Arc<SeriesSet>;

// Handles converting record batches into SeriesSets, and sending them
// to tx
#[derive(Debug)]
pub struct SeriesSetConverter {
    tx: mpsc::Sender<Result<SeriesSet>>,
}

impl SeriesSetConverter {
    pub fn new(tx: mpsc::Sender<Result<SeriesSet>>) -> Self {
        Self { tx }
    }

    /// Convert the results from running a DataFusion plan into the
    /// appropriate SeriesSets, sending them self.tx
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
        table_name: Arc<String>,
        tag_columns: Arc<Vec<Arc<String>>>,
        field_columns: Arc<Vec<Arc<String>>>,
        it: Box<dyn RecordBatchReader + Send>,
    ) -> Result<()> {
        // Make sure that any error that results from processing is sent along
        if let Err(e) = self
            .convert_impl(table_name, tag_columns, field_columns, it)
            .await
        {
            self.tx
                .send(Err(e))
                .await
                .map_err(|send_err| Error::Sending {
                    source: Box::new(send_err),
                })?
        }
        Ok(())
    }

    /// Does the actual conversion logic, but returns any error in processing
    pub async fn convert_impl(
        &mut self,
        table_name: Arc<String>,
        tag_columns: Arc<Vec<Arc<String>>>,
        field_columns: Arc<Vec<Arc<String>>>,
        mut it: Box<dyn RecordBatchReader + Send>,
    ) -> Result<()> {
        // for now, only handle a single record batch
        if let Some(batch) = it.next() {
            let batch = batch.context(ReadingRecordBatch)?;

            if it.next().is_some() {
                // but not yet
                unimplemented!("Computing series across multiple record batches not yet supported");
            }

            let schema = batch.schema();
            // TODO: check that the tag columns are sorted by tag name...

            let timestamp_index = schema.index_of(TIME_COLUMN_NAME).context(ColumnNotFound {
                column_name: TIME_COLUMN_NAME,
            })?;
            let tag_indicies = Self::names_to_indices(&schema, &tag_columns)?;
            let field_indicies = Arc::new(Self::names_to_indices(&schema, &field_columns)?);

            // Algorithm: compute, via bitsets, the rows at which each
            // tag column changes and thereby where the tagset
            // changes. Emit a new SeriesSet at each such transition
            let mut tag_transitions = tag_indicies
                .iter()
                .map(|&col| Self::compute_transitions(&batch, col))
                .collect::<Result<Vec<_>>>()?;

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
            let series_sets = intersections
                .iter()
                .map(|end_row| {
                    let series_set = SeriesSet {
                        table_name: table_name.clone(),
                        tag_keys: Self::get_tag_keys(
                            &batch,
                            start_row as usize,
                            &tag_columns,
                            &tag_indicies,
                        ),
                        timestamp_index,
                        field_indices: field_indicies.clone(),
                        start_row: start_row as usize,
                        num_rows: (end_row - start_row) as usize,
                        batch: batch.clone(),
                    };

                    start_row = end_row;
                    series_set
                })
                .collect::<Vec<_>>();

            for series_set in series_sets {
                self.tx
                    .send(Ok(series_set))
                    .await
                    .map_err(|send_err| Error::Sending {
                        source: Box::new(send_err),
                    })?;
            }
        }
        Ok(())
    }

    // look up which column index correponds to each column name
    fn names_to_indices(schema: &SchemaRef, column_names: &[Arc<String>]) -> Result<Vec<usize>> {
        column_names
            .iter()
            .map(|column_name| {
                schema.index_of(&*column_name).context(ColumnNotFound {
                    column_name: column_name.as_ref(),
                })
            })
            .collect()
    }

    /// returns a bitset with all row indicies where the value of the
    /// batch[col_idx] changes.  Does not include row 0, always includes
    /// the last row, `batch.num_rows() - 1`
    fn compute_transitions(batch: &RecordBatch, col_idx: usize) -> Result<Bitmap> {
        let num_rows = batch.num_rows();

        let mut bitmap = Bitmap::create_with_capacity(num_rows as u32);
        if num_rows < 1 {
            return Ok(bitmap);
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
            _ => unimplemented!(
                "Series transition calculations not supported for type {:?} in column {:?}",
                col.data_type(),
                batch.schema().fields()[col_idx]
            ),
        }

        // for now, always treat the last row as ending a series
        bitmap.add(num_rows as u32);

        Ok(bitmap)
    }

    /// Creates (column_name, column_value) pairs for each column
    /// named in `tag_column_name` at the corresponding index
    /// `tag_indicies`
    fn get_tag_keys(
        batch: &RecordBatch,
        row: usize,
        tag_column_names: &[Arc<String>],
        tag_indicies: &[usize],
    ) -> Vec<(Arc<String>, Arc<String>)> {
        assert_eq!(tag_column_names.len(), tag_indicies.len());

        tag_column_names
            .iter()
            .zip(tag_indicies)
            .map(|(column_name, column_index)| {
                let tag_value: String = batch
                    .column(*column_index)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Tag column was a String")
                    .value(row)
                    .into();
                (column_name.clone(), Arc::new(tag_value))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        csv,
        datatypes::DataType,
        datatypes::Field,
        datatypes::{Schema, SchemaRef},
        record_batch::RecordBatch,
        util::pretty::pretty_format_batches,
    };
    use delorean_arrow::datafusion::physical_plan::common::RecordBatchIterator;

    use super::*;

    #[tokio::test(threaded_scheduler)]
    async fn test_convert_empty() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![]));
        let empty_iterator: Box<dyn RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(schema, vec![]));

        let table_name = "foo";
        let tag_columns = [];
        let field_columns = [];

        let results = convert(table_name, &tag_columns, &field_columns, empty_iterator).await;
        assert_eq!(results.len(), 0);

        Ok(())
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_convert_single_series_no_tags() -> Result<()> {
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
        let series_set = results[0].as_ref().expect("Correctly converted");

        assert_eq!(*series_set.table_name, "foo");
        assert_eq!(series_set.tag_keys.len(), 0);
        assert_eq!(series_set.timestamp_index, 4);
        assert_eq!(series_set.field_indices, Arc::new(vec![2]));
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
            "",
        ];

        let actual_data = pretty_format_batches(&[series_set.batch.clone()])
            .expect("formatting batch")
            .split('\n')
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        assert_eq!(expected_data, actual_data);
        Ok(())
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_convert_single_series_one_tag() -> Result<()> {
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
        let series_set = results[0].as_ref().expect("Correctly converted");

        assert_eq!(*series_set.table_name, "bar");
        assert_eq!(
            series_set.tag_keys,
            str_pair_vec_to_vec(&[("tag_a", "one")])
        );
        assert_eq!(series_set.timestamp_index, 4);
        assert_eq!(series_set.field_indices, Arc::new(vec![2]));
        assert_eq!(series_set.start_row, 0);
        assert_eq!(series_set.num_rows, 2);

        Ok(())
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_convert_one_tag_multi_series() -> Result<()> {
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
        let series_set1 = results[0].as_ref().expect("Correctly converted");

        assert_eq!(*series_set1.table_name, "foo");
        assert_eq!(
            series_set1.tag_keys,
            str_pair_vec_to_vec(&[("tag_a", "one")])
        );
        assert_eq!(series_set1.timestamp_index, 4);
        assert_eq!(series_set1.field_indices, Arc::new(vec![3]));
        assert_eq!(series_set1.start_row, 0);
        assert_eq!(series_set1.num_rows, 3);

        let series_set2 = results[1].as_ref().expect("Correctly converted");

        assert_eq!(*series_set2.table_name, "foo");
        assert_eq!(
            series_set2.tag_keys,
            str_pair_vec_to_vec(&[("tag_a", "two")])
        );
        assert_eq!(series_set2.timestamp_index, 4);
        assert_eq!(series_set2.field_indices, Arc::new(vec![3]));
        assert_eq!(series_set2.start_row, 3);
        assert_eq!(series_set2.num_rows, 2);

        Ok(())
    }

    // two tag columns, three series
    #[tokio::test(threaded_scheduler)]
    async fn test_convert_two_tag_multi_series() -> Result<()> {
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
        let series_set1 = results[0].as_ref().expect("Correctly converted");

        assert_eq!(*series_set1.table_name, "foo");
        assert_eq!(
            series_set1.tag_keys,
            str_pair_vec_to_vec(&[("tag_a", "one"), ("tag_b", "ten")])
        );
        assert_eq!(series_set1.start_row, 0);
        assert_eq!(series_set1.num_rows, 2);

        let series_set2 = results[1].as_ref().expect("Correctly converted");

        assert_eq!(*series_set2.table_name, "foo");
        assert_eq!(
            series_set2.tag_keys,
            str_pair_vec_to_vec(&[("tag_a", "one"), ("tag_b", "eleven")])
        );
        assert_eq!(series_set2.start_row, 2);
        assert_eq!(series_set2.num_rows, 1);

        let series_set3 = results[2].as_ref().expect("Correctly converted");

        assert_eq!(*series_set3.table_name, "foo");
        assert_eq!(
            series_set3.tag_keys,
            str_pair_vec_to_vec(&[("tag_a", "two"), ("tag_b", "eleven")])
        );
        assert_eq!(series_set3.start_row, 3);
        assert_eq!(series_set3.num_rows, 2);

        Ok(())
    }

    /// Test helper: run conversion and return a Vec
    pub async fn convert<'a>(
        table_name: &'a str,
        tag_columns: &'a [&'a str],
        field_columns: &'a [&'a str],
        it: Box<dyn RecordBatchReader + Send>,
    ) -> Vec<Result<SeriesSet>> {
        let (tx, mut rx) = mpsc::channel(1);
        let mut converter = SeriesSetConverter::new(tx);

        let table_name = Arc::new(table_name.into());
        let tag_columns = str_vec_to_arc_vec(tag_columns);
        let field_columns = str_vec_to_arc_vec(field_columns);

        tokio::task::spawn(async move {
            converter
                .convert(table_name, tag_columns, field_columns, it)
                .await
                .expect("Conversion happened without error")
        });

        let mut results = Vec::new();
        while let Some(r) = rx.recv().await {
            results.push(r)
        }
        results
    }

    // convert form that is easy to type in tests to the form that SeriesSetConverter wants
    fn str_vec_to_arc_vec(str_vec: &[&str]) -> Arc<Vec<Arc<String>>> {
        Arc::new(str_vec.iter().map(|s| Arc::new(String::from(*s))).collect())
    }

    // convert form that is easy to type in tests to the form that SeriesSetConverter wants
    fn str_pair_vec_to_vec(str_vec: &[(&str, &str)]) -> Vec<(Arc<String>, Arc<String>)> {
        str_vec
            .iter()
            .map(|(s1, s2)| (Arc::new(String::from(*s1)), Arc::new(String::from(*s2))))
            .collect()
    }

    /// Test helper: parses the csv content into a single record batch arrow arrays
    /// columnar ArrayRef according to the schema
    fn parse_to_record_batch(schema: SchemaRef, data: &str) -> RecordBatch {
        let has_header = false;
        let delimiter = Some(b',');
        let batch_size = 1000;
        let projection = None;
        let mut reader = csv::Reader::new(
            data.as_bytes(),
            schema,
            has_header,
            delimiter,
            batch_size,
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

        first_batch.unwrap()
    }

    fn parse_to_iterator(schema: SchemaRef, data: &str) -> Box<dyn RecordBatchReader + Send> {
        let batch = parse_to_record_batch(schema.clone(), data);
        Box::new(RecordBatchIterator::new(schema, vec![Arc::new(batch)]))
    }
}
