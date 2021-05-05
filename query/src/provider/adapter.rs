//! Holds a stream that ensures chunks have the same (uniform) schema
use std::sync::Arc;

use snafu::Snafu;
use std::task::{Context, Poll};

use arrow::{
    array::new_null_array,
    datatypes::{DataType, SchemaRef},
    error::Result as ArrowResult,
    record_batch::RecordBatch,
};
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;

/// Database schema creation / validation errors.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Internal error creating SchemaAdapterStream: field '{}' does not appear in the output schema",
                    field_name,))]
    InternalLostInputField { field_name: String },

    #[snafu(display("Internal error creating SchemaAdapterStream: input field '{}' had type '{:?}' which is different than output field '{}' which had type '{:?}'",
                    input_field_name, input_field_type, output_field_name, output_field_type,))]
    InternalDataTypeMismatch {
        input_field_name: String,
        input_field_type: DataType,
        output_field_name: String,
        output_field_type: DataType,
    },

    #[snafu(display("Internal error creating SchemaAdapterStream: creating null of type '{:?}' which is different than output field '{}' which had type '{:?}'",
                    field_type, output_field_name, output_field_type,))]
    InternalDataTypeMismatchForNull {
        field_type: DataType,
        output_field_name: String,
        output_field_type: DataType,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// This stream wraps another underlying stream to ensure it produces
/// the specified schema.  If the underlying stream produces a subset
/// of the columns specified in desired schema, this stream creates
/// arrays with NULLs to pad out the missing columns
///
/// For example:
///
/// If a table had schema with Cols A, B, and C, but the chunk (input)
/// stream only produced record batches with columns A and C, this
/// stream would append a column of B / nulls to each record batch
/// that flowed through it
///
/// ```text
///
///                       ┌────────────────┐                         ┌─────────────────────────┐
///                       │ ┌─────┐┌─────┐ │                         │ ┌─────┐┌──────┐┌─────┐  │
///                       │ │  A  ││  C  │ │                         │ │  A  ││  B   ││  C  │  │
///                       │ │  -  ││  -  │ │                         │ │  -  ││  -   ││  -  │  │
/// ┌──────────────┐      │ │  1  ││ 10  │ │     ┌──────────────┐    │ │  1  ││ NULL ││ 10  │  │
/// │    Input     │      │ │  2  ││ 20  │ │     │   Adapter    │    │ │  2  ││ NULL ││ 20  │  │
/// │    Stream    ├────▶ │ │  3  ││ 30  │ │────▶│    Stream    ├───▶│ │  3  ││ NULL ││ 30  │  │
/// └──────────────┘      │ │  4  ││ 40  │ │     └──────────────┘    │ │  4  ││ NULL ││ 40  │  │
///                       │ └─────┘└─────┘ │                         │ └─────┘└──────┘└─────┘  │
///                       │                │                         │                         │
///                       │  Record Batch  │                         │      Record Batch       │
///                       └────────────────┘                         └─────────────────────────┘
/// ```
pub(crate) struct SchemaAdapterStream {
    input: SendableRecordBatchStream,
    /// Output schema of this stream
    /// The schema of `input` is always a subset of output_schema
    output_schema: SchemaRef,
    mappings: Vec<ColumnMapping>,
}

impl std::fmt::Debug for SchemaAdapterStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaAdapterStream")
            .field("input", &"(OPAQUE STREAM)")
            .field("output_schema", &self.output_schema)
            .field("mappings", &self.mappings)
            .finish()
    }
}

impl SchemaAdapterStream {
    /// Try to create a new adapter stream that produces batches with
    /// the specified output_schema
    ///
    /// If the underlying stream produces columns that DO NOT appear
    /// in the output schema, or are different types than the output
    /// schema, an error will be produced.
    pub(crate) fn try_new(
        input: SendableRecordBatchStream,
        output_schema: SchemaRef,
    ) -> Result<Self> {
        let input_schema = input.schema();

        // Figure out how to compute each column in the output
        let mappings = output_schema
            .fields()
            .iter()
            .map(|output_field| {
                let input_field_index = input_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .find(|(_, input_field)| output_field.name() == input_field.name())
                    .map(|(idx, _)| idx);

                if let Some(input_field_index) = input_field_index {
                    ColumnMapping::FromInput(input_field_index)
                } else {
                    ColumnMapping::MakeNull(output_field.data_type().clone())
                }
            })
            .collect::<Vec<_>>();

        // sanity logic checks
        for input_field in input_schema.fields().iter() {
            // that there are no fields in the input schema that are
            // not present in the desired output schema (otherwise we
            // are dropping fields -- theys should have been selected
            // out with projection push down)
            if output_schema
                .fields()
                .iter()
                .find(|output_field| input_field.name() == output_field.name())
                .is_none()
            {
                return InternalLostInputField {
                    field_name: input_field.name(),
                }
                .fail();
            }
        }

        // Verify the mappings match the output type
        for (output_index, mapping) in mappings.iter().enumerate() {
            match mapping {
                ColumnMapping::FromInput(input_index) => {
                    let input_field = input_schema.field(*input_index);
                    let output_field = output_schema.field(output_index);
                    if input_field.data_type() != output_field.data_type() {
                        return InternalDataTypeMismatch {
                            input_field_name: input_field.name(),
                            input_field_type: input_field.data_type().clone(),
                            output_field_name: output_field.name(),
                            output_field_type: output_field.data_type().clone(),
                        }
                        .fail();
                    }
                }
                ColumnMapping::MakeNull(data_type) => {
                    let output_field = output_schema.field(output_index);
                    if data_type != output_field.data_type() {
                        return InternalDataTypeMismatchForNull {
                            field_type: data_type.clone(),
                            output_field_name: output_field.name(),
                            output_field_type: output_field.data_type().clone(),
                        }
                        .fail();
                    }
                }
            }
        }

        Ok(Self {
            input,
            output_schema,
            mappings,
        })
    }

    /// Extends the record batch, if needed, so that it matches the schema
    fn extend_batch(&self, batch: RecordBatch) -> ArrowResult<RecordBatch> {
        let output_columns = self
            .mappings
            .iter()
            .map(|mapping| match mapping {
                ColumnMapping::FromInput(input_index) => Arc::clone(&batch.column(*input_index)),
                ColumnMapping::MakeNull(data_type) => new_null_array(data_type, batch.num_rows()),
            })
            .collect::<Vec<_>>();

        RecordBatch::try_new(Arc::clone(&self.output_schema), output_columns)
    }
}

impl RecordBatchStream for SchemaAdapterStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }
}

impl Stream for SchemaAdapterStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // the Poll result is an Opton<Result<Batch>> so we need a few
        // layers of maps to get at the actual batch, if any
        self.input.as_mut().poll_next(ctx).map(|maybe_result| {
            maybe_result.map(|batch| batch.and_then(|batch| self.extend_batch(batch)))
        })
    }

    // TODO is there a useful size_hint to pass?
}

/// Describes how to create column in the output.
#[derive(Debug)]
enum ColumnMapping {
    /// Output column is found at <index> column of the input schema
    FromInput(usize),
    /// Output colum should be synthesized with nulls of the specified type
    MakeNull(DataType),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::{
        array::{ArrayRef, Int32Array, StringArray},
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };
    use arrow_util::assert_batches_eq;
    use datafusion::physical_plan::common::{collect, SizedRecordBatchStream};
    use test_helpers::assert_contains;

    #[tokio::test]
    async fn same_input_and_output() {
        let batch = make_batch();

        let output_schema = batch.schema();
        let input_stream = SizedRecordBatchStream::new(batch.schema(), vec![Arc::new(batch)]);
        let adapter_stream =
            SchemaAdapterStream::try_new(Box::pin(input_stream), output_schema).unwrap();

        let output = collect(Box::pin(adapter_stream))
            .await
            .expect("Running plan");
        let expected = vec![
            "+---+---+-----+",
            "| a | b | c   |",
            "+---+---+-----+",
            "| 1 | 4 | foo |",
            "| 2 | 5 | bar |",
            "| 3 | 6 | baz |",
            "+---+---+-----+",
        ];
        assert_batches_eq!(&expected, &output);
    }

    #[tokio::test]
    async fn input_different_order_than_output() {
        let batch = make_batch();
        // input has columns in different order than desired output

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Utf8, false),
            Field::new("a", DataType::Int32, false),
        ]));
        let input_stream = SizedRecordBatchStream::new(batch.schema(), vec![Arc::new(batch)]);
        let adapter_stream =
            SchemaAdapterStream::try_new(Box::pin(input_stream), output_schema).unwrap();

        let output = collect(Box::pin(adapter_stream))
            .await
            .expect("Running plan");
        let expected = vec![
            "+---+-----+---+",
            "| b | c   | a |",
            "+---+-----+---+",
            "| 4 | foo | 1 |",
            "| 5 | bar | 2 |",
            "| 6 | baz | 3 |",
            "+---+-----+---+",
        ];
        assert_batches_eq!(&expected, &output);
    }

    #[tokio::test]
    async fn input_subset_of_output() {
        let batch = make_batch();
        // input has subset of columns of the desired otuput. d and e are not present
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Utf8, false),
            Field::new("e", DataType::Float64, false),
            Field::new("b", DataType::Int32, false),
            Field::new("d", DataType::Float32, false),
            Field::new("a", DataType::Int32, false),
        ]));
        let input_stream = SizedRecordBatchStream::new(batch.schema(), vec![Arc::new(batch)]);
        let adapter_stream =
            SchemaAdapterStream::try_new(Box::pin(input_stream), output_schema).unwrap();

        let output = collect(Box::pin(adapter_stream))
            .await
            .expect("Running plan");
        let expected = vec![
            "+-----+---+---+---+---+",
            "| c   | e | b | d | a |",
            "+-----+---+---+---+---+",
            "| foo |   | 4 |   | 1 |",
            "| bar |   | 5 |   | 2 |",
            "| baz |   | 6 |   | 3 |",
            "+-----+---+---+---+---+",
        ];
        assert_batches_eq!(&expected, &output);
    }

    #[tokio::test]
    async fn input_superset_of_columns() {
        let batch = make_batch();

        // No such column "b" in output -- column would be lost
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Utf8, false),
            Field::new("a", DataType::Int32, false),
        ]));
        let input_stream = SizedRecordBatchStream::new(batch.schema(), vec![Arc::new(batch)]);
        let res = SchemaAdapterStream::try_new(Box::pin(input_stream), output_schema);

        assert_contains!(
            res.unwrap_err().to_string(),
            "field 'b' does not appear in the output schema"
        );
    }

    #[tokio::test]
    async fn input_has_different_type() {
        let batch = make_batch();

        // column c has string type in input, output asks float32
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Float32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("a", DataType::Int32, false),
        ]));
        let input_stream = SizedRecordBatchStream::new(batch.schema(), vec![Arc::new(batch)]);
        let res = SchemaAdapterStream::try_new(Box::pin(input_stream), output_schema);

        assert_contains!(res.unwrap_err().to_string(), "input field 'c' had type 'Utf8' which is different than output field 'c' which had type 'Float32'");
    }

    // input has different column types than desired output

    fn make_batch() -> RecordBatch {
        let col_a = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let col_b = Arc::new(Int32Array::from(vec![4, 5, 6]));
        let col_c = Arc::new(StringArray::from(vec!["foo", "bar", "baz"]));

        RecordBatch::try_from_iter(vec![("a", col_a as ArrayRef), ("b", col_b), ("c", col_c)])
            .unwrap()
    }
}
