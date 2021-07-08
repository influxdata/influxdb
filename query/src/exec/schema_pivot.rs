//! This module contains code for the "SchemaPivot" DataFusion
//! extension plan node
//!
//! A SchemaPivot node takes an arbitrary input like
//!
//!  ColA | ColB | ColC
//! ------+------+------
//!   1   | NULL | NULL
//!   2   | 2    | NULL
//!   3   | 2    | NULL
//!
//! And pivots it to a table with a single string column for any
//! columns that had non null values.
//!
//!   non_null_column
//!  -----------------
//!   "ColA"
//!   "ColB"
//!
//! This operation can be used to implement the tag_keys metadata query

use std::{
    any::Any,
    fmt::{self, Debug},
    sync::Arc,
};

use async_trait::async_trait;

use arrow::{
    array::StringBuilder,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    error::{DataFusionError as Error, Result},
    logical_plan::{DFSchemaRef, Expr, LogicalPlan, ToDFSchema, UserDefinedLogicalNode},
    physical_plan::{
        common::SizedRecordBatchStream, DisplayFormatType, Distribution, ExecutionPlan,
        Partitioning, SendableRecordBatchStream,
    },
};

use tokio_stream::StreamExt;

/// Implements the SchemaPivot operation described in `make_schema_pivot`
pub struct SchemaPivotNode {
    input: LogicalPlan,
    schema: DFSchemaRef,
    // these expressions represent what columns are "used" by this
    // node (in this case all of them) -- columns that are not used
    // are optimzied away by datafusion.
    exprs: Vec<Expr>,
}

impl SchemaPivotNode {
    pub fn new(input: LogicalPlan) -> Self {
        let schema = make_schema_pivot_output_schema();

        // Form exprs that refer to all of our input columns (so that
        // datafusion knows not to opimize them away)
        let exprs = input
            .schema()
            .fields()
            .iter()
            .map(|field| Expr::Column(field.qualified_column()))
            .collect::<Vec<_>>();

        Self {
            input,
            schema,
            exprs,
        }
    }
}

impl Debug for SchemaPivotNode {
    /// Use explain format for the Debug format.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for SchemaPivotNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    /// Schema for Pivot is a single string
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.exprs.clone()
    }

    /// For example: `SchemaPivot`
    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SchemaPivot")
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        assert_eq!(inputs.len(), 1, "SchemaPivot: input sizes inconistent");
        assert_eq!(
            exprs.len(),
            self.exprs.len(),
            "SchemaPivot: expression sizes inconistent"
        );
        Arc::new(Self::new(inputs[0].clone()))
    }
}

// ------ The implementation of SchemaPivot code follows -----

/// Create the schema describing the output
pub fn make_schema_pivot_output_schema() -> DFSchemaRef {
    let nullable = false;
    Schema::new(vec![Field::new(
        "non_null_column",
        DataType::Utf8,
        nullable,
    )])
    .to_dfschema_ref()
    .unwrap()
}

/// Physical operator that implements the SchemaPivot operation aginst
/// data types
pub struct SchemaPivotExec {
    input: Arc<dyn ExecutionPlan>,
    /// Output schema
    schema: SchemaRef,
}

impl SchemaPivotExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        Self { input, schema }
    }
}

impl Debug for SchemaPivotExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SchemaPivotExec")
    }
}

#[async_trait]
impl ExecutionPlan for SchemaPivotExec {
    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn output_partitioning(&self) -> Partitioning {
        use Partitioning::*;
        match self.input.output_partitioning() {
            RoundRobinBatch(num_partitions) => RoundRobinBatch(num_partitions),
            // as this node transforms the output schema,  whatever partitioning
            // was present on the input is lost on the output
            Hash(_, num_partitions) => UnknownPartitioning(num_partitions),
            UnknownPartitioning(num_partitions) => UnknownPartitioning(num_partitions),
        }
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(Self {
                input: Arc::clone(&children[0]),
                schema: Arc::clone(&self.schema),
            })),
            _ => Err(Error::Internal(
                "SchemaPivotExec wrong number of children".to_string(),
            )),
        }
    }

    /// Execute one partition and return an iterator over RecordBatch
    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        if self.output_partitioning().partition_count() <= partition {
            return Err(Error::Internal(format!(
                "SchemaPivotExec invalid partition {}",
                partition
            )));
        }

        let mut input_reader = self.input.execute(partition).await?;

        // Algorithm: for each column we haven't seen a value for yet,
        // check each input row;
        //
        // Performance Optimizations: Don't continue scaning columns
        // if we have already seen a non-null value, and stop early we
        // have seen values for all columns.
        let input_schema = self.input.schema();
        let input_fields = input_schema.fields();
        let num_fields = input_fields.len();
        let mut field_indexes_with_seen_values = vec![false; num_fields];
        let mut num_fields_seen_with_values = 0;

        // use a loop so that we release the mutex once we have read each input_batch
        let mut keep_searching = true;
        while keep_searching {
            let input_batch = input_reader.next().await.transpose()?;

            keep_searching = match input_batch {
                Some(input_batch) => {
                    let num_rows = input_batch.num_rows();

                    for (i, seen_value) in field_indexes_with_seen_values.iter_mut().enumerate() {
                        // only check fields we haven't seen values for
                        if !*seen_value {
                            let column = input_batch.column(i);

                            let field_has_values =
                                !column.is_empty() && column.null_count() < num_rows;

                            if field_has_values {
                                *seen_value = true;
                                num_fields_seen_with_values += 1;
                            }
                        }
                    }
                    // need to keep searching if there are still some
                    // fields without values
                    num_fields_seen_with_values < num_fields
                }
                // no more input
                None => false,
            };
        }

        // now, output a string for each column in the input schema
        // that we saw values for
        let mut column_name_builder = StringBuilder::new(num_fields);
        field_indexes_with_seen_values
            .iter()
            .enumerate()
            .filter_map(|(field_index, has_values)| {
                if *has_values {
                    Some(input_fields[field_index].name())
                } else {
                    None
                }
            })
            .try_for_each(|field_name| {
                column_name_builder
                    .append_value(field_name)
                    .map_err(Error::ArrowError)
            })?;

        let batch =
            RecordBatch::try_new(self.schema(), vec![Arc::new(column_name_builder.finish())])?;

        let batches = vec![Arc::new(batch)];
        Ok(Box::pin(SizedRecordBatchStream::new(
            self.schema(),
            batches,
        )))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "SchemaPivotExec")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::exec::stringset::{IntoStringSet, StringSetRef};

    use super::*;
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{Field, Schema, SchemaRef},
    };
    use datafusion::physical_plan::memory::MemoryExec;

    #[tokio::test]
    async fn schema_pivot_exec_all_null() {
        let case = SchemaTestCase {
            input_batches: &[TestBatch {
                a: &[None, None],
                b: &[None, None],
            }],
            expected_output: &[],
        };
        assert_eq!(
            case.pivot().await,
            case.expected_output(),
            "TestCase: {:?}",
            case
        );
    }

    #[tokio::test]
    async fn schema_pivot_exec_both_non_null() {
        let case = SchemaTestCase {
            input_batches: &[TestBatch {
                a: &[Some(1), None],
                b: &[None, Some("foo")],
            }],
            expected_output: &["A", "B"],
        };
        assert_eq!(
            case.pivot().await,
            case.expected_output(),
            "TestCase: {:?}",
            case
        );
    }

    #[tokio::test]
    async fn schema_pivot_exec_one_non_null() {
        let case = SchemaTestCase {
            input_batches: &[TestBatch {
                a: &[Some(1), None],
                b: &[None, None],
            }],
            expected_output: &["A"],
        };
        assert_eq!(
            case.pivot().await,
            case.expected_output(),
            "TestCase: {:?}",
            case
        );
    }

    #[tokio::test]
    async fn schema_pivot_exec_both_non_null_two_record_batches() {
        let case = SchemaTestCase {
            input_batches: &[
                TestBatch {
                    a: &[Some(1), None],
                    b: &[None, None],
                },
                TestBatch {
                    a: &[None, None],
                    b: &[None, Some("foo")],
                },
            ],
            expected_output: &["A", "B"],
        };
        assert_eq!(
            case.pivot().await,
            case.expected_output(),
            "TestCase: {:?}",
            case
        );
    }

    #[tokio::test]
    async fn schema_pivot_exec_one_non_null_in_second_record_batch() {
        let case = SchemaTestCase {
            input_batches: &[
                TestBatch {
                    a: &[None, None],
                    b: &[None, None],
                },
                TestBatch {
                    a: &[None, Some(1), None],
                    b: &[None, Some("foo"), None],
                },
            ],
            expected_output: &["A", "B"],
        };
        assert_eq!(
            case.pivot().await,
            case.expected_output(),
            "TestCase: {:?}",
            case
        );
    }

    #[tokio::test]
    async fn schema_pivot_exec_bad_partition() {
        // ensure passing in a bad partition generates a reasonable error

        let pivot = make_schema_pivot(SchemaTestCase::input_schema(), vec![]);

        let results = pivot.execute(1).await;

        let expected_error = "SchemaPivotExec invalid partition 1";
        let actual_error = match results {
            Ok(_) => "Unexpected success running pivot".into(),
            Err(e) => format!("{:?}", e),
        };

        assert!(
            actual_error.contains(expected_error),
            "expected '{}' not found in '{}'",
            expected_error,
            actual_error
        );
    }

    /// Return a StringSet extracted from the record batch
    async fn reader_to_stringset(mut reader: SendableRecordBatchStream) -> StringSetRef {
        let mut batches = Vec::new();
        // process the record batches one by one
        while let Some(record_batch) = reader.next().await.transpose().expect("reading next batch")
        {
            batches.push(record_batch)
        }
        batches
            .into_stringset()
            .expect("Converted record batch reader into stringset")
    }

    /// return a set for testing
    fn to_stringset(strs: &[&str]) -> StringSetRef {
        let stringset = strs.iter().map(|s| s.to_string()).collect();
        StringSetRef::new(stringset)
    }

    /// Create a schema pivot node with a single input
    fn make_schema_pivot(input_schema: SchemaRef, data: Vec<RecordBatch>) -> SchemaPivotExec {
        let input = make_memory_exec(input_schema, data);
        let output_schema = Arc::new(make_schema_pivot_output_schema().as_ref().clone().into());
        SchemaPivotExec::new(input, output_schema)
    }

    /// Create an ExecutionPlan that produces `data` record batches.
    fn make_memory_exec(schema: SchemaRef, data: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let partitions = vec![data]; // single partition
        let projection = None;

        let memory_exec =
            MemoryExec::try_new(&partitions, schema, projection).expect("creating memory exec");

        Arc::new(memory_exec)
    }

    fn to_string_array(strs: &[Option<&str>]) -> Arc<StringArray> {
        let mut builder = StringBuilder::new(strs.len());
        for s in strs {
            match s {
                Some(s) => builder.append_value(s).expect("appending; string"),
                None => builder.append_null().expect("appending a null"),
            }
        }
        Arc::new(builder.finish())
    }

    // Input schema is (A INT, B STRING)
    #[derive(Debug)]
    struct TestBatch<'a> {
        a: &'a [Option<i64>],
        b: &'a [Option<&'a str>],
    }

    // Input schema is (A INT, B STRING)
    #[derive(Debug)]
    struct SchemaTestCase<'a> {
        // Input record batches, slices of slices (a,b)
        input_batches: &'a [TestBatch<'a>],
        expected_output: &'a [&'a str],
    }

    impl SchemaTestCase<'_> {
        fn input_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("A", DataType::Int64, true),
                Field::new("B", DataType::Utf8, true),
            ]))
        }

        /// return expected output, as StringSet
        fn expected_output(&self) -> StringSetRef {
            to_stringset(self.expected_output)
        }

        /// run the input batches through a schema pivot and return the results
        /// as a StringSetRef
        async fn pivot(&self) -> StringSetRef {
            let schema = Self::input_schema();

            // prepare input
            let input_batches = self
                .input_batches
                .iter()
                .map(|test_batch| {
                    let a_vec = test_batch.a.iter().copied().collect::<Vec<_>>();
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(Int64Array::from(a_vec)),
                            to_string_array(test_batch.b),
                        ],
                    )
                    .expect("Creating new record batch")
                })
                .collect::<Vec<_>>();

            let pivot = make_schema_pivot(schema, input_batches);

            let results = pivot.execute(0).await.expect("Correctly executed pivot");

            reader_to_stringset(results).await
        }
    }
}
