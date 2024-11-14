use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use influxdb3_process_engine::python_call::PythonCall;
use iox_system_tables::IoxSystemTable;
use std::sync::Arc;

pub(super) struct PythonCallTable {
    schema: SchemaRef,
    python_calls: Vec<PythonCall>,
}

fn python_call_schema() -> SchemaRef {
    let columns = vec![
        Field::new("call_name", DataType::Utf8, false),
        Field::new("function_name", DataType::Utf8, false),
        Field::new("code", DataType::Utf8, false),
    ];
    Schema::new(columns).into()
}

impl PythonCallTable {
    pub fn new(python_calls: Vec<PythonCall>) -> Self {
        Self {
            schema: python_call_schema(),
            python_calls,
        }
    }
}

#[async_trait]
impl IoxSystemTable for PythonCallTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    async fn scan(
        &self,
        _filters: Option<Vec<Expr>>,
        _limit: Option<usize>,
    ) -> Result<RecordBatch, DataFusionError> {
        let schema = self.schema();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(
                self.python_calls
                    .iter()
                    .map(|call| Some(call.call_name.clone()))
                    .collect::<StringArray>(),
            )),
            Arc::new(
                self.python_calls
                    .iter()
                    .map(|p| Some(p.function_name.clone()))
                    .collect::<StringArray>(),
            ),
            Arc::new(
                self.python_calls
                    .iter()
                    .map(|p| Some(p.code.clone()))
                    .collect::<StringArray>(),
            ),
        ];
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}
