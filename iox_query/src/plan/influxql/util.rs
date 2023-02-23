use datafusion::common::{DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion::logical_expr::Operator;
use influxdb_influxql_parser::expression::BinaryOperator;
use influxdb_influxql_parser::string::Regex;
use query_functions::clean_non_meta_escapes;
use schema::Schema;
use std::sync::Arc;

pub(super) fn binary_operator_to_df_operator(op: BinaryOperator) -> Operator {
    match op {
        BinaryOperator::Add => Operator::Plus,
        BinaryOperator::Sub => Operator::Minus,
        BinaryOperator::Mul => Operator::Multiply,
        BinaryOperator::Div => Operator::Divide,
        BinaryOperator::Mod => Operator::Modulo,
        BinaryOperator::BitwiseAnd => Operator::BitwiseAnd,
        BinaryOperator::BitwiseOr => Operator::BitwiseOr,
        BinaryOperator::BitwiseXor => Operator::BitwiseXor,
    }
}

/// Return the IOx schema for the specified DataFusion schema.
pub(super) fn schema_from_df(schema: &DFSchema) -> Result<Schema> {
    let s: Arc<arrow::datatypes::Schema> = Arc::new(schema.into());
    s.try_into().map_err(|err| {
        DataFusionError::Internal(format!(
            "unable to convert DataFusion schema to IOx schema: {err}"
        ))
    })
}

/// Container for both the DataFusion and equivalent IOx schema.
pub(super) struct Schemas {
    pub(super) df_schema: DFSchemaRef,
    pub(super) iox_schema: Schema,
}

impl Schemas {
    pub(super) fn new(df_schema: &DFSchemaRef) -> Result<Self> {
        Ok(Self {
            df_schema: Arc::clone(df_schema),
            iox_schema: schema_from_df(df_schema)?,
        })
    }
}

/// Sanitize an InfluxQL regular expression and create a compiled [`regex::Regex`].
pub fn parse_regex(re: &Regex) -> Result<regex::Regex> {
    let pattern = clean_non_meta_escapes(re.as_str());
    regex::Regex::new(&pattern).map_err(|e| {
        DataFusionError::External(format!("invalid regular expression '{re}': {e}").into())
    })
}
