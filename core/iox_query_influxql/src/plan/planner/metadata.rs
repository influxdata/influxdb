use std::sync::Arc;

use arrow::datatypes::Field;
use datafusion::{
    common::{DFSchema, DFSchemaRef, ExprSchema, Result, tree_node::Transformed},
    logical_expr::{
        BinaryExpr, Case, Cast, Expr, LogicalPlan, TryCast,
        expr::{AggregateFunction, Alias, ScalarFunction},
    },
};
use iox_query::transform_plan_schema;

const INFLUXQL_FILLED_KEY: &str = "influxql::filled";

/// Check if a field has any InfluxQL-specific metadata that should be stripped
/// before physical planning.
fn field_has_influxql_metadata(field: &Field) -> bool {
    field.metadata().contains_key(INFLUXQL_FILLED_KEY)
}

/// Extension trait for `Field` to manage InfluxQL-specific metadata.
pub(super) trait FieldExt {
    /// Check if the field is marked as filled for InfluxQL.
    fn is_influxql_filled(&self) -> bool;

    /// Set the filled status for InfluxQL on the field.
    fn set_influxql_filled(&mut self, filled: bool);

    /// Builder-style method to set the filled status for InfluxQL on the field.
    fn with_influxql_filled(mut self, filled: bool) -> Self
    where
        Self: Sized,
    {
        self.set_influxql_filled(filled);
        self
    }
}

impl FieldExt for Field {
    fn is_influxql_filled(&self) -> bool {
        self.metadata()
            .get(INFLUXQL_FILLED_KEY)
            .map(|v| v == "true")
            .unwrap_or(false)
    }

    fn set_influxql_filled(&mut self, filled: bool) {
        self.metadata_mut().insert(
            INFLUXQL_FILLED_KEY.to_string(),
            if filled { "true" } else { "false" }.to_string(),
        );
    }
}

/// Check in an expression is marked as being filled.
pub(super) fn expr_is_influxql_filled(expr: &Expr, schema: &(dyn ExprSchema + 'static)) -> bool {
    match expr {
        Expr::Alias(Alias { expr, .. }) => expr_is_influxql_filled(expr, schema),
        Expr::Column(column) => schema
            .field_from_column(column)
            .map(|f| f.is_influxql_filled())
            .unwrap_or(false),
        Expr::Literal(_, metadata) => metadata
            .as_ref()
            .and_then(|m| m.inner().get(INFLUXQL_FILLED_KEY))
            .map(|v| v == "true")
            .unwrap_or(false),
        Expr::BinaryExpr(BinaryExpr { left, right, .. }) => {
            expr_is_influxql_filled(left, schema) || expr_is_influxql_filled(right, schema)
        }
        Expr::Negative(expr) => expr_is_influxql_filled(expr, schema),
        Expr::Case(Case { when_then_expr, .. }) => when_then_expr
            .iter()
            .any(|(_, expr)| expr_is_influxql_filled(expr, schema)),
        Expr::Cast(Cast { expr, .. }) => expr_is_influxql_filled(expr, schema),
        Expr::TryCast(TryCast { expr, .. }) => expr_is_influxql_filled(expr, schema),
        Expr::ScalarFunction(ScalarFunction { args, .. }) => {
            args.iter().any(|arg| expr_is_influxql_filled(arg, schema))
        }
        Expr::AggregateFunction(AggregateFunction { params, .. }) => params
            .args
            .iter()
            .any(|arg| expr_is_influxql_filled(arg, schema)),
        Expr::WindowFunction(func) => func
            .params
            .args
            .iter()
            .any(|arg| expr_is_influxql_filled(arg, schema)),
        _ => false,
    }
}

pub(super) fn schema_with_influxql_filled(
    schema: DFSchemaRef,
    filled: &[bool],
) -> Result<DFSchemaRef> {
    if schema.fields().len() != filled.len() {
        return crate::error::internal(format!(
            "expected {} entries in filled list, got {}",
            schema.fields().len(),
            filled.len()
        ));
    }

    let md = schema.as_ref().metadata().clone();
    let qualified_fields = schema
        .iter()
        .zip(filled)
        .map(|((qualifier, field), filled)| {
            (
                qualifier.cloned(),
                Arc::new(field.as_ref().clone().with_influxql_filled(*filled)),
            )
        })
        .collect::<Vec<_>>();
    DFSchema::new_with_metadata(qualified_fields, md).map(Arc::new)
}

/// Strip `influxql::filled` metadata from all fields in a schema.
///
/// This is used before physical planning to remove InfluxQL-specific metadata that was used during
/// logical planning but would cause schema mismatch errors if present
///
/// Returns `Transformed::yes` with the new schema if any fields had the metadata
/// removed, or `Transformed::no` with the original schema if no changes were made.
pub(super) fn strip_influxql_metadata_from_schema(
    schema: &DFSchemaRef,
) -> Result<Transformed<DFSchemaRef>> {
    // Check if any field has metadata we need to strip
    let has_metadata = schema
        .fields()
        .iter()
        .any(|f| field_has_influxql_metadata(f));

    if !has_metadata {
        return Ok(Transformed::no(Arc::clone(schema)));
    }

    let md = schema.as_arrow().metadata().clone();
    let qualified_fields = schema
        .iter()
        .map(|(qualifier, field)| {
            let mut metadata = field.metadata().clone();
            metadata.remove(INFLUXQL_FILLED_KEY);
            (
                qualifier.cloned(),
                Arc::new(field.as_ref().clone().with_metadata(metadata)),
            )
        })
        .collect::<Vec<_>>();
    Ok(Transformed::yes(Arc::new(DFSchema::new_with_metadata(
        qualified_fields,
        md,
    )?)))
}

/// Check if any schema in the plan tree has InfluxQL-specific metadata.
fn plan_has_influxql_metadata(plan: &LogicalPlan) -> bool {
    use datafusion::common::tree_node::TreeNode;

    plan.exists(|node| {
        Ok(node
            .schema()
            .fields()
            .iter()
            .any(|f| field_has_influxql_metadata(f)))
    })
    .expect("infallible closure") // exists is infallible when closure is infallible
}

/// Strip `influxql::filled` metadata from all schemas in the logical plan tree.
///
/// This is used before physical planning to remove InfluxQL-specific metadata that was used during
/// logical planning but would cause schema mismatch errors if present
pub(crate) fn strip_influxql_metadata_from_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
    use datafusion::common::tree_node::TreeNode;

    // if no schema in the plan tree has the metadata, avoid
    // calling transform_down which would trigger a DataFusion bug where
    // Extension node schemas are rebuilt and lose their metadata.
    if !plan_has_influxql_metadata(&plan) {
        return Ok(plan);
    }

    // At least one schema has the metadata, so we need to strip it
    plan.transform_down(|plan| {
        transform_plan_schema(plan, |schema| strip_influxql_metadata_from_schema(&schema))
    })
    .map(|transformed| transformed.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use datafusion::{
        common::{Column, DFSchema},
        functions_aggregate::sum::sum_udaf,
        logical_expr::{
            BinaryExpr, Case, Cast, Operator, TryCast, WindowFunctionDefinition,
            expr::{
                AggregateFunction, AggregateFunctionParams, Alias, WindowFunction,
                WindowFunctionParams,
            },
        },
        scalar::ScalarValue,
    };

    fn make_field(name: &str, filled: bool) -> Field {
        Field::new(name, DataType::Int64, true).with_influxql_filled(filled)
    }

    fn make_schema() -> DFSchema {
        DFSchema::from_unqualified_fields(
            vec![
                make_field("filled_col", true),
                make_field("normal_col", false),
            ]
            .into(),
            Default::default(),
        )
        .unwrap()
    }

    #[test]
    fn test_expr_is_influxql_filled_column() {
        let schema = make_schema();

        // Filled column
        let expr = Expr::Column(Column::from_name("filled_col"));
        assert!(expr_is_influxql_filled(&expr, &schema));

        // Non-filled column
        let expr = Expr::Column(Column::from_name("normal_col"));
        assert!(!expr_is_influxql_filled(&expr, &schema));

        // Non-existent column
        let expr = Expr::Column(Column::from_name("missing_col"));
        assert!(!expr_is_influxql_filled(&expr, &schema));
    }

    #[test]
    fn test_expr_is_influxql_filled_alias() {
        let schema = make_schema();

        // Alias wrapping filled column
        let expr = Expr::Alias(Alias::new(
            Expr::Column(Column::from_name("filled_col")),
            None::<&str>,
            "alias",
        ));
        assert!(expr_is_influxql_filled(&expr, &schema));

        // Alias wrapping normal column
        let expr = Expr::Alias(Alias::new(
            Expr::Column(Column::from_name("normal_col")),
            None::<&str>,
            "alias",
        ));
        assert!(!expr_is_influxql_filled(&expr, &schema));
    }

    #[test]
    fn test_expr_is_influxql_filled_literal() {
        let schema = make_schema();

        // Literal with filled metadata
        let mut map = std::collections::BTreeMap::new();
        map.insert(INFLUXQL_FILLED_KEY.to_string(), "true".to_string());
        let metadata = datafusion::logical_expr::expr::FieldMetadata::from(map);
        let expr = Expr::Literal(ScalarValue::Int64(Some(42)), Some(metadata));
        assert!(expr_is_influxql_filled(&expr, &schema));

        // Literal without filled metadata
        let expr = Expr::Literal(ScalarValue::Int64(Some(42)), None);
        assert!(!expr_is_influxql_filled(&expr, &schema));

        // Literal with false filled metadata
        let mut map = std::collections::BTreeMap::new();
        map.insert(INFLUXQL_FILLED_KEY.to_string(), "false".to_string());
        let metadata = datafusion::logical_expr::expr::FieldMetadata::from(map);
        let expr = Expr::Literal(ScalarValue::Int64(Some(42)), Some(metadata));
        assert!(!expr_is_influxql_filled(&expr, &schema));
    }

    #[test]
    fn test_expr_is_influxql_filled_binary_expr() {
        let schema = make_schema();

        // Binary expression with filled left operand
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("filled_col"))),
            op: Operator::Plus,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)), None)),
        });
        assert!(expr_is_influxql_filled(&expr, &schema));

        // Binary expression with filled right operand
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("normal_col"))),
            op: Operator::Plus,
            right: Box::new(Expr::Column(Column::from_name("filled_col"))),
        });
        assert!(expr_is_influxql_filled(&expr, &schema));

        // Binary expression with no filled operands
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::from_name("normal_col"))),
            op: Operator::Plus,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)), None)),
        });
        assert!(!expr_is_influxql_filled(&expr, &schema));
    }

    #[test]
    fn test_expr_is_influxql_filled_negative() {
        let schema = make_schema();

        // Negative of filled column
        let expr = Expr::Negative(Box::new(Expr::Column(Column::from_name("filled_col"))));
        assert!(expr_is_influxql_filled(&expr, &schema));

        // Negative of normal column
        let expr = Expr::Negative(Box::new(Expr::Column(Column::from_name("normal_col"))));
        assert!(!expr_is_influxql_filled(&expr, &schema));
    }

    #[test]
    fn test_expr_is_influxql_filled_case() {
        let schema = make_schema();

        // Case with filled then expression
        let expr = Expr::Case(Case {
            expr: None,
            when_then_expr: vec![(
                Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)), None)),
                Box::new(Expr::Column(Column::from_name("filled_col"))),
            )],
            else_expr: None,
        });
        assert!(expr_is_influxql_filled(&expr, &schema));

        // Case with no filled expressions
        let expr = Expr::Case(Case {
            expr: None,
            when_then_expr: vec![(
                Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)), None)),
                Box::new(Expr::Column(Column::from_name("normal_col"))),
            )],
            else_expr: None,
        });
        assert!(!expr_is_influxql_filled(&expr, &schema));
    }

    #[test]
    fn test_expr_is_influxql_filled_cast() {
        let schema = make_schema();

        // Cast of filled column
        let expr = Expr::Cast(Cast {
            expr: Box::new(Expr::Column(Column::from_name("filled_col"))),
            data_type: DataType::Float64,
        });
        assert!(expr_is_influxql_filled(&expr, &schema));

        // Cast of normal column
        let expr = Expr::Cast(Cast {
            expr: Box::new(Expr::Column(Column::from_name("normal_col"))),
            data_type: DataType::Float64,
        });
        assert!(!expr_is_influxql_filled(&expr, &schema));
    }

    #[test]
    fn test_expr_is_influxql_filled_try_cast() {
        let schema = make_schema();

        // TryCast of filled column
        let expr = Expr::TryCast(TryCast {
            expr: Box::new(Expr::Column(Column::from_name("filled_col"))),
            data_type: DataType::Float64,
        });
        assert!(expr_is_influxql_filled(&expr, &schema));

        // TryCast of normal column
        let expr = Expr::TryCast(TryCast {
            expr: Box::new(Expr::Column(Column::from_name("normal_col"))),
            data_type: DataType::Float64,
        });
        assert!(!expr_is_influxql_filled(&expr, &schema));
    }

    #[test]
    fn test_expr_is_influxql_filled_scalar_function() {
        use datafusion::functions::expr_fn::abs;
        let schema = make_schema();

        // Scalar function with filled argument using datafusion's abs function
        let filled_col = Expr::Column(Column::from_name("filled_col"));
        let expr = abs(filled_col);
        assert!(expr_is_influxql_filled(&expr, &schema));

        // Scalar function with normal argument
        let normal_col = Expr::Column(Column::from_name("normal_col"));
        let expr = abs(normal_col);
        assert!(!expr_is_influxql_filled(&expr, &schema));
    }

    #[test]
    fn test_expr_is_influxql_filled_aggregate_function() {
        let schema = make_schema();

        // Aggregate function with filled argument
        let expr = Expr::AggregateFunction(AggregateFunction {
            func: sum_udaf(),
            params: AggregateFunctionParams {
                args: vec![Expr::Column(Column::from_name("filled_col"))],
                distinct: false,
                filter: None,
                order_by: vec![],
                null_treatment: None,
            },
        });
        assert!(expr_is_influxql_filled(&expr, &schema));

        // Aggregate function with normal argument
        let expr = Expr::AggregateFunction(AggregateFunction {
            func: sum_udaf(),
            params: AggregateFunctionParams {
                args: vec![Expr::Column(Column::from_name("normal_col"))],
                distinct: false,
                filter: None,
                order_by: vec![],
                null_treatment: None,
            },
        });
        assert!(!expr_is_influxql_filled(&expr, &schema));
    }

    #[test]
    fn test_expr_is_influxql_filled_window_function() {
        use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
        let schema = make_schema();

        // Window function with filled argument
        let window_frame = WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::Following(ScalarValue::UInt64(None)),
        );

        let expr = Expr::WindowFunction(Box::new(WindowFunction {
            fun: WindowFunctionDefinition::AggregateUDF(sum_udaf()),
            params: WindowFunctionParams {
                args: vec![Expr::Column(Column::from_name("filled_col"))],
                partition_by: vec![],
                order_by: vec![],
                window_frame,
                filter: None,
                null_treatment: None,
                distinct: false,
            },
        }));
        assert!(expr_is_influxql_filled(&expr, &schema));

        // Window function with normal argument
        let window_frame = WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::Following(ScalarValue::UInt64(None)),
        );

        let expr = Expr::WindowFunction(Box::new(WindowFunction {
            fun: WindowFunctionDefinition::AggregateUDF(sum_udaf()),
            params: WindowFunctionParams {
                args: vec![Expr::Column(Column::from_name("normal_col"))],
                partition_by: vec![],
                order_by: vec![],
                window_frame,
                filter: None,
                null_treatment: None,
                distinct: false,
            },
        }));
        assert!(!expr_is_influxql_filled(&expr, &schema));
    }

    #[test]
    fn test_expr_is_influxql_filled_unsupported() {
        let schema = make_schema();

        // Placeholder expression (returns false for _ wildcard pattern)
        let expr = Expr::Placeholder(datafusion::logical_expr::expr::Placeholder::new(
            "placeholder".to_string(),
            Some(DataType::Int64),
        ));
        assert!(!expr_is_influxql_filled(&expr, &schema));
    }

    #[test]
    fn test_field_ext_trait() {
        let mut field = Field::new("test", DataType::Int64, true);

        // Default should be false
        assert!(!field.is_influxql_filled());

        // Set to true
        field.set_influxql_filled(true);
        assert!(field.is_influxql_filled());

        // Set back to false
        field.set_influxql_filled(false);
        assert!(!field.is_influxql_filled());

        // Test with_influxql_filled builder method
        let field = Field::new("test", DataType::Int64, true).with_influxql_filled(true);
        assert!(field.is_influxql_filled());
    }

    #[test]
    fn test_schema_with_influxql_filled_basic() {
        // Create a simple schema without filled markers
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("col1", DataType::Int64, true),
                Field::new("col2", DataType::Float64, true),
                Field::new("col3", DataType::Utf8, true),
            ]
            .into(),
            Default::default(),
        )
        .unwrap();
        let schema_ref = Arc::new(schema);

        // Mark first and third columns as filled
        let filled = vec![true, false, true];
        let result = schema_with_influxql_filled(schema_ref, &filled).unwrap();

        // Verify the filled markers are set correctly
        assert_eq!(result.fields().len(), 3);
        assert!(result.field(0).is_influxql_filled());
        assert!(!result.field(1).is_influxql_filled());
        assert!(result.field(2).is_influxql_filled());

        // Verify field names and types are preserved
        assert_eq!(result.field(0).name(), "col1");
        assert_eq!(result.field(1).name(), "col2");
        assert_eq!(result.field(2).name(), "col3");
        assert_eq!(result.field(0).data_type(), &DataType::Int64);
        assert_eq!(result.field(1).data_type(), &DataType::Float64);
        assert_eq!(result.field(2).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_schema_with_influxql_filled_all_true() {
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("col1", DataType::Int64, true),
                Field::new("col2", DataType::Float64, true),
            ]
            .into(),
            Default::default(),
        )
        .unwrap();
        let schema_ref = Arc::new(schema);

        // Mark all columns as filled
        let filled = vec![true, true];
        let result = schema_with_influxql_filled(schema_ref, &filled).unwrap();

        assert!(result.field(0).is_influxql_filled());
        assert!(result.field(1).is_influxql_filled());
    }

    #[test]
    fn test_schema_with_influxql_filled_all_false() {
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("col1", DataType::Int64, true),
                Field::new("col2", DataType::Float64, true),
            ]
            .into(),
            Default::default(),
        )
        .unwrap();
        let schema_ref = Arc::new(schema);

        // Mark no columns as filled
        let filled = vec![false, false];
        let result = schema_with_influxql_filled(schema_ref, &filled).unwrap();

        assert!(!result.field(0).is_influxql_filled());
        assert!(!result.field(1).is_influxql_filled());
    }

    #[test]
    fn test_schema_with_influxql_filled_empty() {
        // Create an empty schema
        let schema =
            DFSchema::from_unqualified_fields(Vec::<Field>::new().into(), Default::default())
                .unwrap();
        let schema_ref = Arc::new(schema);

        let filled = vec![];
        let result = schema_with_influxql_filled(schema_ref, &filled).unwrap();

        assert_eq!(result.fields().len(), 0);
    }

    #[test]
    fn test_schema_with_influxql_filled_with_qualifiers() {
        use datafusion::common::TableReference;

        // Create a schema with qualified fields
        let table_ref = TableReference::bare("my_table");
        let schema = DFSchema::from_field_specific_qualified_schema(
            vec![Some(table_ref.clone()), Some(table_ref.clone())],
            &Arc::new(arrow::datatypes::Schema::new(vec![
                Field::new("col1", DataType::Int64, true),
                Field::new("col2", DataType::Float64, true),
            ])),
        )
        .unwrap();
        let schema_ref = Arc::new(schema);

        let filled = vec![true, false];
        let result = schema_with_influxql_filled(Arc::clone(&schema_ref), &filled).unwrap();

        // Verify filled markers
        assert!(result.field(0).is_influxql_filled());
        assert!(!result.field(1).is_influxql_filled());

        // Verify qualifiers are preserved
        let (qualifier1, _) = result.qualified_field(0);
        let (qualifier2, _) = result.qualified_field(1);
        assert!(qualifier1.is_some());
        assert!(qualifier2.is_some());
        assert_eq!(qualifier1, qualifier2);
    }

    #[test]
    fn test_schema_with_influxql_filled_preserves_metadata() {
        use std::collections::HashMap;

        // Create schema with custom metadata
        let mut metadata = HashMap::new();
        metadata.insert("custom_key".to_string(), "custom_value".to_string());

        let schema = DFSchema::from_unqualified_fields(
            vec![Field::new("col1", DataType::Int64, true)].into(),
            metadata.clone(),
        )
        .unwrap();
        let schema_ref = Arc::new(schema);

        let filled = vec![true];
        let result = schema_with_influxql_filled(schema_ref, &filled).unwrap();

        // Verify schema metadata is preserved
        assert_eq!(result.as_ref().metadata(), &metadata);
    }

    #[test]
    fn test_schema_with_influxql_filled_length_mismatch_too_few() {
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("col1", DataType::Int64, true),
                Field::new("col2", DataType::Float64, true),
                Field::new("col3", DataType::Utf8, true),
            ]
            .into(),
            Default::default(),
        )
        .unwrap();
        let schema_ref = Arc::new(schema);

        // Provide too few filled entries
        let filled = vec![true, false];
        let result = schema_with_influxql_filled(schema_ref, &filled);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("expected 3 entries"));
        assert!(err_msg.contains("got 2"));
    }

    #[test]
    fn test_schema_with_influxql_filled_length_mismatch_too_many() {
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("col1", DataType::Int64, true),
                Field::new("col2", DataType::Float64, true),
            ]
            .into(),
            Default::default(),
        )
        .unwrap();
        let schema_ref = Arc::new(schema);

        // Provide too many filled entries
        let filled = vec![true, false, true, false];
        let result = schema_with_influxql_filled(schema_ref, &filled);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("expected 2 entries"));
        assert!(err_msg.contains("got 4"));
    }

    #[test]
    fn test_schema_with_influxql_filled_overrides_existing() {
        // Create a schema with some fields already marked as filled
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("col1", DataType::Int64, true).with_influxql_filled(true),
                Field::new("col2", DataType::Float64, true).with_influxql_filled(true),
            ]
            .into(),
            Default::default(),
        )
        .unwrap();
        let schema_ref = Arc::new(schema);

        // Override with different filled values
        let filled = vec![false, true];
        let result = schema_with_influxql_filled(schema_ref, &filled).unwrap();

        // Verify the new filled values override the old ones
        assert!(!result.field(0).is_influxql_filled());
        assert!(result.field(1).is_influxql_filled());
    }
}
