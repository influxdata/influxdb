use arrow::datatypes::DataType;
use datafusion::{
    common::{tree_node::TreeNodeRewriter, DFSchema},
    error::Result as DatafusionResult,
    logical_expr::{BinaryExpr, ExprSchemable},
    prelude::{binary_expr, lit, Expr},
    scalar::ScalarValue,
};
use predicate::rpc_predicate::{FIELD_COLUMN_NAME, MEASUREMENT_COLUMN_NAME};
use schema::Schema;

/// Rewrites the provided expr such that references to any column that
/// are not present in `schema` become null.
///
/// So for example, if the predicate is
///
/// `(STATE = 'CA') OR (READING >0)`
///
/// but the schema only has `STATE` (and not `READING`), then the
/// predicate is rewritten to
///
/// `(STATE = 'CA') OR (NULL >0)`
///
/// This matches the Influx data model where any value that is not
/// explicitly specified is implicitly NULL. Since different chunks
/// and measurements can have different subsets of the columns, only
/// parts of the predicate make sense.
/// See comments on 'is_null_column'
#[derive(Debug)]
pub struct MissingColumnsToNull<'a> {
    schema: &'a Schema,
    df_schema: DFSchema,
}

impl<'a> MissingColumnsToNull<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        let df_schema: DFSchema = schema
            .as_arrow()
            .as_ref()
            .clone()
            .try_into()
            .expect("Create DF Schema");

        Self { schema, df_schema }
    }

    /// Returns true if `expr` is a `Expr::Column` reference to a
    /// column that doesn't exist in this schema
    fn is_null_column(&self, expr: &Expr) -> bool {
        if let Expr::Column(column) = &expr {
            if column.name != MEASUREMENT_COLUMN_NAME && column.name != FIELD_COLUMN_NAME {
                return self.schema.find_index_of(&column.name).is_none();
            }
        }
        false
    }

    /// Rewrites an arg like col if col refers to a non existent
    /// column into a null literal with "type" of `other_arg`, if possible
    fn rewrite_op_arg(&self, arg: Expr, other_arg: &Expr) -> DatafusionResult<Expr> {
        if self.is_null_column(&arg) {
            let other_datatype = match other_arg.get_type(&self.df_schema) {
                Ok(other_datatype) => other_datatype,
                Err(_) => {
                    // the other arg is also unknown and will be
                    // rewritten, default to Int32 (sins due to
                    // https://github.com/apache/arrow-datafusion/issues/1179)
                    DataType::Int32
                }
            };

            let scalar: ScalarValue = (&other_datatype).try_into()?;
            Ok(Expr::Literal(scalar))
        } else {
            Ok(arg)
        }
    }
}

impl<'a> TreeNodeRewriter for MissingColumnsToNull<'a> {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> DatafusionResult<Expr> {
        // Ideally this would simply find all Expr::Columns and
        // replace them with a constant NULL value. However, doing do
        // is blocked on DF bug
        // https://github.com/apache/arrow-datafusion/issues/1179
        //
        // Until then, we need to know what type of expr the column is
        // being compared with, so workaround by finding the datatype of the other arg
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let left = self.rewrite_op_arg(*left, &right)?;
                let right = self.rewrite_op_arg(*right, &left)?;
                Ok(binary_expr(left, op, right))
            }
            Expr::IsNull(expr) if self.is_null_column(&expr) => Ok(lit(true)),
            Expr::IsNotNull(expr) if self.is_null_column(&expr) => Ok(lit(false)),
            expr => Ok(expr),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use datafusion::{
        common::tree_node::TreeNode,
        prelude::{col, lit},
        scalar::ScalarValue,
    };
    use schema::builder::SchemaBuilder;

    use super::*;

    #[test]
    fn test_missing_colums_to_null() {
        let schema = SchemaBuilder::new()
            .tag("tag")
            .field("str", DataType::Utf8)
            .unwrap()
            .field("int", DataType::Int64)
            .unwrap()
            .field("uint", DataType::UInt64)
            .unwrap()
            .field("float", DataType::Float64)
            .unwrap()
            .field("bool", DataType::Boolean)
            .unwrap()
            .build()
            .unwrap();

        // The fact that these need to be typed is due to
        // https://github.com/apache/arrow-datafusion/issues/1179
        let utf8_null = Expr::Literal(ScalarValue::Utf8(None));
        let int32_null = Expr::Literal(ScalarValue::Int32(None));

        // no rewrite
        let expr = lit(1);
        let expected = expr.clone();
        assert_rewrite(&schema, expr, expected);

        // tag != str (no rewrite)
        let expr = col("tag").not_eq(col("str"));
        let expected = expr.clone();
        assert_rewrite(&schema, expr, expected);

        // tag == str (no rewrite)
        let expr = col("tag").eq(col("str"));
        let expected = expr.clone();
        assert_rewrite(&schema, expr, expected);

        // int < 5 (no rewrite, int part of schema)
        let expr = col("int").lt(lit(5));
        let expected = expr.clone();
        assert_rewrite(&schema, expr, expected);

        // unknown < 5 --> NULL < 5 (unknown not in schema)
        let expr = col("unknown").lt(lit(5));
        let expected = int32_null.clone().lt(lit(5));
        assert_rewrite(&schema, expr, expected);

        // 5 < unknown --> 5 < NULL (unknown not in schema)
        let expr = lit(5).lt(col("unknown"));
        let expected = lit(5).lt(int32_null.clone());
        assert_rewrite(&schema, expr, expected);

        // _measurement < 5 --> _measurement < 5 (special column)
        let expr = col("_measurement").lt(lit(5));
        let expected = expr.clone();
        assert_rewrite(&schema, expr, expected);

        // _field < 5 --> _field < 5 (special column)
        let expr = col("_field").lt(lit(5));
        let expected = expr.clone();
        assert_rewrite(&schema, expr, expected);

        // _field < 5 OR col("unknown") < 5 --> _field < 5 OR (NULL < 5)
        let expr = col("_field").lt(lit(5)).or(col("unknown").lt(lit(5)));
        let expected = col("_field").lt(lit(5)).or(int32_null.clone().lt(lit(5)));
        assert_rewrite(&schema, expr, expected);

        // unknown < unknown2 -->  NULL < NULL (both unknown columns)
        let expr = col("unknown").lt(col("unknown2"));
        let expected = int32_null.clone().lt(int32_null);
        assert_rewrite(&schema, expr, expected);

        // int < 5 OR unknown != "foo"
        let expr = col("int").lt(lit(5)).or(col("unknown").not_eq(lit("foo")));
        let expected = col("int").lt(lit(5)).or(utf8_null.not_eq(lit("foo")));
        assert_rewrite(&schema, expr, expected);

        // int IS NULL
        let expr = col("int").is_null();
        let expected = expr.clone();
        assert_rewrite(&schema, expr, expected);

        // unknown IS NULL --> true
        let expr = col("unknown").is_null();
        let expected = lit(true);
        assert_rewrite(&schema, expr, expected);

        // int IS NOT NULL
        let expr = col("int").is_not_null();
        let expected = expr.clone();
        assert_rewrite(&schema, expr, expected);

        // unknown IS NOT NULL --> false
        let expr = col("unknown").is_not_null();
        let expected = lit(false);
        assert_rewrite(&schema, expr, expected);
    }

    fn assert_rewrite(schema: &Schema, expr: Expr, expected: Expr) {
        let mut rewriter = MissingColumnsToNull::new(schema);
        let rewritten_expr = expr
            .clone()
            .rewrite(&mut rewriter)
            .expect("Rewrite successful");

        assert_eq!(
            &rewritten_expr, &expected,
            "Mismatch rewriting\nInput: {expr}\nRewritten: {rewritten_expr}\nExpected: {expected}"
        );
    }
}
