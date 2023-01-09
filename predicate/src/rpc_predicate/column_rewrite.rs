use datafusion::{
    error::Result as DataFusionResult, logical_expr::expr_rewriter::ExprRewriter, prelude::*,
    scalar::ScalarValue,
};
use schema::{InfluxColumnType, Schema};

/// Logic for rewriting expressions from influxrpc that reference non
/// existent columns, or columns that are not tags, to NULL.
#[derive(Debug)]
pub(crate) struct MissingTagColumnRewriter {
    /// The input schema
    schema: Schema,
}

impl MissingTagColumnRewriter {
    /// Create a new [`MissingTagColumnRewriter`] targeting the given schema
    pub(crate) fn new(schema: Schema) -> Self {
        Self { schema }
    }

    fn tag_column_exists(&self, col: &Column) -> DataFusionResult<bool> {
        // todo a real error here (rpc_predicates shouldn't have table/relation qualifiers)
        assert!(col.relation.is_none());

        let exists = self
            .schema
            .find_index_of(&col.name)
            .map(|i| self.schema.field(i).0)
            .map(|influx_column_type| influx_column_type == InfluxColumnType::Tag)
            .unwrap_or(false);
        Ok(exists)
    }
}

fn lit_null() -> Expr {
    lit(ScalarValue::Utf8(None))
}

impl ExprRewriter for MissingTagColumnRewriter {
    fn mutate(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        Ok(match expr {
            Expr::Column(col) if !self.tag_column_exists(&col)? => lit_null(),
            expr => expr,
        })
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{arrow::datatypes::DataType, logical_expr::expr_rewriter::ExprRewritable};
    use schema::SchemaBuilder;

    use super::*;

    #[test]
    fn all_columns_defined_no_rewrite() {
        // t1 = "foo"
        let expr = col("t1").eq(lit("foo"));
        assert_eq!(rewrite(expr.clone()), expr);

        // t2 = "bar"
        let expr = col("t2").eq(lit("bar"));
        assert_eq!(rewrite(expr.clone()), expr);
    }

    #[test]
    fn all_columns_not_defined() {
        // non_defined = "foo" --> NULL = "foo"
        let expr = col("non_defined").eq(lit("foo"));
        let expected = lit_null().eq(lit("foo"));
        assert_eq!(rewrite(expr), expected);

        // non_defined = 1.4 --> NULL = 1.4
        let expr = col("non_defined").eq(lit(1.4));
        // No type is inferred so this is a literal null string (even though it maybe should be a literal float)
        let expected = lit_null().eq(lit(1.4));
        assert_eq!(rewrite(expr), expected);
    }

    #[test]
    fn some_columns_not_defined() {
        // t1 = "foo" AND non_defined = "bar" --> t1 = "foo" and NULL = "bar"
        let expr = col("t1")
            .eq(lit("foo"))
            .and(col("non_defined").eq(lit("bar")));
        let expected = col("t1").eq(lit("foo")).and(lit_null().eq(lit("bar")));
        assert_eq!(rewrite(expr), expected);
    }

    #[test]
    fn column_is_field() {
        let expr = col("f1").eq(lit(31));
        let expected = lit_null().eq(lit(31));
        assert_eq!(rewrite(expr), expected);
    }

    fn rewrite(expr: Expr) -> Expr {
        let schema = SchemaBuilder::new()
            .tag("t1")
            .tag("t2")
            .field("f1", DataType::Int64)
            .unwrap()
            .build()
            .unwrap();

        let mut rewriter = MissingTagColumnRewriter::new(schema);
        expr.rewrite(&mut rewriter).unwrap()
    }
}
