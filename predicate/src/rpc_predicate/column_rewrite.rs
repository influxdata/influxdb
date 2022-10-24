use std::sync::Arc;

use datafusion::{
    error::Result as DataFusionResult, logical_expr::expr_rewriter::ExprRewriter, prelude::*,
    scalar::ScalarValue,
};
use schema::Schema;

/// Logic for rewriting expressions from influxrpc that reference non
/// existent columns to NULL
#[derive(Debug)]
pub(crate) struct MissingColumnRewriter {
    /// The input schema
    schema: Arc<Schema>,
}

impl MissingColumnRewriter {
    /// Create a new [`MissingColumnRewriter`] targeting the given schema
    pub(crate) fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }

    fn column_exists(&self, col: &Column) -> DataFusionResult<bool> {
        // todo a real error here (rpc_predicates shouldn't have table/relation qualifiers)
        assert!(col.relation.is_none());

        if self.schema.find_index_of(&col.name).is_some() {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

fn lit_null() -> Expr {
    lit(ScalarValue::Utf8(None))
}

impl ExprRewriter for MissingColumnRewriter {
    fn mutate(&mut self, expr: Expr) -> DataFusionResult<Expr> {
        Ok(match expr {
            Expr::Column(col) if !self.column_exists(&col)? => lit_null(),
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

        // f1 > 1.0
        let expr = col("f1").gt(lit(1.0));
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

    fn rewrite(expr: Expr) -> Expr {
        let schema = SchemaBuilder::new()
            .tag("t1")
            .field("f1", DataType::Int64)
            .unwrap()
            .build()
            .unwrap();

        let mut rewriter = MissingColumnRewriter::new(Arc::new(schema));
        expr.rewrite(&mut rewriter).unwrap()
    }
}
