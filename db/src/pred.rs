//! This module contains code to convert between query predicates and
//! the predicates required by the various storage formats

use std::convert::TryFrom;

use predicate::predicate::Predicate;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error translating predicate: {}", msg))]
    ReadBufferPredicate { msg: String, pred: Predicate },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Converts a [`predicate::predicate::Predicate`] into [`read_buffer::Predicate`],
/// suitable for evaluating on the ReadBuffer.
///
/// NOTE: a valid Read Buffer predicate is not guaranteed to be applicable to an
/// arbitrary Read Buffer chunk, because the applicability of a predicate
/// depends on the schema of the chunk.
///
/// Callers should validate predicates against chunks they are to be executed
/// against using `read_buffer::Chunk::validate_predicate`
pub fn to_read_buffer_predicate(predicate: &Predicate) -> Result<read_buffer::Predicate> {
    // Try to convert non-time column expressions into binary expressions
    // that are compatible with the read buffer.
    match predicate
        .exprs
        .iter()
        .map(read_buffer::BinaryExpr::try_from)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(exprs) => {
            // Construct a `ReadBuffer` predicate with or without
            // InfluxDB-specific expressions on the time column.
            Ok(match predicate.range {
                Some(range) => {
                    read_buffer::Predicate::with_time_range(&exprs, range.start, range.end)
                }
                None => read_buffer::Predicate::new(exprs),
            })
        }
        Err(e) => Err(Error::ReadBufferPredicate {
            msg: e,
            pred: predicate.clone(),
        }),
    }
}

#[cfg(test)]
pub mod test {
    use super::*;
    use datafusion::logical_plan::{col, lit, Expr};

    use datafusion::scalar::ScalarValue;
    use predicate::predicate::PredicateBuilder;
    use read_buffer::BinaryExpr as RBBinaryExpr;
    use read_buffer::Predicate as RBPredicate;

    #[test]
    fn into_read_buffer_predicate() {
        let cases = vec![
            // empty predicate
            (PredicateBuilder::default().build(), RBPredicate::default()),
            // just a time range
            (
                PredicateBuilder::default()
                    .timestamp_range(100, 2000)
                    .build(),
                RBPredicate::with_time_range(&[], 100, 2000),
            ),
            // just a single non-time-range expression
            (
                PredicateBuilder::default()
                    .add_expr(col("track").eq(lit("Star Roving")))
                    .build(),
                RBPredicate::new(vec![RBBinaryExpr::from(("track", "=", "Star Roving"))]),
            ),
            // multiple non-time-range expressions
            (
                PredicateBuilder::default()
                    .add_expr(col("track").eq(lit("Star Roving")))
                    .add_expr(col("counter").gt(lit(2992_i64)))
                    .build(),
                RBPredicate::new(vec![
                    RBBinaryExpr::from(("track", "=", "Star Roving")),
                    RBBinaryExpr::from(("counter", ">", 2992_i64)),
                ]),
            ),
            // a bit of everything
            (
                PredicateBuilder::default()
                    .timestamp_range(100, 2000)
                    .add_expr(col("track").eq(lit("Star Roving")))
                    .add_expr(col("counter").gt(lit(2992_i64)))
                    .build(),
                RBPredicate::with_time_range(
                    &[
                        RBBinaryExpr::from(("track", "=", "Star Roving")),
                        RBBinaryExpr::from(("counter", ">", 2992_i64)),
                    ],
                    100,
                    2000,
                ),
            ),
        ];

        for (predicate, exp) in cases {
            assert_eq!(to_read_buffer_predicate(&predicate).unwrap(), exp);
        }

        let cases = vec![
            // not a binary expression
            (
                PredicateBuilder::default().add_expr(lit(100_i64)).build(),
                "unsupported expression type Int64(100)",
            ),
            // left side must be a column
            (
                PredicateBuilder::default()
                    .add_expr(lit("The Stove &").eq(lit("The Toaster")))
                    .build(),
                "unsupported expression Utf8(\"The Stove &\") Eq Utf8(\"The Toaster\")",
            ),
            // unsupported operator LIKE
            (
                PredicateBuilder::default()
                    .add_expr(col("track").like(lit("Star Roving")))
                    .build(),
                "unsupported operator Like",
            ),
            // right side must be a literal
            (
                PredicateBuilder::default()
                    .add_expr(col("Intermezzo 1").eq(Expr::Wildcard))
                    .build(),
                "unsupported expression #Intermezzo 1 Eq *",
            ),
            // binary expression like foo = NULL not supported
            (
                PredicateBuilder::default()
                    .add_expr(col("track").eq(Expr::Literal(ScalarValue::Utf8(None))))
                    .build(),
                "NULL literal not supported",
            ),
        ];

        for (predicate, exp) in cases {
            match to_read_buffer_predicate(&predicate).unwrap_err() {
                Error::ReadBufferPredicate { msg, pred: _ } => {
                    assert_eq!(msg, exp.to_owned());
                }
            }
        }
    }
}
