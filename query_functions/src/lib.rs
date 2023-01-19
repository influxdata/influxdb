//! DataFusion User Defined Functions (UDF/ UDAF) for IOx
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]

use datafusion::{
    execution::FunctionRegistry,
    prelude::{lit, Expr, SessionContext},
};
use group_by::WindowDuration;
use window::EncodedWindowDuration;

/// Grouping by structs
pub mod group_by;

/// Regular Expressions
mod regex;

/// Selector Functions
pub mod selectors;

/// window_bounds expressions
mod window;

/// gap filling expressions
pub mod gapfill;

/// Function registry
mod registry;

pub use crate::regex::clean_non_meta_escapes;
pub use crate::regex::REGEX_MATCH_UDF_NAME;
pub use crate::regex::REGEX_NOT_MATCH_UDF_NAME;

/// Return an Expr that invokes a InfluxRPC compatible regex match to
/// determine which values satisfy the pattern. Equivalent to:
///
/// ```text
/// col ~= /pattern/
/// ```
pub fn regex_match_expr(input: Expr, pattern: String) -> Expr {
    registry()
        .udf(regex::REGEX_MATCH_UDF_NAME)
        .expect("RegexMatch function not registered")
        .call(vec![input, lit(pattern)])
}

/// Return an Expr that invokes a InfluxRPC compatible regex match to
/// determine which values do not satisfy the pattern. Equivalent to:
///
/// ```text
/// col !~ /pattern/
/// ```
pub fn regex_not_match_expr(input: Expr, pattern: String) -> Expr {
    registry()
        .udf(regex::REGEX_NOT_MATCH_UDF_NAME)
        .expect("NotRegexMatch function not registered")
        .call(vec![input, lit(pattern)])
}

/// Create a DataFusion `Expr` that invokes `window_bounds` with the
/// appropriate every and offset arguments at runtime
pub fn make_window_bound_expr(
    time_arg: Expr,
    every: WindowDuration,
    offset: WindowDuration,
) -> Expr {
    let encoded_every: EncodedWindowDuration = every.into();
    let encoded_offset: EncodedWindowDuration = offset.into();

    registry()
        .udf(window::WINDOW_BOUNDS_UDF_NAME)
        .expect("WindowBounds function not registered")
        .call(vec![
            time_arg,
            lit(encoded_every.ty),
            lit(encoded_every.field1),
            lit(encoded_every.field2),
            lit(encoded_offset.ty),
            lit(encoded_offset.field1),
            lit(encoded_offset.field2),
        ])
}

/// Return an [`FunctionRegistry`] with the implementations of IOx UDFs
pub fn registry() -> &'static dyn FunctionRegistry {
    registry::instance()
}

/// registers scalar functions so they can be invoked via SQL
pub fn register_scalar_functions(ctx: &SessionContext) {
    let registry = registry();
    for f in registry.udfs() {
        let udf = registry.udf(&f).unwrap();
        ctx.register_udf(udf.as_ref().clone())
    }
}

#[cfg(test)]
mod test {
    use arrow::{
        array::{ArrayRef, StringArray, TimestampNanosecondArray},
        record_batch::RecordBatch,
    };
    use datafusion::{assert_batches_eq, prelude::col};
    use datafusion_util::context_with_table;
    use std::sync::Arc;

    use super::*;

    /// plumbing test to validate registry is connected. functions are
    /// tested more thoroughly in their own modules
    #[tokio::test]
    async fn test_regex_match_expr() {
        let batch = RecordBatch::try_from_iter(vec![(
            "data",
            Arc::new(StringArray::from(vec!["Foo", "Bar", "FooBar"])) as ArrayRef,
        )])
        .unwrap();

        let ctx = context_with_table(batch);
        let result = ctx
            .table("t")
            .await
            .unwrap()
            .filter(regex_match_expr(col("data"), "Foo".into()))
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = vec![
            "+--------+",
            "| data   |",
            "+--------+",
            "| Foo    |",
            "| FooBar |",
            "+--------+",
        ];

        assert_batches_eq!(&expected, &result);
    }

    /// plumbing test to validate registry is connected. functions are
    /// tested more thoroughly in their own modules
    #[tokio::test]
    async fn test_regex_not_match_expr() {
        let batch = RecordBatch::try_from_iter(vec![(
            "data",
            Arc::new(StringArray::from(vec!["Foo", "Bar", "FooBar"])) as ArrayRef,
        )])
        .unwrap();

        let ctx = context_with_table(batch);
        let result = ctx
            .table("t")
            .await
            .unwrap()
            .filter(regex_not_match_expr(col("data"), "Foo".into()))
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = vec!["+------+", "| data |", "+------+", "| Bar  |", "+------+"];

        assert_batches_eq!(&expected, &result);
    }

    /// plumbing test to validate registry is connected. functions are
    /// tested more thoroughly in their own modules
    #[tokio::test]
    async fn test_make_window_bound_expr() {
        let batch = RecordBatch::try_from_iter(vec![(
            "time",
            Arc::new(TimestampNanosecondArray::from(vec![Some(1000), Some(2000)])) as ArrayRef,
        )])
        .unwrap();

        let each = WindowDuration::Fixed { nanoseconds: 100 };
        let every = WindowDuration::Fixed { nanoseconds: 200 };

        let ctx = context_with_table(batch);
        let result = ctx
            .table("t")
            .await
            .unwrap()
            .select(vec![
                col("time"),
                make_window_bound_expr(col("time"), each, every).alias("bound"),
            ])
            .unwrap()
            .collect()
            .await
            .unwrap();

        let expected = vec![
            "+----------------------------+-------------------------------+",
            "| time                       | bound                         |",
            "+----------------------------+-------------------------------+",
            "| 1970-01-01T00:00:00.000001 | 1970-01-01T00:00:00.000001100 |",
            "| 1970-01-01T00:00:00.000002 | 1970-01-01T00:00:00.000002100 |",
            "+----------------------------+-------------------------------+",
        ];

        assert_batches_eq!(&expected, &result);
    }
}
