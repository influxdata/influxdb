//! DataFusion User Defined Functions (UDF/ UDAF) for IOx
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use datafusion::{
    logical_plan::{Expr, FunctionRegistry},
    prelude::lit,
};

/// Grouping by structs
pub mod group_by;

/// Regular Expressions
mod regex;

/// Flux selector expressions
pub mod selectors;

/// Time window and groupin
pub mod window;

/// Function registry
mod registry;

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

/// Return an [`FunctionRegistry`] with the implementations of IOx UDFs
pub fn registry() -> &'static dyn FunctionRegistry {
    registry::instance()
}
