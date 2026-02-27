//! Handling of InfluxQL style `LIMIT` and `OFFSET` clauses.
//!
//! This module provides functionality to apply `LIMIT` and `OFFSET`
//! clauses individually to time series data. It is designed to be
//! compatible with older version of InfluxDB that applied `LIMIT` and
//! `OFFSET` conditions in each iterator before they are combined.

use arrow::compute::SortOptions;
use datafusion::{
    common::{Result, internal_err},
    execution::context::SessionState,
    logical_expr::{Expr, LogicalPlan},
    physical_plan::{ExecutionPlan, expressions::PhysicalSortExpr},
    scalar::ScalarValue,
    sql::sqlparser::ast::NullTreatment,
};
use std::sync::Arc;

mod logical;
mod physical;

pub use logical::{LimitExpr, SeriesLimit};
pub use physical::{PhysicalLimitExpr, SeriesLimitExec};

/// Plan a SeriesLimit logical node into a physical SeriesLimitExec.
///
/// This function converts the logical representation of per-series limiting
/// into a physical execution plan that can be executed by DataFusion.
///
/// # Arguments
///
/// * `session_state` - The DataFusion session state for creating physical expressions
/// * `series_limit` - The logical SeriesLimit node to plan
/// * `logical_inputs` - The logical input plans (must be exactly 1)
/// * `physical_inputs` - The physical input plans (must be exactly 1)
///
/// # Returns
///
/// Returns a `SeriesLimitExec` physical execution plan on success, or an error
/// if the inputs are invalid or expression conversion fails.
///
/// # Errors
///
/// This function returns an error if:
/// - The number of logical or physical inputs is not exactly 1
/// - Skip or fetch expressions cannot be evaluated to usize values
/// - Expression conversion from logical to physical fails
/// - Default value conversion fails
pub(crate) fn plan_series_limit(
    session_state: &SessionState,
    series_limit: &SeriesLimit,
    logical_inputs: &[&LogicalPlan],
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<SeriesLimitExec> {
    // Validate inputs
    let input_dfschema = match logical_inputs {
        [input] => input.schema().as_ref(),
        _ => {
            return internal_err!(
                "SeriesLimitExec: wrong number of logical inputs; expected 1, found {}",
                logical_inputs.len()
            );
        }
    };

    let phys_input = match physical_inputs {
        [input] => Arc::clone(input),
        _ => {
            return internal_err!(
                "SeriesLimitExec: wrong number of physical inputs; expected 1, found {}",
                physical_inputs.len()
            );
        }
    };

    // Convert series expressions to physical
    let series_expr = series_limit
        .series_expr
        .iter()
        .map(|expr| session_state.create_physical_expr(expr.clone(), input_dfschema))
        .collect::<Result<Vec<_>>>()?;

    // Convert order expressions to physical
    let order_expr = series_limit
        .order_expr
        .iter()
        .map(|sort_expr| {
            Ok(PhysicalSortExpr {
                expr: session_state.create_physical_expr(sort_expr.expr.clone(), input_dfschema)?,
                options: SortOptions {
                    descending: !sort_expr.asc,
                    nulls_first: sort_expr.nulls_first,
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // Convert limit expressions to physical
    let limit_expr = series_limit
        .limit_expr
        .iter()
        .map(|le| {
            let expr = session_state.create_physical_expr(le.expr.clone(), input_dfschema)?;
            let ignore_nulls = matches!(le.null_treatment, NullTreatment::IgnoreNulls);

            // Evaluate the default value expression to get a ScalarValue
            let default_value = if let Expr::Literal(scalar, _) = &le.default_value {
                scalar.clone()
            } else {
                return internal_err!(
                    "SeriesLimit default_value must be a literal, got: {:?}",
                    le.default_value
                );
            };

            Ok(PhysicalLimitExpr::new(expr, ignore_nulls, default_value))
        })
        .collect::<Result<Vec<_>>>()?;

    // Evaluate skip and fetch expressions
    let skip = if let Some(skip_expr) = &series_limit.skip {
        if let Expr::Literal(ScalarValue::UInt64(Some(skip_val)), _) = skip_expr.as_ref() {
            *skip_val as usize
        } else if let Expr::Literal(ScalarValue::Int64(Some(skip_val)), _) = skip_expr.as_ref() {
            if *skip_val < 0 {
                return internal_err!("SeriesLimit skip must be non-negative, got: {}", skip_val);
            }
            *skip_val as usize
        } else {
            return internal_err!(
                "SeriesLimit skip must be a non-negative integer literal, got: {:?}",
                skip_expr
            );
        }
    } else {
        0
    };

    let fetch = if let Some(fetch_expr) = &series_limit.fetch {
        if let Expr::Literal(ScalarValue::UInt64(Some(fetch_val)), _) = fetch_expr.as_ref() {
            Some(*fetch_val as usize)
        } else if let Expr::Literal(ScalarValue::Int64(Some(fetch_val)), _) = fetch_expr.as_ref() {
            if *fetch_val < 0 {
                return internal_err!("SeriesLimit fetch must be non-negative, got: {}", fetch_val);
            }
            Some(*fetch_val as usize)
        } else {
            return internal_err!(
                "SeriesLimit fetch must be a non-negative integer literal, got: {:?}",
                fetch_expr
            );
        }
    } else {
        None
    };

    SeriesLimitExec::try_new(phys_input, series_expr, order_expr, limit_expr, skip, fetch)
}
