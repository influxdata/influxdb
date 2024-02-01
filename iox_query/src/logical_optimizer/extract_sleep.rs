use std::sync::Arc;

use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::{
    common::{tree_node::TreeNodeRewriter, DFSchema},
    error::DataFusionError,
    logical_expr::{expr_rewriter::rewrite_preserving_name, Extension, LogicalPlan},
    optimizer::{OptimizerConfig, OptimizerRule},
    prelude::{lit, Expr},
    scalar::ScalarValue,
};
use query_functions::SLEEP_UDF_NAME;

use crate::exec::sleep::SleepNode;

/// Rewrites the ["sleep" UDF](SLEEP_UDF_NAME) to a NULL expression and a [`SleepNode`].
///
/// See [`crate::exec::sleep`] for more details.
#[derive(Debug, Clone)]
pub struct ExtractSleep {}

impl ExtractSleep {
    /// Create new optimizer rule.
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ExtractSleep {
    fn name(&self) -> &str {
        "extract_sleep"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::error::Result<Option<LogicalPlan>> {
        optimize(plan).map(Some)
    }
}

fn optimize(plan: &LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
    let new_inputs = plan
        .inputs()
        .iter()
        .map(|input| optimize(input))
        .collect::<Result<Vec<_>, DataFusionError>>()?;

    let mut schema =
        new_inputs
            .iter()
            .map(|input| input.schema())
            .fold(DFSchema::empty(), |mut lhs, rhs| {
                lhs.merge(rhs);
                lhs
            });

    schema.merge(plan.schema());

    let mut expr_rewriter = Rewriter::default();

    let new_exprs = plan
        .expressions()
        .into_iter()
        .map(|expr| rewrite_preserving_name(expr, &mut expr_rewriter))
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    let mut plan = plan.with_new_exprs(new_exprs, &new_inputs)?;

    if !expr_rewriter.found_exprs.is_empty() {
        plan = LogicalPlan::Extension(Extension {
            node: Arc::new(SleepNode::new(plan, expr_rewriter.found_exprs)),
        });
    }

    Ok(plan)
}

#[derive(Default)]
struct Rewriter {
    found_exprs: Vec<Expr>,
}

impl TreeNodeRewriter for Rewriter {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> Result<Expr, DataFusionError> {
        match expr {
            Expr::ScalarFunction(ScalarFunction { func_def, mut args }) => {
                if func_def.name() == SLEEP_UDF_NAME {
                    self.found_exprs.append(&mut args);
                    return Ok(lit(ScalarValue::Null));
                }

                Ok(Expr::ScalarFunction(ScalarFunction { func_def, args }))
            }
            _ => Ok(expr),
        }
    }
}
