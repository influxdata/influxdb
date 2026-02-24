use std::sync::Arc;

use datafusion::common::tree_node::TreeNode;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::optimizer::AnalyzerRule;
use datafusion::{
    common::tree_node::{Transformed, TreeNodeRewriter},
    error::DataFusionError,
    logical_expr::{Extension, LogicalPlan},
    prelude::{Expr, lit},
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

impl AnalyzerRule for ExtractSleep {
    fn name(&self) -> &str {
        "extract_sleep"
    }
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        extract_sleep(plan).map(|t| t.data)
    }
}

fn extract_sleep(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    plan.transform(|plan| {
        let mut expr_rewriter = Rewriter::default();
        plan.map_expressions(|expr| {
            let original_name = expr.name_for_alias()?;
            expr.rewrite(&mut expr_rewriter)?
                .map_data(|expr| expr.alias_if_changed(original_name))
        })?
        .transform_data(|plan| {
            if !expr_rewriter.found_exprs.is_empty() {
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(SleepNode::new(plan, expr_rewriter.found_exprs)),
                })))
            } else {
                Ok(Transformed::no(plan))
            }
        })
    })
}

#[derive(Default)]
struct Rewriter {
    found_exprs: Vec<Expr>,
}

impl TreeNodeRewriter for Rewriter {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>, DataFusionError> {
        match expr {
            Expr::ScalarFunction(ScalarFunction { func, mut args }) => {
                if func.name() == SLEEP_UDF_NAME {
                    self.found_exprs.append(&mut args);
                    return Ok(Transformed::yes(lit(ScalarValue::Null)));
                }

                Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction {
                    func,
                    args,
                })))
            }
            _ => Ok(Transformed::no(expr)),
        }
    }
}
