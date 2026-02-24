use datafusion::{
    common::Result,
    common::tree_node::{TreeNodeRecursion, TreeNodeVisitor},
    logical_expr::{Expr, ScalarUDF, expr::ScalarFunction},
};
use query_functions::gapfill::{GapFillWrapper, InterpolateUDF, LocfUDF};
use std::sync::Arc;

/// Representation of the virtual scalar functions used to include gap
/// filling in an SQL query.
///
/// These functions are removed from the Aggregate node and used to
/// determine the parameters for the GapFill node.
#[derive(Debug, Clone)]
pub(super) enum VirtualFunction {
    GapFill(Arc<ScalarUDF>),
    Locf,
    Interpolate,
}

impl VirtualFunction {
    /// Create a new VirtualFunction from `expr` if it represents a
    /// function call to a virtual function.
    pub(super) fn maybe_from_expr(expr: &Expr) -> Option<Self> {
        match expr {
            Expr::ScalarFunction(ScalarFunction { func, args: _ }) => {
                let func_any = func.inner().as_any();
                if let Some(gapfill) = func_any.downcast_ref::<GapFillWrapper>() {
                    Some(Self::GapFill(Arc::clone(gapfill.inner())))
                } else if func_any.is::<LocfUDF>() {
                    Some(Self::Locf)
                } else if func_any.is::<InterpolateUDF>() {
                    Some(Self::Interpolate)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Return the UDF for the date_bin function wrapped by this
    /// virtual function, if this is a GapFill function.
    pub(super) fn date_bin_udf(&self) -> Option<&Arc<ScalarUDF>> {
        match self {
            Self::GapFill(udf) => Some(udf),
            _ => None,
        }
    }
}

/// Visitor that finds virtual functions in a tree of expressions.
pub(super) struct VirtualFunctionFinder<'v> {
    pub(super) functions: &'v mut Vec<VirtualFunction>,
}

impl<'v> VirtualFunctionFinder<'v> {
    /// Create a new VirtualFunctionFinder that will populate the
    /// `functions` vector.
    pub(super) fn new(functions: &'v mut Vec<VirtualFunction>) -> Self {
        Self { functions }
    }
}

impl<'n> TreeNodeVisitor<'n> for VirtualFunctionFinder<'_> {
    type Node = Expr;

    fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        if let Some(function) = VirtualFunction::maybe_from_expr(node) {
            self.functions.push(function);
        }
        Ok(TreeNodeRecursion::Continue)
    }
}
