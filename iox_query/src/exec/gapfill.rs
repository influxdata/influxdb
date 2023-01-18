//! This module contains code that implements
//! a gap-filling extension to DataFusion

use std::sync::Arc;

use datafusion::{
    common::{DFSchema, DFSchemaRef},
    error::Result,
    logical_expr::{utils::exprlist_to_fields, LogicalPlan, UserDefinedLogicalNode},
    prelude::Expr,
};

/// A logical node that represents the gap filling operation.
#[derive(Clone, Debug)]
pub struct GapFill {
    input: Arc<LogicalPlan>,
    group_expr: Vec<Expr>,
    aggr_expr: Vec<Expr>,
    schema: DFSchemaRef,
    time_column: Expr,
}

impl GapFill {
    pub fn try_new(
        input: Arc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        time_column: Expr,
    ) -> Result<Self> {
        let all_expr = group_expr.iter().chain(aggr_expr.iter());
        let schema = DFSchema::new_with_metadata(
            exprlist_to_fields(all_expr, &input)?,
            input.schema().metadata().clone(),
        )?
        .into();
        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            schema,
            time_column,
        })
    }
}

impl UserDefinedLogicalNode for GapFill {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.group_expr
            .iter()
            .chain(self.aggr_expr.iter())
            .cloned()
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GapFill: groupBy=[{:?}], aggr=[{:?}], time_column={}",
            self.group_expr, self.aggr_expr, self.time_column
        )
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        let mut group_expr: Vec<_> = exprs.to_vec();
        let aggr_expr = group_expr.split_off(self.group_expr.len());
        let gapfill = Self::try_new(
            Arc::new(inputs[0].clone()),
            group_expr,
            aggr_expr,
            self.time_column.clone(),
        )
        .expect("should not fail");
        Arc::new(gapfill)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::{
        error::Result,
        logical_expr::{logical_plan, Extension},
        prelude::col,
    };

    fn table_scan() -> Result<LogicalPlan> {
        let schema = Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("loc", DataType::Utf8, false),
            Field::new("temp", DataType::Float64, false),
        ]);
        logical_plan::table_scan(Some("temps"), &schema, None)?.build()
    }

    #[test]
    fn fmt_logical_plan() -> Result<()> {
        // This test case does not make much sense but
        // just verifies we can construct a logical gapfill node
        // and show its plan.
        let scan = table_scan()?;
        let gapfill = GapFill::try_new(
            Arc::new(scan),
            vec![col("loc"), col("time")],
            vec![col("temp")],
            col("time"),
        )?;
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(gapfill),
        });
        let expected = "GapFill: groupBy=[[loc, time]], aggr=[[temp]], time_column=time\
                      \n  TableScan: temps";
        assert_eq!(expected, format!("{}", plan.display_indent()));
        Ok(())
    }
}
