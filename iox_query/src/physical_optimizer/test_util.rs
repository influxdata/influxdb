use std::sync::Arc;

use datafusion::{
    config::ConfigOptions, physical_optimizer::PhysicalOptimizerRule, physical_plan::ExecutionPlan,
};
use serde::Serialize;

use crate::test::format_execution_plan;

#[derive(Debug, Serialize)]
pub struct OptimizationTest {
    input: Vec<String>,
    output: Result<Vec<String>, String>,

    #[serde(skip_serializing)]
    output_plan: Option<Arc<dyn ExecutionPlan>>,
}

impl OptimizationTest {
    pub fn new<O>(input_plan: Arc<dyn ExecutionPlan>, opt: O) -> Self
    where
        O: PhysicalOptimizerRule,
    {
        Self::new_with_config(input_plan, opt, &ConfigOptions::default())
    }

    pub fn new_with_config<O>(
        input_plan: Arc<dyn ExecutionPlan>,
        opt: O,
        config: &ConfigOptions,
    ) -> Self
    where
        O: PhysicalOptimizerRule,
    {
        let input = format_execution_plan(&input_plan);

        let output_result = opt.optimize(input_plan, config);
        let output_plan = output_result.as_ref().ok().cloned();
        let output = output_result
            .map(|plan| format_execution_plan(&plan))
            .map_err(|e| e.to_string());

        Self {
            input,
            output,
            output_plan,
        }
    }

    pub fn output_plan(&self) -> Option<&Arc<dyn ExecutionPlan>> {
        self.output_plan.as_ref()
    }
}
