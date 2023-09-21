use std::sync::Arc;

use datafusion::{
    config::ConfigOptions,
    error::DataFusionError,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{ExecutionPlan, Partitioning},
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

        let input_schema = input_plan.schema();

        let output_result = opt.optimize(input_plan, config);
        let output_plan = output_result.as_ref().ok().cloned();
        let output = output_result
            .and_then(|plan| {
                if opt.schema_check() && (plan.schema() != input_schema) {
                    Err(DataFusionError::External(
                        format!(
                            "Schema mismatch:\n\nBefore:\n{:?}\n\nAfter:\n{:?}",
                            input_schema,
                            plan.schema()
                        )
                        .into(),
                    ))
                } else {
                    Ok(plan)
                }
            })
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

/// Check if given partitioning is [`Partitioning::UnknownPartitioning`] with the given count.
///
/// This is needed because [`PartialEq`] for [`Partitioning`] is specified as "unknown != unknown".
#[track_caller]
pub fn assert_unknown_partitioning(partitioning: Partitioning, n: usize) {
    match partitioning {
        Partitioning::UnknownPartitioning(n2) if n == n2 => {}
        _ => panic!(
            "Unexpected partitioning, wanted: {:?}, got: {:?}",
            Partitioning::UnknownPartitioning(n),
            partitioning
        ),
    }
}
