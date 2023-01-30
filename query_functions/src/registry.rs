use std::{collections::HashSet, sync::Arc};

use datafusion::{
    common::{DataFusionError, Result as DataFusionResult},
    execution::FunctionRegistry,
    logical_expr::{AggregateUDF, ScalarUDF},
};
use once_cell::sync::Lazy;

use crate::{gapfill, regex, window};

static REGISTRY: Lazy<IOxFunctionRegistry> = Lazy::new(IOxFunctionRegistry::new);

/// Lookup for all DataFusion User Defined Functions used by IOx
#[derive(Debug)]
pub(crate) struct IOxFunctionRegistry {}

impl IOxFunctionRegistry {
    fn new() -> Self {
        Self {}
    }
}

impl FunctionRegistry for IOxFunctionRegistry {
    fn udfs(&self) -> HashSet<String> {
        [
            gapfill::DATE_BIN_GAPFILL_UDF_NAME,
            regex::REGEX_MATCH_UDF_NAME,
            regex::REGEX_NOT_MATCH_UDF_NAME,
            window::WINDOW_BOUNDS_UDF_NAME,
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn udf(&self, name: &str) -> DataFusionResult<Arc<ScalarUDF>> {
        match name {
            gapfill::DATE_BIN_GAPFILL_UDF_NAME => Ok(gapfill::DATE_BIN_GAPFILL.clone()),
            regex::REGEX_MATCH_UDF_NAME => Ok(regex::REGEX_MATCH_UDF.clone()),
            regex::REGEX_NOT_MATCH_UDF_NAME => Ok(regex::REGEX_NOT_MATCH_UDF.clone()),
            window::WINDOW_BOUNDS_UDF_NAME => Ok(window::WINDOW_BOUNDS_UDF.clone()),
            _ => Err(DataFusionError::Plan(format!(
                "IOx FunctionRegistry does not contain function '{name}'"
            ))),
        }
    }

    fn udaf(&self, name: &str) -> DataFusionResult<Arc<AggregateUDF>> {
        Err(DataFusionError::Plan(format!(
            "IOx FunctionRegistry does not contain user defined aggregate function '{name}'"
        )))
    }
}

/// Return a reference to the global function registry
pub(crate) fn instance() -> &'static IOxFunctionRegistry {
    &REGISTRY
}
