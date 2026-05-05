use std::{
    collections::HashSet,
    sync::{Arc, LazyLock},
};

use datafusion::{
    common::{DataFusionError, Result as DataFusionResult},
    execution::{FunctionRegistry, registry::MemoryFunctionRegistry},
    logical_expr::{AggregateUDF, ScalarUDF, WindowUDF, planner::ExprPlanner},
};

use crate::{
    date_bin_wallclock, derivative, difference, gapfill, regex, sleep, to_timestamp, tz, window,
};

/// Contains IOx UDFs
static IOX_REGISTRY: LazyLock<IOxFunctionRegistry> = LazyLock::new(IOxFunctionRegistry::new);

/// Contains IOx & Datafusion UDFs
static ALL_REGISTRY: LazyLock<Box<dyn FunctionRegistry + 'static + Send + Sync>> =
    LazyLock::new(|| {
        let mut registry = MemoryFunctionRegistry::new();
        datafusion::functions::register_all(&mut registry).expect("should register datafusion UDF");

        let iox_registry = IOxFunctionRegistry::new();
        for fn_name in iox_registry.udfs() {
            registry
                .register_udf(
                    iox_registry
                        .udf(&fn_name)
                        .expect("iox registry is missing udf"),
                )
                .expect("should register iox UDF");
        }
        for fn_name in iox_registry.udafs() {
            registry
                .register_udaf(
                    iox_registry
                        .udaf(&fn_name)
                        .expect("iox registry is missing udaf"),
                )
                .expect("should register iox UDAF");
        }
        for fn_name in iox_registry.udwfs() {
            registry
                .register_udwf(
                    iox_registry
                        .udwf(&fn_name)
                        .expect("iox registry is missing udwf"),
                )
                .expect("should register iox UDWF");
        }
        Box::new(registry)
    });

/// Lookup for all DataFusion User Defined Functions used by IOx
#[derive(Debug)]
pub(crate) struct IOxFunctionRegistry {}

impl IOxFunctionRegistry {
    fn new() -> Self {
        Self {}
    }
}

impl FunctionRegistry for IOxFunctionRegistry {
    fn udafs(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn udwfs(&self) -> HashSet<String> {
        [
            derivative::DERIVATIVE_UDWF_NAME,
            difference::DIFFERENCE_UDWF_NAME,
            derivative::NON_NEGATIVE_DERIVATIVE_UDWF_NAME,
            difference::NON_NEGATIVE_DIFFERENCE_UDWF_NAME,
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn udfs(&self) -> HashSet<String> {
        [
            to_timestamp::TO_TIMESTAMP_FUNCTION_NAME,
            gapfill::DATE_BIN_GAPFILL_UDF_NAME,
            gapfill::DATE_BIN_WALLCLOCK_GAPFILL_UDF_NAME,
            gapfill::LOCF_UDF_NAME,
            gapfill::INTERPOLATE_UDF_NAME,
            regex::REGEX_MATCH_UDF_NAME,
            regex::REGEX_NOT_MATCH_UDF_NAME,
            sleep::SLEEP_UDF_NAME,
            window::WINDOW_BOUNDS_UDF_NAME,
            date_bin_wallclock::DATE_BIN_WALLCLOCK_UDF_NAME,
            tz::TZ_UDF_NAME,
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn udf(&self, name: &str) -> DataFusionResult<Arc<ScalarUDF>> {
        match name {
            to_timestamp::TO_TIMESTAMP_FUNCTION_NAME => Ok(to_timestamp::TO_TIMESTAMP_UDF.clone()),
            gapfill::DATE_BIN_GAPFILL_UDF_NAME => Ok(gapfill::DATE_BIN_GAPFILL.clone()),
            gapfill::DATE_BIN_WALLCLOCK_GAPFILL_UDF_NAME => {
                Ok(gapfill::DATE_BIN_WALLCLOCK_GAPFILL.clone())
            }
            gapfill::LOCF_UDF_NAME => Ok(gapfill::LOCF.clone()),
            gapfill::INTERPOLATE_UDF_NAME => Ok(gapfill::INTERPOLATE.clone()),
            regex::REGEX_MATCH_UDF_NAME => Ok(regex::REGEX_MATCH_UDF.clone()),
            regex::REGEX_NOT_MATCH_UDF_NAME => Ok(regex::REGEX_NOT_MATCH_UDF.clone()),
            sleep::SLEEP_UDF_NAME => Ok(sleep::SLEEP_UDF.clone()),
            window::WINDOW_BOUNDS_UDF_NAME => Ok(window::WINDOW_BOUNDS_UDF.clone()),
            date_bin_wallclock::DATE_BIN_WALLCLOCK_UDF_NAME => {
                Ok(date_bin_wallclock::DATE_BIN_WALLCLOCK_UDF.clone())
            }
            tz::TZ_UDF_NAME => Ok(tz::TZ_UDF.clone()),
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

    fn udwf(&self, name: &str) -> DataFusionResult<Arc<WindowUDF>> {
        match name {
            derivative::DERIVATIVE_UDWF_NAME => Ok(derivative::DERIVATIVE_UDWF.clone()),
            difference::DIFFERENCE_UDWF_NAME => Ok(difference::DIFFERENCE_UDWF.clone()),
            derivative::NON_NEGATIVE_DERIVATIVE_UDWF_NAME => {
                Ok(derivative::NON_NEGATIVE_DERIVATIVE_UDWF.clone())
            }
            difference::NON_NEGATIVE_DIFFERENCE_UDWF_NAME => {
                Ok(difference::NON_NEGATIVE_DIFFERENCE_UDWF.clone())
            }
            _ => Err(DataFusionError::Plan(format!(
                "IOx FunctionRegistry does not contain function '{name}'"
            ))),
        }
    }

    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        vec![]
    }
}

/// Return a reference to the global iox function registry
pub(crate) fn instance_iox() -> &'static IOxFunctionRegistry {
    &IOX_REGISTRY
}

/// Return a reference to the global iox & datafusion function registry
pub(crate) fn instance() -> &'static dyn FunctionRegistry {
    &**ALL_REGISTRY
}
