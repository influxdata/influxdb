use datafusion::common::Result;

/// An error that was the result of an invalid InfluxQL query.
pub(crate) fn query<T>(s: impl Into<String>) -> Result<T> {
    Err(map::query(s))
}

/// An unexpected error whilst planning that represents a bug in IOx.
pub(crate) fn internal<T>(s: impl Into<String>) -> Result<T> {
    Err(map::internal(s))
}

/// The specified `feature` is not implemented.
pub(crate) fn not_implemented<T>(feature: impl Into<String>) -> Result<T> {
    Err(map::not_implemented(feature))
}

/// Functions that return a DataFusionError rather than a `Result<T, DataFusionError>`
/// making them convenient to use with functions like `map_err`.
pub(crate) mod map {
    use datafusion::common::DataFusionError;
    use influxdb_influxql_parser::time_range::ExprError;
    use thiserror::Error;

    #[derive(Debug, Error)]
    enum PlannerError {
        /// An unexpected error that represents a bug in IOx.
        ///
        /// The message is prefixed with `InfluxQL internal error: `,
        /// which may be used by clients to identify internal InfluxQL
        /// errors.
        #[error("InfluxQL internal error: {0}")]
        Internal(String),
    }

    /// An error that was the result of an invalid InfluxQL query.
    pub(crate) fn query(s: impl Into<String>) -> DataFusionError {
        DataFusionError::Plan(s.into())
    }

    /// An unexpected error whilst planning that represents a bug in IOx.
    pub(crate) fn internal(s: impl Into<String>) -> DataFusionError {
        DataFusionError::External(Box::new(PlannerError::Internal(s.into())))
    }

    /// The specified `feature` is not implemented.
    pub(crate) fn not_implemented(feature: impl Into<String>) -> DataFusionError {
        DataFusionError::NotImplemented(feature.into())
    }

    /// Map an [`ExprError`] to a DataFusion error.
    pub(crate) fn expr_error(err: ExprError) -> DataFusionError {
        match err {
            ExprError::Expression(s) => query(s),
            ExprError::Internal(s) => internal(s),
        }
    }

    #[cfg(test)]
    mod test {
        use crate::error::map::PlannerError;

        #[test]
        fn test_planner_error_display() {
            // The InfluxQL internal error:
            assert!(PlannerError::Internal("****".to_owned())
                .to_string()
                .starts_with("InfluxQL internal error: "))
        }
    }
}
