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
    use thiserror::Error;

    #[derive(Debug, Error)]
    enum PlannerError {
        /// An unexpected error that represents a bug in IOx.
        #[error("internal: {0}")]
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
}
