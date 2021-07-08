//! This module contains definitions for Timeseries specific Grouping
//! and Aggregate functions in IOx, designed to be compatible with
//! InfluxDB classic

use datafusion::logical_plan::Expr;
use snafu::Snafu;

use crate::func::window;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Aggregate not yet supported {}. See https://github.com/influxdata/influxdb_iox/issues/480",
        agg
    ))]
    AggregateNotSupported { agg: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, PartialEq, Copy)]

/// TimeSeries specific aggregates or selector functions
///
/// Aggregates are as in typical databases: they combine a column of
/// data into a single scalar values by some arithemetic calculation.
///
/// Selector_functions are similar to aggregates in that reduce a
/// column of data into a single row by *selecting* a single row.  In
/// other words, they can return the timestamp value from the
/// associated row in addition to its value.
pub enum Aggregate {
    /// Aggregate: the sum of all values in the column
    Sum,

    /// Aggregate: the total number of column values
    Count,

    /// Selector: Selects the minimum value of a column and the
    /// associated timestamp. In the case of multiple rows with the
    /// same min value, the earliest timestamp is used
    Min,

    /// Selector: Selects the maximum value of a column and the
    /// associated timestamp. In the case of multiple rows with the
    /// same max value, the earliest timestamp is used
    Max,

    /// Selector: Selects the value of a column with the minimum
    /// timestamp and the associated timestamp. In the case of
    /// multiple rows with the min timestamp, one is abritrarily
    /// chosen
    First,

    /// Selector: Selects the value of a column with the minimum
    /// timestamp and the associated timestamp. In the case of
    /// multiple rows with the min timestamp, one is abritrarily
    /// chosen
    Last,

    /// Aggregate: Average (geometric mean) column's value
    Mean,

    /// No grouping is applied
    None,
}

/// Represents some duration in time
#[derive(Debug, Clone, PartialEq)]
pub enum WindowDuration {
    /// Variable sized window,
    Variable { months: i64, negative: bool },

    /// fixed size, in nanoseconds
    Fixed { nanoseconds: i64 },
}

impl Aggregate {
    /// Create the appropriate DataFusion expression for this aggregate
    pub fn to_datafusion_expr(self, input: Expr) -> Result<Expr> {
        use datafusion::logical_plan::{avg, count, max, min, sum};
        match self {
            Self::Sum => Ok(sum(input)),
            Self::Count => Ok(count(input)),
            Self::Min => Ok(min(input)),
            Self::Max => Ok(max(input)),
            Self::First => AggregateNotSupported { agg: "First" }.fail(),
            Self::Last => AggregateNotSupported { agg: "Last" }.fail(),
            Self::Mean => Ok(avg(input)),
            Self::None => AggregateNotSupported { agg: "None" }.fail(),
        }
    }
}

impl WindowDuration {
    pub fn empty() -> Self {
        Self::Fixed { nanoseconds: 0 }
    }

    pub fn from_nanoseconds(nanoseconds: i64) -> Self {
        Self::Fixed { nanoseconds }
    }

    pub fn from_months(months: i64, negative: bool) -> Self {
        Self::Variable { months, negative }
    }
}

// Translation to the structures for the underlying window
// implementation
impl From<&WindowDuration> for window::Duration {
    fn from(window_duration: &WindowDuration) -> Self {
        match window_duration {
            WindowDuration::Variable { months, negative } => {
                Self::from_months_with_negative(*months, *negative)
            }
            WindowDuration::Fixed { nanoseconds } => Self::from_nsecs(*nanoseconds),
        }
    }
}
