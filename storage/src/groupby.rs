//! This module contains definitions for Timeseries specific Grouping
//! and Aggregate functions in IOx, designed to be compatible with
//! InfluxDB classic

#[derive(Debug, Clone, PartialEq)]
pub enum Aggregate {
    Sum,
    Count,
    Min,
    Max,
    First,
    Last,
    Mean,
}

/// Defines the different ways series can be grouped and aggregated
#[derive(Debug, Clone, PartialEq)]
pub enum GroupByAndAggregate {
    /// group by a set of (Tag) columns, applying agg to each field
    Columns {
        agg: Aggregate,
        group_columns: Vec<String>,
    },

    /// Group by a "window" in time, applying agg to each field
    ///
    /// The window is defined in terms three values:
    ///
    /// time: timestamp
    /// every: Duration
    /// offset: Duration
    ///
    /// The bounds are then calculated at a high level by
    /// bounds = truncate((time_column_reference + offset), every)
    ///
    /// Where the truncate function is different depending on the
    /// specific Duration
    ///
    /// This structure is different than the input (typically from gRPC)
    /// and the underyling calculation (in window.rs), so that we can do
    /// the input validation checking when creating this structure (rather
    /// than in window.rs). The alternate would be to pass the structure
    /// more directly from gRPC to window.rs, which would require less
    /// translation but more error checking in window.rs.
    Window {
        agg: Aggregate,
        every: WindowDuration,
        offset: WindowDuration,
    },
}

/// Represents some duration in time
#[derive(Debug, Clone, PartialEq)]
pub enum WindowDuration {
    /// Variable sized window,
    Variable { months: i64, negative: bool },

    /// fixed size, in nanoseconds
    Fixed { nanoseconds: i64 },
}

impl WindowDuration {
    pub fn from_nanoseconds(nanoseconds: i64) -> Self {
        Self::Fixed { nanoseconds }
    }

    pub fn from_months(months: i64, negative: bool) -> Self {
        Self::Variable { months, negative }
    }
}
