use std::sync::Arc;

use datafusion::logical_plan::LogicalPlan;

use crate::exec::field::FieldColumns;

/// A plan that can be run to produce a logical stream of time series,
/// as represented as sequence of SeriesSets from a single DataFusion
/// plan, optionally grouped in some way.
///
/// TODO: remove the tag/field designations below and attach a
/// `Schema` to the plan (which has the tag and field column
/// information natively)
#[derive(Debug)]
pub struct SeriesSetPlan {
    /// The table name this came from
    pub table_name: Arc<str>,

    /// Datafusion plan to execute. The plan must produce
    /// RecordBatches that have:
    ///
    /// * fields for each name in `tag_columns` and `field_columns`
    /// * a timestamp column called 'time'
    /// * each column in tag_columns must be a String (Utf8)
    pub plan: LogicalPlan,

    /// The names of the columns that define tags.
    ///
    /// Note these are `Arc` strings because they are duplicated for
    /// *each* resulting `SeriesSet` that is produced when this type
    /// of plan is executed.
    pub tag_columns: Vec<Arc<str>>,

    /// The names of the columns which are "fields"
    pub field_columns: FieldColumns,
}

impl SeriesSetPlan {
    /// Create a SeriesSetPlan that will not produce any Group items
    pub fn new_from_shared_timestamp(
        table_name: Arc<str>,
        plan: LogicalPlan,
        tag_columns: Vec<Arc<str>>,
        field_columns: Vec<Arc<str>>,
    ) -> Self {
        Self::new(table_name, plan, tag_columns, field_columns.into())
    }

    /// Create a SeriesSetPlan that will not produce any Group items
    pub fn new(
        table_name: Arc<str>,
        plan: LogicalPlan,
        tag_columns: Vec<Arc<str>>,
        field_columns: FieldColumns,
    ) -> Self {
        Self {
            table_name,
            plan,
            tag_columns,
            field_columns,
        }
    }
}

/// A container for plans which each produce a logical stream of
/// timeseries (from across many potential tables).
#[derive(Debug, Default)]
pub struct SeriesSetPlans {
    /// Plans the generate Series, ordered by table_name.
    ///
    /// Each plan produces output that is sorted by tag keys (tag
    /// column values) and then time.
    pub plans: Vec<SeriesSetPlan>,

    /// grouping keys, if any, that specify how the output series should be
    /// sorted (aka grouped). If empty, means no grouping is needed
    ///
    /// There are several special values that are possible in `group_keys`:
    ///
    /// 1. _field (means group by field column name)
    /// 2. _measurement (means group by the table name)
    /// 3. _time (means group by the time column)
    pub group_columns: Option<Vec<Arc<str>>>,
}

impl SeriesSetPlans {
    pub fn into_inner(self) -> Vec<SeriesSetPlan> {
        self.plans
    }
}

impl SeriesSetPlans {
    /// Create a new, ungrouped SeriesSetPlans
    pub fn new(plans: Vec<SeriesSetPlan>) -> Self {
        Self {
            plans,
            group_columns: None,
        }
    }

    /// Group the created SeriesSetPlans
    pub fn grouped_by(self, group_columns: Vec<Arc<str>>) -> Self {
        Self {
            group_columns: Some(group_columns),
            ..self
        }
    }
}
