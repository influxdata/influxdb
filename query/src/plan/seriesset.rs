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
    pub table_name: Arc<String>,

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
    pub tag_columns: Vec<Arc<String>>,

    /// The names of the columns which are "fields"
    pub field_columns: FieldColumns,

    /// If present, how many of the series_set_plan::tag_columns
    /// should be used to compute the group
    pub num_prefix_tag_group_columns: Option<usize>,
}

impl SeriesSetPlan {
    /// Create a SeriesSetPlan that will not produce any Group items
    pub fn new_from_shared_timestamp(
        table_name: Arc<String>,
        plan: LogicalPlan,
        tag_columns: Vec<Arc<String>>,
        field_columns: Vec<Arc<String>>,
    ) -> Self {
        Self::new(table_name, plan, tag_columns, field_columns.into())
    }

    /// Create a SeriesSetPlan that will not produce any Group items
    pub fn new(
        table_name: Arc<String>,
        plan: LogicalPlan,
        tag_columns: Vec<Arc<String>>,
        field_columns: FieldColumns,
    ) -> Self {
        let num_prefix_tag_group_columns = None;

        Self {
            table_name,
            plan,
            tag_columns,
            field_columns,
            num_prefix_tag_group_columns,
        }
    }

    /// Create a SeriesSetPlan that will produce Group items, according to
    /// num_prefix_tag_group_columns.
    pub fn grouped(mut self, num_prefix_tag_group_columns: usize) -> Self {
        self.num_prefix_tag_group_columns = Some(num_prefix_tag_group_columns);
        self
    }
}

/// A container for plans which each produce a logical stream of
/// timeseries (from across many potential tables).
#[derive(Debug, Default)]
pub struct SeriesSetPlans {
    pub plans: Vec<SeriesSetPlan>,
}

impl SeriesSetPlans {
    pub fn into_inner(self) -> Vec<SeriesSetPlan> {
        self.plans
    }
}

impl From<Vec<SeriesSetPlan>> for SeriesSetPlans {
    fn from(plans: Vec<SeriesSetPlan>) -> Self {
        Self { plans }
    }
}
