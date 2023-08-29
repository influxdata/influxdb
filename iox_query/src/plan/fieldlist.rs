use datafusion::logical_expr::LogicalPlan;

use crate::exec::fieldlist::Field;
use std::collections::BTreeMap;

pub type FieldSet = BTreeMap<String, Field>;

/// A plan which produces a logical set of Fields (e.g. InfluxDB
/// Fields with name, and data type, and last_timestamp).
///
/// known_values has a set of pre-computed values to be merged with
/// the extra_plans.
#[derive(Debug, Default)]
pub struct FieldListPlan {
    /// Known values
    pub known_values: FieldSet,
    /// General plans
    pub extra_plans: Vec<LogicalPlan>,
}

impl From<Vec<LogicalPlan>> for FieldListPlan {
    /// Create FieldList plan from a DataFusion LogicalPlan node, each
    /// of which must produce fields in the correct format. The output
    /// of each plan will be included into the final set.
    fn from(plans: Vec<LogicalPlan>) -> Self {
        Self {
            known_values: FieldSet::new(),
            extra_plans: plans,
        }
    }
}

impl From<LogicalPlan> for FieldListPlan {
    /// Create a StringSet plan from a single DataFusion LogicalPlan
    /// node, which must produce fields in the correct format
    fn from(plan: LogicalPlan) -> Self {
        Self::from(vec![plan])
    }
}

impl FieldListPlan {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append the other plan to ourselves
    pub fn append_other(mut self, other: Self) -> Self {
        self.extra_plans.extend(other.extra_plans);
        self.known_values.extend(other.known_values);
        self
    }

    /// Append a single field to the known set of fields in this builder
    pub fn append_field(&mut self, s: Field) {
        self.known_values.insert(s.name.clone(), s);
    }
}
