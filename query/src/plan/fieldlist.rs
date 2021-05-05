use datafusion::logical_plan::LogicalPlan;

/// A plan which produces a logical set of Fields (e.g. InfluxDB
/// Fields with name, and data type, and last_timestamp).
#[derive(Debug, Default)]
pub struct FieldListPlan {
    pub plans: Vec<LogicalPlan>,
}

impl FieldListPlan {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a new plan to this list of plans
    pub fn append(mut self, plan: LogicalPlan) -> Self {
        self.plans.push(plan);
        self
    }
}
