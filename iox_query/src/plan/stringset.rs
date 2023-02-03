use std::sync::Arc;

use arrow_util::util::str_iter_to_batch;
use datafusion::logical_expr::LogicalPlan;

/// The name of the column containing table names returned by a call to
/// `table_names`.
const TABLE_NAMES_COLUMN_NAME: &str = "table";

use crate::{
    exec::stringset::{StringSet, StringSetRef},
    util::make_scan_plan,
};

use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Internal error converting to arrow: {}", source))]
    InternalConvertingToArrow { source: arrow::error::ArrowError },

    #[snafu(display("Internal error creating a plan for stringset: {}", source))]
    InternalPlanningStringSet {
        source: datafusion::error::DataFusionError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A plan which produces a logical set of Strings (e.g. tag
/// values). This includes variants with pre-calculated results as
/// well a variant that runs a full on DataFusion plan.
#[derive(Debug)]
pub enum StringSetPlan {
    /// The results are known from metadata only without having to run
    /// an actual datafusion plan
    Known(StringSetRef),

    /// A DataFusion plan(s) to execute. Each plan must produce
    /// RecordBatches with exactly one String column, though the
    /// values produced by the plan may be repeated
    ///
    /// TODO: it would be cool to have a single datafusion LogicalPlan
    /// that merged all the results together. However, no such Union
    /// node exists at the time of writing, so we do the unioning in IOx
    Plan(Vec<LogicalPlan>),
}

impl From<StringSetRef> for StringSetPlan {
    /// Create a StringSetPlan from a StringSetRef
    fn from(set: StringSetRef) -> Self {
        Self::Known(set)
    }
}

impl From<StringSet> for StringSetPlan {
    /// Create a StringSetPlan from a StringSet result, wrapping the error type
    /// appropriately
    fn from(set: StringSet) -> Self {
        Self::Known(StringSetRef::new(set))
    }
}

impl From<Vec<LogicalPlan>> for StringSetPlan {
    /// Create StringSet plan from a DataFusion LogicalPlan node, each
    /// of which must produce a single output Utf8 column. The output
    /// of each plan will be included into the final set.
    fn from(plans: Vec<LogicalPlan>) -> Self {
        Self::Plan(plans)
    }
}

impl From<LogicalPlan> for StringSetPlan {
    /// Create a StringSet plan from a single DataFusion LogicalPlan
    /// node which produces a single output Utf8 column.
    fn from(plan: LogicalPlan) -> Self {
        Self::Plan(vec![plan])
    }
}

/// Builder for StringSet plans for appending multiple plans together
///
/// If the values are known beforehand, the builder merges the
/// strings, otherwise it falls back to generic plans
#[derive(Debug, Default)]
pub struct StringSetPlanBuilder {
    /// Known strings
    strings: StringSet,
    /// General plans
    plans: Vec<LogicalPlan>,
}

impl StringSetPlanBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append the strings from the passed plan into ourselves if possible, or
    /// passes on the plan
    pub fn append_other(mut self, other: StringSetPlan) -> Self {
        match other {
            StringSetPlan::Known(ssref) => match Arc::try_unwrap(ssref) {
                Ok(mut ss) => {
                    self.strings.append(&mut ss);
                }
                Err(ssref) => {
                    for s in ssref.iter() {
                        if !self.strings.contains(s) {
                            self.strings.insert(s.clone());
                        }
                    }
                }
            },
            StringSetPlan::Plan(mut other_plans) => self.plans.append(&mut other_plans),
        }

        self
    }

    /// Return true if we know already that `s` is contained in the
    /// StringSet. Note that if `contains()` returns false, `s` may be
    /// in the stringset after execution.
    pub fn contains(&self, s: impl AsRef<str>) -> bool {
        self.strings.contains(s.as_ref())
    }

    /// Append a single string to the known set of strings in this builder
    pub fn append_string(&mut self, s: impl Into<String>) {
        self.strings.insert(s.into());
    }

    /// returns an iterator over the currently known strings in this builder
    pub fn known_strings_iter(&self) -> impl Iterator<Item = &String> {
        self.strings.iter()
    }

    /// Create a StringSetPlan that produces the deduplicated (union)
    /// of all plans `append`ed to this builder.
    pub fn build(self) -> Result<StringSetPlan> {
        let Self { strings, mut plans } = self;

        if plans.is_empty() {
            // only a known set of strings
            Ok(StringSetPlan::Known(Arc::new(strings)))
        } else {
            // Had at least one general plan, so need to use general
            // purpose plan for the known strings
            if !strings.is_empty() {
                let batch =
                    str_iter_to_batch(TABLE_NAMES_COLUMN_NAME, strings.into_iter().map(Some))
                        .context(InternalConvertingToArrowSnafu)?;

                let plan = make_scan_plan(batch).context(InternalPlanningStringSetSnafu)?;

                plans.push(plan)
            }

            Ok(StringSetPlan::Plan(plans))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::exec::{Executor, ExecutorType};

    use super::*;

    #[test]
    fn test_builder_empty() {
        let plan = StringSetPlanBuilder::new().build().unwrap();
        let empty_ss = StringSet::new().into();
        if let StringSetPlan::Known(ss) = plan {
            assert_eq!(ss, empty_ss)
        } else {
            panic!("unexpected type: {plan:?}")
        }
    }

    #[test]
    fn test_builder_strings_only() {
        let plan = StringSetPlanBuilder::new()
            .append_other(to_string_set(&["foo", "bar"]).into())
            .append_other(to_string_set(&["bar", "baz"]).into())
            .build()
            .unwrap();

        let expected_ss = to_string_set(&["foo", "bar", "baz"]).into();

        if let StringSetPlan::Known(ss) = plan {
            assert_eq!(ss, expected_ss)
        } else {
            panic!("unexpected type: {plan:?}")
        }
    }

    #[derive(Debug)]
    struct TestError {}

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "this is an error")
        }
    }

    impl std::error::Error for TestError {}

    #[tokio::test]
    async fn test_builder_plan() {
        let batch = str_iter_to_batch("column_name", vec![Some("from_a_plan")]).unwrap();
        let df_plan = make_scan_plan(batch).unwrap();

        // when a df plan is appended the whole plan should be different
        let plan = StringSetPlanBuilder::new()
            .append_other(to_string_set(&["foo", "bar"]).into())
            .append_other(vec![df_plan].into())
            .append_other(to_string_set(&["baz"]).into())
            .build()
            .unwrap();

        let expected_ss = to_string_set(&["foo", "bar", "baz", "from_a_plan"]).into();

        assert!(matches!(plan, StringSetPlan::Plan(_)));
        let exec = Executor::new_testing();
        let ctx = exec.new_context(ExecutorType::Query);
        let ss = ctx.to_string_set(plan).await.unwrap();
        assert_eq!(ss, expected_ss);
    }

    fn to_string_set(v: &[&str]) -> StringSet {
        v.iter().map(|s| s.to_string()).collect::<StringSet>()
    }
}
