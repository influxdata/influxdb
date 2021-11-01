use query::exec::IOxExecutionContext;
use query::plan::seriesset::SeriesSetPlans;

/// Run a series set plan to completion and produce a Vec<String> representation
///
/// # Panics
///
/// Panics if there is an error executing a plan, or if unexpected series set
/// items are returned.
#[cfg(test)]
pub async fn run_series_set_plan(ctx: &IOxExecutionContext, plans: SeriesSetPlans) -> Vec<String> {
    ctx.to_series_and_groups(plans)
        .await
        .expect("running plans")
        .into_iter()
        .map(|series_or_group| series_or_group.to_string())
        .collect()
}
