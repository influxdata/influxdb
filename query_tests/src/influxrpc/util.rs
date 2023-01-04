use datafusion::error::DataFusionError;
use datafusion::prelude::{col, lit, when, Expr};
use iox_query::exec::IOxSessionContext;
use iox_query::plan::seriesset::SeriesSetPlans;

/// Run a series set plan to completion and produce a Vec<String> representation
///
/// # Panics
///
/// Panics if there is an error executing a plan, or if unexpected series set
/// items are returned.
#[cfg(test)]
pub async fn run_series_set_plan(ctx: &IOxSessionContext, plans: SeriesSetPlans) -> Vec<String> {
    run_series_set_plan_maybe_error(ctx, plans)
        .await
        .expect("running plans")
}

/// Run a series set plan to completion and produce a Result<Vec<String>> representation
#[cfg(test)]
pub async fn run_series_set_plan_maybe_error(
    ctx: &IOxSessionContext,
    plans: SeriesSetPlans,
) -> Result<Vec<String>, DataFusionError> {
    use std::sync::Arc;

    use futures::TryStreamExt;

    ctx.to_series_and_groups(plans, Arc::clone(&ctx.inner().runtime_env().memory_pool))
        .await?
        .map_ok(|series_or_group| series_or_group.to_string())
        .try_collect()
        .await
}

/// https://github.com/influxdata/influxdb_iox/issues/3635
/// model what happens when a field is treated like a tag compared to ''
///
/// CASE WHEN system" IS NULL THEN '' ELSE system END
pub fn make_empty_tag_ref_expr(tag_name: &str) -> Expr {
    when(col(tag_name).is_null(), lit(""))
        .otherwise(col(tag_name))
        .unwrap()
}
