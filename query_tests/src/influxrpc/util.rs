use query::exec::IOxExecutionContext;
use query::{
    exec::seriesset::{
        series::{Group, Series},
        SeriesSetItem,
    },
    plan::seriesset::SeriesSetPlans,
};

/// Run a series set plan to completion and produce a Vec<String> representation
///
/// # Panics
///
/// Panics if there is an error executing a plan, or if unexpected series set
/// items are returned.
#[cfg(test)]
pub async fn run_series_set_plan(ctx: &IOxExecutionContext, plans: SeriesSetPlans) -> Vec<String> {
    use std::convert::TryInto;

    let series_sets = ctx.to_series_set(plans).await.expect("running plans");

    series_sets
        .into_iter()
        .map(|item| match item {
            SeriesSetItem::GroupStart(group_description) => {
                let group: Group = group_description.into();
                vec![group.to_string()]
            }
            SeriesSetItem::Data(series_set) => {
                let series: Vec<Series> = series_set.try_into().expect("converting series");
                series.into_iter().map(|s| s.to_string()).collect()
            }
        })
        .flatten()
        .collect()
}
