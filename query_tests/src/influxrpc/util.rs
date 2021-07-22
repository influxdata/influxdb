use std::sync::Arc;

use arrow_util::display::pretty_format_batches;
use query::{
    exec::{
        field::FieldIndexes,
        seriesset::{SeriesSet, SeriesSetItem},
        Executor,
    },
    plan::seriesset::SeriesSetPlans,
};

/// Format the field indexes into strings
pub fn dump_field_indexes(f: FieldIndexes) -> Vec<String> {
    f.as_slice()
        .iter()
        .map(|field_index| {
            format!(
                "  (value_index: {}, timestamp_index: {})",
                field_index.value_index, field_index.timestamp_index
            )
        })
        .collect()
}

/// Format a the vec of Arc strings paris into strings
pub fn dump_arc_vec(v: Vec<(Arc<str>, Arc<str>)>) -> Vec<String> {
    v.into_iter()
        .map(|(k, v)| format!("  ({}, {})", k, v))
        .collect()
}

/// Format a series set into a format that is easy to compare in tests
pub fn dump_series_set(s: SeriesSet) -> Vec<String> {
    let mut f = vec!["SeriesSet".into()];
    f.push(format!("table_name: {}", s.table_name));
    f.push("tags".to_string());
    f.extend(dump_arc_vec(s.tags).into_iter());
    f.push("field_indexes:".to_string());
    f.extend(dump_field_indexes(s.field_indexes).into_iter());
    f.push(format!("start_row: {}", s.start_row));
    f.push(format!("num_rows: {}", s.num_rows));
    f.push("Batches:".into());
    let formatted_batch = pretty_format_batches(&[s.batch]).unwrap();
    f.extend(formatted_batch.trim().split('\n').map(|s| s.to_string()));

    f
}

/// Run a series set plan to completion and produce a Vec<String> representation
///
/// # Panics
///
/// Panics if there is an error executing a plan, or if unexpected series set
/// items are returned.
#[cfg(test)]
pub async fn run_series_set_plan(executor: Arc<Executor>, plans: SeriesSetPlans) -> Vec<String> {
    let results = executor.to_series_set(plans).await;

    let mut results = results
        .unwrap()
        .into_iter()
        .map(|item| {
            if let SeriesSetItem::Data(series_set) = item {
                series_set
            } else {
                panic!(
                    "Unexpected result from converting. Expected SeriesSetItem::Data, got: {:?}",
                    item
                )
            }
        })
        .collect::<Vec<_>>();

    // sort the results so that we can reliably compare
    results.sort_by(|r1, r2| {
        r1.table_name
            .cmp(&r2.table_name)
            .then(r1.tags.cmp(&r2.tags))
    });

    results
        .into_iter()
        .map(|s| dump_series_set(s).into_iter())
        .flatten()
        .collect::<Vec<_>>()
}
