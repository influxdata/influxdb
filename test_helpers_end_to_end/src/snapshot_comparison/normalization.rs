use crate::snapshot_comparison::queries::Query;
use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use std::{borrow::Cow, collections::HashMap};
use uuid::Uuid;

/// Replace table row separators of flexible width with fixed with. This is required
/// because the original timing values may differ in "printed width", so the table
/// cells have different widths and hence the separators / borders. E.g.:
///
///   `+--+--+`     -> `----------`
///   `+--+------+` -> `----------`
///
/// Note that we're kinda inexact with our regex here, but it gets the job done.
static REGEX_LINESEP: Lazy<Regex> = Lazy::new(|| Regex::new(r#"[+-]{6,}"#).expect("linesep regex"));

/// Similar to the row separator issue above, the table columns are right-padded
/// with spaces. Due to the different "printed width" of the timing values, we need
/// to normalize this padding as well. E.g.:
///
///   `        |`  -> `    |`
///   `         |` -> `    |`
static REGEX_COL: Lazy<Regex> = Lazy::new(|| Regex::new(r#"\s+\|"#).expect("col regex"));

fn normalize_for_variable_width(s: Cow<str>) -> String {
    let s = REGEX_LINESEP.replace_all(&s, "----------");
    REGEX_COL.replace_all(&s, "    |").to_string()
}

pub(crate) fn normalize_results(query: &Query, mut current_results: Vec<String>) -> Vec<String> {
    // normalize UUIDs, if requested
    if query.normalized_uuids() {
        let regex_uuid = Regex::new("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
            .expect("UUID regex");
        let regex_dirs = Regex::new(r#"[0-9]+/[0-9]+/[0-9]+/[0-9]+"#).expect("directory regex");

        let mut seen: HashMap<String, u128> = HashMap::new();
        current_results = current_results
            .into_iter()
            .map(|s| {
                let s = regex_uuid.replace_all(&s, |s: &Captures| {
                    let next = seen.len() as u128;
                    Uuid::from_u128(
                        *seen
                            .entry(s.get(0).unwrap().as_str().to_owned())
                            .or_insert(next),
                    )
                    .to_string()
                });

                let s = normalize_for_variable_width(s);

                regex_dirs.replace_all(&s, "1/1/1/1").to_string()
            })
            .collect();
    }

    // normalize metrics, if requested
    if query.normalized_metrics() {
        // Parse regex once and apply to all rows. See description around the `replace...` calls on
        // why/how the regexes are used.
        let regex_metrics = Regex::new(r#"metrics=\[([^\]]*)\]"#).expect("metrics regex");
        let regex_timing = Regex::new(r#"[0-9]+(\.[0-9]+)?.s"#).expect("timing regex");

        current_results = current_results
            .into_iter()
            .map(|s| {
                // Replace timings with fixed value, e.g.:
                //
                //   `1s`     -> `1.234ms`
                //   `1.2ms`  -> `1.234ms`
                //   `10.2μs` -> `1.234ms`
                let s = regex_timing.replace_all(&s, "1.234ms");

                let s = normalize_for_variable_width(s);

                // Metrics are currently ordered by value (not by key), so different timings may
                // reorder them. We "parse" the list and normalize the sorting. E.g.:
                //
                // `metrics=[]`             => `metrics=[]`
                // `metrics=[foo=1, bar=2]` => `metrics=[bar=2, foo=1]`
                // `metrics=[foo=2, bar=1]` => `metrics=[bar=1, foo=2]`
                regex_metrics
                    .replace_all(&s, |c: &Captures| {
                        let mut metrics: Vec<_> = c[1].split(", ").collect();
                        metrics.sort();
                        format!("metrics=[{}]", metrics.join(", "))
                    })
                    .to_string()
            })
            .collect();
    }

    // normalize Filters, if requested
    //
    // Converts:
    // FilterExec: time@2 < -9223372036854775808 OR time@2 > 1640995204240217000
    //
    // to
    // FilterExec: <REDACTED>
    if query.normalized_filters() {
        let filter_regex = Regex::new("FilterExec: .*").expect("filter regex");
        current_results = current_results
            .into_iter()
            .map(|s| {
                filter_regex
                    .replace_all(&s, |_: &Captures| "FilterExec: <REDACTED>")
                    .to_string()
            })
            .collect();
    }

    current_results
}
