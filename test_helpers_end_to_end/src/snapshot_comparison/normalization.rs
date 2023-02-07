use arrow::record_batch::RecordBatch;
use arrow_util::{display::pretty_format_batches, test_util::sort_record_batch};
use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use std::{borrow::Cow, collections::HashMap};
use uuid::Uuid;

/// Match the parquet UUID
///
/// For example, given
/// `32/51/216/13452/1d325760-2b20-48de-ab48-2267b034133d.parquet`
///
/// matches `1d325760-2b20-48de-ab48-2267b034133d`
static REGEX_UUID: Lazy<Regex> = Lazy::new(|| {
    Regex::new("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}").expect("UUID regex")
});

/// Match the parquet directory names
/// For example, given
/// `32/51/216/13452/1d325760-2b20-48de-ab48-2267b034133d.parquet`
///
/// matches `32/51/216/13452`
static REGEX_DIRS: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"[0-9]+/[0-9]+/[0-9]+/[0-9]+"#).expect("directory regex"));

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

/// Matches line like `metrics=[foo=1, bar=2]`
static REGEX_METRICS: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"metrics=\[([^\]]*)\]"#).expect("metrics regex"));

/// Matches things like  `1s`, `1.2ms` and `10.2μs`
static REGEX_TIMING: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"[0-9]+(\.[0-9]+)?.s"#).expect("timing regex"));

/// Matches things like `FilterExec: time@2 < -9223372036854775808 OR time@2 > 1640995204240217000`
static REGEX_FILTER: Lazy<Regex> =
    Lazy::new(|| Regex::new("FilterExec: .*").expect("filter regex"));

fn normalize_for_variable_width(s: Cow<str>) -> String {
    let s = REGEX_LINESEP.replace_all(&s, "----------");
    REGEX_COL.replace_all(&s, "    |").to_string()
}

/// A query to run with optional annotations
#[derive(Debug, PartialEq, Eq, Default)]
pub struct Normalizer {
    /// If true, results are sorted first
    pub sorted_compare: bool,

    /// If true, replace UUIDs with static placeholders.
    pub normalized_uuids: bool,

    /// If true, normalize timings in queries by replacing them with
    /// static placeholders, for example:
    ///
    /// `1s`     -> `1.234ms`
    pub normalized_metrics: bool,

    /// if true, normalize filter predicates for explain plans
    /// `FilterExec: <REDACTED>`
    pub normalized_filters: bool,
}

impl Normalizer {
    #[cfg(test)]
    pub fn new() -> Self {
        Default::default()
    }

    /// Take the output of running the query and apply the specified normalizations to them
    pub fn normalize_results(&self, mut results: Vec<RecordBatch>) -> Vec<String> {
        // compare against sorted results, if requested
        if self.sorted_compare && !results.is_empty() {
            let schema = results[0].schema();
            let batch =
                arrow::compute::concat_batches(&schema, &results).expect("concatenating batches");
            results = vec![sort_record_batch(batch)];
        }

        let mut current_results = pretty_format_batches(&results)
            .unwrap()
            .trim()
            .lines()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();

        // normalize UUIDs, if requested
        if self.normalized_uuids {
            let mut seen: HashMap<String, u128> = HashMap::new();
            current_results = current_results
                .into_iter()
                .map(|s| {
                    // Rewrite  parquet directory names like
                    // `51/216/13452/1d325760-2b20-48de-ab48-2267b034133d.parquet`
                    //
                    // to:
                    // 1/1/1/1/00000000-0000-0000-0000-000000000000.parquet

                    let s = REGEX_UUID.replace_all(&s, |s: &Captures| {
                        let next = seen.len() as u128;
                        Uuid::from_u128(
                            *seen
                                .entry(s.get(0).unwrap().as_str().to_owned())
                                .or_insert(next),
                        )
                        .to_string()
                    });

                    let s = normalize_for_variable_width(s);
                    REGEX_DIRS.replace_all(&s, "1/1/1/1").to_string()
                })
                .collect();
        }

        // normalize metrics, if requested
        if self.normalized_metrics {
            current_results = current_results
                .into_iter()
                .map(|s| {
                    // Replace timings with fixed value, e.g.:
                    //
                    //   `1s`     -> `1.234ms`
                    //   `1.2ms`  -> `1.234ms`
                    //   `10.2μs` -> `1.234ms`
                    let s = REGEX_TIMING.replace_all(&s, "1.234ms");

                    let s = normalize_for_variable_width(s);

                    // Metrics are currently ordered by value (not by key), so different timings may
                    // reorder them. We "parse" the list and normalize the sorting. E.g.:
                    //
                    // `metrics=[]`             => `metrics=[]`
                    // `metrics=[foo=1, bar=2]` => `metrics=[bar=2, foo=1]`
                    // `metrics=[foo=2, bar=1]` => `metrics=[bar=1, foo=2]`
                    REGEX_METRICS
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
        if self.normalized_filters {
            current_results = current_results
                .into_iter()
                .map(|s| {
                    REGEX_FILTER
                        .replace_all(&s, |_: &Captures| "FilterExec: <REDACTED>")
                        .to_string()
                })
                .collect();
        }

        current_results
    }

    /// Adds information on what normalizations were applied to the input
    pub fn add_description(&self, output: &mut Vec<String>) {
        if self.sorted_compare {
            output.push("-- Results After Sorting".into())
        }
        if self.normalized_uuids {
            output.push("-- Results After Normalizing UUIDs".into())
        }
        if self.normalized_metrics {
            output.push("-- Results After Normalizing Metrics".into())
        }
        if self.normalized_filters {
            output.push("-- Results After Normalizing Filters".into())
        }
    }
}
