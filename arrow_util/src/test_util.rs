//! A collection of testing functions for arrow based code
use std::sync::Arc;

use crate::display::pretty_format_batches;
use arrow::{
    array::{new_null_array, ArrayRef, StringArray},
    compute::kernels::sort::{lexsort, SortColumn, SortOptions},
    datatypes::Schema,
    error::ArrowError,
    record_batch::RecordBatch,
};
use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use std::{borrow::Cow, collections::HashMap};
use uuid::Uuid;

/// Compares the formatted output with the pretty formatted results of
/// record batches. This is a macro so errors appear on the correct line
///
/// Designed so that failure output can be directly copy/pasted
/// into the test code as expected results.
///
/// Expects to be called about like this:
/// assert_batches_eq(expected_lines: &[&str], chunks: &[RecordBatch])
#[macro_export]
macro_rules! assert_batches_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        let expected_lines: Vec<String> =
            $EXPECTED_LINES.into_iter().map(|s| s.to_string()).collect();

        let actual_lines = arrow_util::test_util::batches_to_lines($CHUNKS);

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

/// Compares formatted output of a record batch with an expected
/// vector of strings in a way that order does not matter.
/// This is a macro so errors appear on the correct line
///
/// Designed so that failure output can be directly copy/pasted
/// into the test code as expected results.
///
/// Expects to be called about like this:
///
/// `assert_batch_sorted_eq!(expected_lines: &[&str], batches: &[RecordBatch])`
#[macro_export]
macro_rules! assert_batches_sorted_eq {
    ($EXPECTED_LINES: expr, $CHUNKS: expr) => {
        let expected_lines: Vec<String> = $EXPECTED_LINES.iter().map(|&s| s.into()).collect();
        let expected_lines = arrow_util::test_util::sort_lines(expected_lines);

        let actual_lines = arrow_util::test_util::batches_to_sorted_lines($CHUNKS);

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

/// Converts the [`RecordBatch`]es into a pretty printed output suitable for
/// comparing in tests
///
/// Example:
///
/// ```text
/// "+-----+------+------+--------------------------------+",
/// "| foo | host | load | time                           |",
/// "+-----+------+------+--------------------------------+",
/// "|     | a    | 1.0  | 1970-01-01T00:00:00.000000011Z |",
/// "|     | a    | 14.0 | 1970-01-01T00:00:00.000010001Z |",
/// "|     | a    | 3.0  | 1970-01-01T00:00:00.000000033Z |",
/// "|     | b    | 5.0  | 1970-01-01T00:00:00.000000011Z |",
/// "|     | z    | 0.0  | 1970-01-01T00:00:00Z           |",
/// "+-----+------+------+--------------------------------+",
/// ```
pub fn batches_to_lines(batches: &[RecordBatch]) -> Vec<String> {
    crate::display::pretty_format_batches(batches)
        .unwrap()
        .trim()
        .lines()
        .map(|s| s.to_string())
        .collect()
}

/// Converts the [`RecordBatch`]es into a pretty printed output suitable for
/// comparing in tests where sorting does not matter.
pub fn batches_to_sorted_lines(batches: &[RecordBatch]) -> Vec<String> {
    sort_lines(batches_to_lines(batches))
}

/// Sorts the lines (assumed to be the output of `batches_to_lines` for stable comparison)
pub fn sort_lines(mut lines: Vec<String>) -> Vec<String> {
    // sort except for header + footer
    let num_lines = lines.len();
    if num_lines > 3 {
        lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
    }
    lines
}

// sort a record batch by all columns (to provide a stable output order for test
// comparison)
pub fn sort_record_batch(batch: RecordBatch) -> RecordBatch {
    let sort_input: Vec<SortColumn> = batch
        .columns()
        .iter()
        .map(|col| SortColumn {
            values: Arc::clone(col),
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        })
        .collect();

    let sort_output = lexsort(&sort_input, None).expect("Sorting to complete");

    RecordBatch::try_new(batch.schema(), sort_output).unwrap()
}

/// Return a new `StringArray` where each element had a normalization
/// function `norm` applied.
pub fn normalize_string_array<N>(arr: &StringArray, norm: N) -> ArrayRef
where
    N: Fn(&str) -> String,
{
    let normalized: StringArray = arr.iter().map(|s| s.map(&norm)).collect();
    Arc::new(normalized)
}

/// Return a new set of `RecordBatch`es where the function `norm` has
/// applied to all `StringArray` rows.
pub fn normalize_batches<N>(batches: Vec<RecordBatch>, norm: N) -> Vec<RecordBatch>
where
    N: Fn(&str) -> String,
{
    // The idea here is is to get a function that normalizes strings
    // and apply it to each StringArray element by element
    batches
        .into_iter()
        .map(|batch| {
            let new_columns: Vec<_> = batch
                .columns()
                .iter()
                .map(|array| {
                    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
                        normalize_string_array(array, &norm)
                    } else {
                        Arc::clone(array)
                    }
                })
                .collect();

            RecordBatch::try_new(batch.schema(), new_columns)
                .expect("error occurred during normalization")
        })
        .collect()
}

/// Equalize batch schemas by creating NULL columns.
pub fn equalize_batch_schemas(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, ArrowError> {
    let common_schema = Arc::new(Schema::try_merge(
        batches.iter().map(|batch| batch.schema().as_ref().clone()),
    )?);

    Ok(batches
        .into_iter()
        .map(|batch| {
            let batch_schema = batch.schema();
            let columns = common_schema
                .fields()
                .iter()
                .map(|field| match batch_schema.index_of(field.name()) {
                    Ok(idx) => Arc::clone(batch.column(idx)),
                    Err(_) => new_null_array(field.data_type(), batch.num_rows()),
                })
                .collect();
            RecordBatch::try_new(Arc::clone(&common_schema), columns).unwrap()
        })
        .collect())
}

/// Match the parquet UUID
///
/// For example, given
/// `32/51/216/13452/1d325760-2b20-48de-ab48-2267b034133d.parquet`
///
/// matches `1d325760-2b20-48de-ab48-2267b034133d`
pub static REGEX_UUID: Lazy<Regex> = Lazy::new(|| {
    Regex::new("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}").expect("UUID regex")
});

/// Match the parquet directory names
/// For example, given
/// `51/216/1a3f45021a3f45021a3f45021a3f45021a3f45021a3f45021a3f45021a3f4502/1d325760-2b20-48de-ab48-2267b034133d.parquet`
///
/// matches `51/216/1a3f45021a3f45021a3f45021a3f45021a3f45021a3f45021a3f45021a3f4502`
static REGEX_DIRS: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"[0-9]+/[0-9]+/[0-9a-f]+/"#).expect("directory regex"));

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
static REGEX_COL: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+\|").expect("col regex"));

/// Matches line like `metrics=[foo=1, bar=2]`
static REGEX_METRICS: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"metrics=\[([^\]]*)\]").expect("metrics regex"));

/// Matches things like  `1s`, `1.2ms` and `10.2μs`
static REGEX_TIMING: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"[0-9]+(\.[0-9]+)?.s").expect("timing regex"));

/// Matches things like `FilterExec: .*` and `ParquetExec: .*`
///
/// Should be used in combination w/ [`REGEX_TIME_OP`].
static REGEX_FILTER: Lazy<Regex> = Lazy::new(|| {
    Regex::new("(?P<prefix>(FilterExec)|(ParquetExec): )(?P<expr>.*)").expect("filter regex")
});

/// Matches things like `time@3 < -9223372036854775808` and `time_min@2 > 1641031200399937022`
static REGEX_TIME_OP: Lazy<Regex> = Lazy::new(|| {
    Regex::new("(?P<prefix>time((_min)|(_max))?@[0-9]+ [<>=]=? )(?P<value>-?[0-9]+)")
        .expect("time opt regex")
});

fn normalize_for_variable_width(s: Cow<'_, str>) -> String {
    let s = REGEX_LINESEP.replace_all(&s, "----------");
    REGEX_COL.replace_all(&s, "    |").to_string()
}

pub fn strip_table_lines(s: Cow<'_, str>) -> String {
    let s = REGEX_LINESEP.replace_all(&s, "----------");
    REGEX_COL.replace_all(&s, "").to_string()
}

fn normalize_time_ops(s: &str) -> String {
    REGEX_TIME_OP
        .replace_all(s, |c: &Captures<'_>| {
            let prefix = c.name("prefix").expect("always captures").as_str();
            format!("{prefix}<REDACTED>")
        })
        .to_string()
}

/// A query to run with optional annotations
#[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
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

    /// if `true`, render tables without borders.
    pub no_table_borders: bool,
}

impl Normalizer {
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
                    // Rewrite Parquet directory names like
                    // `51/216/1a3f45021a3f45021a3f45021a3f45021a3f45021a3f45021a3f45021a3f4502/1d325760-2b20-48de-ab48-2267b034133d.parquet`
                    //
                    // to:
                    // 1/1/1/00000000-0000-0000-0000-000000000000.parquet

                    let s = REGEX_UUID.replace_all(&s, |s: &Captures<'_>| {
                        let next = seen.len() as u128;
                        Uuid::from_u128(
                            *seen
                                .entry(s.get(0).unwrap().as_str().to_owned())
                                .or_insert(next),
                        )
                        .to_string()
                    });

                    let s = normalize_for_variable_width(s);
                    REGEX_DIRS.replace_all(&s, "1/1/1/").to_string()
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
                        .replace_all(&s, |c: &Captures<'_>| {
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
        // ParquetExec: limit=None, partitions={...}, predicate=time@2 > 1640995204240217000, pruning_predicate=time@2 > 1640995204240217000, output_ordering=[...], projection=[...]
        //
        // to
        // FilterExec: time@2 < <REDACTED> OR time@2 > <REDACTED>
        // ParquetExec: limit=None, partitions={...}, predicate=time@2 > <REDACTED>, pruning_predicate=time@2 > <REDACTED>, output_ordering=[...], projection=[...]
        if self.normalized_filters {
            current_results = current_results
                .into_iter()
                .map(|s| {
                    REGEX_FILTER
                        .replace_all(&s, |c: &Captures<'_>| {
                            let prefix = c.name("prefix").expect("always captrues").as_str();

                            let expr = c.name("expr").expect("always captures").as_str();
                            let expr = normalize_time_ops(expr);

                            format!("{prefix}{expr}")
                        })
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
        if self.no_table_borders {
            output.push("-- Results After No Table Borders".into())
        }
    }
}
