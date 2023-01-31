use crate::{run_sql, MiniCluster};
use arrow_util::{display::pretty_format_batches, test_util::sort_record_batch};
use regex::{Captures, Regex};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Could not read case file '{:?}': {}", path, source))]
    ReadingCaseFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(context(false))]
    MakingOutputPath { source: OutputPathError },

    #[snafu(display("Could not write to output file '{:?}': {}", output_path, source))]
    WritingToOutputFile {
        output_path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Could not read expected file '{:?}': {}", path, source))]
    ReadingExpectedFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display(
        "Contents of output '{:?}' does not match contents of expected '{:?}'",
        output_path,
        expected_path,
    ))]
    OutputMismatch {
        output_path: PathBuf,
        expected_path: PathBuf,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn run(
    cluster: &mut MiniCluster,
    input_path: PathBuf,
    setup_name: String,
    contents: String,
) -> Result<()> {
    // create output and expected output
    let output_path = make_output_path(&input_path)?;
    let expected_path = input_path.with_extension("expected");

    println!("Running case in {:?}", input_path);
    println!("  writing output to {:?}", output_path);
    println!("  expected output in {:?}", expected_path);
    println!("Processing contents:\n{}", contents);

    let queries = TestQueries::from_lines(contents.lines());

    let mut output = vec![];
    output.push(format!("-- Test Setup: {setup_name}"));

    for q in queries.iter() {
        output.push(format!("-- SQL: {}", q.sql()));
        if q.sorted_compare() {
            output.push("-- Results After Sorting".into())
        }
        if q.normalized_uuids() {
            output.push("-- Results After Normalizing UUIDs".into())
        }
        if q.normalized_metrics() {
            output.push("-- Results After Normalizing Metrics".into())
        }
        if q.normalized_filters() {
            output.push("-- Results After Normalizing Filters".into())
        }

        let results = run_query(cluster, q).await?;
        output.extend(results);
    }

    fs::write(&output_path, output.join("\n")).context(WritingToOutputFileSnafu {
        output_path: &output_path,
    })?;

    // Now, compare to expected results
    let expected_data = fs::read_to_string(&expected_path)
        .context(ReadingExpectedFileSnafu { path: &input_path })?;
    let expected_contents: Vec<_> = expected_data.lines().map(|s| s.to_string()).collect();

    if expected_contents != output {
        let expected_path = make_absolute(&expected_path);
        let output_path = make_absolute(&output_path);

        println!("Expected output does not match actual output");
        println!("  expected output in {:?}", expected_path);
        println!("  actual output in {:?}", output_path);
        println!("Possibly helpful commands:");
        println!("  # See diff");
        println!("  diff -du {:?} {:?}", expected_path, output_path);
        println!("  # Update expected");
        println!("  cp -f {:?} {:?}", output_path, expected_path);

        OutputMismatchSnafu {
            output_path,
            expected_path,
        }
        .fail()
    } else {
        Ok(())
    }
}

#[derive(Debug, Snafu)]
pub enum OutputPathError {
    #[snafu(display("Input path has no file stem: '{:?}'", path))]
    NoFileStem { path: PathBuf },

    #[snafu(display("Input path has no parent?!: '{:?}'", path))]
    NoParent { path: PathBuf },
}

/// Return output path for input path.
///
/// This converts `some/prefix/in/foo.sql` (or other file extensions) to `some/prefix/out/foo.out`.
fn make_output_path(input: &Path) -> Result<PathBuf, OutputPathError> {
    let stem = input.file_stem().context(NoFileStemSnafu { path: input })?;

    // go two levels up (from file to dir, from dir to parent dir)
    let parent = input.parent().context(NoParentSnafu { path: input })?;
    let parent = parent.parent().context(NoParentSnafu { path: parent })?;
    let mut out = parent.to_path_buf();

    // go one level down (from parent dir to out-dir)
    out.push("out");

    // make best effort attempt to create output directory if it
    // doesn't exist (it does not on a fresh checkout)
    if !out.exists() {
        if let Err(e) = std::fs::create_dir(&out) {
            panic!("Could not create output directory {out:?}: {e}");
        }
    }

    // set file name and ext
    out.push(stem);
    out.set_extension("out");

    Ok(out)
}

/// Return the absolute path to `path`, regardless of if it exists on the local filesystem
fn make_absolute(path: &Path) -> PathBuf {
    let mut absolute = std::env::current_dir().expect("cannot get current working directory");
    absolute.extend(path);
    absolute
}

async fn run_query(cluster: &MiniCluster, query: &Query) -> Result<Vec<String>> {
    let sql = query.sql();

    let mut results = run_sql(
        sql,
        cluster.namespace(),
        cluster.querier().querier_grpc_connection(),
    )
    .await;

    // compare against sorted results, if requested
    if query.sorted_compare() && !results.is_empty() {
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
    if query.normalized_uuids() {
        let regex_uuid = Regex::new("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
            .expect("UUID regex");
        let regex_dirs = Regex::new(r#"[0-9]+/[0-9]+/[0-9]+/[0-9+]"#).expect("directory regex");

        let mut seen: HashMap<String, u128> = HashMap::new();
        current_results = current_results
            .into_iter()
            .map(|s| {
                let s = regex_uuid
                    .replace_all(&s, |s: &Captures| {
                        let next = seen.len() as u128;
                        Uuid::from_u128(
                            *seen
                                .entry(s.get(0).unwrap().as_str().to_owned())
                                .or_insert(next),
                        )
                        .to_string()
                    })
                    .to_string();

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
        let regex_linesep = Regex::new(r#"[+-]{6,}"#).expect("linesep regex");
        let regex_col = Regex::new(r#"\s+\|"#).expect("col regex");

        current_results = current_results
            .into_iter()
            .map(|s| {
                // Replace timings with fixed value, e.g.:
                //
                //   `1s`     -> `1.234ms`
                //   `1.2ms`  -> `1.234ms`
                //   `10.2μs` -> `1.234ms`
                let s = regex_timing.replace_all(&s, "1.234ms");

                // Replace table row separators of flexible width with fixed with. This is required
                // because the original timing values may differ in "printed width", so the table
                // cells have different widths and hence the separators / borders. E.g.:
                //
                //   `+--+--+`     -> `----------`
                //   `+--+------+` -> `----------`
                //
                // Note that we're kinda inexact with our regex here, but it gets the job done.
                let s = regex_linesep.replace_all(&s, "----------");

                // Similar to the row separator issue above, the table columns are right-padded
                // with spaces. Due to the different "printed width" of the timing values, we need
                // to normalize this padding as well. E.g.:
                //
                //   `        |`  -> `    |`
                //   `         |` -> `    |`
                let s = regex_col.replace_all(&s, "    |");

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

    Ok(current_results)
}

/// A query to run with optional annotations
#[derive(Debug, PartialEq, Eq, Default)]
pub struct Query {
    /// If true, results are sorted first prior to comparison, meaning that differences in the
    /// output order compared with expected order do not cause a diff
    sorted_compare: bool,

    /// If true, replace UUIDs with static placeholders.
    normalized_uuids: bool,

    /// If true, normalize timings in queries by replacing them with
    /// static placeholders, for example:
    ///
    /// `1s`     -> `1.234ms`
    normalized_metrics: bool,

    /// if true, normalize filter predicates for explain plans
    /// `FilterExec: <REDACTED>`
    normalized_filters: bool,

    /// The SQL string
    sql: String,
}

impl Query {
    #[cfg(test)]
    fn new(sql: impl Into<String>) -> Self {
        let sql = sql.into();
        Self {
            sorted_compare: false,
            normalized_uuids: false,
            normalized_metrics: false,
            normalized_filters: false,
            sql,
        }
    }

    #[cfg(test)]
    fn with_sorted_compare(mut self) -> Self {
        self.sorted_compare = true;
        self
    }

    /// Get a reference to the query's sql.
    pub fn sql(&self) -> &str {
        self.sql.as_ref()
    }

    /// Get the query's sorted compare.
    pub fn sorted_compare(&self) -> bool {
        self.sorted_compare
    }

    /// Get queries normalized UUID
    pub fn normalized_uuids(&self) -> bool {
        self.normalized_uuids
    }

    /// Use normalized timing values
    pub fn normalized_metrics(&self) -> bool {
        self.normalized_metrics
    }

    /// Use normalized filter plans
    pub fn normalized_filters(&self) -> bool {
        self.normalized_filters
    }
}

#[derive(Debug, Default)]
struct QueryBuilder {
    query: Query,
}

impl QueryBuilder {
    fn new() -> Self {
        Default::default()
    }

    fn push_str(&mut self, s: &str) {
        self.query.sql.push_str(s)
    }

    fn push(&mut self, c: char) {
        self.query.sql.push(c)
    }

    fn sorted_compare(&mut self) {
        self.query.sorted_compare = true;
    }

    fn normalized_uuids(&mut self) {
        self.query.normalized_uuids = true;
    }

    fn normalize_metrics(&mut self) {
        self.query.normalized_metrics = true;
    }

    fn normalize_filters(&mut self) {
        self.query.normalized_filters = true;
    }

    fn is_empty(&self) -> bool {
        self.query.sql.is_empty()
    }

    /// Creates a Query and resets this builder to default
    fn build_and_reset(&mut self) -> Option<Query> {
        (!self.is_empty()).then(|| std::mem::take(&mut self.query))
    }
}

/// Poor man's parser to find all the SQL queries in an input file
#[derive(Debug, PartialEq, Eq)]
pub struct TestQueries {
    queries: Vec<Query>,
}

impl TestQueries {
    /// find all queries (more or less a fancy split on `;`
    pub fn from_lines<I, S>(lines: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut queries = vec![];
        let mut builder = QueryBuilder::new();

        lines.into_iter().for_each(|line| {
            let line = line.as_ref().trim();
            const COMPARE_STR: &str = "-- IOX_COMPARE: ";
            if line.starts_with(COMPARE_STR) {
                let (_, options) = line.split_at(COMPARE_STR.len());
                for option in options.split(',') {
                    let option = option.trim();
                    match option {
                        "sorted" => {
                            builder.sorted_compare();
                        }
                        "uuid" => {
                            builder.normalized_uuids();
                        }
                        "metrics" => {
                            builder.normalize_metrics();
                        }
                        "filters" => {
                            builder.normalize_filters();
                        }
                        _ => {}
                    }
                }
            }

            if line.starts_with("--") {
                return;
            }
            if line.is_empty() {
                return;
            }

            // replace newlines
            if !builder.is_empty() {
                builder.push(' ');
            }
            builder.push_str(line);

            // declare queries when we see a semicolon at the end of the line
            if line.ends_with(';') {
                if let Some(q) = builder.build_and_reset() {
                    queries.push(q);
                }
            }
        });

        if let Some(q) = builder.build_and_reset() {
            queries.push(q);
        }

        Self { queries }
    }

    // Get an iterator over the queries
    pub fn iter(&self) -> impl Iterator<Item = &Query> {
        self.queries.iter()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_queries() {
        let input = r#"
-- This is a test
select * from foo;
-- another comment

select * from bar;
-- This query has been commented out and should not be seen
-- select * from baz;
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(
            queries,
            TestQueries {
                queries: vec![
                    Query::new("select * from foo;"),
                    Query::new("select * from bar;"),
                ]
            }
        )
    }

    #[test]
    fn test_parse_queries_no_ending_semi() {
        let input = r#"
select * from foo;
-- no ending semi colon
select * from bar
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(
            queries,
            TestQueries {
                queries: vec![
                    Query::new("select * from foo;"),
                    Query::new("select * from bar")
                ]
            }
        )
    }

    #[test]
    fn test_parse_queries_mulit_line() {
        let input = r#"
select
  *
from
  foo;

select * from bar;

"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(
            queries,
            TestQueries {
                queries: vec![
                    Query::new("select * from foo;"),
                    Query::new("select * from bar;"),
                ]
            }
        )
    }

    #[test]
    fn test_parse_queries_empty() {
        let input = r#"
-- This is a test
-- another comment
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(queries, TestQueries { queries: vec![] })
    }

    #[test]
    fn test_parse_queries_sorted_compare() {
        let input = r#"
select * from foo;

-- The second query should be compared to expected after sorting
-- IOX_COMPARE: sorted
select * from bar;

-- Since this query is not annotated, it should not use exected sorted
select * from baz;
select * from baz2;

-- IOX_COMPARE: sorted
select * from waz;
-- (But the compare should work subsequently)
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(
            queries,
            TestQueries {
                queries: vec![
                    Query::new("select * from foo;"),
                    Query::new("select * from bar;").with_sorted_compare(),
                    Query::new("select * from baz;"),
                    Query::new("select * from baz2;"),
                    Query::new("select * from waz;").with_sorted_compare(),
                ]
            }
        )
    }

    #[test]
    fn test_parse_queries_sorted_compare_after() {
        let input = r#"
select * from foo;
-- IOX_COMPARE: sorted
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(
            queries,
            TestQueries {
                queries: vec![Query::new("select * from foo;")]
            }
        )
    }

    #[test]
    fn test_parse_queries_sorted_compare_not_match_ignored() {
        let input = r#"
-- IOX_COMPARE: something_else
select * from foo;
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(
            queries,
            TestQueries {
                queries: vec![Query::new("select * from foo;")]
            }
        )
    }
}
