//! Basic test runner that runs queries in files and compares the output to the expected results

mod parse;
mod setup;

use arrow::record_batch::RecordBatch;
use arrow_util::{display::pretty_format_batches, test_util::sort_record_batch};
use iox_query::frontend::sql::SqlQueryPlanner;
use regex::{Captures, Regex};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    collections::HashMap,
    io::LineWriter,
    io::Write,
    path::{Path, PathBuf},
};
use uuid::Uuid;

use self::{
    parse::{Query, TestQueries},
    setup::TestSetup,
};
use crate::scenarios::{DbScenario, DbSetup};

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Can not find case file '{:?}': {}", path, source))]
    NoCaseFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("case file '{:?}' is not UTF8 {}", path, source))]
    CaseFileNotUtf8 {
        path: PathBuf,
        source: std::string::FromUtf8Error,
    },

    #[snafu(display("expected file '{:?}' is not UTF8 {}", path, source))]
    ExpectedFileNotUtf8 {
        path: PathBuf,
        source: std::string::FromUtf8Error,
    },

    #[snafu(display("can not open output file '{:?}': {}", output_path, source))]
    CreatingOutputFile {
        output_path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("can not write to output file '{:?}': {}", output_path, source))]
    WritingToOutputFile {
        output_path: PathBuf,
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

    #[snafu(display(
        "Answers produced by scenario {} differ from previous answer\
         \n\nprevious:\n{}\n\ncurrent:\n{}\n\n",
        scenario_name,
        previous_results.join("\n"),
        current_results.join("\n"),
    ))]
    ScenarioMismatch {
        scenario_name: String,
        previous_results: Vec<String>,
        current_results: Vec<String>,
    },

    #[snafu(display("Test Setup Error: {}", source))]
    SetupError { source: self::setup::Error },

    #[snafu(display("Error writing to runner log: {}", source))]
    LogIO { source: std::io::Error },

    #[snafu(display("IO inner while flushing buffer: {}", source))]
    FlushingBuffer { source: std::io::Error },

    #[snafu(display("Input path has no file stem: '{:?}'", path))]
    NoFileStem { path: PathBuf },

    #[snafu(display("Input path has no parent?!: '{:?}'", path))]
    NoParent { path: PathBuf },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Allow automatic conversion from IO errors
impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Self::LogIO { source }
    }
}

/// The case runner. It writes its test log output to the `Write`
/// output stream
pub struct Runner<W: Write> {
    log: LineWriter<W>,
}

impl<W: Write> std::fmt::Debug for Runner<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Runner<W>")
    }
}

/// Struct that calls `print!` to print out its data. Used rather than
/// `std::io::stdout` which is not captured by the result test runner
/// for some reason. This writer expects to get valid utf8 sequences
pub struct PrintWriter {}
impl Write for PrintWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        print!("{}", String::from_utf8_lossy(buf));
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Runner<PrintWriter> {
    /// Create a new runner which writes to std::out
    pub fn new() -> Self {
        let log = LineWriter::new(PrintWriter {});
        Self { log }
    }

    pub fn flush(mut self) -> Result<Self> {
        self.log.flush()?;
        Ok(self)
    }
}

impl<W: Write> Runner<W> {
    pub fn new_with_writer(log: W) -> Self {
        let log = LineWriter::new(log);
        Self { log }
    }

    /// Consume self and return the inner `Write` instance
    pub fn into_inner(self) -> Result<W> {
        let Self { mut log } = self;

        log.flush()?;
        let log = match log.into_inner() {
            Ok(log) => log,
            Err(e) => {
                panic!("Error flushing runner's buffer: {}", e.error());
            }
        };

        Ok(log)
    }

    /// Run the test case of the specified `input_path`
    ///
    /// Produces output at `../out/<input_path>.out`
    ///
    /// Compares it to an expected result at `<input_path>.expected`
    pub async fn run(&mut self, input_path: impl Into<PathBuf>) -> Result<()> {
        let input_path = input_path.into();
        // create output and expected output
        let output_path = make_output_path(&input_path)?;
        let expected_path = input_path.with_extension("expected");

        writeln!(self.log, "Running case in {:?}", input_path)?;
        writeln!(self.log, "  writing output to {:?}", output_path)?;
        writeln!(self.log, "  expected output in {:?}", expected_path)?;

        let contents = std::fs::read(&input_path).context(NoCaseFileSnafu { path: &input_path })?;
        let contents =
            String::from_utf8(contents).context(CaseFileNotUtf8Snafu { path: &input_path })?;

        writeln!(self.log, "Processing contents:\n{}", contents)?;
        let test_setup = TestSetup::try_from_lines(contents.lines()).context(SetupSnafu)?;
        let queries = TestQueries::from_lines(contents.lines());
        writeln!(self.log, "Using test setup:\n{}", test_setup)?;

        // Make a place to store output files
        let output_file = std::fs::File::create(&output_path).context(CreatingOutputFileSnafu {
            output_path: output_path.clone(),
        })?;

        let mut output = vec![];
        output.push(format!("-- Test Setup: {}", test_setup.setup_name()));

        let db_setup = test_setup.get_setup().context(SetupSnafu)?;
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

            output.append(&mut self.run_query(q, db_setup.as_ref()).await?);
        }

        let mut output_file = LineWriter::new(output_file);
        for o in &output {
            writeln!(&mut output_file, "{}", o).with_context(|_| WritingToOutputFileSnafu {
                output_path: output_path.clone(),
            })?;
        }
        output_file
            .flush()
            .with_context(|_| WritingToOutputFileSnafu {
                output_path: output_path.clone(),
            })?;

        std::mem::drop(output_file);

        // Now, compare to expected results
        let expected_data = std::fs::read(&expected_path).ok().unwrap_or_default();

        let expected_contents: Vec<_> = String::from_utf8(expected_data)
            .context(ExpectedFileNotUtf8Snafu {
                path: &expected_path,
            })?
            .lines()
            .map(|s| s.to_string())
            .collect();

        if expected_contents != output {
            let expected_path = make_absolute(&expected_path);
            let output_path = make_absolute(&output_path);

            writeln!(self.log, "Expected output does not match actual output")?;
            writeln!(self.log, "  expected output in {:?}", expected_path)?;
            writeln!(self.log, "  actual output in {:?}", output_path)?;
            writeln!(self.log, "Possibly helpful commands:")?;
            writeln!(self.log, "  # See diff")?;
            writeln!(self.log, "  diff -du {:?} {:?}", expected_path, output_path)?;
            writeln!(self.log, "  # Update expected")?;
            writeln!(self.log, "  cp -f {:?} {:?}", output_path, expected_path)?;

            OutputMismatchSnafu {
                output_path,
                expected_path,
            }
            .fail()
        } else {
            Ok(())
        }
    }

    /// runs the specified query against each scenario in `db_setup`
    /// and compares them for equality with each other. If they all
    /// produce the same answer, that answer is returned as pretty
    /// printed strings.
    ///
    /// If there is a mismatch the runner panics
    ///
    /// Note this does not (yet) understand how to compare results
    /// while ignoring output order
    async fn run_query(&mut self, query: &Query, db_setup: &dyn DbSetup) -> Result<Vec<String>> {
        let sql = query.sql();
        let mut previous_results = vec![];

        writeln!(self.log, "SQL: '{:#?}'", sql)?;

        for scenario in db_setup.make().await {
            let DbScenario {
                scenario_name, db, ..
            } = scenario;

            writeln!(self.log, "  Running scenario '{}'", scenario_name)?;
            let planner = SqlQueryPlanner::default();
            let ctx = db.new_query_context(None);

            let physical_plan = planner
                .query(sql, &ctx)
                .await
                .expect("built plan successfully");

            let mut results: Vec<RecordBatch> =
                ctx.collect(physical_plan).await.expect("Running plan");

            // compare against sorted results, if requested
            if query.sorted_compare() && !results.is_empty() {
                let schema = results[0].schema();
                let batch = arrow::compute::concat_batches(&schema, &results)
                    .expect("concatenating batches");
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
                let regex =
                    Regex::new("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
                        .expect("UUID regex");
                let mut seen: HashMap<String, u128> = HashMap::new();
                current_results = current_results
                    .into_iter()
                    .map(|s| {
                        regex
                            .replace_all(&s, |s: &Captures| {
                                let next = seen.len() as u128;
                                Uuid::from_u128(
                                    *seen
                                        .entry(s.get(0).unwrap().as_str().to_owned())
                                        .or_insert(next),
                                )
                                .to_string()
                            })
                            .to_string()
                    })
                    .collect();
            }

            // normalize metrics, if requested
            if query.normalized_metrics() {
                // Parse regex once and apply to all rows. See description around the `replace...` calls on why/how the
                // regexes are used.
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

                        // Replace table row separators of flexible width with fixed with. This is required because the
                        // original timing values may differ in "printed width", so the table cells have different
                        // widths and hence the separators / borders. E.g.:
                        //
                        //   `+--+--+`     -> `----------`
                        //   `+--+------+` -> `----------`
                        //
                        // Note that we're kinda inexact with our regex here, but it gets the job done.
                        let s = regex_linesep.replace_all(&s, "----------");

                        // Similar to the row separator issue above, the table columns are right-padded with spaces. Due
                        // to the different "printed width" of the timing values, we need to normalize this padding as
                        // well. E.g.:
                        //
                        //   `        |`  -> `    |`
                        //   `         |` -> `    |`
                        let s = regex_col.replace_all(&s, "    |");

                        // Metrics are currently ordered by value (not by key), so different timings may reorder them.
                        // We "parse" the list and normalize the sorting. E.g.:
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

            if !previous_results.is_empty() && previous_results != current_results {
                let err = ScenarioMismatchSnafu {
                    scenario_name,
                    previous_results,
                    current_results,
                }
                .build();
                writeln!(self.log, "    Err: {}", err)?;
                return Err(err);
            }
            previous_results = current_results;
        }
        Ok(previous_results)
    }
}

/// Return output path for input path.
///
/// This converts `some/prefix/in/foo.sql` (or other file extensions) to `some/prefix/out/foo.out`.
pub fn make_output_path(input: &Path) -> Result<PathBuf> {
    let stem = input.file_stem().context(NoFileStemSnafu { path: input })?;

    // go two levels up (from file to dir, from dir to parent dir)
    let parent = input.parent().context(NoParentSnafu { path: input })?;
    let parent = parent.parent().context(NoParentSnafu { path: parent })?;
    let mut out = parent.to_path_buf();

    // go one level down (from parent dir to out-dir)
    out.push("out");

    // set file name and ext
    // The PathBuf API is somewhat confusing: `set_file_name` will replace the last component (which at this point is
    // the "out"). However we wanna create a file out of the stem and the extension. So as a somewhat hackish
    // workaround first push a placeholder that is then replaced.
    out.push("placeholder");
    out.set_file_name(stem);
    out.set_extension("out");

    Ok(out)
}

/// Return the absolute path to `path`, regardless of if it exists or
/// not on the local filesystem
fn make_absolute(path: &Path) -> PathBuf {
    let mut absolute = std::env::current_dir().expect("can not get current working directory");
    absolute.extend(path);
    absolute
}

pub fn read_file(path: &Path) -> String {
    let output_contents = std::fs::read(path).expect("Can read file");
    String::from_utf8(output_contents).expect("utf8")
}

#[cfg(test)]
mod test {
    use test_helpers::{assert_contains, tmp_dir};

    use super::*;

    #[tokio::test]
    async fn runner_positive() {
        let input = r#"
-- Runner test, positive
-- IOX_SETUP: TwoMeasurements

-- Only a single query
SELECT * from disk;
"#;
        let expected = r#"-- Test Setup: TwoMeasurements
-- SQL: SELECT * from disk;
+-------+--------+--------------------------------+
| bytes | region | time                           |
+-------+--------+--------------------------------+
| 99    | east   | 1970-01-01T00:00:00.000000200Z |
+-------+--------+--------------------------------+
"#;

        let results = run_case(input, expected).await;

        // ensure that the generated output and expected output match
        assert_eq!(results.output_contents, expected);

        // Test should have succeeded
        results.runner_result.expect("successful run");

        // examine the output log and ensure it contains expected results
        assert_contains!(
            &results.runner_log,
            format!("writing output to {:?}", &results.output_path)
        );
        assert_contains!(
            &results.runner_log,
            format!("expected output in {:?}", &results.expected_path)
        );
        assert_contains!(&results.runner_log, "Setup: TwoMeasurements");
        assert_contains!(&results.runner_log, "Running scenario");
    }

    #[tokio::test]
    async fn runner_negative() {
        let input = r#"
-- Runner test, positive
-- IOX_SETUP: TwoMeasurements

-- Only a single query
SELECT * from disk;
"#;
        let expected = r#"-- Test Setup: TwoMeasurements
-- SQL: SELECT * from disk;
+-------+--------+--------------------------------+
| bytes | region | time                           |
+-------+--------+--------------------------------+
| 99    | east   | 1970-01-01T00:00:00.000000200Z |
+-------+--------+--------------------------------+
"#;

        let results = run_case(input, "this is not correct").await;

        // ensure that the generated output and expected output match
        assert_eq!(results.output_contents, expected);

        // Test should have failed
        let err_string = results.runner_result.unwrap_err().to_string();
        assert_contains!(
            err_string,
            format!(
                "Contents of output '{:?}' does not match contents of expected '{:?}'",
                &results.output_path, &results.expected_path
            )
        );

        // examine the output log and ensure it contains expected resouts
        assert_contains!(
            &results.runner_log,
            format!("writing output to {:?}", &results.output_path)
        );
        assert_contains!(
            &results.runner_log,
            format!("expected output in {:?}", &results.expected_path)
        );
        assert_contains!(&results.runner_log, "Setup: TwoMeasurements");
    }

    /// Ensure differences in sort order produce output errors
    #[tokio::test]
    async fn runner_different_sorts_error() {
        let input = r#"
-- Runner test, positive
-- IOX_SETUP: TwoMeasurements

-- Only a single query
SELECT * from cpu ORDER BY time DESC;
"#;
        let expected = r#"-- Test Setup: TwoMeasurements
-- SQL: SELECT * from cpu ORDER BY time DESC;
+--------+--------------------------------+------+
| region | time                           | user |
+--------+--------------------------------+------+
| west   | 1970-01-01T00:00:00.000000150Z | 21   |
| west   | 1970-01-01T00:00:00.000000100Z | 23.2 |
+--------+--------------------------------+------+
"#;

        let results = run_case(input, expected).await;

        // ensure that the generated output and expected output match
        assert_eq!(results.output_contents, expected);
        results.runner_result.unwrap();

        // now, however, if the results are in a different order
        // expect an output mismatch

        let expected = r#"-- Test Setup: TwoMeasurements
-- SQL: SELECT * from cpu ORDER BY time DESC;
+--------+--------------------------------+------+
| region | time                           | user |
+--------+--------------------------------+------+
| west   | 1970-01-01T00:00:00.000000100Z | 23.2 |
| west   | 1970-01-01T00:00:00.000000150Z | 21   |
+--------+--------------------------------+------+
"#;
        let results = run_case(input, expected).await;

        // ensure that the generated output and expected output match
        results.runner_result.unwrap_err();
        assert_contains!(
            &results.runner_log,
            "Expected output does not match actual output"
        );
    }

    /// Ensure differences in sort order does NOT produce output error
    #[tokio::test]
    async fn runner_different_sorts_with_sorted_compare() {
        let input = r#"
-- Runner test, positive
-- IOX_SETUP: TwoMeasurements

-- IOX_COMPARE: sorted
SELECT * from cpu ORDER BY time DESC;
"#;
        // note the output is not sorted `DESC` in time (it is ASC)
        let expected = r#"-- Test Setup: TwoMeasurements
-- SQL: SELECT * from cpu ORDER BY time DESC;
-- Results After Sorting
+--------+--------------------------------+------+
| region | time                           | user |
+--------+--------------------------------+------+
| west   | 1970-01-01T00:00:00.000000100Z | 23.2 |
| west   | 1970-01-01T00:00:00.000000150Z | 21   |
+--------+--------------------------------+------+
"#;
        let results = run_case(input, expected).await;

        // ensure that the generated output and expected output match
        assert_eq!(results.output_contents, expected);
        results.runner_result.unwrap();
    }

    /// Result of running the test_input with an expected output
    struct RunResult {
        /// Result of running the test case
        runner_result: Result<()>,

        /// The path that expected file was located in
        expected_path: PathBuf,

        /// The output file that the runner actually produced
        output_contents: String,

        /// The path that the output file was written to
        output_path: PathBuf,

        // The log the runner produced
        runner_log: String,
    }

    /// Uses the test runner to run the expected input and compares it
    /// to the expected output, returning the runner used to do the
    /// comparison as well as the result of the run
    async fn run_case(test_input: &str, expected_output: &str) -> RunResult {
        let (_tmp_dir, input_file) = make_in_file(test_input);
        let output_path = make_output_path(&input_file).unwrap();
        let expected_path = input_file.with_extension("expected");

        // write expected output
        std::fs::write(&expected_path, expected_output).unwrap();

        let mut runner = Runner::new_with_writer(vec![]);
        let runner_result = runner.run(&input_file).await;
        let output_contents = read_file(&output_path);
        let runner_log = runner_to_log(runner);

        RunResult {
            runner_result,
            expected_path,
            output_contents,
            output_path,
            runner_log,
        }
    }

    fn make_in_file<C: AsRef<[u8]>>(contents: C) -> (tempfile::TempDir, PathBuf) {
        let dir = tmp_dir().expect("create temp dir");
        let in_dir = dir.path().join("in");
        std::fs::create_dir(&in_dir).expect("create in-dir");

        let out_dir = dir.path().join("out");
        std::fs::create_dir(out_dir).expect("create out-dir");

        let mut file = in_dir;
        file.push("foo.sql");

        std::fs::write(&file, contents).expect("writing data to temp file");
        (dir, file)
    }

    fn runner_to_log(runner: Runner<Vec<u8>>) -> String {
        let runner_log = runner.into_inner().expect("getting inner");
        String::from_utf8(runner_log).expect("output was utf8")
    }
}
