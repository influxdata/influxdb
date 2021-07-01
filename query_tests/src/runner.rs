//! Basic test runner that runs queries in files and compares the output to the expected results

mod parse;
mod setup;

use arrow::record_batch::RecordBatch;
use query::{exec::ExecutorType, frontend::sql::SqlQueryPlanner};
use snafu::{ResultExt, Snafu};
use std::{
    io::BufWriter,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use self::{parse::TestQueries, setup::TestSetup};
use crate::scenarios::{DbScenario, DbSetup};

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
        expected_path
    ))]
    OutputMismatch {
        output_path: PathBuf,
        expected_path: PathBuf,
    },

    #[snafu(display(
        "Answers produced by scenario {} differ from previous answer\
         \n\nprevious:\n\n{:#?}\ncurrent:\n\n{:#?}\n\n",
        scenario_name,
        previous_results,
        current_results
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
    log: BufWriter<W>,
}

impl<W: Write> std::fmt::Debug for Runner<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Runner<W>")
    }
}

impl Runner<std::io::Stdout> {
    /// Create a new runner which writes to std::out
    pub fn new() -> Self {
        let log = BufWriter::new(std::io::stdout());
        Self { log }
    }

    pub fn flush(mut self) -> Result<Self> {
        self.log.flush()?;
        Ok(self)
    }
}

impl<W: Write> Runner<W> {
    pub fn new_with_writer(log: W) -> Self {
        let log = BufWriter::new(log);
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
    /// Produces output at `input_path`.out
    ///
    /// Compares it to an expected result at `input_path`.expected
    ///
    /// Returns Ok on success, or Err() on failure
    pub async fn run(&mut self, input_path: impl Into<PathBuf>) -> Result<()> {
        let input_path = input_path.into();
        // create output and expected output
        let output_path = input_path.with_extension("out");
        let expected_path = input_path.with_extension("expected");

        writeln!(self.log, "Running case in {:?}", input_path)?;
        writeln!(self.log, "  writing output to {:?}", output_path)?;
        writeln!(self.log, "  expected output in {:?}", expected_path)?;

        let contents = std::fs::read(&input_path).context(NoCaseFile { path: &input_path })?;
        let contents =
            String::from_utf8(contents).context(CaseFileNotUtf8 { path: &input_path })?;

        writeln!(self.log, "Processing contents:\n{}", contents)?;
        let test_setup = TestSetup::try_from_lines(contents.lines()).context(SetupError)?;
        let queries = TestQueries::from_lines(contents.lines());
        writeln!(self.log, "Using test setup:\n{}", test_setup)?;

        // Make a place to store output files
        let output_file = std::fs::File::create(&output_path).context(CreatingOutputFile {
            output_path: output_path.clone(),
        })?;

        let mut output = vec![];
        output.push(format!("-- Test Setup: {}", test_setup.setup_name()));

        let db_setup = test_setup.get_setup().context(SetupError)?;
        for q in queries.iter() {
            output.push(format!("-- SQL: {}", q));

            output.append(&mut self.run_query(q, db_setup.as_ref()).await?);
        }

        let mut output_file = BufWriter::new(output_file);
        for o in &output {
            writeln!(&mut output_file, "{}", o).with_context(|| WritingToOutputFile {
                output_path: output_path.clone(),
            })?;
        }
        output_file.flush().with_context(|| WritingToOutputFile {
            output_path: output_path.clone(),
        })?;

        std::mem::drop(output_file);

        // Now, compare to expected results
        let expected_data = std::fs::read(&expected_path)
            .ok() // no output is fine
            .unwrap_or_else(Vec::new);

        let expected_contents: Vec<_> = String::from_utf8(expected_data)
            .context(ExpectedFileNotUtf8 {
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
            OutputMismatch {
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
    async fn run_query(&mut self, sql: &str, db_setup: &dyn DbSetup) -> Result<Vec<String>> {
        let mut previous_results = vec![];

        for scenario in db_setup.make().await {
            let DbScenario {
                scenario_name, db, ..
            } = scenario;
            let db = Arc::new(db);

            writeln!(self.log, "Running scenario '{}'", scenario_name)?;
            writeln!(self.log, "SQL: '{:#?}'", sql)?;
            let planner = SqlQueryPlanner::default();
            let executor = db.executor();

            let physical_plan = planner
                .query(db, &sql, executor.as_ref())
                .expect("built plan successfully");

            let results: Vec<RecordBatch> = executor
                .collect(physical_plan, ExecutorType::Query)
                .await
                .expect("Running plan");

            let current_results = arrow::util::pretty::pretty_format_batches(&results)
                .unwrap()
                .trim()
                .lines()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();

            if !previous_results.is_empty() && previous_results != current_results {
                return ScenarioMismatch {
                    scenario_name,
                    previous_results,
                    current_results,
                }
                .fail();
            }
            previous_results = current_results;
        }
        Ok(previous_results)
    }
}

/// Return the absolute path to `path`, regardless of if it exists or
/// not on the local filesystem
fn make_absolute(path: &Path) -> PathBuf {
    let mut absolute = std::env::current_dir().expect("can not get current working directory");
    absolute.extend(path);
    absolute
}

#[cfg(test)]
mod test {
    use test_helpers::assert_contains;

    use super::*;

    const TEST_INPUT: &str = r#"
-- Runner test, positive
-- IOX_SETUP: TwoMeasurements

-- Only a single query
SELECT * from disk;
"#;

    const EXPECTED_OUTPUT: &str = r#"-- Test Setup: TwoMeasurements
-- SQL: SELECT * from disk;
+-------+--------+-------------------------------+
| bytes | region | time                          |
+-------+--------+-------------------------------+
| 99    | east   | 1970-01-01 00:00:00.000000200 |
+-------+--------+-------------------------------+
"#;

    #[tokio::test]
    async fn runner_positive() {
        let input_file = test_helpers::make_temp_file(TEST_INPUT);
        let output_path = input_file.path().with_extension("out");
        let expected_path = input_file.path().with_extension("expected");

        // write expected output
        std::fs::write(&expected_path, EXPECTED_OUTPUT).unwrap();

        let mut runner = Runner::new_with_writer(vec![]);
        let runner_results = runner.run(&input_file.path()).await;

        // ensure that the generated output and expected output match
        let output_contents = read_file(&output_path);
        assert_eq!(output_contents, EXPECTED_OUTPUT);

        // Test should have succeeded
        runner_results.expect("successful run");

        // examine the output log and ensure it contains expected resouts
        let runner_log = runner_to_log(runner);
        assert_contains!(&runner_log, format!("writing output to {:?}", &output_path));
        assert_contains!(
            &runner_log,
            format!("expected output in {:?}", &expected_path)
        );
        assert_contains!(&runner_log, "Setup: TwoMeasurements");
        assert_contains!(
            &runner_log,
            "Running scenario 'Data in open chunk of mutable buffer'"
        );
    }

    #[tokio::test]
    async fn runner_negative() {
        let input_file = test_helpers::make_temp_file(TEST_INPUT);
        let output_path = input_file.path().with_extension("out");
        let expected_path = input_file.path().with_extension("expected");

        // write incorrect expected output
        std::fs::write(&expected_path, "this is not correct").unwrap();

        let mut runner = Runner::new_with_writer(vec![]);
        let runner_results = runner.run(&input_file.path()).await;

        // ensure that the generated output and expected output match
        let output_contents = read_file(&output_path);
        assert_eq!(output_contents, EXPECTED_OUTPUT);

        // Test should have failed
        let err_string = runner_results.unwrap_err().to_string();
        assert_contains!(
            err_string,
            format!(
                "Contents of output '{:?}' does not match contents of expected '{:?}'",
                &output_path, &expected_path
            )
        );

        // examine the output log and ensure it contains expected resouts
        let runner_log = runner_to_log(runner);
        assert_contains!(&runner_log, format!("writing output to {:?}", &output_path));
        assert_contains!(
            &runner_log,
            format!("expected output in {:?}", &expected_path)
        );
        assert_contains!(&runner_log, "Setup: TwoMeasurements");
    }

    fn read_file(path: &Path) -> String {
        let output_contents = std::fs::read(path).expect("Can read file");
        String::from_utf8(output_contents).expect("utf8")
    }

    fn runner_to_log(runner: Runner<Vec<u8>>) -> String {
        let runner_log = runner.into_inner().expect("getting inner");
        String::from_utf8(runner_log).expect("output was utf8")
    }
}
