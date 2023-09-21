mod queries;

use crate::{
    snapshot_comparison::queries::TestQueries, try_run_influxql, try_run_sql, MiniCluster,
};
use arrow::record_batch::RecordBatch;
use arrow_flight::error::FlightError;
use arrow_util::test_util::{sort_record_batch, Normalizer, REGEX_UUID};
use influxdb_iox_client::format::influxql::{write_columnar, Options, TableBorders};
use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use snafu::{OptionExt, ResultExt, Snafu};
use sqlx::types::Uuid;
use std::collections::HashMap;
use std::{
    fmt::{Display, Formatter},
    fs,
    path::{Path, PathBuf},
};
use tonic::Code;

use self::queries::Query;

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

/// Match the parquet directory names
/// For example, given
/// `51/216/1a3f45021a3f45021a3f45021a3f45021a3f45021a3f45021a3f45021a3f4502/1d325760-2b20-48de-ab48-2267b034133d.parquet`
///
/// matches `51/216/1a3f45021a3f45021a3f45021a3f45021a3f45021a3f45021a3f45021a3f4502`
pub static REGEX_DIRS: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"([0-9]+)/([0-9]+)/([0-9a-f]{64})"#).expect("directory regex"));

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Language {
    #[default]
    Sql,
    InfluxQL,
}

impl Language {
    fn normalize_results(&self, n: &Normalizer, results: Vec<RecordBatch>) -> Vec<String> {
        match self {
            Language::Sql => n.normalize_results(results),
            Language::InfluxQL => {
                let results = if n.sorted_compare && !results.is_empty() {
                    let schema = results[0].schema();
                    let batch = arrow::compute::concat_batches(&schema, &results)
                        .expect("concatenating batches");
                    vec![sort_record_batch(batch)]
                } else {
                    results
                };

                let options = if n.no_table_borders {
                    Options {
                        borders: TableBorders::None,
                    }
                } else {
                    Options::default()
                };

                let mut buf = Vec::<u8>::new();
                write_columnar(&mut buf, &results, options).unwrap();

                let mut seen: HashMap<String, u128> = HashMap::new();

                unsafe { String::from_utf8_unchecked(buf) }
                    .trim()
                    .lines()
                    .map(|s| {
                        // replace any UUIDs
                        let s = REGEX_UUID.replace_all(s, |c: &Captures<'_>| {
                            let next = seen.len() as u128;
                            Uuid::from_u128(
                                *seen
                                    .entry(c.get(0).unwrap().as_str().to_owned())
                                    .or_insert(next),
                            )
                            .to_string()
                        });

                        let s = REGEX_DIRS.replace_all(&s, |_c: &Captures<'_>| {
                            // this ensures 15/232/5 is replaced with a string of a known width
                            //              1/1/1
                            ["1", "1", "1"].join("/")
                        });

                        // Need to remove trailing spaces when using no_borders
                        let s = if n.no_table_borders {
                            s.trim_end()
                        } else {
                            s.as_ref()
                        };

                        s.to_string()
                    })
                    .collect::<Vec<_>>()
            }
        }
    }
}

impl Display for Language {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Language::Sql => Display::fmt("SQL", f),
            Language::InfluxQL => Display::fmt("InfluxQL", f),
        }
    }
}

pub async fn run(
    cluster: &mut MiniCluster,
    input_path: PathBuf,
    setup_name: String,
    contents: String,
    language: Language,
) -> Result<()> {
    // create output and expected output
    let output_path = make_output_path(&input_path)?;
    let expected_path = {
        let mut p = input_path.clone();
        let ext = p
            .extension()
            .expect("input path missing extension")
            .to_str()
            .expect("input path extension is not valid UTF-8");
        p.set_extension(format!("{ext}.expected"));
        p
    };

    println!("Running case in {input_path:?}");
    println!("  writing output to {output_path:?}");
    println!("  expected output in {expected_path:?}");
    println!("Processing contents:\n{contents}");

    let queries = TestQueries::from_lines(contents.lines(), language);

    let mut output = vec![];
    output.push(format!("-- Test Setup: {setup_name}"));

    for q in queries.iter() {
        output.push(format!("-- {}: {}", language, q.text()));
        q.add_description(&mut output);
        let results = run_query(cluster, q).await?;
        output.extend(results);
    }

    fs::write(&output_path, output.join("\n")).context(WritingToOutputFileSnafu {
        output_path: &output_path,
    })?;

    // Now, compare to expected results
    let expected_data = fs::read_to_string(&expected_path).context(ReadingExpectedFileSnafu {
        path: &expected_path,
    })?;
    let expected_contents: Vec<_> = expected_data.lines().map(|s| s.to_string()).collect();

    if expected_contents != output {
        let expected_path = make_absolute(&expected_path);
        let output_path = make_absolute(&output_path);

        if std::env::var("CI")
            .map(|value| value == "true")
            .unwrap_or(false)
        {
            // In CI, print out the contents because it's inconvenient to access the files and
            // you're not going to update the files there.
            println!("Expected output does not match actual output");
            println!(
                "Diff: \n\n{}",
                String::from_utf8(
                    std::process::Command::new("diff")
                        .arg("-du")
                        .arg(&expected_path)
                        .arg(&output_path)
                        .output()
                        .unwrap()
                        .stdout
                )
                .unwrap()
            );
        } else {
            // When you're not in CI, print out instructions for analyzing the content or updating
            // the snapshot.
            println!("Expected output does not match actual output");
            println!("  expected output in {expected_path:?}");
            println!("  actual output in {output_path:?}");
            println!("Possibly helpful commands:");
            println!("  # See diff");
            println!("  diff -du {expected_path:?} {output_path:?}");
            println!("  # Update expected");
            println!("  cp -f {output_path:?} {expected_path:?}");
        }

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

    #[snafu(display("Input path missing file extension: '{:?}'", path))]
    MissingFileExt { path: PathBuf },

    #[snafu(display("Input path has no parent?!: '{:?}'", path))]
    NoParent { path: PathBuf },
}

/// Return output path for input path.
///
/// This converts `some/prefix/in/foo.sql` (or other file extensions) to `some/prefix/out/foo.sql.out`.
fn make_output_path(input: &Path) -> Result<PathBuf, OutputPathError> {
    let stem = input.file_stem().context(NoFileStemSnafu { path: input })?;
    let ext = input
        .extension()
        .context(MissingFileExtSnafu { path: input })?;

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
    out.set_extension(format!(
        "{}.out",
        ext.to_str().expect("extension is not valid UTF-8")
    ));

    Ok(out)
}

/// Return the absolute path to `path`, regardless of if it exists on the local filesystem
fn make_absolute(path: &Path) -> PathBuf {
    let mut absolute = std::env::current_dir().expect("cannot get current working directory");
    absolute.extend(path);
    absolute
}

async fn run_query(cluster: &MiniCluster, query: &Query) -> Result<Vec<String>> {
    let (query_text, language) = (query.text(), query.language());
    let result = match language {
        Language::Sql => {
            try_run_sql(
                query_text,
                cluster.namespace(),
                cluster.querier().querier_grpc_connection(),
                None,
                true,
            )
            .await
        }
        Language::InfluxQL => {
            try_run_influxql(
                query_text,
                cluster.namespace(),
                cluster.querier().querier_grpc_connection(),
                None,
            )
            .await
        }
    };

    let batches = match result {
        Ok((mut batches, schema)) => {
            batches.push(RecordBatch::new_empty(schema));
            batches
        }
        Err(influxdb_iox_client::flight::Error::ArrowFlightError(FlightError::Tonic(status)))
            if status.code() == Code::InvalidArgument =>
        {
            return Ok(status.message().lines().map(str::to_string).collect())
        }
        Err(e) => panic!("error running query '{query_text}': {e}"),
    };

    Ok(query.normalize_results(batches, language))
}
