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
use snafu::{OptionExt, Snafu};
use sqlx::types::Uuid;
use std::collections::HashMap;
use std::{
    fmt::{Display, Formatter},
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
    input_file_path: PathBuf,
    setup_name: String,
    contents: String,
    language: Language,
) -> Result<()> {
    // create output and expected output
    let test_name = input_file_path
        .file_name()
        .expect("input path missing file path")
        .to_str()
        .expect("input path file path is not valid UTF-8");

    let output_path = input_file_path.parent().context(NoParentSnafu {
        path: &input_file_path,
    })?;
    let output_path = make_absolute(output_path);

    println!("Running case in {input_file_path:?}");
    println!("Producing output in {output_path:?}");
    println!("Processing contents:\n{contents}");

    let queries = TestQueries::from_lines(contents.lines(), language);

    //Build up the test output line by line
    let mut output = vec![];
    output.push(format!("-- Test Setup: {setup_name}"));

    for q in queries.iter() {
        q.add_comments(&mut output);
        output.push(format!("-- {}: {}", language, q.text()));
        q.add_description(&mut output);
        let results = run_query(cluster, q).await?;
        output.extend(results);
    }

    // Configure insta to send the results to query_tests/out/<test_name>.sql.snap
    let mut settings = insta::Settings::clone_current();
    settings.set_snapshot_path(output_path);
    settings.set_prepend_module_to_snapshot(false);
    settings.bind(|| {
        let test_output = output.join("\n");
        insta::assert_snapshot!(test_name, test_output); // panic on failure
    });

    Ok(())
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
