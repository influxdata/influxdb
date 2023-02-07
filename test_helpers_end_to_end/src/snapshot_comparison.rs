mod normalization;
mod queries;

use crate::snapshot_comparison::queries::TestQueries;
use crate::{run_influxql, run_sql, MiniCluster};
use snafu::{OptionExt, ResultExt, Snafu};
use std::fmt::{Display, Formatter};
use std::{
    fs,
    path::{Path, PathBuf},
};

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

#[derive(Debug, Clone, Copy, Default)]
pub enum Language {
    #[default]
    Sql,
    InfluxQL,
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

    let queries = TestQueries::from_lines(contents.lines());

    let mut output = vec![];
    output.push(format!("-- Test Setup: {setup_name}"));

    for q in queries.iter() {
        output.push(format!("-- {}: {}", language, q.text()));
        q.add_description(&mut output);
        let results = run_query(cluster, q, language).await?;
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

async fn run_query(
    cluster: &MiniCluster,
    query: &Query,
    language: Language,
) -> Result<Vec<String>> {
    let query_text = query.text();

    let results = match language {
        Language::Sql => {
            run_sql(
                query_text,
                cluster.namespace(),
                cluster.querier().querier_grpc_connection(),
            )
            .await
        }
        Language::InfluxQL => {
            run_influxql(
                query_text,
                cluster.namespace(),
                cluster.querier().querier_grpc_connection(),
            )
            .await
        }
    };

    Ok(query.normalize_results(results))
}
