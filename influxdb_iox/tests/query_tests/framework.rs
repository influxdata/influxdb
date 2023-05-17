//! The common test code that drives the tests in the [`cases`][super::cases] module.

use observability_deps::tracing::*;
use snafu::{OptionExt, Snafu};
use std::{
    fmt::{Debug, Display},
    fs,
    path::PathBuf,
};
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest};

/// The kind of chunks the test should set up by default. Choosing `All` will run the test twice,
/// once with all chunks in the ingester and once with all chunks in Parquet. Choosing `Ingester`
/// will set up an ingester that effectively never persists, so if you want to persist some of the
/// chunks, you will need to use `Step::Persist` explicitly. Choosing `Parquet` will set up an
/// ingester that persists everything as fast as possible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChunkStage {
    /// Set up all chunks in the ingester, with all persistence-related settings high enough to
    /// make persistence effectively never happen.
    Ingester,

    /// Set up all chunks persisted in Parquet, as fast as possible.
    Parquet,

    /// Run tests against all of the previous states in this enum.
    All,
}

impl Display for ChunkStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ingester => write!(f, "Ingester (do not persist, except on demand)"),
            Self::Parquet => write!(f, "Parquet (persist as fast as possible)"),
            Self::All => write!(f, "All (run with every other variant in this enum)"),
        }
    }
}

impl IntoIterator for ChunkStage {
    type Item = Self;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            // If `All` is specified, run the test twice, once with all chunks in the ingester and
            // then once with all chunks in Parquet.
            Self::All => vec![Self::Ingester, Self::Parquet].into_iter(),
            other => vec![other].into_iter(),
        }
    }
}

/// Struct to orchestrate the test setup and assertions based on the `.sql` file specified in the
/// `input` field and the chunk stages specified in `chunk_stage`.
#[derive(Debug)]
pub struct TestCase {
    pub input: &'static str,
    pub chunk_stage: ChunkStage,
}

impl TestCase {
    pub async fn run(&self) {
        let database_url = maybe_skip_integration!();

        for chunk_stage in self.chunk_stage {
            info!("Using ChunkStage::{chunk_stage}");

            // Setup that differs by chunk stage.
            let mut cluster = match chunk_stage {
                ChunkStage::Ingester => {
                    MiniCluster::create_shared_never_persist(database_url.clone()).await
                }
                ChunkStage::Parquet => MiniCluster::create_shared(database_url.clone()).await,
                ChunkStage::All => unreachable!("See `impl IntoIterator for ChunkStage`"),
            };

            let given_input_path: PathBuf = self.input.into();
            let mut input_path = PathBuf::from("tests/query_tests/");
            input_path.push(given_input_path.clone());
            let contents = fs::read_to_string(&input_path).unwrap_or_else(|_| {
                panic!("Could not read test case file `{}`", input_path.display())
            });

            let setup =
                TestSetup::try_from_lines(contents.lines()).expect("Could not get TestSetup");
            let setup_name = setup.setup_name();
            info!("Using setup {setup_name}");

            // Run the setup steps and the QueryAndCompare step
            let setup_steps = super::setups::SETUPS
                .get(setup_name)
                .unwrap_or_else(|| panic!("Could not find setup with key `{setup_name}`"));

            // Check that the specified chunk_stage is compatible with the setup steps. If the test
            // setup is persisting on-demand, `ChunkStage::Parquet` will sometimes persist too
            // fast, so don't do that.
            if chunk_stage == ChunkStage::Parquet
                && setup_steps.iter().any(|step| matches!(step, Step::Persist))
            {
                panic!(
                    "This test is using `ChunkStage::Parquet` which persists as fast as possible, \
                    but the setup steps are persisting writes on-demand with `Step::Persist`. This \
                    may cause flaky failures, so this `TestCase` should have `chunk_stage: \
                    ChunkStage::Ingester` instead!"
                );
            }

            let test_step = match given_input_path.extension() {
                Some(ext) if ext == "sql" => Step::QueryAndCompare {
                    input_path,
                    setup_name: setup_name.into(),
                    contents,
                },
                Some(ext) if ext == "influxql" => Step::InfluxQLQueryAndCompare {
                    input_path,
                    setup_name: setup_name.into(),
                    contents,
                },
                _ => panic!(
                    "invalid language extension for path {}: expected sql or influxql",
                    self.input
                ),
            };

            // Run the tests
            StepTest::new(
                &mut cluster,
                setup_steps.iter().chain(std::iter::once(&test_step)),
            )
            .run()
            .await;
        }
    }
}

/// The magic value to look for in the `.sql` files to determine which setup steps to run based on
/// the setup name that appears in the file after this string.
const IOX_SETUP_NEEDLE: &str = "-- IOX_SETUP: ";

/// Encapsulates the setup needed for a test
///
/// Currently supports the following commands
///
/// # Run the specified setup:
/// # -- IOX_SETUP: SetupName
#[derive(Debug, PartialEq, Eq)]
pub struct TestSetup {
    setup_name: String,
}

#[derive(Debug, Snafu)]
pub enum TestSetupError {
    #[snafu(display(
        "No setup found. Looking for lines that start with '{}'",
        IOX_SETUP_NEEDLE
    ))]
    SetupNotFoundInFile {},

    #[snafu(display(
        "Only one setup is supported. Previously saw setup '{}' and now saw '{}'",
        setup_name,
        new_setup_name
    ))]
    SecondSetupFound {
        setup_name: String,
        new_setup_name: String,
    },
}

impl TestSetup {
    /// return the name of the setup that has been parsed
    pub fn setup_name(&self) -> &str {
        &self.setup_name
    }

    /// Create a new TestSetup object from the lines
    pub fn try_from_lines<I, S>(lines: I) -> Result<Self, TestSetupError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut parser = lines.into_iter().filter_map(|line| {
            let line = line.as_ref().trim();
            line.strip_prefix(IOX_SETUP_NEEDLE)
                .map(|setup_name| setup_name.trim().to_string())
        });

        let setup_name = parser.next().context(SetupNotFoundInFileSnafu)?;

        if let Some(new_setup_name) = parser.next() {
            return SecondSetupFoundSnafu {
                setup_name,
                new_setup_name,
            }
            .fail();
        }

        Ok(Self { setup_name })
    }
}

/// Who tests the test framework? This module does!
#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_lines() {
        let lines = vec!["Foo", "bar", "-- IOX_SETUP: MySetup", "goo"];
        let setup = TestSetup::try_from_lines(lines).unwrap();
        assert_eq!(
            setup,
            TestSetup {
                setup_name: "MySetup".into()
            }
        );
    }

    #[test]
    fn test_parse_lines_extra_whitespace() {
        let lines = vec!["Foo", "  -- IOX_SETUP:   MySetup   "];
        let setup = TestSetup::try_from_lines(lines).unwrap();
        assert_eq!(
            setup,
            TestSetup {
                setup_name: "MySetup".into()
            }
        );
    }

    #[test]
    fn test_parse_lines_setup_name_with_whitespace() {
        let lines = vec!["Foo", "  -- IOX_SETUP:   My Awesome  Setup   "];
        let setup = TestSetup::try_from_lines(lines).unwrap();
        assert_eq!(
            setup,
            TestSetup {
                setup_name: "My Awesome  Setup".into()
            }
        );
    }

    #[test]
    fn test_parse_lines_none() {
        let lines = vec!["Foo", " MySetup   "];
        let setup = TestSetup::try_from_lines(lines).unwrap_err().to_string();
        assert_eq!(
            setup,
            "No setup found. Looking for lines that start with '-- IOX_SETUP: '"
        );
    }

    #[test]
    fn test_parse_lines_multi() {
        let lines = vec!["Foo", "-- IOX_SETUP:   MySetup", "-- IOX_SETUP:   MySetup2"];
        let setup = TestSetup::try_from_lines(lines).unwrap_err().to_string();
        assert_eq!(
            setup,
            "Only one setup is supported. Previously saw setup 'MySetup' and now saw 'MySetup2'"
        );
    }
}
