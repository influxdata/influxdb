#![cfg(test)]

//! Tests of various queries for data in various states.

use observability_deps::tracing::*;
use snafu::{OptionExt, Snafu};
use std::{fmt::Debug, fs, path::PathBuf};
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest};

mod cases;
mod setups;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChunkStage {
    /// In ingester.
    Ingester,

    /// In a Parquet file, persisted by the ingester. Now managed by the querier.
    Parquet,

    /// Run tests against all of the previous states in this enum.
    All,
}

impl IntoIterator for ChunkStage {
    type Item = ChunkStage;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            ChunkStage::All => vec![ChunkStage::Ingester, ChunkStage::Parquet].into_iter(),
            other => vec![other].into_iter(),
        }
    }
}

#[derive(Debug)]
struct TestCase {
    input: &'static str,
    chunk_stage: ChunkStage,
}

impl TestCase {
    async fn run(&self) {
        let database_url = maybe_skip_integration!();

        for chunk_stage in self.chunk_stage {
            info!("Using ChunkStage::{chunk_stage:?}");

            // Setup that differs by chunk stage. These need to be non-shared clusters; if they're
            // shared, then the tests that run in parallel and persist at particular times mess
            // with each other because persistence applies to everything in the ingester.
            let mut cluster = match chunk_stage {
                ChunkStage::Ingester => {
                    MiniCluster::create_non_shared2_never_persist(database_url.clone()).await
                }
                ChunkStage::Parquet => MiniCluster::create_non_shared2(database_url.clone()).await,
                ChunkStage::All => unreachable!("See `impl IntoIterator for ChunkStage`"),
            };

            // TEMPORARY: look in `query_tests` for all case files; change this if we decide to
            // move them
            let given_input_path: PathBuf = self.input.into();
            let mut input_path = PathBuf::from("../query_tests/");
            input_path.push(given_input_path);
            let contents = fs::read_to_string(&input_path).unwrap_or_else(|_| {
                panic!("Could not read test case file `{}`", input_path.display())
            });

            let setup =
                TestSetup::try_from_lines(contents.lines()).expect("Could not get TestSetup");
            let setup_name = setup.setup_name();
            info!("Using setup {setup_name}");

            // Run the setup steps and the QueryAndCompare step
            let setup_steps = crate::setups::SETUPS
                .get(setup_name)
                .unwrap_or_else(|| panic!("Could not find setup with key `{setup_name}`"))
                .iter();
            let test_step = Step::QueryAndCompare {
                input_path,
                setup_name: setup_name.into(),
                contents,
            };

            // Run the tests
            StepTest::new(&mut cluster, setup_steps.chain(std::iter::once(&test_step)))
                .run()
                .await;
        }
    }
}

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
