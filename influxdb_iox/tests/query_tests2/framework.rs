//! The common test code that drives the tests in the [`cases`][super::cases] module.

use observability_deps::tracing::*;
use snafu::{OptionExt, Snafu};
use std::{fmt::Debug, fs, path::PathBuf};
use test_helpers_end_to_end::{maybe_skip_integration, MiniCluster, Step, StepTest};

/// The kind of chunks the test should set up by default. Choosing `All` will run the test twice,
/// once with all chunks in the ingester and once with all chunks in Parquet. Choosing `Ingester`
/// will set up an ingester that effectively never persists, so if you want to persist some of the
/// chunks, you will need to use `Step::Persist` explicitly. Choosing `Parquet` will set up an
/// ingester that persists everything as fast as possible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChunkStage {
    /// Set up all chunks in the ingester set up to go through the write buffer (Kafka). This is
    /// temporary until the switch to the Kafkaless architecture is complete.
    Ingester,

    /// Set up all chunks persisted in Parquet, as fast as possible, through the old ingester and
    /// write buffer (Kafka). This is temporary until the switch to the Kafkaless architecture is
    /// complete.
    Parquet,

    /// Run tests against all of the previous states in this enum.
    All,
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

/// Which architecture is being used in this test run. This enum and running the tests twice is temporary until the Kafkaful architecture is retired.
#[derive(Debug, Copy, Clone)]
pub enum IoxArchitecture {
    /// Use the "standard" MiniCluster that uses ingester, router, querier, compactor with a write
    /// buffer (aka Kafka). This is slated for retirement soon.
    Kafkaful,
    /// Use the "RPC write"/"version 2" MiniCluster that uses ingester2, router2, querier2 without
    /// a write buffer. This will soon be the only architecture.
    Kafkaless,
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

        for arch in [IoxArchitecture::Kafkaful, IoxArchitecture::Kafkaless] {
            for chunk_stage in self.chunk_stage {
                info!("Using IoxArchitecture::{arch:?} and ChunkStage::{chunk_stage:?}");

                // Setup that differs by architecture and chunk stage. In the Kafka architecture,
                // these need to be non-shared clusters; if they're shared, then the tests that run
                // in parallel and persist at particular times mess with each other because
                // persistence applies to everything in the ingester.
                let mut cluster = match (arch, chunk_stage) {
                    (IoxArchitecture::Kafkaful, ChunkStage::Ingester) => {
                        MiniCluster::create_non_shared_standard_never_persist(database_url.clone())
                            .await
                    }
                    (IoxArchitecture::Kafkaful, ChunkStage::Parquet) => {
                        MiniCluster::create_non_shared_standard(database_url.clone()).await
                    }
                    (IoxArchitecture::Kafkaless, ChunkStage::Ingester) => {
                        MiniCluster::create_shared2_never_persist(database_url.clone()).await
                    }
                    (IoxArchitecture::Kafkaless, ChunkStage::Parquet) => {
                        MiniCluster::create_shared2(database_url.clone()).await
                    }
                    (_, ChunkStage::All) => unreachable!("See `impl IntoIterator for ChunkStage`"),
                };

                let given_input_path: PathBuf = self.input.into();
                let mut input_path = PathBuf::from("tests/query_tests2/");
                input_path.push(given_input_path);
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
                    .unwrap_or_else(|| panic!("Could not find setup with key `{setup_name}`"))
                    .iter()
                    // When we've switched over to the Kafkaless architecture, this map can be
                    // removed.
                    .flat_map(|step| match (arch, step) {
                        // If we're using the old architecture and the test steps include
                        // `WaitForPersist2`, swap it with `WaitForPersist` instead.
                        (IoxArchitecture::Kafkaful, Step::WaitForPersisted2 { .. }) => {
                            vec![&Step::WaitForPersisted]
                        }
                        // If we're using the old architecture and the test steps include
                        // `WriteLineProtocol`, wait for the data to be readable after writing.
                        (IoxArchitecture::Kafkaful, Step::WriteLineProtocol { .. }) => {
                            vec![step, &Step::WaitForReadable]
                        }
                        (_, other) => vec![other],
                    });

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
