//! Basic test runner that runs queries in files and compares the output to the expected results

use snafu::{OptionExt, Snafu};
use std::{fmt::Display, sync::Arc};

use crate::scenarios::{get_all_setups, get_db_setup, DbSetup};

const IOX_SETUP_NEEDLE: &str = "-- IOX_SETUP: ";

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "No setup found. Looking for lines that start with '{}'",
        IOX_SETUP_NEEDLE
    ))]
    SetupNotFound {},

    #[snafu(display("No setup named '{}' found. Perhaps you need to define it in scenarios.rs. Known setups: {:?}",
                    setup_name, known_setups))]
    NamedSetupNotFound {
        setup_name: String,
        known_setups: Vec<String>,
    },

    #[snafu(display(
        "Only one setup is supported. Previously say setup '{}' and now saw '{}'",
        setup_name,
        new_setup_name
    ))]
    SecondSetupFound {
        setup_name: String,
        new_setup_name: String,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Encapsulates the setup needed for a test
///
/// Currently supports the following commands
///
/// # Run the specified setup:
/// # -- IOX_SETUP: SetupName
#[derive(Debug, PartialEq)]
pub struct TestSetup {
    setup_name: String,
}

impl Display for TestSetup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "TestSetup")?;
        writeln!(f, "  Setup: {}", self.setup_name)?;

        Ok(())
    }
}

impl TestSetup {
    /// return the name of the setup that has been parsed
    pub fn setup_name(&self) -> &str {
        &self.setup_name
    }

    /// Return the instance of `DbSetup` that can get the setups needed for testing
    pub fn get_setup(&self) -> Result<Arc<dyn DbSetup>> {
        let setup_name = self.setup_name();
        get_db_setup(setup_name).with_context(|| {
            let known_setups = get_all_setups()
                .keys()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
            NamedSetupNotFound {
                setup_name,
                known_setups,
            }
        })
    }

    /// Create a new TestSetup object from the lines
    pub fn try_from_lines<I, S>(lines: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut parser = lines.into_iter().filter_map(|line| {
            let line = line.as_ref().trim();
            line.strip_prefix(IOX_SETUP_NEEDLE)
                .map(|setup_name| setup_name.trim().to_string())
        });

        let setup_name = parser.next().context(SetupNotFound)?;

        if let Some(new_setup_name) = parser.next() {
            return SecondSetupFound {
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
            "Only one setup is supported. Previously say setup 'MySetup' and now saw 'MySetup2'"
        );
    }
}
