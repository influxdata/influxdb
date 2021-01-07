use snafu::Snafu;
use std::{borrow::Cow, ops::RangeInclusive};

/// Length constraints for a database name.
///
/// A `RangeInclusive` is a closed interval, covering [1, 64]
const LENGTH_CONSTRAINT: RangeInclusive<usize> = 1..=64;

/// Database name validation errors.
#[derive(Debug, Snafu)]
pub enum DatabaseNameError {
    #[snafu(display(
        "Database name {} length must be between {} and {} characters",
        name,
        LENGTH_CONSTRAINT.start(),
        LENGTH_CONSTRAINT.end()
    ))]
    LengthConstraint { name: String },

    #[snafu(display(
        "Database name {} contains invalid characters (allowed: alphanumeric, _ and -)",
        name
    ))]
    BadChars { name: String },
}

/// A correctly formed database name.
///
/// Using this wrapper type allows the consuming code to enforce the invariant
/// that only valid names are provided.
///
/// This type derefs to a `str` and therefore can be used in place of anything
/// that is expecting a `str`:
///
/// ```rust
/// # use data_types::DatabaseName;
/// fn print_database(s: &str) {
///     println!("database name: {}", s);
/// }
///
/// let db = DatabaseName::new("data").unwrap();
/// print_database(&db);
/// ```
///
/// But this is not reciprocal - functions that wish to accept only
/// pre-validated names can use `DatabaseName` as a parameter.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct DatabaseName<'a>(Cow<'a, str>);

impl<'a> DatabaseName<'a> {
    pub fn new<T: Into<Cow<'a, str>>>(name: T) -> Result<Self, DatabaseNameError> {
        let name: Cow<'a, str> = name.into();

        if !LENGTH_CONSTRAINT.contains(&name.len()) {
            return Err(DatabaseNameError::LengthConstraint {
                name: name.to_string(),
            });
        }

        // Validate the name contains only valid characters.
        //
        // NOTE: If changing these characters, please update the error message
        // above.
        if !name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return Err(DatabaseNameError::BadChars {
                name: name.to_string(),
            });
        }

        Ok(Self(name))
    }
}

impl<'a> std::convert::From<DatabaseName<'a>> for String {
    fn from(name: DatabaseName<'a>) -> Self {
        name.0.to_string()
    }
}

impl<'a> std::convert::TryFrom<&'a str> for DatabaseName<'a> {
    type Error = DatabaseNameError;

    fn try_from(v: &'a str) -> Result<Self, Self::Error> {
        Self::new(v)
    }
}

impl<'a> std::convert::TryFrom<String> for DatabaseName<'a> {
    type Error = DatabaseNameError;

    fn try_from(v: String) -> Result<Self, Self::Error> {
        Self::new(v)
    }
}

impl<'a> std::ops::Deref for DatabaseName<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<'a> std::fmt::Display for DatabaseName<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;

    #[test]
    fn test_deref() {
        let db = DatabaseName::new("my_example_name").unwrap();
        assert_eq!(&*db, "my_example_name");
    }

    #[test]
    fn test_too_short() {
        let name = "".to_string();
        let got = DatabaseName::try_from(name).unwrap_err();

        assert!(matches!(
            got,
            DatabaseNameError::LengthConstraint { name: _n }
        ));
    }

    #[test]
    fn test_too_long() {
        let name = "my_example_name_that_is_quite_a_bit_longer_than_allowed_even_though_database_names_can_be_quite_long_bananas".to_string();
        let got = DatabaseName::try_from(name).unwrap_err();

        assert!(matches!(
            got,
            DatabaseNameError::LengthConstraint { name: _n }
        ));
    }

    #[test]
    fn test_bad_chars() {
        let got = DatabaseName::new("example!").unwrap_err();
        assert!(matches!(got, DatabaseNameError::BadChars { name: _n }));
    }
}
