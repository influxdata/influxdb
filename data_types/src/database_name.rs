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
        "Database name '{}' contains invalid character. Character number {} is a control which is not allowed.", name, bad_char_offset
    ))]
    BadChars {
        bad_char_offset: usize,
        name: String,
    },
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
        if let Some(bad_char_offset) = name.chars().position(|c| c.is_control()) {
            return BadChars {
                bad_char_offset,
                name,
            }
            .fail();
        };

        Ok(Self(name))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl<'a> std::convert::From<DatabaseName<'a>> for String {
    fn from(name: DatabaseName<'a>) -> Self {
        name.0.to_string()
    }
}

impl<'a> std::convert::From<&DatabaseName<'a>> for String {
    fn from(name: &DatabaseName<'a>) -> Self {
        name.to_string()
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
        self.as_str()
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
    use test_helpers::assert_contains;

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
    fn test_bad_chars_null() {
        let got = DatabaseName::new("example\x00").unwrap_err();
        assert_contains!(got.to_string() , "Database name 'example\x00' contains invalid character. Character number 7 is a control which is not allowed.");
    }

    #[test]
    fn test_bad_chars_high_control() {
        let got = DatabaseName::new("\u{007f}example").unwrap_err();
        assert_contains!(got.to_string() , "Database name '\u{007f}example' contains invalid character. Character number 0 is a control which is not allowed.");
    }

    #[test]
    fn test_bad_chars_tab() {
        let got = DatabaseName::new("example\tdb").unwrap_err();
        assert_contains!(got.to_string() , "Database name 'example\tdb' contains invalid character. Character number 7 is a control which is not allowed.");
    }

    #[test]
    fn test_bad_chars_newline() {
        let got = DatabaseName::new("my_example\ndb").unwrap_err();
        assert_contains!(got.to_string() , "Database name 'my_example\ndb' contains invalid character. Character number 10 is a control which is not allowed.");
    }

    #[test]
    fn test_ok_chars() {
        let db = DatabaseName::new("my-example-db_with_underscores and spaces").unwrap();
        assert_eq!(&*db, "my-example-db_with_underscores and spaces");
    }
}
