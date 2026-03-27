use std::{fmt::Display, ops::RangeInclusive, sync::Arc};

use thiserror::Error;

/// Length constraints for a [`DatabaseName`] name.
///
/// A `RangeInclusive` is a closed interval, covering [1, 511]
///
/// This length constraint comes from the combination of:
///
///   - The database name (255 bytes)
///   - A forward slash '/' (1 byte)
///   - Retention policy name (255 byte)
///
/// That is, full names are in the format of `{DATABASE_NAME}/{RETENTION_POLICY_NAME}`.
/// Providing us with a total of 511 bytes, this also means that we maintain
/// compatability with InfluxDB v1.
const LENGTH_CONSTRAINT: RangeInclusive<usize> = 1..=511;

/// New type to ensure that length constraints are upheld in error messages.
///
/// This avoids returning a, potentially, very large string which was an invalid
/// database name.
#[derive(Debug)]
pub struct TruncatedString(Arc<str>);

impl TruncatedString {
    pub fn new(s: impl Into<Arc<str>>) -> Self {
        let s: Arc<str> = s.into();
        Self(s[0..s.as_ref().floor_char_boundary(*LENGTH_CONSTRAINT.end())].into())
    }

    pub fn inner(&self) -> &str {
        self.0.as_ref()
    }
}

impl Display for TruncatedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner())
    }
}

/// Allowlist of chars for a [`DatabaseName`].
fn is_allowed(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '/' | '_' | '-')
}

/// [`DatabaseName`] name validation errors.
#[derive(Debug, Error)]
pub enum DatabaseNameError {
    /// The provided database name does not fall within the valid length of a
    /// database.
    #[error(
        "database name {} length must be between {} and {} characters",
        name,
        LENGTH_CONSTRAINT.start(),
        LENGTH_CONSTRAINT.end()
    )]
    LengthConstraint {
        /// The user-provided database that failed validation.
        name: TruncatedString,
    },

    /// The provided database name contains an unacceptable character.
    #[error(
        "database name '{}' contains invalid character, character number {} \
        is not allowed",
        name,
        bad_char_offset
    )]
    BadChars {
        /// The zero-indexed (multi-byte) character position that failed
        /// validation.
        bad_char_offset: usize,
        /// The user-provided database that failed validation.
        name: TruncatedString,
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
/// # use influxdb3_types::DatabaseName;
/// fn print_database(s: &str) {
///     println!("database name: {}", s);
/// }
///
/// let ns = DatabaseName::new("data").unwrap();
/// print_database(&ns);
/// ```
///
/// But this is not reciprocal - functions that wish to accept only
/// pre-validated names can use `DatabaseName` as a parameter.
///
/// # Context
///
/// This type is modified from InfluxDB IOx's NamespaceName equivalent
/// type with some alterations made for Monolith, such as a different
/// length constraint.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DatabaseName(Arc<str>);

impl DatabaseName {
    /// Create a new, valid DatabaseName.
    pub fn new<T: Into<Arc<str>>>(name: T) -> Result<Self, DatabaseNameError> {
        let name = name.into();

        if !LENGTH_CONSTRAINT.contains(&name.len()) {
            return Err(DatabaseNameError::LengthConstraint {
                name: TruncatedString::new(name),
            });
        }

        // Validate the name contains only valid characters.
        //
        // NOTE: If changing these characters, please update the error message
        // above.
        if let Some(bad_char_offset) = name.chars().position(|c| !is_allowed(c)) {
            return Err(DatabaseNameError::BadChars {
                bad_char_offset,
                name: TruncatedString::new(name),
            });
        };

        Ok(Self(name))
    }

    /// Borrow a string slice of the name.
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl From<DatabaseName> for String {
    fn from(name: DatabaseName) -> Self {
        name.0.to_string()
    }
}

impl std::convert::From<&DatabaseName> for String {
    fn from(name: &DatabaseName) -> Self {
        name.0.to_string()
    }
}

impl<'a> std::convert::TryFrom<&'a str> for DatabaseName {
    type Error = DatabaseNameError;

    fn try_from(v: &'a str) -> Result<Self, Self::Error> {
        Self::new(v)
    }
}

impl<'a> std::convert::TryFrom<&'a String> for DatabaseName {
    type Error = DatabaseNameError;

    fn try_from(v: &'a String) -> Result<Self, Self::Error> {
        Self::new(v.as_str())
    }
}

impl std::convert::TryFrom<String> for DatabaseName {
    type Error = DatabaseNameError;

    fn try_from(v: String) -> Result<Self, Self::Error> {
        Self::new(v)
    }
}

impl std::ops::Deref for DatabaseName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<[u8]> for DatabaseName {
    fn as_ref(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}

impl std::fmt::Display for DatabaseName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests;
