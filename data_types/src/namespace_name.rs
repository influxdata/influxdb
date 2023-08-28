use std::{borrow::Cow, ops::RangeInclusive};

use thiserror::Error;

/// Length constraints for a [`NamespaceName`] name.
///
/// A `RangeInclusive` is a closed interval, covering [1, 64]
const LENGTH_CONSTRAINT: RangeInclusive<usize> = 1..=64;

/// Allowlist of chars for a [`NamespaceName`] name.
///
/// '/' | '_' | '-' are utilized by the platforms.
fn is_allowed(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '/' | '_' | '-')
}

/// Errors returned when attempting to construct a [`NamespaceName`] from an org
/// & bucket string pair.
#[derive(Debug, Error)]
pub enum OrgBucketMappingError {
    /// An error returned when the org, or bucket string contains invalid
    /// characters.
    #[error("invalid namespace name: {0}")]
    InvalidNamespaceName(#[from] NamespaceNameError),

    /// Either the org, or bucket is an empty string.
    #[error("missing org/bucket value")]
    NoOrgBucketSpecified,
}

/// [`NamespaceName`] name validation errors.
#[derive(Debug, Error)]
pub enum NamespaceNameError {
    /// The provided namespace name does not fall within the valid length of a
    /// namespace.
    #[error(
        "namespace name {} length must be between {} and {} characters",
        name,
        LENGTH_CONSTRAINT.start(),
        LENGTH_CONSTRAINT.end()
    )]
    LengthConstraint {
        /// The user-provided namespace that failed validation.
        name: String,
    },

    /// The provided namespace name contains an unacceptable character.
    #[error(
        "namespace name '{}' contains invalid character, character number {} \
        is not whitelisted",
        name,
        bad_char_offset
    )]
    BadChars {
        /// The zero-indexed (multi-byte) character position that failed
        /// validation.
        bad_char_offset: usize,
        /// The user-provided namespace that failed validation.
        name: String,
    },
}

/// A correctly formed namespace name.
///
/// Using this wrapper type allows the consuming code to enforce the invariant
/// that only valid names are provided.
///
/// This type derefs to a `str` and therefore can be used in place of anything
/// that is expecting a `str`:
///
/// ```rust
/// # use data_types::NamespaceName;
/// fn print_namespace(s: &str) {
///     println!("namespace name: {}", s);
/// }
///
/// let ns = NamespaceName::new("data").unwrap();
/// print_namespace(&ns);
/// ```
///
/// But this is not reciprocal - functions that wish to accept only
/// pre-validated names can use `NamespaceName` as a parameter.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceName<'a>(Cow<'a, str>);

impl<'a> NamespaceName<'a> {
    /// Create a new, valid NamespaceName.
    pub fn new<T: Into<Cow<'a, str>>>(name: T) -> Result<Self, NamespaceNameError> {
        let name: Cow<'a, str> = name.into();

        if !LENGTH_CONSTRAINT.contains(&name.len()) {
            return Err(NamespaceNameError::LengthConstraint {
                name: name.to_string(),
            });
        }

        // Validate the name contains only valid characters.
        //
        // NOTE: If changing these characters, please update the error message
        // above.
        if let Some(bad_char_offset) = name.chars().position(|c| !is_allowed(c)) {
            return Err(NamespaceNameError::BadChars {
                bad_char_offset,
                name: name.to_string(),
            });
        };

        Ok(Self(name))
    }

    /// Borrow a string slice of the name.
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }

    /// Map an InfluxDB 2.X org & bucket into an IOx NamespaceName.
    ///
    /// This function ensures the mapping is unambiguous by encoding any
    /// non-alphanumeric characters in both `org` and `bucket` in addition to
    /// the validation performed in [`NamespaceName::new()`].
    pub fn from_org_and_bucket<O: AsRef<str>, B: AsRef<str>>(
        org: O,
        bucket: B,
    ) -> Result<Self, OrgBucketMappingError> {
        let org = org.as_ref();
        let bucket = bucket.as_ref();

        if org.is_empty() || bucket.is_empty() {
            return Err(OrgBucketMappingError::NoOrgBucketSpecified);
        }

        Ok(Self::new(format!("{}_{}", org, bucket))?)
    }
}

impl<'a> std::convert::From<NamespaceName<'a>> for String {
    fn from(name: NamespaceName<'a>) -> Self {
        name.0.to_string()
    }
}

impl<'a> std::convert::From<&NamespaceName<'a>> for String {
    fn from(name: &NamespaceName<'a>) -> Self {
        name.to_string()
    }
}

impl<'a> std::convert::TryFrom<&'a str> for NamespaceName<'a> {
    type Error = NamespaceNameError;

    fn try_from(v: &'a str) -> Result<Self, Self::Error> {
        Self::new(v)
    }
}

impl<'a> std::convert::TryFrom<String> for NamespaceName<'a> {
    type Error = NamespaceNameError;

    fn try_from(v: String) -> Result<Self, Self::Error> {
        Self::new(v)
    }
}

impl<'a> std::ops::Deref for NamespaceName<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl<'a> AsRef<[u8]> for NamespaceName<'a> {
    fn as_ref(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}

impl<'a> std::fmt::Display for NamespaceName<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_org_bucket_map_db_ok() {
        let got = NamespaceName::from_org_and_bucket("org", "bucket")
            .expect("failed on valid DB mapping");

        assert_eq!(got.as_str(), "org_bucket");
    }

    #[test]
    fn test_org_bucket_map_db_contains_underscore() {
        let got = NamespaceName::from_org_and_bucket("my_org", "bucket").unwrap();
        assert_eq!(got.as_str(), "my_org_bucket");

        let got = NamespaceName::from_org_and_bucket("org", "my_bucket").unwrap();
        assert_eq!(got.as_str(), "org_my_bucket");

        let got = NamespaceName::from_org_and_bucket("org", "my__bucket").unwrap();
        assert_eq!(got.as_str(), "org_my__bucket");

        let got = NamespaceName::from_org_and_bucket("my_org", "my_bucket").unwrap();
        assert_eq!(got.as_str(), "my_org_my_bucket");
    }

    #[test]
    fn test_org_bucket_map_db_contains_underscore_and_percent() {
        let err = NamespaceName::from_org_and_bucket("my%5Forg", "bucket");
        assert!(matches!(
            err,
            Err(OrgBucketMappingError::InvalidNamespaceName { .. })
        ));

        let err = NamespaceName::from_org_and_bucket("my%5Forg_", "bucket");
        assert!(matches!(
            err,
            Err(OrgBucketMappingError::InvalidNamespaceName { .. })
        ));
    }

    #[test]
    fn test_bad_namespace_name_fails_validation() {
        let err = NamespaceName::from_org_and_bucket("org", "bucket?");
        assert!(matches!(
            err,
            Err(OrgBucketMappingError::InvalidNamespaceName { .. })
        ));

        let err = NamespaceName::from_org_and_bucket("org!", "bucket");
        assert!(matches!(
            err,
            Err(OrgBucketMappingError::InvalidNamespaceName { .. })
        ));
    }

    #[test]
    fn test_empty_org_bucket() {
        let err = NamespaceName::from_org_and_bucket("", "")
            .expect_err("should fail with empty org/bucket valuese");
        assert!(matches!(err, OrgBucketMappingError::NoOrgBucketSpecified));
    }

    #[test]
    fn test_deref() {
        let db = NamespaceName::new("my_example_name").unwrap();
        assert_eq!(&*db, "my_example_name");
    }

    #[test]
    fn test_too_short() {
        let name = "".to_string();
        let got = NamespaceName::try_from(name).unwrap_err();

        assert!(matches!(
            got,
            NamespaceNameError::LengthConstraint { name: _n }
        ));
    }

    #[test]
    fn test_too_long() {
        let name = "my_example_name_that_is_quite_a_bit_longer_than_allowed_even_though_database_names_can_be_quite_long_bananas".to_string();
        let got = NamespaceName::try_from(name).unwrap_err();

        assert!(matches!(
            got,
            NamespaceNameError::LengthConstraint { name: _n }
        ));
    }

    #[test]
    fn test_bad_chars_null() {
        let got = NamespaceName::new("example\x00").unwrap_err();
        assert_eq!(got.to_string() , "namespace name 'example\x00' contains invalid character, character number 7 is not whitelisted");
    }

    #[test]
    fn test_bad_chars_high_control() {
        let got = NamespaceName::new("\u{007f}example").unwrap_err();
        assert_eq!(got.to_string() , "namespace name '\u{007f}example' contains invalid character, character number 0 is not whitelisted");
    }

    #[test]
    fn test_bad_chars_tab() {
        let got = NamespaceName::new("example\tdb").unwrap_err();
        assert_eq!(got.to_string() , "namespace name 'example\tdb' contains invalid character, character number 7 is not whitelisted");
    }

    #[test]
    fn test_bad_chars_newline() {
        let got = NamespaceName::new("my_example\ndb").unwrap_err();
        assert_eq!(got.to_string() , "namespace name 'my_example\ndb' contains invalid character, character number 10 is not whitelisted");
    }

    #[test]
    fn test_bad_chars_whitespace() {
        let got = NamespaceName::new("my_example db").unwrap_err();
        assert_eq!(got.to_string() , "namespace name 'my_example db' contains invalid character, character number 10 is not whitelisted");
    }

    #[test]
    fn test_bad_chars_single_quote() {
        let got = NamespaceName::new("my_example'db").unwrap_err();
        assert_eq!(got.to_string() , "namespace name 'my_example\'db' contains invalid character, character number 10 is not whitelisted");
    }

    #[test]
    fn test_ok_chars() {
        let db =
            NamespaceName::new("my-example-db_with_underscores/and/fwd/slash/AndCaseSensitive")
                .unwrap();
        assert_eq!(
            &*db,
            "my-example-db_with_underscores/and/fwd/slash/AndCaseSensitive"
        );

        let db = NamespaceName::new("a_ã_京").unwrap();
        assert_eq!(&*db, "a_ã_京");
    }
}
