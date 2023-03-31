use std::{borrow::Cow, ops::RangeInclusive};

use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use snafu::{ResultExt, Snafu};

/// Length constraints for a [`NamespaceName`] name.
///
/// A `RangeInclusive` is a closed interval, covering [1, 64]
const LENGTH_CONSTRAINT: RangeInclusive<usize> = 1..=64;

/// Errors returned when attempting to construct a [`NamespaceName`] from an org
/// & bucket string pair.
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum OrgBucketMappingError {
    #[snafu(display("Invalid namespace name: {}", source))]
    InvalidNamespaceName { source: NamespaceNameError },

    #[snafu(display("missing org/bucket value"))]
    NotSpecified,
}

/// Map an InfluxDB 2.X org & bucket into an IOx NamespaceName.
///
/// This function ensures the mapping is unambiguous by requiring both `org` and
/// `bucket` to not contain the `_` character in addition to the
/// [`NamespaceName`] validation.
pub fn org_and_bucket_to_namespace<'a, O: AsRef<str>, B: AsRef<str>>(
    org: O,
    bucket: B,
) -> Result<NamespaceName<'a>, OrgBucketMappingError> {
    const SEPARATOR: char = '_';

    let org: Cow<'_, str> = utf8_percent_encode(org.as_ref(), NON_ALPHANUMERIC).into();
    let bucket: Cow<'_, str> = utf8_percent_encode(bucket.as_ref(), NON_ALPHANUMERIC).into();

    // An empty org or bucket is not acceptable.
    if org.is_empty() || bucket.is_empty() {
        return Err(OrgBucketMappingError::NotSpecified);
    }

    let db_name = format!("{}{}{}", org.as_ref(), SEPARATOR, bucket.as_ref());

    NamespaceName::new(db_name).context(InvalidNamespaceNameSnafu)
}

/// [`NamespaceName`] name validation errors.
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum NamespaceNameError {
    #[snafu(display(
        "Namespace name {} length must be between {} and {} characters",
        name,
        LENGTH_CONSTRAINT.start(),
        LENGTH_CONSTRAINT.end()
    ))]
    LengthConstraint { name: String },

    #[snafu(display(
        "Namespace name '{}' contains invalid character. \
        Character number {} is a control which is not allowed.",
        name,
        bad_char_offset
    ))]
    BadChars {
        bad_char_offset: usize,
        name: String,
    },
}

/// A correctly formed Namespace name.
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
        if let Some(bad_char_offset) = name.chars().position(|c| c.is_control()) {
            return BadCharsSnafu {
                bad_char_offset,
                name,
            }
            .fail();
        };

        Ok(Self(name))
    }

    /// Borrow a string slice of the name.
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
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
        let got = org_and_bucket_to_namespace("org", "bucket").expect("failed on valid DB mapping");

        assert_eq!(got.as_str(), "org_bucket");
    }

    #[test]
    fn test_org_bucket_map_db_contains_underscore() {
        let got = org_and_bucket_to_namespace("my_org", "bucket").unwrap();
        assert_eq!(got.as_str(), "my%5Forg_bucket");

        let got = org_and_bucket_to_namespace("org", "my_bucket").unwrap();
        assert_eq!(got.as_str(), "org_my%5Fbucket");

        let got = org_and_bucket_to_namespace("org", "my__bucket").unwrap();
        assert_eq!(got.as_str(), "org_my%5F%5Fbucket");

        let got = org_and_bucket_to_namespace("my_org", "my_bucket").unwrap();
        assert_eq!(got.as_str(), "my%5Forg_my%5Fbucket");
    }

    #[test]
    fn test_org_bucket_map_db_contains_underscore_and_percent() {
        let got = org_and_bucket_to_namespace("my%5Forg", "bucket").unwrap();
        assert_eq!(got.as_str(), "my%255Forg_bucket");

        let got = org_and_bucket_to_namespace("my%5Forg_", "bucket").unwrap();
        assert_eq!(got.as_str(), "my%255Forg%5F_bucket");
    }

    #[test]
    fn test_bad_namespace_name_is_encoded() {
        let got = org_and_bucket_to_namespace("org", "bucket?").unwrap();
        assert_eq!(got.as_str(), "org_bucket%3F");

        let got = org_and_bucket_to_namespace("org!", "bucket").unwrap();
        assert_eq!(got.as_str(), "org%21_bucket");
    }

    #[test]
    fn test_empty_org_bucket() {
        let err = org_and_bucket_to_namespace("", "")
            .expect_err("should fail with empty org/bucket valuese");
        assert!(matches!(err, OrgBucketMappingError::NotSpecified));
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
        assert_eq!(got.to_string() , "Namespace name 'example\x00' contains invalid character. Character number 7 is a control which is not allowed.");
    }

    #[test]
    fn test_bad_chars_high_control() {
        let got = NamespaceName::new("\u{007f}example").unwrap_err();
        assert_eq!(got.to_string() , "Namespace name '\u{007f}example' contains invalid character. Character number 0 is a control which is not allowed.");
    }

    #[test]
    fn test_bad_chars_tab() {
        let got = NamespaceName::new("example\tdb").unwrap_err();
        assert_eq!(got.to_string() , "Namespace name 'example\tdb' contains invalid character. Character number 7 is a control which is not allowed.");
    }

    #[test]
    fn test_bad_chars_newline() {
        let got = NamespaceName::new("my_example\ndb").unwrap_err();
        assert_eq!(got.to_string() , "Namespace name 'my_example\ndb' contains invalid character. Character number 10 is a control which is not allowed.");
    }

    #[test]
    fn test_ok_chars() {
        let db = NamespaceName::new("my-example-db_with_underscores and spaces").unwrap();
        assert_eq!(&*db, "my-example-db_with_underscores and spaces");
    }
}
