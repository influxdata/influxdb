use std::borrow::Cow;

use crate::{DatabaseName, DatabaseNameError};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum OrgBucketMappingError {
    #[snafu(display("Invalid database name: {}", source))]
    InvalidDatabaseName { source: DatabaseNameError },

    #[snafu(display("missing org/bucket value"))]
    NotSpecified,
}

/// Map an InfluxDB 2.X org & bucket into an IOx DatabaseName.
///
/// This function ensures the mapping is unambiguous by requiring both `org` and
/// `bucket` to not contain the `_` character in addition to the
/// [`DatabaseName`] validation.
pub fn org_and_bucket_to_database<'a, O: AsRef<str>, B: AsRef<str>>(
    org: O,
    bucket: B,
) -> Result<DatabaseName<'a>, OrgBucketMappingError> {
    const SEPARATOR: char = '_';

    let org: Cow<'_, str> = utf8_percent_encode(org.as_ref(), NON_ALPHANUMERIC).into();
    let bucket: Cow<'_, str> = utf8_percent_encode(bucket.as_ref(), NON_ALPHANUMERIC).into();

    // An empty org or bucket is not acceptable.
    if org.is_empty() || bucket.is_empty() {
        return Err(OrgBucketMappingError::NotSpecified);
    }

    let db_name = format!("{}{}{}", org.as_ref(), SEPARATOR, bucket.as_ref());

    DatabaseName::new(db_name).context(InvalidDatabaseNameSnafu)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_org_bucket_map_db_ok() {
        let got = org_and_bucket_to_database("org", "bucket").expect("failed on valid DB mapping");

        assert_eq!(got.as_str(), "org_bucket");
    }

    #[test]
    fn test_org_bucket_map_db_contains_underscore() {
        let got = org_and_bucket_to_database("my_org", "bucket").unwrap();
        assert_eq!(got.as_str(), "my%5Forg_bucket");

        let got = org_and_bucket_to_database("org", "my_bucket").unwrap();
        assert_eq!(got.as_str(), "org_my%5Fbucket");

        let got = org_and_bucket_to_database("org", "my__bucket").unwrap();
        assert_eq!(got.as_str(), "org_my%5F%5Fbucket");

        let got = org_and_bucket_to_database("my_org", "my_bucket").unwrap();
        assert_eq!(got.as_str(), "my%5Forg_my%5Fbucket");
    }

    #[test]
    fn test_org_bucket_map_db_contains_underscore_and_percent() {
        let got = org_and_bucket_to_database("my%5Forg", "bucket").unwrap();
        assert_eq!(got.as_str(), "my%255Forg_bucket");

        let got = org_and_bucket_to_database("my%5Forg_", "bucket").unwrap();
        assert_eq!(got.as_str(), "my%255Forg%5F_bucket");
    }

    #[test]
    fn test_bad_database_name_is_encoded() {
        let got = org_and_bucket_to_database("org", "bucket?").unwrap();
        assert_eq!(got.as_str(), "org_bucket%3F");

        let got = org_and_bucket_to_database("org!", "bucket").unwrap();
        assert_eq!(got.as_str(), "org%21_bucket");
    }

    #[test]
    fn test_empty_org_bucket() {
        let err = org_and_bucket_to_database("", "")
            .expect_err("should fail with empty org/bucket valuese");
        assert!(matches!(err, OrgBucketMappingError::NotSpecified));
    }
}
