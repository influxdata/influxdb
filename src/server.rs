pub mod http_routes;
pub mod rpc;

use data_types::{DatabaseName, DatabaseNameError};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum OrgBucketMappingError {
    #[snafu(display(
        "Internal error accessing org {}, bucket {}: the '_' character is reserved",
        org,
        bucket_name,
    ))]
    InvalidBucketOrgName { org: String, bucket_name: String },

    #[snafu(display("Invalid database name: {}", source))]
    InvalidDatabaseName { source: DatabaseNameError },
}

/// Map an InfluxDB 2.X org & bucket into an IOx DatabaseName.
///
/// This function ensures the mapping is unambiguous by requiring both `org` and
/// `bucket` to not contain the `_` character in addition to the
/// [`DatabaseName`] validation.
pub(crate) fn org_and_bucket_to_database<'a, O: AsRef<str>, B: AsRef<str>>(
    org: O,
    bucket: B,
) -> Result<DatabaseName<'a>, OrgBucketMappingError> {
    const SEPARATOR: char = '_';

    // Ensure neither the org, nor the bucket contain the separator character.
    if org.as_ref().chars().any(|c| c == SEPARATOR)
        || bucket.as_ref().chars().any(|c| c == SEPARATOR)
    {
        return InvalidBucketOrgName {
            bucket_name: bucket.as_ref(),
            org: org.as_ref(),
        }
        .fail();
    }

    let db_name = format!("{}{}{}", org.as_ref(), SEPARATOR, bucket.as_ref());

    DatabaseName::new(db_name).context(InvalidDatabaseName)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_org_bucket_map_db_ok() {
        let got = org_and_bucket_to_database("org", "bucket").expect("failed on valid DB mapping");

        assert_eq!(&*got, "org_bucket");
    }

    #[test]
    fn test_org_bucket_map_db_contains_underscore() {
        let err = org_and_bucket_to_database("my_org", "bucket").unwrap_err();
        assert!(matches!(err, OrgBucketMappingError::InvalidBucketOrgName {..}));

        let err = org_and_bucket_to_database("org", "my_bucket").unwrap_err();
        assert!(matches!(err, OrgBucketMappingError::InvalidBucketOrgName {..}));
    }

    #[test]
    fn test_bad_database_name() {
        let err = org_and_bucket_to_database("org!", "bucket?").unwrap_err();
        assert!(matches!(err, OrgBucketMappingError::InvalidDatabaseName {..}));
    }
}
