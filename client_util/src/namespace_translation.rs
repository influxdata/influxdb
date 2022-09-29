//! Contains logic to map namespace back/forth to org/bucket

use thiserror::Error;

/// Errors returned by namespace parsing
#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid namespace '{namespace}': {reason}")]
    InvalidNamespace { namespace: String, reason: String },
}

impl Error {
    fn new(namespace: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidNamespace {
            namespace: namespace.into(),
            reason: reason.into(),
        }
    }
}

/// Splits up the namespace name into org_id and bucket_id
pub fn split_namespace(namespace: &str) -> Result<(&str, &str), Error> {
    let mut iter = namespace.split('_');
    let org_id = iter.next().ok_or_else(|| Error::new(namespace, "empty"))?;

    if org_id.is_empty() {
        return Err(Error::new(namespace, "No org_id found"));
    }

    let bucket_id = iter
        .next()
        .ok_or_else(|| Error::new(namespace, "Could not find '_'"))?;

    if bucket_id.is_empty() {
        return Err(Error::new(namespace, "No bucket_id found"));
    }

    if iter.next().is_some() {
        return Err(Error::new(namespace, "More than one '_'"));
    }

    Ok((org_id, bucket_id))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn split_good() {
        assert_eq!(split_namespace("foo_bar").unwrap(), ("foo", "bar"));
    }

    #[test]
    #[should_panic(expected = "No org_id found")]
    fn split_bad_empty() {
        split_namespace("").unwrap();
    }

    #[test]
    #[should_panic(expected = "No org_id found")]
    fn split_bad_only_underscore() {
        split_namespace("_").unwrap();
    }

    #[test]
    #[should_panic(expected = "No org_id found")]
    fn split_bad_empty_org_id() {
        split_namespace("_ff").unwrap();
    }

    #[test]
    #[should_panic(expected = "No bucket_id found")]
    fn split_bad_empty_bucket_id() {
        split_namespace("ff_").unwrap();
    }

    #[test]
    #[should_panic(expected = "More than one '_'")]
    fn split_too_many() {
        split_namespace("ff_bf_").unwrap();
    }

    #[test]
    #[should_panic(expected = "More than one '_'")]
    fn split_way_too_many() {
        split_namespace("ff_bf_dfd_3_f").unwrap();
    }
}
