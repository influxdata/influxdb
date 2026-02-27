//! A mechanism to hint what size the underlying object has.
//!
//! # Usage
//!
//! ```
//! # use object_store_size_hinting::{hint_size, extract_size_hint};
//! let options = hint_size(13);
//! let (_options, maybe_size) = extract_size_hint(options);
//! assert_eq!(maybe_size.unwrap(), 13);
//! ```
//!
//! # Implementation
//! We pass the size hint as a [GET extension](GetOptions::extensions).
use http::Extensions;
use object_store::GetOptions;

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

/// Object that encodes/holds the size hint.
#[derive(Debug, Clone)]
struct SizeHint(u64);

/// Embed a size hint within [`GetOptions`].
///
/// Use [`extract_size_hint`] to extract the size again.
pub fn hint_size(size: u64) -> GetOptions {
    let mut extensions = Extensions::default();
    extensions.insert(SizeHint(size));

    GetOptions {
        extensions,
        ..Default::default()
    }
}

/// Extract size hint from [`GetOptions`].
///
/// Returns the options with the potential size information stripped and the extracted size -- if there was a hint.
///
/// Use [`hint_size`] to create a size hint.
pub fn extract_size_hint(mut options: GetOptions) -> (GetOptions, Option<u64>) {
    let hint = options.extensions.remove::<SizeHint>().map(|hint| hint.0);
    (options, hint)
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use object_store_mock::WrappedGetOptions;

    use super::*;

    #[test]
    fn test_roundtrip() {
        assert_roundtrip(0);
        assert_roundtrip(1);
        assert_roundtrip(1337);
        assert_roundtrip(u64::MAX);
    }

    #[test]
    fn test_extract_wipes_hint() {
        let options = hint_size(42);

        let (options, hint) = extract_size_hint(options);
        assert_eq!(hint, Some(42));

        let (options, hint) = extract_size_hint(options);
        assert_eq!(hint, None);

        let (_options, hint) = extract_size_hint(options);
        assert_eq!(hint, None);
    }

    #[test]
    fn test_keep_fields() {
        let mut options = options_all_fields_set();
        options.extensions.insert(SizeHint(13));

        assert_extract(options, Some(13));
    }

    #[track_caller]
    fn assert_roundtrip(size: u64) {
        let options = hint_size(size);
        assert_extract(options, Some(size));
    }

    #[track_caller]
    fn assert_extract(options: GetOptions, maybe_size: Option<u64>) {
        let (returned_options, extracted_size) = extract_size_hint(options.clone());

        let mut expected_options = options.clone();
        expected_options.extensions.remove::<SizeHint>();

        assert_eq!(
            WrappedGetOptions::from(returned_options),
            WrappedGetOptions::from(expected_options)
        );
        assert_eq!(extracted_size, maybe_size);
    }

    fn options_all_fields_set() -> GetOptions {
        let mut extensions = Extensions::new();
        extensions.insert(TestExt);

        GetOptions {
            if_match: Some("val-if-match".to_owned()),
            if_none_match: Some("val-if-none-match".to_owned()),
            if_modified_since: Some(Utc.timestamp_nanos(42)),
            if_unmodified_since: Some(Utc.timestamp_nanos(43)),
            range: Some((0..13).into()),
            version: Some("val-version".to_owned()),
            head: true,
            extensions,
        }
    }

    #[derive(Debug, Clone)]
    struct TestExt;
}
