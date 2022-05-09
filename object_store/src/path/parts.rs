use percent_encoding::{percent_decode_str, percent_encode, AsciiSet, CONTROLS};
use std::borrow::Cow;

use super::DELIMITER;

// percent_encode's API needs this as a byte
const DELIMITER_BYTE: u8 = DELIMITER.as_bytes()[0];

/// The PathPart type exists to validate the directory/file names that form part
/// of a path.
///
/// A PathPart instance is guaranteed to be non-empty and to contain no `/`
/// characters as it can only be constructed by going through the `from` impl.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct PathPart<'a> {
    pub(super) raw: Cow<'a, str>,
}

/// Characters we want to encode.
const INVALID: &AsciiSet = &CONTROLS
    // The delimiter we are reserving for internal hierarchy
    .add(DELIMITER_BYTE)
    // Characters AWS recommends avoiding for object keys
    // https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
    .add(b'\\')
    .add(b'{')
    // TODO: Non-printable ASCII characters (128–255 decimal characters)
    .add(b'^')
    .add(b'}')
    .add(b'%')
    .add(b'`')
    .add(b']')
    .add(b'"') // " <-- my editor is confused about double quotes within single quotes
    .add(b'>')
    .add(b'[')
    .add(b'~')
    .add(b'<')
    .add(b'#')
    .add(b'|')
    // Characters Google Cloud Storage recommends avoiding for object names
    // https://cloud.google.com/storage/docs/naming-objects
    .add(b'\r')
    .add(b'\n')
    .add(b'*')
    .add(b'?');

impl<'a> From<&'a str> for PathPart<'a> {
    fn from(v: &'a str) -> Self {
        let inner = match v {
            // We don't want to encode `.` generally, but we do want to disallow parts of paths
            // to be equal to `.` or `..` to prevent file system traversal shenanigans.
            "." => "%2E".into(),
            ".." => "%2E%2E".into(),
            other => percent_encode(other.as_bytes(), INVALID).into(),
        };
        Self { raw: inner }
    }
}

impl From<String> for PathPart<'static> {
    fn from(s: String) -> Self {
        Self {
            raw: Cow::Owned(PathPart::from(s.as_ref()).raw.into_owned()),
        }
    }
}

impl<'a> std::fmt::Display for PathPart<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        percent_decode_str(self.raw.as_ref())
            .decode_utf8()
            .expect("Valid UTF-8 that came from String")
            .fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_part_delimiter_gets_encoded() {
        let part: PathPart<'_> = "foo/bar".into();
        assert_eq!(part.raw, "foo%2Fbar");
        assert_eq!(part.to_string(), "foo/bar")
    }

    #[test]
    fn path_part_given_already_encoded_string() {
        let part: PathPart<'_> = "foo%2Fbar".into();
        assert_eq!(part.raw, "foo%252Fbar");
        assert_eq!(part.to_string(), "foo%2Fbar");
    }

    #[test]
    fn path_part_cant_be_one_dot() {
        let part: PathPart<'_> = ".".into();
        assert_eq!(part.raw, "%2E");
        assert_eq!(part.to_string(), ".");
    }

    #[test]
    fn path_part_cant_be_two_dots() {
        let part: PathPart<'_> = "..".into();
        assert_eq!(part.raw, "%2E%2E");
        assert_eq!(part.to_string(), "..");
    }
}
