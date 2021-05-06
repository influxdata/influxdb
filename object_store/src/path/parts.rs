use percent_encoding::{percent_decode_str, percent_encode, AsciiSet, CONTROLS};

use super::DELIMITER;

// percent_encode's API needs this as a byte
const DELIMITER_BYTE: u8 = DELIMITER.as_bytes()[0];

// special encoding of the empty string part.
// Using '%' is the safest character since it will always be used in the
// output of percent_encode no matter how we evolve the INVALID AsciiSet over
// time.
const EMPTY: &str = "%";

/// The PathPart type exists to validate the directory/file names that form part
/// of a path.
///
/// A PathPart instance is guaranteed to be non-empty and to contain no `/`
/// characters as it can only be constructed by going through the `from` impl.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct PathPart(pub(super) String);

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

impl From<&str> for PathPart {
    fn from(v: &str) -> Self {
        match v {
            // We don't want to encode `.` generally, but we do want to disallow parts of paths
            // to be equal to `.` or `..` to prevent file system traversal shenanigans.
            "." => Self(String::from("%2E")),
            ".." => Self(String::from("%2E%2E")),

            // Every string except the empty string will be percent encoded.
            // The empty string will be transformed into a sentinel value EMPTY
            // which can safely be a prefix of an encoded value since it will be
            // fully matched at decode time (see impl Display for PathPart).
            "" => Self(String::from(EMPTY)),
            other => Self(percent_encode(other.as_bytes(), INVALID).to_string()),
        }
    }
}

impl std::fmt::Display for PathPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0[..] {
            EMPTY => "".fmt(f),
            _ => percent_decode_str(&self.0)
                .decode_utf8()
                .expect("Valid UTF-8 that came from String")
                .fmt(f),
        }
    }
}

impl PathPart {
    /// Encode as string.
    pub fn encoded(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_part_delimiter_gets_encoded() {
        let part: PathPart = "foo/bar".into();
        assert_eq!(part, PathPart(String::from("foo%2Fbar")));
    }

    #[test]
    fn path_part_gets_decoded_for_display() {
        let part: PathPart = "foo/bar".into();
        assert_eq!(part.to_string(), "foo/bar");
    }

    #[test]
    fn path_part_given_already_encoded_string() {
        let part: PathPart = "foo%2Fbar".into();
        assert_eq!(part, PathPart(String::from("foo%252Fbar")));
        assert_eq!(part.to_string(), "foo%2Fbar");
    }

    #[test]
    fn path_part_cant_be_one_dot() {
        let part: PathPart = ".".into();
        assert_eq!(part, PathPart(String::from("%2E")));
        assert_eq!(part.to_string(), ".");
    }

    #[test]
    fn path_part_cant_be_two_dots() {
        let part: PathPart = "..".into();
        assert_eq!(part, PathPart(String::from("%2E%2E")));
        assert_eq!(part.to_string(), "..");
    }

    #[test]
    fn path_part_cant_be_empty() {
        let part: PathPart = "".into();
        assert_eq!(part, PathPart(String::from(EMPTY)));
        assert_eq!(part.to_string(), "");
    }

    #[test]
    fn empty_is_safely_encoded() {
        let part: PathPart = EMPTY.into();
        assert_eq!(
            part,
            PathPart(percent_encode(EMPTY.as_bytes(), INVALID).to_string())
        );
        assert_eq!(part.to_string(), EMPTY);
    }
}
