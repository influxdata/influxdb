use super::{DirsAndFileName, ObjectStorePath, PathPart};

use std::{
    fmt, mem,
    path::{is_separator, PathBuf},
};

/// An object storage location suitable for passing to disk based object
/// storage.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilePath {
    inner: FilePathRepresentation,
}

impl ObjectStorePath for FilePath {
    fn set_file_name(&mut self, part: impl Into<String>) {
        self.inner = mem::take(&mut self.inner).set_file_name(part);
    }

    fn push_dir(&mut self, part: impl Into<String>) {
        self.inner = mem::take(&mut self.inner).push_dir(part);
    }

    fn push_all_dirs<'a>(&mut self, parts: impl AsRef<[&'a str]>) {
        self.inner = mem::take(&mut self.inner).push_all_dirs(parts);
    }

    fn to_raw(&self) -> String {
        self.to_path_buf().display().to_string()
    }
}

impl Ord for FilePath {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.inner.cmp(&other.inner)
    }
}

impl PartialOrd for FilePath {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl FilePath {
    /// Creates a file storage location from a `PathBuf` without parsing or
    /// allocating unless other methods are called on this instance that
    /// need it.
    ///
    /// The "nature" of path (i.e. if it is a directory or file) will be guessed. So paths ending with a
    /// separator (e.g. `/foo/bar/` on Linux) are treated as a directory. However for all other paths (like `/foo/bar`
    /// on Linux) it is not clear if a directory or file is meant w/o inspecting the underlying store. To workaround
    /// that there is the `assume_directory` flag which will treat ambiguous paths as directories. If set to `false`,
    /// these cases will be treated as files.
    pub fn raw(path: impl Into<PathBuf>, assume_directory: bool) -> Self {
        let path = path.into();
        Self {
            inner: FilePathRepresentation::Raw(path, assume_directory),
        }
    }

    /// Creates a filesystem `PathBuf` location by using the standard library's
    /// `PathBuf` building implementation appropriate for the current
    /// platform.
    pub fn to_path_buf(&self) -> PathBuf {
        use FilePathRepresentation::*;

        match &self.inner {
            Raw(path, _) => path.to_owned(),
            Parsed(dirs_and_file_name) => {
                let mut path: PathBuf = dirs_and_file_name
                    .directories
                    .iter()
                    .map(PathPart::encoded)
                    .collect();
                if let Some(file_name) = &dirs_and_file_name.file_name {
                    path.push(file_name.encoded());
                }
                path
            }
        }
    }

    /// Add the parts of `path` to the end of this path. Notably does
    /// *not* behave as `PathBuf::push` does: there is no way to replace the
    /// root. If `self` has a file name, that will be removed, then the
    /// directories of `path` will be appended, then any file name of `path`
    /// will be assigned to `self`.
    pub fn push_path(&mut self, path: &Self) {
        self.inner = mem::take(&mut self.inner).push_path(path)
    }

    /// Add a `PathPart` to the end of the path's directories.
    pub fn push_part_as_dir(&mut self, part: &PathPart) {
        self.inner = mem::take(&mut self.inner).push_part_as_dir(part);
    }

    /// Whether the prefix is the start of this path or not.
    pub fn prefix_matches(&self, prefix: &Self) -> bool {
        self.inner.prefix_matches(&prefix.inner)
    }

    /// Returns all directory and file name `PathParts` in `self` after the
    /// specified `prefix`. Ignores any `file_name` part of `prefix`.
    /// Returns `None` if `self` dosen't start with `prefix`.
    pub fn parts_after_prefix(&self, prefix: &Self) -> Option<Vec<PathPart>> {
        self.inner.parts_after_prefix(&prefix.inner)
    }

    /// Remove this path's file name, if there is one.
    pub fn unset_file_name(&mut self) {
        self.inner = mem::take(&mut self.inner).unset_file_name();
    }
}

impl From<FilePath> for DirsAndFileName {
    fn from(file_path: FilePath) -> Self {
        file_path.inner.into()
    }
}

impl From<DirsAndFileName> for FilePath {
    fn from(dirs_and_file_name: DirsAndFileName) -> Self {
        Self {
            inner: FilePathRepresentation::Parsed(dirs_and_file_name),
        }
    }
}

impl fmt::Display for FilePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.to_raw())
    }
}

#[derive(Debug, Clone, Eq)]
enum FilePathRepresentation {
    // raw: native path representation and also remember if we always assume it is a directory
    Raw(PathBuf, bool),
    Parsed(DirsAndFileName),
}

impl Default for FilePathRepresentation {
    fn default() -> Self {
        Self::Parsed(DirsAndFileName::default())
    }
}

impl PartialEq for FilePathRepresentation {
    fn eq(&self, other: &Self) -> bool {
        use FilePathRepresentation::*;
        match (self, other) {
            (Parsed(self_parts), Parsed(other_parts)) => self_parts == other_parts,
            (Parsed(self_parts), _) => {
                let other_parts: DirsAndFileName = other.to_owned().into();
                *self_parts == other_parts
            }
            (_, Parsed(other_parts)) => {
                let self_parts: DirsAndFileName = self.to_owned().into();
                self_parts == *other_parts
            }
            _ => {
                let self_parts: DirsAndFileName = self.to_owned().into();
                let other_parts: DirsAndFileName = other.to_owned().into();
                self_parts == other_parts
            }
        }
    }
}

impl PartialOrd for FilePathRepresentation {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FilePathRepresentation {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use FilePathRepresentation::*;
        match (self, other) {
            (Parsed(self_parts), Parsed(other_parts)) => self_parts.cmp(other_parts),
            (Parsed(self_parts), _) => {
                let other_parts: DirsAndFileName = other.to_owned().into();
                self_parts.cmp(&other_parts)
            }
            (_, Parsed(other_parts)) => {
                let self_parts: DirsAndFileName = self.to_owned().into();
                self_parts.cmp(other_parts)
            }
            _ => {
                let self_parts: DirsAndFileName = self.to_owned().into();
                let other_parts: DirsAndFileName = other.to_owned().into();
                self_parts.cmp(&other_parts)
            }
        }
    }
}

impl FilePathRepresentation {
    fn push_dir(self, part: impl Into<String>) -> Self {
        let mut dirs_and_file_name: DirsAndFileName = self.into();

        dirs_and_file_name.push_dir(part);
        Self::Parsed(dirs_and_file_name)
    }

    fn push_all_dirs<'a>(self, parts: impl AsRef<[&'a str]>) -> Self {
        let mut dirs_and_file_name: DirsAndFileName = self.into();

        dirs_and_file_name.push_all_dirs(parts);
        Self::Parsed(dirs_and_file_name)
    }

    fn set_file_name(self, part: impl Into<String>) -> Self {
        let mut dirs_and_file_name: DirsAndFileName = self.into();

        dirs_and_file_name.set_file_name(part);
        Self::Parsed(dirs_and_file_name)
    }

    fn unset_file_name(self) -> Self {
        let mut dirs_and_file_name: DirsAndFileName = self.into();

        dirs_and_file_name.unset_file_name();
        Self::Parsed(dirs_and_file_name)
    }

    /// Add the parts of `path` to the end of this path. Notably does
    /// *not* behave as `PathBuf::push` does: there is no way to replace the
    /// root. If `self` has a file name, that will be removed, then the
    /// directories of `path` will be appended, then any file name of `path`
    /// will be assigned to `self`.
    fn push_path(self, path: &FilePath) -> Self {
        let DirsAndFileName {
            directories: path_dirs,
            file_name: path_file_name,
        } = path.inner.to_owned().into();
        let mut dirs_and_file_name: DirsAndFileName = self.into();

        dirs_and_file_name.directories.extend(path_dirs);
        dirs_and_file_name.file_name = path_file_name;

        Self::Parsed(dirs_and_file_name)
    }

    /// Add a `PathPart` to the end of the path's directories.
    fn push_part_as_dir(self, part: &PathPart) -> Self {
        let mut dirs_and_file_name: DirsAndFileName = self.into();

        dirs_and_file_name.push_part_as_dir(part);

        Self::Parsed(dirs_and_file_name)
    }

    fn prefix_matches(&self, prefix: &Self) -> bool {
        use FilePathRepresentation::*;
        match (self, prefix) {
            (Parsed(self_parts), Parsed(prefix_parts)) => self_parts.prefix_matches(prefix_parts),
            (Parsed(self_parts), _) => {
                let prefix_parts: DirsAndFileName = prefix.to_owned().into();
                self_parts.prefix_matches(&prefix_parts)
            }
            (_, Parsed(prefix_parts)) => {
                let self_parts: DirsAndFileName = self.to_owned().into();
                self_parts.prefix_matches(prefix_parts)
            }
            _ => {
                let self_parts: DirsAndFileName = self.to_owned().into();
                let prefix_parts: DirsAndFileName = prefix.to_owned().into();
                self_parts.prefix_matches(&prefix_parts)
            }
        }
    }

    /// Returns all directory and file name `PathParts` in `self` after the
    /// specified `prefix`. Ignores any `file_name` part of `prefix`.
    /// Returns `None` if `self` dosen't start with `prefix`.
    fn parts_after_prefix(&self, prefix: &Self) -> Option<Vec<PathPart>> {
        use FilePathRepresentation::*;
        match (self, prefix) {
            (Parsed(self_parts), Parsed(prefix_parts)) => {
                self_parts.parts_after_prefix(prefix_parts)
            }
            (Parsed(self_parts), _) => {
                let prefix_parts: DirsAndFileName = prefix.to_owned().into();
                self_parts.parts_after_prefix(&prefix_parts)
            }
            (_, Parsed(prefix_parts)) => {
                let self_parts: DirsAndFileName = self.to_owned().into();
                self_parts.parts_after_prefix(prefix_parts)
            }
            _ => {
                let self_parts: DirsAndFileName = self.to_owned().into();
                let prefix_parts: DirsAndFileName = prefix.to_owned().into();
                self_parts.parts_after_prefix(&prefix_parts)
            }
        }
    }
}

impl From<FilePathRepresentation> for DirsAndFileName {
    fn from(file_path_rep: FilePathRepresentation) -> Self {
        use FilePathRepresentation::*;

        match file_path_rep {
            Raw(path, assume_directory) => {
                let mut parts: Vec<PathPart> = path
                    .iter()
                    .flat_map(|s| s.to_os_string().into_string().map(PathPart))
                    .collect();

                if !assume_directory && !parts.is_empty() && !is_a_directory(&path) {
                    let file_name = Some(parts.pop().expect("cannot be empty"));
                    Self {
                        directories: parts,
                        file_name,
                    }
                } else {
                    Self {
                        directories: parts,
                        file_name: None,
                    }
                }
            }
            Parsed(dirs_and_file_name) => dirs_and_file_name,
        }
    }
}

/// Checks if the path is for sure an directory (i.e. ends with a separator).
fn is_a_directory(path: &std::path::Path) -> bool {
    if let Some(s) = path.to_str() {
        if let Some(c) = s.chars().last() {
            return is_separator(c);
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use crate::parsed_path;

    use super::*;

    #[test]
    fn path_buf_to_dirs_and_file_name_conversion() {
        // Last section ending in `.json` is a file name
        let path_buf: PathBuf = "/one/two/blah.json".into();
        let file_path = FilePath::raw(path_buf, false);
        let parts: DirsAndFileName = file_path.into();
        let mut expected_parts = parsed_path!(["/", "one", "two"], "blah.json");
        expected_parts.directories[0] = PathPart("/".to_string()); // not escaped
        assert_eq!(parts, expected_parts);

        // Last section ending in `.segment` is a file name
        let path_buf: PathBuf = "/one/two/blah.segment".into();
        let file_path = FilePath::raw(path_buf, false);
        let parts: DirsAndFileName = file_path.into();
        let mut expected_parts = parsed_path!(["/", "one", "two"], "blah.segment");
        expected_parts.directories[0] = PathPart("/".to_string()); // not escaped
        assert_eq!(parts, expected_parts);

        // Last section ending in `.parquet` is a file name
        let path_buf: PathBuf = "/one/two/blah.parquet".into();
        let file_path = FilePath::raw(path_buf, false);
        let parts: DirsAndFileName = file_path.into();
        let mut expected_parts = parsed_path!(["/", "one", "two"], "blah.parquet");
        expected_parts.directories[0] = PathPart("/".to_string()); // not escaped
        assert_eq!(parts, expected_parts);

        // Last section ending in `.txt` is NOT a file name; we don't recognize that
        // extension
        let path_buf: PathBuf = "/one/two/blah.txt".into();
        let file_path = FilePath::raw(path_buf, false);
        let parts: DirsAndFileName = file_path.into();
        let mut expected_parts = parsed_path!(["/", "one", "two"], "blah.txt");
        expected_parts.directories[0] = PathPart("/".to_string()); // not escaped
        assert_eq!(parts, expected_parts);

        // Last section containing a `.` isn't a file name
        let path_buf: PathBuf = "/one/two/blah.blah".into();
        let file_path = FilePath::raw(path_buf, false);
        let parts: DirsAndFileName = file_path.into();
        let mut expected_parts = parsed_path!(["/", "one", "two"], "blah.blah");
        expected_parts.directories[0] = PathPart("/".to_string()); // not escaped
        assert_eq!(parts, expected_parts);

        // Last section starting with a `.` isn't a file name (macos temp dirs do this)
        let path_buf: PathBuf = "/one/two/.blah".into();
        let file_path = FilePath::raw(path_buf, false);
        let parts: DirsAndFileName = file_path.into();
        let mut expected_parts = parsed_path!(["/", "one", "two"], ".blah");
        expected_parts.directories[0] = PathPart("/".to_string()); // not escaped
        assert_eq!(parts, expected_parts);
    }

    #[test]
    fn conversions() {
        // dir and file name
        let path_buf: PathBuf = "foo/bar/blah.json".into();
        let file_path = FilePath::raw(path_buf, false);
        let parts: DirsAndFileName = file_path.into();

        let expected_parts = parsed_path!(["foo", "bar"], "blah.json");
        assert_eq!(parts, expected_parts);

        // dir, no file name
        let path_buf: PathBuf = "foo/bar/".into();
        let file_path = FilePath::raw(path_buf, false);
        let parts: DirsAndFileName = file_path.into();

        let expected_parts = parsed_path!(["foo", "bar"]);
        assert_eq!(parts, expected_parts);

        // same but w/o the final marker
        let path_buf: PathBuf = "foo/bar".into();
        let file_path = FilePath::raw(path_buf, false);
        let parts: DirsAndFileName = file_path.into();

        let expected_parts = parsed_path!(["foo"], "bar");
        assert_eq!(parts, expected_parts);

        // same but w/o the final marker, but forced to be a directory
        let path_buf: PathBuf = "foo/bar".into();
        let file_path = FilePath::raw(path_buf, true);
        let parts: DirsAndFileName = file_path.into();

        let expected_parts = parsed_path!(["foo", "bar"]);
        assert_eq!(parts, expected_parts);

        // no dir, file name
        let path_buf: PathBuf = "blah.json".into();
        let file_path = FilePath::raw(path_buf, false);
        let parts: DirsAndFileName = file_path.into();

        let expected_parts = parsed_path!([], "blah.json");
        assert_eq!(parts, expected_parts);

        // empty
        let path_buf: PathBuf = "".into();
        let file_path = FilePath::raw(path_buf, false);
        let parts: DirsAndFileName = file_path.into();

        let expected_parts = parsed_path!();
        assert_eq!(parts, expected_parts);

        // weird file name
        let path_buf: PathBuf = "blah.x".into();
        let file_path = FilePath::raw(path_buf, false);
        let parts: DirsAndFileName = file_path.into();

        let expected_parts = parsed_path!("blah.x");
        assert_eq!(parts, expected_parts);
    }

    #[test]
    fn equality() {
        let path_buf: PathBuf = "foo/bar/blah.json".into();
        let file_path = FilePath::raw(path_buf, false);
        let parts: DirsAndFileName = file_path.clone().into();
        let parsed: FilePath = parts.into();

        assert_eq!(file_path, parsed);
    }

    #[test]
    fn ordering() {
        let a_path_buf: PathBuf = "foo/bar/a.json".into();
        let a_file_path = FilePath::raw(&a_path_buf, false);
        let a_parts: DirsAndFileName = a_file_path.into();
        let a_parsed: FilePath = a_parts.into();

        let b_path_buf: PathBuf = "foo/bar/b.json".into();
        let b_file_path = FilePath::raw(&b_path_buf, false);

        assert!(a_path_buf < b_path_buf);
        assert!(
            a_parsed < b_file_path,
            "a was not less than b: a = {:#?}\nb = {:#?}",
            a_parsed,
            b_file_path
        );
    }

    #[test]
    fn path_display() {
        let a_path_buf: PathBuf = "foo/bar/a.json".into();
        let expected_display = a_path_buf.display().to_string();
        let a_file_path = FilePath::raw(&a_path_buf, false);

        assert_eq!(a_file_path.to_string(), expected_display);

        let a_parts: DirsAndFileName = a_file_path.into();
        let a_parsed: FilePath = a_parts.into();

        assert_eq!(a_parsed.to_string(), expected_display);
    }

    #[test]
    fn filepath_display() {
        let a_path_buf: PathBuf = "/foo/bar/a.json".into();
        let a_file_path = FilePath::raw(&a_path_buf, false);

        assert_eq!(format!("{}", a_file_path), "/foo/bar/a.json");
    }
}
