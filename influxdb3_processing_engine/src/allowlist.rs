//! The operator's plugin package allowlist.
//!
//! The allowlist is a standard pip requirements file passed with
//! `--plugin-package-allowlist`. It names the packages users may install, the
//! versions they may install, and (through pip options such as `--index-url`)
//! where pip may fetch them from. It is used in one of two modes, chosen by its
//! contents:
//!
//! * **Ranges**: every entry is a PEP 508 dependency specifier, e.g.
//!   `numpy>=2,<3`. Only the packages users ask for are checked; their
//!   dependencies are constrained only if the allowlist lists them.
//! * **Hashed**: every entry is pinned with `==` and carries one or more
//!   `--hash=` options, e.g. the output of
//!   `uv pip compile --generate-hashes`. The file lists the whole dependency
//!   closure, and pip refuses any package without a matching hash.
//!
//! A file cannot mix modes. The server passes the file to every `pip install`
//! as `-c <file>`, so pip enforces the versions, hashes, and index options
//! itself; the checks here reject a request before pip runs and give a clear
//! error.

use crate::environment::PluginEnvironmentError;
use pep508_rs::pep440_rs::{Operator, Version, VersionSpecifiers};
use pep508_rs::{PackageName, Requirement, VerbatimUrl, VersionOrUrl};
use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// How an allowlist restricts installs. Set by the file's contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllowlistMode {
    /// Entries are version ranges; pip checks requested packages against them.
    Ranges,
    /// Entries are exact pins with hashes; pip installs only hashed packages.
    Hashed,
}

impl fmt::Display for AllowlistMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ranges => f.write_str("ranges"),
            Self::Hashed => f.write_str("hashed"),
        }
    }
}

/// One requirement line of the allowlist.
#[derive(Debug, Clone)]
struct AllowlistEntry {
    /// The allowed versions. Empty means any version.
    specifiers: VersionSpecifiers,
    /// The exact version in hashed mode.
    pinned: Option<Version>,
    /// Whether the entry has an environment marker (`; python_version …`).
    has_marker: bool,
}

/// A parsed, validated allowlist file.
#[derive(Debug, Clone)]
pub struct PackageAllowlist {
    path: PathBuf,
    mode: AllowlistMode,
    entries: BTreeMap<PackageName, Vec<AllowlistEntry>>,
}

/// A logical requirements-file line: `\` continuations joined, comments
/// removed. `line_number` is the line the logical line starts on.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct LogicalLine {
    pub(crate) line_number: usize,
    pub(crate) text: String,
}

/// Split requirements-file contents into logical lines the way pip does:
/// join lines ending in `\` (a comment line ends the continuation), then drop
/// comments (`#` at the start of a line or after whitespace) and blank lines.
pub(crate) fn logical_lines(contents: &str) -> Vec<LogicalLine> {
    let is_comment = |line: &str| line.trim_start().starts_with('#');
    let mut joined: Vec<(usize, String)> = Vec::new();
    let mut pending: Option<(usize, String)> = None;
    for (index, line) in contents.lines().enumerate() {
        let line_number = index + 1;
        if !line.ends_with('\\') || is_comment(line) {
            match pending.take() {
                Some((start, mut text)) => {
                    // A comment inside a continuation ends it; pip prefixes a
                    // space so the comment is stripped below.
                    if is_comment(line) {
                        text.push(' ');
                    }
                    text.push_str(line);
                    joined.push((start, text));
                }
                None => joined.push((line_number, line.to_string())),
            }
        } else {
            let head = line.trim_end_matches('\\');
            match &mut pending {
                Some((_, text)) => text.push_str(head),
                None => pending = Some((line_number, head.to_string())),
            }
        }
    }
    if let Some(last) = pending {
        joined.push(last);
    }
    joined
        .into_iter()
        .filter_map(|(line_number, text)| {
            let text = strip_comment(&text).trim().to_string();
            (!text.is_empty()).then_some(LogicalLine { line_number, text })
        })
        .collect()
}

/// Remove a `#` comment that starts the line or follows whitespace.
fn strip_comment(line: &str) -> &str {
    let bytes = line.as_bytes();
    for (index, &byte) in bytes.iter().enumerate() {
        if byte == b'#' && (index == 0 || bytes[index - 1].is_ascii_whitespace()) {
            return &line[..index];
        }
    }
    line
}

/// A requirement line split into its requirement text and its trailing
/// per-requirement options. pip treats the first whitespace-separated token
/// starting with `-` as the start of the options.
pub(crate) fn split_requirement_options(line: &str) -> (String, Vec<&str>) {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    let split = tokens
        .iter()
        .position(|token| token.starts_with('-'))
        .unwrap_or(tokens.len());
    (tokens[..split].join(" "), tokens[split..].to_vec())
}

/// The pip options an allowlist may set on a line of their own. Each entry is
/// `(long name, short name, takes a value)`. Anything else, including nested
/// `-r`/`-c` files and editable installs, is rejected so the allowlist is one
/// self-contained file.
const ALLOWED_GLOBAL_OPTIONS: &[(&str, Option<&str>, bool)] = &[
    ("--index-url", Some("-i"), true),
    ("--extra-index-url", None, true),
    ("--no-index", None, false),
    ("--find-links", Some("-f"), true),
    ("--trusted-host", None, true),
    ("--only-binary", None, true),
    ("--no-binary", None, true),
    ("--prefer-binary", None, false),
    ("--pre", None, false),
    ("--require-hashes", None, false),
];

/// Hash algorithms pip accepts in `--hash`.
const ALLOWED_HASH_ALGORITHMS: &[&str] = &["sha256", "sha384", "sha512"];

/// Validate an option-only line and report whether it is `--require-hashes`.
fn parse_global_options(line: &str) -> Result<bool, String> {
    let mut require_hashes = false;
    let mut tokens = line.split_whitespace();
    while let Some(token) = tokens.next() {
        let (flag, inline_value) = match token.split_once('=') {
            Some((flag, value)) if flag.starts_with("--") => (flag, Some(value)),
            _ => (token, None),
        };
        let matched = ALLOWED_GLOBAL_OPTIONS
            .iter()
            .find_map(|&(long, short, takes_value)| {
                if flag == long {
                    Some((long, takes_value, inline_value))
                } else if let Some(short) = short
                    && let Some(rest) = token.strip_prefix(short)
                {
                    // `-i URL` or `-iURL`.
                    Some((long, takes_value, (!rest.is_empty()).then_some(rest)))
                } else {
                    None
                }
            });
        let Some((long, takes_value, inline_value)) = matched else {
            return Err(format!(
                "unsupported pip option '{flag}'; allowed options are {}",
                ALLOWED_GLOBAL_OPTIONS
                    .iter()
                    .map(|(long, _, _)| *long)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        };
        if takes_value {
            let has_value = inline_value.is_some_and(|value| !value.is_empty())
                || tokens.next().is_some_and(|value| !value.starts_with('-'));
            if !has_value {
                return Err(format!("option '{long}' requires a value"));
            }
        } else if inline_value.is_some() {
            return Err(format!("option '{long}' does not take a value"));
        }
        if long == "--require-hashes" {
            require_hashes = true;
        }
    }
    Ok(require_hashes)
}

/// Validate the per-requirement options of an allowlist entry and return how
/// many hashes it carries. Only `--hash=<algorithm>:<hex digest>` is allowed.
fn parse_hash_options(options: &[&str]) -> Result<usize, String> {
    let mut hashes = 0;
    let mut tokens = options.iter();
    while let Some(&token) = tokens.next() {
        let value = if let Some(value) = token.strip_prefix("--hash=") {
            value
        } else if token == "--hash" {
            tokens
                .next()
                .copied()
                .ok_or_else(|| "option '--hash' requires a value".to_string())?
        } else {
            return Err(format!(
                "unsupported option '{token}' on a requirement; only --hash is allowed"
            ));
        };
        let valid = value.split_once(':').is_some_and(|(algorithm, digest)| {
            ALLOWED_HASH_ALGORITHMS.contains(&algorithm)
                && !digest.is_empty()
                && digest.chars().all(|c| c.is_ascii_hexdigit())
        });
        if !valid {
            return Err(format!(
                "invalid hash '{value}'; expected <algorithm>:<hex digest> with algorithm one of {}",
                ALLOWED_HASH_ALGORITHMS.join(", ")
            ));
        }
        hashes += 1;
    }
    Ok(hashes)
}

/// Parse a PEP 508 requirement that names a package by version, not by URL.
pub(crate) fn parse_named_requirement(text: &str) -> Result<Requirement, String> {
    let requirement = Requirement::<VerbatimUrl>::from_str(text)
        .map_err(|error| format!("not a valid PEP 508 dependency specifier: {error}"))?;
    if let Some(VersionOrUrl::Url(_)) = requirement.version_or_url {
        return Err("direct URL references ('name @ url') are not allowed".to_string());
    }
    Ok(requirement)
}

/// The version specifiers of a requirement; empty when it has none.
fn specifiers_of(requirement: &Requirement) -> VersionSpecifiers {
    match &requirement.version_or_url {
        Some(VersionOrUrl::VersionSpecifier(specifiers)) => specifiers.clone(),
        _ => VersionSpecifiers::empty(),
    }
}

/// The exact version of a requirement pinned with a single `==` or `===`
/// (no wildcard), if it is pinned that way.
fn exact_pin(specifiers: &VersionSpecifiers) -> Option<Version> {
    match specifiers.iter().collect::<Vec<_>>().as_slice() {
        [specifier] if matches!(specifier.operator(), Operator::Equal | Operator::ExactEqual) => {
            Some(specifier.version().clone())
        }
        _ => None,
    }
}

/// An allowlist requirement line before the mode is known.
struct ParsedEntry {
    line_number: usize,
    line: String,
    name: PackageName,
    entry: AllowlistEntry,
    hashes: usize,
}

impl PackageAllowlist {
    /// Read and validate an allowlist file. Fails on the first problem so the
    /// server refuses to start with an invalid allowlist.
    pub fn from_file(path: &Path) -> Result<Self, PluginEnvironmentError> {
        let unreadable = |source| PluginEnvironmentError::AllowlistFileUnreadable {
            path: path.display().to_string(),
            source,
        };
        // pip receives this path on every install, so resolve it now rather
        // than relative to whatever the working directory is then.
        let path = std::fs::canonicalize(path).map_err(unreadable)?;
        let contents = std::fs::read_to_string(&path).map_err(unreadable)?;
        Self::parse(&path, &contents)
    }

    /// Validate allowlist contents. `path` is used for errors and is what the
    /// server later passes to pip.
    pub fn parse(path: &Path, contents: &str) -> Result<Self, PluginEnvironmentError> {
        let path_text = path.display().to_string();
        let invalid = |line: &LogicalLine, reason: String| {
            PluginEnvironmentError::AllowlistFileInvalidEntry {
                path: path_text.clone(),
                line_number: line.line_number,
                line: line.text.clone(),
                reason,
            }
        };

        let mut require_hashes_line: Option<usize> = None;
        let mut parsed: Vec<ParsedEntry> = Vec::new();
        for line in logical_lines(contents) {
            if line.text.starts_with('-') {
                if parse_global_options(&line.text).map_err(|reason| invalid(&line, reason))? {
                    require_hashes_line.get_or_insert(line.line_number);
                }
                continue;
            }
            let (requirement_text, options) = split_requirement_options(&line.text);
            let hashes = parse_hash_options(&options).map_err(|reason| invalid(&line, reason))?;
            let requirement = parse_named_requirement(&requirement_text)
                .map_err(|reason| invalid(&line, reason))?;
            if !requirement.extras.is_empty() {
                return Err(invalid(
                    &line,
                    "extras are not allowed in the allowlist; list the package without extras \
                     and list each extra's dependencies separately"
                        .to_string(),
                ));
            }
            let specifiers = specifiers_of(&requirement);
            parsed.push(ParsedEntry {
                line_number: line.line_number,
                line: line.text.clone(),
                name: requirement.name.clone(),
                entry: AllowlistEntry {
                    pinned: exact_pin(&specifiers),
                    specifiers,
                    has_marker: !requirement.marker.is_true(),
                },
                hashes,
            });
        }

        if parsed.is_empty() {
            return Err(PluginEnvironmentError::AllowlistEmpty { path: path_text });
        }

        // The mode comes from the contents: any hash (or `--require-hashes`)
        // makes it hashed, and then every entry must be a hashed pin.
        let first_hashed = parsed
            .iter()
            .find(|entry| entry.hashes > 0)
            .map(|entry| entry.line_number)
            .or(require_hashes_line);
        let mode = match first_hashed {
            Some(hashed_line) => {
                if let Some(unhashed) = parsed.iter().find(|entry| entry.hashes == 0) {
                    return Err(PluginEnvironmentError::AllowlistMixedModes {
                        path: path_text,
                        hashed_line,
                        unhashed_line: unhashed.line_number,
                        line: unhashed.line.clone(),
                    });
                }
                if let Some(unpinned) = parsed.iter().find(|entry| entry.entry.pinned.is_none()) {
                    return Err(PluginEnvironmentError::AllowlistFileInvalidEntry {
                        path: path_text,
                        line_number: unpinned.line_number,
                        line: unpinned.line.clone(),
                        reason: "hashed entries must pin an exact version with '=='".to_string(),
                    });
                }
                AllowlistMode::Hashed
            }
            None => AllowlistMode::Ranges,
        };

        let mut entries: BTreeMap<PackageName, Vec<AllowlistEntry>> = BTreeMap::new();
        let mut first_line: BTreeMap<PackageName, usize> = BTreeMap::new();
        for parsed_entry in parsed {
            // pip applies every constraint for a name at once, so a repeated
            // name without markers to separate the entries could never be
            // satisfied.
            if let Some(&previous) = first_line.get(&parsed_entry.name) {
                let all_marked = parsed_entry.entry.has_marker
                    && entries[&parsed_entry.name].iter().all(|e| e.has_marker);
                if !all_marked {
                    return Err(PluginEnvironmentError::AllowlistFileInvalidEntry {
                        path: path_text,
                        line_number: parsed_entry.line_number,
                        line: parsed_entry.line,
                        reason: format!(
                            "'{}' is already listed on line {previous}; combine the version \
                             specifiers into one entry, or give each entry an environment marker",
                            parsed_entry.name
                        ),
                    });
                }
            }
            first_line
                .entry(parsed_entry.name.clone())
                .or_insert(parsed_entry.line_number);
            entries
                .entry(parsed_entry.name)
                .or_default()
                .push(parsed_entry.entry);
        }

        Ok(Self {
            path: path.to_path_buf(),
            mode,
            entries,
        })
    }

    /// The allowlist file, passed to pip as `-c <file>`.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The mode set by the file's contents.
    pub fn mode(&self) -> AllowlistMode {
        self.mode
    }

    /// The number of distinct packages listed.
    pub fn package_count(&self) -> usize {
        self.entries.len()
    }

    /// Describe the allowed versions of a package for error messages.
    fn describe_allowed(entries: &[AllowlistEntry]) -> String {
        entries
            .iter()
            .map(|entry| {
                if entry.specifiers.is_empty() {
                    "any version".to_string()
                } else {
                    entry.specifiers.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(" or ")
    }

    /// Check a user's requirement against the allowlist.
    ///
    /// The package must be listed. In hashed mode, at least one pinned version
    /// must satisfy the user's version specifiers. In ranges mode, an exact
    /// `==` request must fall within a listed range; other specifiers are left
    /// to pip, which applies the allowlist as constraints and fails when no
    /// version satisfies both.
    pub fn check(
        &self,
        requirement: &Requirement,
        spec: &str,
    ) -> Result<(), PluginEnvironmentError> {
        let Some(entries) = self.entries.get(&requirement.name) else {
            return Err(PluginEnvironmentError::PackageNotAllowlisted {
                name: requirement.name.to_string(),
            });
        };
        let requested = specifiers_of(requirement);
        let allowed = match self.mode {
            AllowlistMode::Hashed => entries
                .iter()
                .filter_map(|entry| entry.pinned.as_ref())
                .any(|version| requested.contains(version)),
            AllowlistMode::Ranges => match exact_pin(&requested) {
                Some(version) => entries
                    .iter()
                    .any(|entry| entry.specifiers.contains(&version)),
                None => true,
            },
        };
        if allowed {
            Ok(())
        } else {
            Err(PluginEnvironmentError::PackageVersionNotAllowlisted {
                spec: spec.trim().to_string(),
                name: requirement.name.to_string(),
                allowlisted: Self::describe_allowed(entries),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(contents: &str) -> Result<PackageAllowlist, PluginEnvironmentError> {
        PackageAllowlist::parse(Path::new("allowlist.txt"), contents)
    }

    fn check(allowlist: &PackageAllowlist, spec: &str) -> Result<(), PluginEnvironmentError> {
        let requirement = parse_named_requirement(spec).expect("valid spec");
        allowlist.check(&requirement, spec)
    }

    const RANGES: &str = "\
# allowlist.txt
--index-url https://download.pytorch.org/whl/cpu
--extra-index-url https://pypi.org/simple
--only-binary :all:

accelerate>=1.15
chronos-forecasting~=2.3
numpy>=2,<3
torch==2.6.0+cpu
transformers>=5.20
";

    const HASHED: &str = "\
# This file was autogenerated by uv via the following command:
#    uv pip compile allowlist.txt --generate-hashes -o allowlist-hashed.txt
--index-url https://download.pytorch.org/whl/cpu
--extra-index-url https://pypi.org/simple
--only-binary :all:

accelerate==1.15.0 \\
    --hash=sha256:7f2a
    # via -r allowlist.txt
numpy==2.2.6 \\
    --hash=sha256:0386 \\
    --hash=sha256:b7e1
    # via
    #   accelerate
    #   torch
torch==2.6.0+cpu \\
    --hash=sha256:24c9
";

    #[test]
    fn logical_lines_follow_pip_rules() {
        let lines = logical_lines(
            "# header\n\nnumpy==1 \\\n    --hash=sha256:aa \\\n    --hash=sha256:bb\n    # via x\n\
             six==1.0  # inline\nurl#frag\n",
        );
        assert_eq!(
            lines,
            vec![
                LogicalLine {
                    line_number: 3,
                    text: "numpy==1     --hash=sha256:aa     --hash=sha256:bb".to_string(),
                },
                LogicalLine {
                    line_number: 7,
                    text: "six==1.0".to_string(),
                },
                // `#` not preceded by whitespace is not a comment.
                LogicalLine {
                    line_number: 8,
                    text: "url#frag".to_string(),
                },
            ]
        );
    }

    #[test]
    fn logical_lines_comment_ends_continuation() {
        let lines = logical_lines("numpy==1 \\\n# comment\nsix\n");
        assert_eq!(
            lines.iter().map(|l| l.text.as_str()).collect::<Vec<_>>(),
            vec!["numpy==1", "six"]
        );
    }

    #[test]
    fn ranges_allowlist_parses() {
        let allowlist = parse(RANGES).unwrap();
        assert_eq!(allowlist.mode(), AllowlistMode::Ranges);
        assert_eq!(allowlist.package_count(), 5);
    }

    #[test]
    fn ranges_allowlist_checks_names_and_exact_pins() {
        let allowlist = parse(RANGES).unwrap();
        for ok in [
            "numpy",
            "NumPy",
            "numpy==2.2.6",
            "numpy>=2.1",
            // Not provably inside the range; pip's -c decides.
            "numpy<2",
            "chronos_forecasting==2.9",
            "torch==2.6.0+cpu",
            "transformers[torch]>=5.20",
            "numpy; python_version >= '3.10'",
        ] {
            check(&allowlist, ok).unwrap_or_else(|e| panic!("{ok}: {e}"));
        }
        let err = check(&allowlist, "scipy").unwrap_err();
        assert!(
            matches!(&err, PluginEnvironmentError::PackageNotAllowlisted { name } if name == "scipy"),
            "{err:?}"
        );
        for bad in ["numpy==1.26.4", "chronos-forecasting==3.0", "torch==2.6.0"] {
            let err = check(&allowlist, bad).unwrap_err();
            assert!(
                matches!(
                    err,
                    PluginEnvironmentError::PackageVersionNotAllowlisted { .. }
                ),
                "{bad}: {err:?}"
            );
        }
        let err = check(&allowlist, "numpy==1.26.4").unwrap_err();
        assert_eq!(
            err.to_string(),
            "'numpy==1.26.4' is not allowed. Allowlisted version(s) of 'numpy': >=2, <3. \
             Contact your administrator to request a different version."
        );
    }

    #[test]
    fn hashed_allowlist_parses_uv_output() {
        let allowlist = parse(HASHED).unwrap();
        assert_eq!(allowlist.mode(), AllowlistMode::Hashed);
        assert_eq!(allowlist.package_count(), 3);
    }

    #[test]
    fn hashed_allowlist_checks_against_pins() {
        let allowlist = parse(HASHED).unwrap();
        for ok in ["numpy", "numpy==2.2.6", "numpy>=2,<3", "torch==2.6.0+cpu"] {
            check(&allowlist, ok).unwrap_or_else(|e| panic!("{ok}: {e}"));
        }
        for bad in ["numpy==2.2.5", "numpy<2", "accelerate>=2"] {
            let err = check(&allowlist, bad).unwrap_err();
            assert!(
                matches!(
                    err,
                    PluginEnvironmentError::PackageVersionNotAllowlisted { .. }
                ),
                "{bad}: {err:?}"
            );
        }
        assert!(matches!(
            check(&allowlist, "transformers").unwrap_err(),
            PluginEnvironmentError::PackageNotAllowlisted { .. }
        ));
    }

    #[test]
    fn hashed_allowlist_with_markers_allows_each_pin() {
        // `uv pip compile --universal` splits a package by marker.
        let allowlist = parse(
            "numpy==1.26.4 ; python_version < '3.10' --hash=sha256:aa\n\
             numpy==2.2.6 ; python_version >= '3.10' --hash=sha256:bb\n",
        )
        .unwrap();
        check(&allowlist, "numpy==1.26.4").unwrap();
        check(&allowlist, "numpy==2.2.6").unwrap();
        assert!(check(&allowlist, "numpy==2.0.0").is_err());
    }

    #[test]
    fn mixed_modes_are_rejected() {
        let err = parse("numpy==2.2.6 --hash=sha256:aa\nsix==1.17.0\n").unwrap_err();
        assert!(
            matches!(
                err,
                PluginEnvironmentError::AllowlistMixedModes {
                    hashed_line: 1,
                    unhashed_line: 2,
                    ..
                }
            ),
            "{err:?}"
        );
        // `--require-hashes` makes the file hashed too.
        let err = parse("--require-hashes\nnumpy>=2\n").unwrap_err();
        assert!(
            matches!(err, PluginEnvironmentError::AllowlistMixedModes { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn hashed_entries_must_be_exact_pins() {
        for bad in [
            "numpy>=2 --hash=sha256:aa\n",
            "numpy==2.* --hash=sha256:aa\n",
            "numpy --hash=sha256:aa\n",
        ] {
            let err = parse(bad).unwrap_err();
            assert!(
                matches!(
                    err,
                    PluginEnvironmentError::AllowlistFileInvalidEntry { .. }
                ),
                "{bad}: {err:?}"
            );
        }
    }

    #[test]
    fn invalid_entries_are_rejected() {
        for bad in [
            "-r other.txt",
            "-c constraints.txt",
            "-e .",
            "--use-feature fast-deps",
            "--index-url",
            "--pre=yes",
            "numpy>=2 --config-settings x=y",
            "numpy==2 --hash=md5:aa",
            "numpy==2 --hash=sha256:zz",
            "numpy==2 --hash",
            "numpy >= ",
            "https://example.com/pkg.whl",
            "./local/pkg",
            "pkg @ https://example.com/pkg.whl",
            "transformers[torch]>=5",
            "numpy>=2\nnumpy<3",
            "numpy>=2 ; python_version >= '3.10'\nnumpy<3",
        ] {
            let err = parse(bad).unwrap_err();
            assert!(
                matches!(
                    err,
                    PluginEnvironmentError::AllowlistFileInvalidEntry { .. }
                ),
                "{bad:?}: {err:?}"
            );
        }
    }

    #[test]
    fn option_forms_are_accepted() {
        let allowlist = parse(
            "-i https://a.example/simple\n-ihttps://b.example/simple\n\
             --index-url=https://c.example/simple\n--extra-index-url https://d.example/simple\n\
             --no-binary :none: --prefer-binary --pre\n--find-links /wheels\n\
             --trusted-host a.example\n--no-index\nnumpy\n",
        )
        .unwrap();
        assert_eq!(allowlist.mode(), AllowlistMode::Ranges);
    }

    #[test]
    fn empty_allowlist_is_rejected() {
        let err = parse("# nothing\n--index-url https://pypi.org/simple\n").unwrap_err();
        assert!(
            matches!(err, PluginEnvironmentError::AllowlistEmpty { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn unreadable_file_is_reported() {
        let err = PackageAllowlist::from_file(Path::new("/nonexistent/allowlist.txt")).unwrap_err();
        assert!(
            matches!(err, PluginEnvironmentError::AllowlistFileUnreadable { .. }),
            "{err:?}"
        );
    }
}
