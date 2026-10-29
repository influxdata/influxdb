use crate::environment::PluginEnvironmentError::PluginEnvironmentDisabled;

use crate::virtualenv::{VenvError, find_python, initialize_venv};
use observability_deps::tracing::{debug, info, warn};
use pyo3::prelude::PyAnyMethods;
use pyo3::{PyResult, Python};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, OnceLock};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PluginEnvironmentError {
    #[error("Package manager not available: {0}")]
    PackageManagerNotFound(String),
    #[error("External call failed: {0}")]
    InstallationFailed(#[from] std::io::Error),
    #[error("Plugin environment management is disabled")]
    PluginEnvironmentDisabled,

    #[error("Virtual environment error: {0}")]
    VenvError(#[from] VenvError),

    #[error("Failed to list packages: {0}")]
    PackageListFailed(String),

    #[error(
        "Package installation has been disabled. Contact your administrator for more information."
    )]
    PackageInstallationDisabled,

    #[error(
        "'{name}' is not in the package allowlist. Contact your administrator to request that it be added to the allowlist."
    )]
    PackageNotAllowlisted { name: String },

    #[error("'{name}=={requested}' is not supported. Allowlisted version: {allowlisted}.")]
    PackageVersionNotAllowlisted {
        name: String,
        requested: String,
        allowlisted: String,
    },

    #[error("failed to read package allowlist file '{path}': {source}")]
    AllowlistFileUnreadable {
        path: String,
        source: std::io::Error,
    },

    #[error(
        "invalid entry in package allowlist file '{path}' on line {line_number}: {reason} ('{line}')"
    )]
    AllowlistFileInvalidEntry {
        path: String,
        line_number: usize,
        line: String,
        reason: String,
    },

    #[error("failed to read requirements file '{path}': {source}")]
    RequirementsFileUnreadable {
        path: String,
        source: std::io::Error,
    },

    #[error("requirements file entry on line {line_number} is not allowed: {reason} ('{line}')")]
    RequirementsEntryNotAllowed {
        line_number: usize,
        line: String,
        reason: String,
    },
}

/// Normalize a PyPI project name per PEP 503: lowercase and collapse any run of
/// `-`, `_`, or `.` into a single `-`. Comparisons against the approved list are
/// done on normalized names so `Foo_Bar` and `foo-bar` are treated as the same
/// package.
pub fn normalize_package_name(name: &str) -> String {
    let mut normalized = String::with_capacity(name.len());
    let mut last_was_separator = false;
    for ch in name.chars() {
        if matches!(ch, '-' | '_' | '.') {
            if !last_was_separator && !normalized.is_empty() {
                normalized.push('-');
            }
            last_was_separator = true;
        } else {
            normalized.extend(ch.to_lowercase());
            last_was_separator = false;
        }
    }
    // Trim a trailing separator that may have been pushed before end-of-string.
    while normalized.ends_with('-') {
        normalized.pop();
    }
    normalized
}

/// Split a user-supplied package spec into its name and optional pinned version.
///
/// Only the exact-pin form `name==version` is recognized as carrying a version;
/// any other spec (bare name, or a name with a non-`==` operator like `>=`) is
/// returned with `version = None` and the operator/text left attached to the
/// name portion so approval validation can reject it as unpinned. The returned
/// name is *not* normalized; callers normalize as needed.
pub fn parse_spec(spec: &str) -> (String, Option<String>) {
    let spec = spec.trim();
    if let Some((name, version)) = spec.split_once("==") {
        (name.trim().to_string(), Some(version.trim().to_string()))
    } else {
        (spec.to_string(), None)
    }
}

/// Whether `name` is a valid PEP 508 project name.
fn is_valid_package_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// Whether `version` is a single exact version, not a range or wildcard.
fn is_exact_version(version: &str) -> bool {
    !version.is_empty()
        && !version
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '<' | '>' | '=' | '!' | '~' | ',' | '*'))
}

/// Remove a pip-style comment: `#` at the start of the line or after
/// whitespace, through the end of the line.
fn strip_comment(line: &str) -> &str {
    let mut previous_is_space = true;
    for (index, ch) in line.char_indices() {
        if ch == '#' && previous_is_space {
            return &line[..index];
        }
        previous_is_space = ch.is_whitespace();
    }
    line
}

/// One logical line of a requirements file: physical lines joined across
/// trailing `\` continuations, with comments removed. `line_number` is the
/// first physical line.
struct LogicalLine {
    line_number: usize,
    text: String,
}

fn logical_lines(contents: &str) -> Vec<LogicalLine> {
    let mut lines = Vec::new();
    let mut current: Option<LogicalLine> = None;
    for (index, raw_line) in contents.lines().enumerate() {
        let stripped = strip_comment(raw_line).trim();
        let (body, continues) = match stripped.strip_suffix('\\') {
            Some(body) => (body.trim(), true),
            None => (stripped, false),
        };
        match current.as_mut() {
            Some(line) => {
                line.text.push(' ');
                line.text.push_str(body);
            }
            None => {
                current = Some(LogicalLine {
                    line_number: index + 1,
                    text: body.to_string(),
                })
            }
        }
        if !continues
            && let Some(line) = current.take()
            && !line.text.trim().is_empty()
        {
            lines.push(line);
        }
    }
    if let Some(line) = current
        && !line.text.trim().is_empty()
    {
        lines.push(line);
    }
    lines
}

/// One approved package: its exact version and any environment marker.
#[derive(Debug, Clone)]
struct AllowlistedEntry {
    version: String,
    marker: Option<String>,
}

/// The packages an operator has approved for end-user installation, read from
/// the `--plugin-package-allowlist` file. The file is a pip requirements file:
/// one pinned `name==version` per package, optionally with `--hash` options
/// (as written by `pip-compile --generate-hashes`), environment markers, and
/// index options (`--index-url`, `--extra-index-url`, `--only-binary`,
/// `--prefer-binary`) before the first entry. pip reads the index options from
/// the file itself.
///
/// If every entry carries a hash, installs are hash-verified; if none do,
/// versions are pinned only. A mix is rejected.
#[derive(Debug, Clone)]
pub struct AllowlistedPackages {
    /// The allowlist file, passed to pip as `-c` (no hashes) or `-r` (hashed).
    path: PathBuf,
    /// PEP 503 normalized name to its single approved version.
    packages: BTreeMap<String, AllowlistedEntry>,
    /// Whether every entry carries at least one `--hash`.
    hashed: bool,
}

impl AllowlistedPackages {
    /// Build an approved-packages set from a file. Returns
    /// `AllowlistFileUnreadable` if the file cannot be read and the first
    /// parse error otherwise.
    pub fn from_file(path: &Path) -> Result<Self, PluginEnvironmentError> {
        let contents = std::fs::read_to_string(path).map_err(|source| {
            PluginEnvironmentError::AllowlistFileUnreadable {
                path: path.display().to_string(),
                source,
            }
        })?;
        Self::from_str_lines(&contents, path)
    }

    /// Parse approved-packages contents read from `path`. Blank lines and
    /// comments are skipped and `\` continuations are joined, as pip does.
    pub fn from_str_lines(contents: &str, path: &Path) -> Result<Self, PluginEnvironmentError> {
        let invalid_entry = |line_number: usize, line: &str, reason: &str| {
            PluginEnvironmentError::AllowlistFileInvalidEntry {
                path: path.display().to_string(),
                line_number,
                line: line.to_string(),
                reason: reason.to_string(),
            }
        };
        // Normalized name to (entry, first line number).
        let mut packages: BTreeMap<String, (AllowlistedEntry, usize)> = BTreeMap::new();
        let mut any_hashed = false;
        let mut first_unhashed: Option<(usize, String)> = None;

        for LogicalLine { line_number, text } in logical_lines(contents) {
            let invalid = |reason: &str| invalid_entry(line_number, &text, reason);

            if text.starts_with('-') {
                let (option, value) = match text.split_once(['=', ' ']) {
                    Some((option, value)) => (option, value.trim()),
                    None => (text.as_str(), ""),
                };
                let takes_value = match option {
                    "-i" | "--index-url" | "--extra-index-url" | "--only-binary" => true,
                    "--prefer-binary" => false,
                    "-e" | "--editable" => {
                        return Err(invalid("editable installs are not allowed"));
                    }
                    "-r" | "--requirement" | "-c" | "--constraint" => {
                        return Err(invalid("nested requirement files are not allowed"));
                    }
                    _ => {
                        return Err(invalid(
                            "only --index-url, --extra-index-url, --only-binary and \
                             --prefer-binary options are allowed",
                        ));
                    }
                };
                if !packages.is_empty() {
                    return Err(invalid(
                        "index options must appear before any package entry",
                    ));
                }
                if takes_value == value.is_empty() {
                    return Err(invalid(if takes_value {
                        "option is missing a value"
                    } else {
                        "option does not take a value"
                    }));
                }
                continue;
            }

            // A requirement, optionally followed by `--hash` options.
            let (requirement, options) = match text.find(" --") {
                Some(index) => (text[..index].trim(), &text[index..]),
                None => (text.trim(), ""),
            };
            let mut hashed = false;
            let mut tokens = options.split_whitespace();
            while let Some(token) = tokens.next() {
                if token.starts_with("--hash=") || (token == "--hash" && tokens.next().is_some()) {
                    hashed = true;
                } else {
                    return Err(invalid("only --hash options may follow a package entry"));
                }
            }

            let lowered = requirement.to_ascii_lowercase();
            if lowered.contains("://")
                || ["git+", "hg+", "svn+", "bzr+"]
                    .iter()
                    .any(|prefix| lowered.starts_with(prefix))
            {
                return Err(invalid("URLs and VCS references are not allowed"));
            }
            if requirement.contains('@') {
                return Err(invalid("direct references ('name @ url') are not allowed"));
            }
            if requirement.starts_with(['/', '.', '~']) {
                return Err(invalid("local path entries are not allowed"));
            }

            // Environment markers are kept for pip to evaluate at install time.
            let (spec, marker) = match requirement.split_once(';') {
                Some((spec, marker)) => (spec.trim(), Some(marker.trim().to_string())),
                None => (requirement, None),
            };
            let (name, version) = parse_spec(spec);
            // pip rejects a `-c` file whose entries carry extras.
            if name.contains('[') {
                return Err(invalid(
                    "extras are not allowed; list the packages an extra adds as their own \
                     entries (pip-compile --strip-extras)",
                ));
            }
            let version = match version {
                Some(version) if is_valid_package_name(&name) && is_exact_version(&version) => {
                    version
                }
                _ => return Err(invalid("entries must be pinned as 'name==version'")),
            };

            let normalized = normalize_package_name(&name);
            if let Some((existing, first_line)) = packages.get(&normalized) {
                if existing.version != version {
                    return Err(invalid(&format!(
                        "'{name}' is already listed with version {} on line {first_line}; \
                         list one version per package",
                        existing.version
                    )));
                }
            } else {
                packages.insert(
                    normalized,
                    (AllowlistedEntry { version, marker }, line_number),
                );
            }

            if hashed {
                any_hashed = true;
            } else if first_unhashed.is_none() {
                first_unhashed = Some((line_number, requirement.to_string()));
            }
        }

        if any_hashed && let Some((line_number, line)) = first_unhashed {
            return Err(invalid_entry(
                line_number,
                &line,
                "entry has no hash, but other entries do; hash every entry \
                 (pip-compile --generate-hashes) or remove all hashes",
            ));
        }

        Ok(Self {
            path: path.to_path_buf(),
            packages: packages
                .into_iter()
                .map(|(name, (entry, _))| (name, entry))
                .collect(),
            hashed: any_hashed,
        })
    }

    /// Number of approved packages.
    pub fn len(&self) -> usize {
        self.packages.len()
    }

    /// Whether the file approves no packages.
    pub fn is_empty(&self) -> bool {
        self.packages.is_empty()
    }

    /// Whether installs are hash-verified.
    pub fn is_hashed(&self) -> bool {
        self.hashed
    }

    /// Check a single user-supplied spec against the approved list and return
    /// the exact `name==version` to install, with the entry's environment
    /// marker if it has one. A bare name resolves to the approved version.
    pub fn resolve_spec(&self, spec: &str) -> Result<String, PluginEnvironmentError> {
        let (name, version) = parse_spec(spec);
        let name = name.trim();
        let Some(entry) = self.packages.get(&normalize_package_name(name)) else {
            return Err(PluginEnvironmentError::PackageNotAllowlisted {
                name: name.to_string(),
            });
        };
        match version {
            Some(requested) if requested != entry.version => {
                Err(PluginEnvironmentError::PackageVersionNotAllowlisted {
                    name: name.to_string(),
                    requested,
                    allowlisted: entry.version.clone(),
                })
            }
            _ => Ok(match &entry.marker {
                Some(marker) => format!("{name}=={}; {marker}", entry.version),
                None => format!("{name}=={}", entry.version),
            }),
        }
    }

    /// Resolve every spec, failing on the first rejected one.
    pub fn resolve_all(&self, specs: &[String]) -> Result<Vec<String>, PluginEnvironmentError> {
        specs.iter().map(|spec| self.resolve_spec(spec)).collect()
    }

    /// Check every entry of a user-supplied requirements file and return the
    /// exact specs to install. Used for the `-r requirements.txt` path so it
    /// cannot bypass the allowlist that `install_packages` enforces.
    ///
    /// Only simple `name==version` or bare `name` requirement lines are
    /// permitted. Lines that pip would treat as something other than a plain
    /// requirement (URLs, VCS refs, local paths, editable installs, index
    /// options, nested requirement includes, and line continuations) are
    /// rejected, because they could pull code the allowlist never approved.
    /// Blank lines and `#` comments are skipped. Each surviving line is checked
    /// with [`AllowlistedPackages::resolve_spec`], so version rules match the
    /// `install_packages` path exactly.
    fn resolve_requirements(&self, contents: &str) -> Result<Vec<String>, PluginEnvironmentError> {
        let mut specs = Vec::new();
        for (index, raw_line) in contents.lines().enumerate() {
            let line_number = index + 1;
            let line = strip_comment(raw_line).trim();
            if line.is_empty() {
                continue;
            }

            let reject = |reason: &str| PluginEnvironmentError::RequirementsEntryNotAllowed {
                line_number,
                line: line.to_string(),
                reason: reason.to_string(),
            };

            // A trailing backslash continues the line, changing pip's parsing;
            // reject rather than try to reassemble.
            if line.ends_with('\\') {
                return Err(reject("line continuations are not allowed"));
            }
            // pip options: -e/--editable, -i/--index-url/--extra-index-url,
            // -r/--requirement, -c/--constraint, -f/--find-links, hashes, etc.
            if line.starts_with('-') {
                return Err(reject(
                    "pip options (-e, -i, --index-url, -r, -c, --hash, …) are not allowed",
                ));
            }
            // URLs, VCS refs, and local/editable paths.
            let lowered = line.to_ascii_lowercase();
            if lowered.contains("://")
                || lowered.starts_with("git+")
                || lowered.starts_with("hg+")
                || lowered.starts_with("svn+")
                || lowered.starts_with("bzr+")
            {
                return Err(reject("URLs and VCS references are not allowed"));
            }
            // `name @ url` direct references.
            if line.contains('@') {
                return Err(reject("direct references ('name @ url') are not allowed"));
            }
            // Local path requirements (absolute, home, or explicit relative).
            if line.starts_with('/') || line.starts_with('.') || line.starts_with('~') {
                return Err(reject("local path requirements are not allowed"));
            }
            // Environment-marker separator leaves the pin ambiguous for the
            // allowlist; reject to keep the gate strict.
            if line.contains(';') {
                return Err(reject("environment markers are not allowed"));
            }

            // Whatever remains must be a package the allowlist approves.
            specs.push(self.resolve_spec(line)?);
        }
        Ok(specs)
    }
}

/// The server-side policy applied to every pip install: an optional package
/// allowlist file. A [`PipManager`] holds one of these and consults it before
/// shelling out to pip.
#[derive(Debug, Clone, Default)]
pub struct PipPolicy {
    /// The parsed allowlist, when `--plugin-package-allowlist` is set.
    pub allowlist: Option<AllowlistedPackages>,
}

impl PipPolicy {
    /// A policy that imposes no restrictions. Matches the historical behavior.
    pub fn unrestricted() -> Self {
        Self::default()
    }

    /// Build a policy from operator config. The allowlist file is read and
    /// parsed early so a bad file fails startup rather than the first install
    /// request.
    pub fn from_config(allowlist_file: Option<&Path>) -> Result<Self, PluginEnvironmentError> {
        let Some(path) = allowlist_file else {
            return Ok(Self::unrestricted());
        };
        let allowlist = AllowlistedPackages::from_file(path)?;
        if allowlist.is_empty() {
            warn!(
                path = %path.display(),
                "plugin package allowlist has no entries; every package install will be rejected"
            );
        }
        info!(
            path = %path.display(),
            entries = allowlist.len(),
            hashes_enforced = allowlist.is_hashed(),
            "plugin package allowlist loaded"
        );
        Ok(Self {
            allowlist: Some(allowlist),
        })
    }

    /// Check install specs against the allowlist, if one is configured, and
    /// return the exact specs to install. With no allowlist the specs are
    /// returned unchanged.
    fn resolve_packages(&self, packages: &[String]) -> Result<Vec<String>, PluginEnvironmentError> {
        match &self.allowlist {
            Some(approved) => approved.resolve_all(packages),
            None => Ok(packages.to_vec()),
        }
    }

    /// Read a requirements file (by path) server-side and resolve it against
    /// the allowlist, so a client-supplied `-r` path cannot bypass it. pip is
    /// given the resolved specs, never the file, so the file is read once.
    /// Returns `None` when no allowlist is configured: the caller passes the
    /// file to pip unchecked.
    fn resolve_requirements_file(
        &self,
        path: &str,
    ) -> Result<Option<Vec<String>>, PluginEnvironmentError> {
        let Some(approved) = &self.allowlist else {
            return Ok(None);
        };
        let contents = std::fs::read_to_string(path).map_err(|source| {
            PluginEnvironmentError::RequirementsFileUnreadable {
                path: path.to_string(),
                source,
            }
        })?;
        approved.resolve_requirements(&contents).map(Some)
    }

    /// Build the argument list that follows `pip install` for a request naming
    /// `specs` (already resolved against the allowlist).
    ///
    /// pip only checks hashes from *requirement* inputs (command-line specs or
    /// a `-r` file) and ignores hashes in a `-c` constraints file, so:
    ///
    /// * **No hashes**: the user's specs drive the install, and the allowlist
    ///   file is passed as `-c <file>` so dependencies listed there are pinned
    ///   too.
    ///
    /// * **Hashed**: the allowlist file is installed as `-r <file>` with
    ///   `--require-hashes`, so every package in it is installed and checked.
    ///   The user's specs are omitted: a hashless spec on the command line would
    ///   fail the hash check, and the resolver already confirmed each one is in
    ///   the file.
    ///
    /// Either way pip reads the file's index options itself.
    fn pip_install_args(&self, specs: &[String]) -> Vec<OsString> {
        let Some(allowlist) = &self.allowlist else {
            return specs.iter().map(OsString::from).collect();
        };
        let mut args = Vec::new();
        if allowlist.hashed {
            args.push(OsString::from("-r"));
            args.push(allowlist.path.as_os_str().to_owned());
            args.push(OsString::from("--require-hashes"));
        } else {
            args.extend(specs.iter().map(OsString::from));
            args.push(OsString::from("-c"));
            args.push(allowlist.path.as_os_str().to_owned());
        }
        args
    }
}

pub trait PythonEnvironmentManager: Debug + Send + Sync + 'static {
    fn init_pyenv(
        &self,
        plugin_dir: Option<&Path>,
        virtual_env_location: Option<&PathBuf>,
    ) -> Result<(), PluginEnvironmentError>;
    fn install_packages(&self, packages: Vec<String>) -> Result<(), PluginEnvironmentError>;

    fn install_requirements(&self, requirements_path: String)
    -> Result<(), PluginEnvironmentError>;

    fn uninstall_packages(&self, packages: Vec<String>) -> Result<(), PluginEnvironmentError>;
}

#[derive(Debug, Clone)]
pub struct PipManager {
    policy: Arc<PipPolicy>,
}

impl PipManager {
    /// A `PipManager` with no allowlist. Matches the historical behavior.
    pub fn unrestricted() -> Self {
        Self {
            policy: Arc::new(PipPolicy::unrestricted()),
        }
    }

    /// A `PipManager` governed by the given policy.
    pub fn with_policy(policy: Arc<PipPolicy>) -> Self {
        Self { policy }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct DisabledManager;

fn is_valid_venv(venv_path: &Path) -> bool {
    cfg_select! {
        windows => venv_path.join("Scripts").join("activate.bat").exists(),
        _ => venv_path.join("bin").join("activate").exists(),
    }
}

impl PythonEnvironmentManager for PipManager {
    fn init_pyenv(
        &self,
        plugin_dir: Option<&Path>,
        virtual_env_location: Option<&PathBuf>,
    ) -> Result<(), PluginEnvironmentError> {
        let plugin_dir = plugin_dir.expect("plugin dir is set if using pip");
        let venv_path = venv_path_for(plugin_dir, virtual_env_location);

        if !is_valid_venv(&venv_path) {
            let python_exe = find_python();
            Command::new(python_exe)
                .arg("-m")
                .arg("venv")
                .arg(&venv_path)
                .output()?;
        }

        initialize_venv(&venv_path)?;
        Ok(())
    }

    fn install_packages(&self, packages: Vec<String>) -> Result<(), PluginEnvironmentError> {
        // Enforce the approved-packages policy before shelling out to pip so an
        // unapproved package never reaches the network.
        let packages = self.policy.resolve_packages(&packages)?;

        let python_exe = find_python();
        let output = Command::new(python_exe)
            .arg("-m")
            .arg("pip")
            .arg("install")
            .args(self.policy.pip_install_args(&packages))
            .output()?;
        if !output.status.success() {
            return Err(PluginEnvironmentError::PackageManagerNotFound(format!(
                "pip install failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(())
    }
    fn install_requirements(
        &self,
        requirements_path: String,
    ) -> Result<(), PluginEnvironmentError> {
        // Enforce the allowlist on the requirements file before shelling out, so
        // the `-r` path cannot bypass the gate that `install_packages` applies.
        let args = match self.policy.resolve_requirements_file(&requirements_path)? {
            // pip treats a requirements file with no entries as a no-op.
            Some(specs) if specs.is_empty() => return Ok(()),
            Some(specs) => self.policy.pip_install_args(&specs),
            None => vec![OsString::from("-r"), OsString::from(&requirements_path)],
        };

        let python_exe = find_python();
        let output = Command::new(python_exe)
            .arg("-m")
            .arg("pip")
            .arg("install")
            .args(args)
            .output()?;
        if !output.status.success() {
            return Err(PluginEnvironmentError::PackageManagerNotFound(format!(
                "pip install -r failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(())
    }

    fn uninstall_packages(&self, packages: Vec<String>) -> Result<(), PluginEnvironmentError> {
        // Treats an already-absent package as a warning with a zero exit
        // so uninstall is idempotent.
        let python_exe = find_python();
        let output = Command::new(python_exe)
            .arg("-m")
            .arg("pip")
            .arg("uninstall")
            .arg("-y")
            .args(&packages)
            .output()?;
        if !output.status.success() {
            return Err(PluginEnvironmentError::PackageManagerNotFound(format!(
                "pip uninstall failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(())
    }
}

fn sorted_unique_package_names(mut package_names: Vec<String>) -> Vec<String> {
    package_names.sort();
    package_names.dedup();
    package_names
}

fn list_installed_packages_from_python(py: Python<'_>) -> PyResult<Vec<String>> {
    let importlib_metadata = py.import("importlib.metadata")?;
    let distributions = importlib_metadata.call_method0("distributions")?;
    let mut package_names = Vec::new();

    for distribution in distributions.try_iter()? {
        let distribution = distribution?;
        let metadata = distribution.getattr("metadata")?;
        if let Some(name) = metadata
            .call_method1("get", ("Name",))?
            .extract::<Option<String>>()?
            && !name.is_empty()
        {
            package_names.push(name);
        }
    }

    Ok(sorted_unique_package_names(package_names))
}

pub fn list_installed_packages() -> Result<Vec<String>, PluginEnvironmentError> {
    Python::try_attach(list_installed_packages_from_python)
        .unwrap_or_else(|| Ok(Vec::new()))
        .map_err(|error| PluginEnvironmentError::PackageListFailed(error.to_string()))
}

impl PythonEnvironmentManager for DisabledManager {
    fn init_pyenv(
        &self,
        plugin_dir: Option<&Path>,
        _virtual_env_location: Option<&PathBuf>,
    ) -> Result<(), PluginEnvironmentError> {
        // DisabledManager means no package manager (pip) is available or
        // we do not want to turn the processing engine on.
        //
        // If we're trying to initialize a Python environment, we should fail
        // only if the plugin_dir is set

        if plugin_dir.is_some() {
            Err(PluginEnvironmentError::PackageManagerNotFound(
                "pip package manager is not available. Please install Python with pip".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    fn install_packages(&self, _packages: Vec<String>) -> Result<(), PluginEnvironmentError> {
        Err(PluginEnvironmentDisabled)
    }

    fn install_requirements(
        &self,
        _requirements_path: String,
    ) -> Result<(), PluginEnvironmentError> {
        Err(PluginEnvironmentDisabled)
    }

    fn uninstall_packages(&self, _packages: Vec<String>) -> Result<(), PluginEnvironmentError> {
        Err(PluginEnvironmentDisabled)
    }
}

/// A package manager that disables package installation while allowing
/// the processing engine to function normally for triggers and plugins.
/// Used when --package-manager disabled is set.
#[derive(Debug, Copy, Clone)]
pub struct DisabledPackageManager;

impl PythonEnvironmentManager for DisabledPackageManager {
    fn init_pyenv(
        &self,
        _plugin_dir: Option<&Path>,
        _virtual_env_location: Option<&PathBuf>,
    ) -> Result<(), PluginEnvironmentError> {
        // Allow normal initialization - the processing engine should still work
        // We assume the virtual environment is already set up
        Ok(())
    }

    fn install_packages(&self, _packages: Vec<String>) -> Result<(), PluginEnvironmentError> {
        Err(PluginEnvironmentError::PackageInstallationDisabled)
    }

    fn install_requirements(
        &self,
        _requirements_path: String,
    ) -> Result<(), PluginEnvironmentError> {
        Err(PluginEnvironmentError::PackageInstallationDisabled)
    }

    fn uninstall_packages(&self, _packages: Vec<String>) -> Result<(), PluginEnvironmentError> {
        Err(PluginEnvironmentError::PackageInstallationDisabled)
    }
}

/// A test-only package manager that always succeeds without doing anything.
/// This is used for tests that need to validate plugin filenames and create triggers
/// but don't actually need Python or package management functionality.
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub struct TestManager;

#[cfg(test)]
impl PythonEnvironmentManager for TestManager {
    fn init_pyenv(
        &self,
        _plugin_dir: Option<&Path>,
        _virtual_env_location: Option<&PathBuf>,
    ) -> Result<(), PluginEnvironmentError> {
        // Always succeed for tests
        Ok(())
    }

    fn install_packages(&self, _packages: Vec<String>) -> Result<(), PluginEnvironmentError> {
        // Always succeed for tests
        Ok(())
    }

    fn install_requirements(
        &self,
        _requirements_path: String,
    ) -> Result<(), PluginEnvironmentError> {
        // Always succeed for tests
        Ok(())
    }

    fn uninstall_packages(&self, _packages: Vec<String>) -> Result<(), PluginEnvironmentError> {
        // Always succeed for tests
        Ok(())
    }
}

/// The location of the one-per-process venv, set once by [`get_or_init_venv`].
static VENV_PATH: OnceLock<PathBuf> = OnceLock::new();

/// The outcome of building the venv at [`VENV_PATH`], computed at most once.
/// The error is a `String` so the result can be cloned out for every waiter
/// (`VenvError` is not `Clone`).
static VENV_BUILD: OnceLock<Result<(), String>> = OnceLock::new();

/// The venv directory PipManager uses: an explicit `--virtual-env-location`, or
/// `.venv` under the plugin dir.
pub fn venv_path_for(plugin_dir: &Path, virtual_env_location: Option<&PathBuf>) -> PathBuf {
    match virtual_env_location {
        Some(path) => path.clone(),
        None => plugin_dir.join(".venv"),
    }
}

/// Create the venv at [`VENV_PATH`] if it does not already exist. This is the
/// seconds-long step; it runs once and is shared by every [`VenvHandle::ready`]
/// caller and the background thread spawned by [`get_or_init_venv`].
fn build_venv() -> Result<(), String> {
    let venv_path = VENV_PATH
        .get()
        .expect("get_or_init_venv sets VENV_PATH before the build runs");

    if !is_valid_venv(venv_path) {
        let python_exe = find_python();
        debug!(
            "Running: {} -m venv {}",
            python_exe.display(),
            venv_path.display()
        );
        let output = Command::new(&python_exe)
            .arg("-m")
            .arg("venv")
            .arg(venv_path)
            .output()
            .map_err(|error| format!("failed to run python -m venv: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "python -m venv failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }
    Ok(())
}

/// Start building the one-per-process venv on a background thread so it overlaps
/// with the rest of startup. Call [`VenvHandle::ready`] to wait for it.
///
/// Idempotent like [`OnceLock::get_or_init`]: the first call kicks off the
/// build on a background thread; subsequent calls skip the spawn entirely and
/// return a handle whose [`VenvHandle::ready`] reads the cached result without
/// rebuilding.
pub fn get_or_init_venv(venv_path: PathBuf) -> VenvHandle {
    let _ = VENV_PATH.set(venv_path);
    // Skip the spawn if the build already ran; `ready` will just read the cache.
    let build = VENV_BUILD.get().is_none().then(spawn_venv_build);
    VenvHandle { build }
}

/// Spawn the background thread that fills [`VENV_BUILD`].
fn spawn_venv_build() -> std::thread::JoinHandle<()> {
    std::thread::spawn(|| {
        VENV_BUILD.get_or_init(build_venv);
    })
}

/// A handle to the one-per-process venv build started by [`get_or_init_venv`].
/// Holds the build thread's `JoinHandle` on the call that kicked off the build;
/// `None` when the build was already cached and no thread was spawned. The
/// private field makes this a move-only, [`get_or_init_venv`]-only handle.
#[derive(Debug)]
pub struct VenvHandle {
    build: Option<std::thread::JoinHandle<()>>,
}

impl VenvHandle {
    /// Block until the background venv build finishes.
    ///
    /// The returned [`ReadyVenv`] is the only handle to the built venv and the
    /// entry point for operating on it (e.g. [`ReadyVenv::determine_package_manager`]).
    /// Because nothing else can construct a `ReadyVenv`, any code that touches
    /// the venv must wait on the build first — the compiler enforces it.
    pub fn ready(self) -> Result<ReadyVenv, PluginEnvironmentError> {
        // If this call kicked off the build, wait for the thread; otherwise the
        // cached result is already available and there is no thread to join.
        if let Some(handle) = self.build {
            let _ = handle.join();
        }
        VENV_BUILD
            .get_or_init(build_venv)
            .clone()
            .map_err(PluginEnvironmentError::PackageManagerNotFound)?;
        let path = VENV_PATH
            .get()
            .expect("get_or_init_venv set VENV_PATH")
            .clone();
        Ok(ReadyVenv { path })
    }
}

/// A venv that has finished building. Hands out operations that require the
/// venv to already exist on disk.
#[derive(Debug)]
pub struct ReadyVenv {
    path: PathBuf,
}

impl ReadyVenv {
    /// Probe pip *inside* the built venv and pick the matching manager:
    /// `PipManager` if `python -m pip` works there, otherwise `DisabledManager`.
    ///
    /// pip lives in the venv, not the interpreter: `python -m venv` bootstraps
    /// pip via `ensurepip`, which can produce a working pip even when the
    /// interpreter ships none of its own. Probing the venv is therefore the
    /// reliable signal, where probing the system interpreter is not.
    pub fn determine_package_manager(
        &self,
        policy: Arc<PipPolicy>,
    ) -> Arc<dyn PythonEnvironmentManager> {
        // A venv always contains `bin/python`; `python3` is only a symlink that
        // may be absent.
        let venv_python = cfg_select! {
            windows => self.path.join("Scripts").join("python.exe"),
            _ => self.path.join("bin").join("python"),
        };
        debug!("Running: {} -m pip --version", venv_python.display());
        let pip_available = Command::new(&venv_python)
            .args(["-m", "pip", "--version"])
            .output()
            .is_ok_and(|output| output.status.success());
        if pip_available {
            Arc::new(PipManager::with_policy(policy))
        } else {
            Arc::new(DisabledManager)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn venv_path_for_uses_explicit_virtual_env_location() {
        let explicit = PathBuf::from("/some/custom/venv");
        let plugin_dir = Path::new("/plugins");
        assert_eq!(venv_path_for(plugin_dir, Some(&explicit)), explicit);
    }

    #[test]
    fn venv_path_for_defaults_to_dotvenv_under_plugin_dir() {
        let plugin_dir = Path::new("/plugins");
        assert_eq!(venv_path_for(plugin_dir, None), plugin_dir.join(".venv"));
    }

    #[test]
    fn normalize_package_name_follows_pep503() {
        assert_eq!(normalize_package_name("Foo"), "foo");
        assert_eq!(normalize_package_name("Foo_Bar"), "foo-bar");
        assert_eq!(normalize_package_name("foo.bar"), "foo-bar");
        assert_eq!(normalize_package_name("foo---bar"), "foo-bar");
        assert_eq!(normalize_package_name("Foo._-Bar"), "foo-bar");
        assert_eq!(normalize_package_name("numpy"), "numpy");
    }

    #[test]
    fn parse_spec_splits_exact_pin() {
        assert_eq!(
            parse_spec("numpy==2.1.3"),
            ("numpy".to_string(), Some("2.1.3".to_string()))
        );
        assert_eq!(parse_spec("numpy"), ("numpy".to_string(), None));
        // A non-`==` operator is left attached to the name so it is treated as
        // unpinned by approval validation.
        assert_eq!(parse_spec("numpy>=2.0"), ("numpy>=2.0".to_string(), None));
        assert_eq!(
            parse_spec("  numpy == 2.1.3  "),
            ("numpy".to_string(), Some("2.1.3".to_string()))
        );
    }

    fn approved(contents: &str) -> AllowlistedPackages {
        AllowlistedPackages::from_str_lines(contents, Path::new("test"))
            .expect("valid approved list")
    }

    fn approved_err(contents: &str) -> PluginEnvironmentError {
        AllowlistedPackages::from_str_lines(contents, Path::new("test")).unwrap_err()
    }

    const HASHED: &str = "\
#
# This file is autogenerated by pip-compile
#
--index-url https://download.pytorch.org/whl/cpu
--extra-index-url https://pypi.org/simple

numpy==1.26.4 \\
    --hash=sha256:aaa \\
    --hash=sha256:bbb
    # via
    #   -r requirements.in
    #   pandas
pandas==2.2.2 \\
    --hash=sha256:ccc
    # via -r requirements.in
six==1.16.0 --hash sha256:ddd
";

    #[test]
    fn approved_packages_parses_and_skips_blanks_and_comments() {
        let list =
            approved("# a comment\n\nnumpy==2.1.3\n  # indented comment\nPandas==2.2.0  # why\n");
        assert_eq!(list.len(), 2);
        assert!(!list.is_hashed());
        assert_eq!(list.resolve_spec("numpy==2.1.3").unwrap(), "numpy==2.1.3");
        // Name normalization: `Pandas` approved as `pandas`.
        assert_eq!(list.resolve_spec("pandas==2.2.0").unwrap(), "pandas==2.2.0");
    }

    #[test]
    fn approved_packages_rejects_unpinned_and_range_entries() {
        for bad in [
            "numpy",
            "numpy>=2.0",
            "numpy==2.*",
            "numpy==",
            "numpy>=1,==2.0",
        ] {
            let err = approved_err(&format!("{bad}\n"));
            match err {
                PluginEnvironmentError::AllowlistFileInvalidEntry {
                    line_number: 1,
                    reason,
                    ..
                } => assert_eq!(reason, "entries must be pinned as 'name==version'", "{bad}"),
                other => panic!("expected AllowlistFileInvalidEntry for {bad:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn approved_packages_rejects_non_index_entries() {
        for (bad, reason) in [
            ("-e ./my_local_lib", "editable installs are not allowed"),
            (
                "git+https://github.com/example/tool.git@v1.0",
                "URLs and VCS references are not allowed",
            ),
            (
                "mylib @ https://example.com/mylib-1.0-py3-none-any.whl",
                "URLs and VCS references are not allowed",
            ),
            (
                "mylib @ file:mylib.whl",
                "direct references ('name @ url') are not allowed",
            ),
            (
                "/opt/wheels/private-1.0-py3-none-any.whl",
                "local path entries are not allowed",
            ),
            (
                "-r more-packages.txt",
                "nested requirement files are not allowed",
            ),
            (
                "-c constraints.txt",
                "nested requirement files are not allowed",
            ),
            (
                "--find-links /opt/wheels",
                "only --index-url, --extra-index-url, --only-binary and --prefer-binary options are allowed",
            ),
            (
                "numpy==1.26.4 --no-binary :all:",
                "only --hash options may follow a package entry",
            ),
        ] {
            match approved_err(&format!("{bad}\n")) {
                PluginEnvironmentError::AllowlistFileInvalidEntry { reason: got, .. } => {
                    assert_eq!(got, reason, "{bad}")
                }
                other => panic!("expected AllowlistFileInvalidEntry for {bad:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn approved_packages_rejects_two_versions_of_one_package() {
        let err = approved_err("matplotlib==3.8.4\nnumpy==1.26.4\nMatplotlib==3.9.2\n");
        assert_eq!(
            err.to_string(),
            "invalid entry in package allowlist file 'test' on line 3: 'Matplotlib' is already \
             listed with version 3.8.4 on line 1; list one version per package \
             ('Matplotlib==3.9.2')"
        );
        // The same version listed twice is harmless.
        assert_eq!(approved("numpy==1.26.4\nnumpy==1.26.4\n").len(), 1);
    }

    #[test]
    fn approved_packages_accepts_markers() {
        let list = approved("colorama==0.4.6 ; sys_platform == \"win32\"\n");
        // The marker travels with the spec so pip skips it on other platforms.
        assert_eq!(
            list.resolve_spec("colorama").unwrap(),
            "colorama==0.4.6; sys_platform == \"win32\""
        );
    }

    #[test]
    fn approved_packages_rejects_extras() {
        // Rejected with or without hashes, so the rule does not depend on mode.
        for bad in [
            "requests[socks]==2.32.3",
            "requests[socks]==2.32.3 --hash=sha256:aaa",
        ] {
            match approved_err(&format!("{bad}\n")) {
                PluginEnvironmentError::AllowlistFileInvalidEntry { reason, .. } => assert_eq!(
                    reason,
                    "extras are not allowed; list the packages an extra adds as their own \
                     entries (pip-compile --strip-extras)",
                    "{bad}"
                ),
                other => panic!("expected AllowlistFileInvalidEntry for {bad:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn approved_packages_parses_pip_compile_hashed_output() {
        let list = approved(HASHED);
        assert!(list.is_hashed());
        assert_eq!(list.len(), 3);
        assert_eq!(list.resolve_spec("six").unwrap(), "six==1.16.0");
    }

    #[test]
    fn approved_packages_rejects_mixed_hashes() {
        let err = approved_err("torch==2.4.1+cpu \\\n    --hash=sha256:aaa\nfilelock==3.16.1\n");
        match err {
            PluginEnvironmentError::AllowlistFileInvalidEntry {
                line_number,
                line,
                reason,
                ..
            } => {
                assert_eq!(line_number, 3);
                assert_eq!(line, "filelock==3.16.1");
                assert_eq!(
                    reason,
                    "entry has no hash, but other entries do; hash every entry \
                     (pip-compile --generate-hashes) or remove all hashes"
                );
            }
            other => panic!("expected AllowlistFileInvalidEntry, got {other:?}"),
        }
        // The unhashed entry may come first.
        assert!(matches!(
            approved_err("filelock==3.16.1\ntorch==2.4.1+cpu --hash=sha256:aaa\n"),
            PluginEnvironmentError::AllowlistFileInvalidEntry { line_number: 1, .. }
        ));
    }

    #[test]
    fn approved_packages_rejects_index_option_after_entry() {
        let err = approved_err("numpy==1.26.4\n--index-url https://example.com/simple\n");
        match err {
            PluginEnvironmentError::AllowlistFileInvalidEntry {
                line_number: 2,
                reason,
                ..
            } => assert_eq!(reason, "index options must appear before any package entry"),
            other => panic!("expected AllowlistFileInvalidEntry, got {other:?}"),
        }
        // `=` form and `-i` short form are accepted at the top.
        let list = approved(
            "-i https://a.example/simple\n--extra-index-url=https://b.example/simple\nnumpy==1.26.4\n",
        );
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn approved_packages_empty_file_is_valid_and_rejects_everything() {
        let list = approved("# No packages approved yet\n");
        assert!(list.is_empty());
        assert!(matches!(
            list.resolve_spec("numpy==1.26.4").unwrap_err(),
            PluginEnvironmentError::PackageNotAllowlisted { .. }
        ));
    }

    #[test]
    fn resolve_spec_not_on_list() {
        let list = approved("numpy==2.1.3\n");
        let err = list.resolve_spec("scipy==1.0.0").unwrap_err();
        match err {
            PluginEnvironmentError::PackageNotAllowlisted { name } => assert_eq!(name, "scipy"),
            other => panic!("expected PackageNotAllowlisted, got {other:?}"),
        }
    }

    #[test]
    fn resolve_spec_wrong_version() {
        let list = approved("numpy==2.1.3\n");
        let err = list.resolve_spec("numpy==1.0.0").unwrap_err();
        assert_eq!(
            err.to_string(),
            "'numpy==1.0.0' is not supported. Allowlisted version: 2.1.3."
        );
    }

    #[test]
    fn resolve_spec_bare_name_resolves_to_listed_version() {
        let list = approved("numpy==2.1.3\n");
        assert_eq!(list.resolve_spec("numpy").unwrap(), "numpy==2.1.3");
    }

    #[test]
    fn resolve_spec_normalizes_requested_name() {
        let list = approved("scikit-learn==1.5.1\n");
        assert_eq!(
            list.resolve_spec("Scikit_Learn==1.5.1").unwrap(),
            "Scikit_Learn==1.5.1"
        );
        assert_eq!(
            list.resolve_spec("scikit.learn").unwrap(),
            "scikit.learn==1.5.1"
        );
    }

    #[test]
    fn resolve_all_fails_on_first_bad_spec() {
        let list = approved("numpy==2.1.3\n");
        let err = list
            .resolve_all(&["numpy==2.1.3".to_string(), "scipy==1.0.0".to_string()])
            .unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::PackageNotAllowlisted { .. }
        ));
    }

    #[test]
    fn approved_packages_from_file_reports_unreadable() {
        let missing = Path::new("/nonexistent/approved-packages.txt");
        let err = AllowlistedPackages::from_file(missing).unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::AllowlistFileUnreadable { .. }
        ));
    }

    #[test]
    fn pip_policy_unrestricted_approves_everything() {
        let policy = PipPolicy::unrestricted();
        assert_eq!(
            policy.resolve_packages(&["anything".to_string()]).unwrap(),
            vec!["anything".to_string()]
        );
        assert_eq!(
            policy.pip_install_args(&["numpy==2.1.3".to_string()]),
            vec![OsString::from("numpy==2.1.3")]
        );
    }

    #[test]
    fn pip_policy_from_config_fails_fast_on_bad_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("allowlist.txt");
        std::fs::write(&path, "numpy\n").unwrap();
        assert!(matches!(
            PipPolicy::from_config(Some(&path)).unwrap_err(),
            PluginEnvironmentError::AllowlistFileInvalidEntry { .. }
        ));
    }

    #[test]
    fn pip_install_args_without_hashes_pins_with_c() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("allowlist.txt");
        std::fs::write(
            &path,
            "--index-url https://a.example/simple\nnumpy==2.1.3\n",
        )
        .unwrap();
        let policy = PipPolicy::from_config(Some(&path)).unwrap();

        // pip reads `--index-url` from the `-c` file, so it is not repeated here.
        let expected = vec![
            OsString::from("numpy==2.1.3"),
            OsString::from("-c"),
            path.into_os_string(),
        ];
        let specs = policy.resolve_packages(&["numpy".to_string()]).unwrap();
        assert_eq!(policy.pip_install_args(&specs), expected);

        // The `-r` path installs the resolved specs, not the user's file.
        let user_reqs = tmp.path().join("user-reqs.txt");
        std::fs::write(&user_reqs, "numpy\n").unwrap();
        let specs = policy
            .resolve_requirements_file(user_reqs.to_str().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(policy.pip_install_args(&specs), expected);
    }

    #[test]
    fn pip_install_args_hashed_installs_the_whole_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("allowlist.txt");
        std::fs::write(&path, HASHED).unwrap();
        let policy = PipPolicy::from_config(Some(&path)).unwrap();

        let expected = vec![
            OsString::from("-r"),
            path.clone().into_os_string(),
            OsString::from("--require-hashes"),
        ];
        let specs = policy.resolve_packages(&["pandas".to_string()]).unwrap();
        assert_eq!(policy.pip_install_args(&specs), expected);

        let user_reqs = tmp.path().join("user-reqs.txt");
        std::fs::write(&user_reqs, "pandas\n").unwrap();
        let specs = policy
            .resolve_requirements_file(user_reqs.to_str().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(policy.pip_install_args(&specs), expected);
    }

    #[test]
    fn requirements_allows_approved_entries() {
        let list = approved("numpy==2.1.3\npandas==2.2.0\n");
        let reqs = "# a comment\n\nnumpy==2.1.3\npandas  # inline comment\n";
        assert_eq!(
            list.resolve_requirements(reqs).unwrap(),
            vec!["numpy==2.1.3".to_string(), "pandas==2.2.0".to_string()]
        );
        assert!(list.resolve_requirements("# nothing\n").unwrap().is_empty());
    }

    #[test]
    fn requirements_rejects_unapproved_package() {
        let list = approved("numpy==2.1.3\n");
        let err = list.resolve_requirements("scipy==1.11.0\n").unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::PackageNotAllowlisted { .. }
        ));
    }

    #[test]
    fn requirements_rejects_wrong_version() {
        let list = approved("numpy==2.1.3\n");
        let err = list.resolve_requirements("numpy==2.5.3\n").unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::PackageVersionNotAllowlisted { .. }
        ));
    }

    #[test]
    fn requirements_rejects_urls_vcs_paths_and_options() {
        let list = approved("numpy==2.1.3\n");
        for bad in [
            "https://example.com/evil-1.0-py3-none-any.whl",
            "git+https://github.com/foo/bar.git",
            "-e .",
            "--index-url https://evil.example.com/simple",
            "-r other-requirements.txt",
            "./local-package",
            "/abs/path/pkg",
            "evilpkg @ https://example.com/evil.whl",
            "numpy==2.1.3; python_version < '3.12'",
            "numpy==2.1.3 \\",
        ] {
            let err = list.resolve_requirements(&format!("{bad}\n")).unwrap_err();
            assert!(
                matches!(
                    err,
                    PluginEnvironmentError::RequirementsEntryNotAllowed { .. }
                ),
                "expected rejection for {bad:?}, got {err:?}"
            );
        }
    }

    #[test]
    fn requirements_without_allowlist_is_noop() {
        // No allowlist configured => the file is not read here and pip gets it
        // as `-r` unchanged.
        let policy = PipPolicy::unrestricted();
        assert!(
            policy
                .resolve_requirements_file("/nonexistent/requirements.txt")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn requirements_file_unreadable_reports_error() {
        let policy = PipPolicy {
            allowlist: Some(approved("numpy==2.1.3\n")),
        };
        let err = policy
            .resolve_requirements_file("/nonexistent/requirements.txt")
            .unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::RequirementsFileUnreadable { .. }
        ));
    }

    #[test]
    fn install_requirements_with_no_entries_is_a_noop() {
        // pip is never run, matching pip's own handling of an empty `-r` file.
        let tmp = TempDir::new().unwrap();
        let user_reqs = tmp.path().join("user-reqs.txt");
        std::fs::write(&user_reqs, "# nothing to install\n").unwrap();
        let manager = PipManager::with_policy(Arc::new(PipPolicy {
            allowlist: Some(approved("numpy==2.1.3\n")),
        }));
        assert!(
            manager
                .install_requirements(user_reqs.to_str().unwrap().to_string())
                .is_ok()
        );
    }

    /// Exercises the full `get_or_init_venv` → `ready` → `determine_package_manager`
    /// contract, including idempotency. The process-wide `VENV_PATH`/`VENV_BUILD`
    /// statics mean only one test in this binary can drive the lifecycle, so every
    /// state-touching contract is checked together here.
    #[test]
    fn venv_handle_lifecycle_and_idempotency() {
        let tmp = TempDir::new().unwrap();
        let first_path = tmp.path().join("first_venv");
        let second_path = tmp.path().join("second_venv");

        // First call kicks off the build; `ready` blocks until it finishes.
        let first_ready = get_or_init_venv(first_path.clone())
            .ready()
            .expect("first venv build should succeed");
        assert_eq!(first_ready.path, first_path);
        assert!(
            is_valid_venv(&first_path),
            "first venv should exist on disk after ready()"
        );

        // The probe runs against the just-built venv; `python -m venv` bootstraps
        // pip via ensurepip, so a freshly built venv should yield `PipManager`.
        let manager = first_ready.determine_package_manager(Arc::new(PipPolicy::unrestricted()));
        let manager_debug = format!("{manager:?}");
        assert!(
            manager_debug.contains("PipManager"),
            "expected PipManager from a fresh venv, got: {manager_debug}"
        );

        // Second call with a *different* path: idempotent. `ready` returns the
        // cached result and never tries to build at `second_path`.
        let second_ready = get_or_init_venv(second_path.clone())
            .ready()
            .expect("second call should reuse the cached build");
        assert_eq!(
            second_ready.path, first_path,
            "second call must observe the cached path, not the new argument"
        );
        assert!(
            !second_path.exists(),
            "no second build should have happened at second_path"
        );
    }

    #[test]
    fn disabled_package_manager_rejects_uninstall() {
        // Disabling package management must also block removal, not just install,
        // so the install/uninstall API surface is uniformly gated.
        let err = DisabledPackageManager
            .uninstall_packages(vec!["numpy".to_string()])
            .unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::PackageInstallationDisabled
        ));
    }

    #[test]
    fn disabled_manager_rejects_uninstall() {
        let err = DisabledManager
            .uninstall_packages(vec!["numpy".to_string()])
            .unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::PluginEnvironmentDisabled
        ));
    }
}
