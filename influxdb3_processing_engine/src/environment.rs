use crate::environment::PluginEnvironmentError::PluginEnvironmentDisabled;

use crate::virtualenv::{VenvError, find_python, initialize_venv};
use observability_deps::tracing::debug;
use pyo3::prelude::PyAnyMethods;
use pyo3::{PyResult, Python};
use std::collections::{BTreeMap, BTreeSet};
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

    #[error("'{name}=={requested}' is not supported. Allowlisted version(s): {allowlisted}.")]
    PackageVersionNotAllowlisted {
        name: String,
        requested: String,
        allowlisted: String,
    },

    #[error(
        "'{name}' must be installed with a pinned version, e.g. '{name}=={example_version}'."
    )]
    PackageVersionRequired {
        name: String,
        example_version: String,
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

    #[error(
        "--plugin-package-constraints-mode require-hashes requires --plugin-package-constraints to be set"
    )]
    RequireHashesWithoutConstraints,

    #[error("failed to read requirements file '{path}': {source}")]
    RequirementsFileUnreadable {
        path: String,
        source: std::io::Error,
    },

    #[error(
        "requirements file entry on line {line_number} is not allowed: {reason} ('{line}')"
    )]
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

/// The set of packages an operator has approved for end-user installation,
/// keyed by PEP 503 normalized name. Each name maps to the set of exact
/// versions approved for it.
#[derive(Debug, Clone, Default)]
pub struct AllowlistedPackages {
    packages: BTreeMap<String, BTreeSet<String>>,
}

impl AllowlistedPackages {
    /// Build an approved-packages set from a file. Each non-blank, non-comment
    /// line must be a pinned `name==version` spec. Returns
    /// `AllowlistFileUnreadable` if the file cannot be read and
    /// `AllowlistFileInvalidEntry` on the first malformed line.
    pub fn from_file(path: &Path) -> Result<Self, PluginEnvironmentError> {
        let contents = std::fs::read_to_string(path).map_err(|source| {
            PluginEnvironmentError::AllowlistFileUnreadable {
                path: path.display().to_string(),
                source,
            }
        })?;
        Self::from_str_lines(&contents, &path.display().to_string())
    }

    /// Parse approved-packages contents. Blank lines and lines whose first
    /// non-whitespace character is `#` are skipped. Every other line must be a
    /// pinned `name==version`; an unpinned or malformed entry is rejected.
    pub fn from_str_lines(
        contents: &str,
        path_for_errors: &str,
    ) -> Result<Self, PluginEnvironmentError> {
        let mut packages: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (index, raw_line) in contents.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (name, version) = parse_spec(line);
            let invalid = |reason: &str| PluginEnvironmentError::AllowlistFileInvalidEntry {
                path: path_for_errors.to_string(),
                line_number: index + 1,
                line: line.to_string(),
                reason: reason.to_string(),
            };
            let normalized = normalize_package_name(&name);
            if normalized.is_empty() {
                return Err(invalid("missing package name"));
            }
            match version {
                Some(version) if !version.is_empty() => {
                    packages.entry(normalized).or_default().insert(version);
                }
                _ => {
                    return Err(invalid(
                        "entries must be pinned as 'name==version'",
                    ));
                }
            }
        }
        Ok(Self { packages })
    }

    /// The approved versions for a normalized package name.
    fn allowlisted_versions(&self, normalized_name: &str) -> Option<&BTreeSet<String>> {
        self.packages.get(normalized_name)
    }

    /// Validate a single user-supplied spec against the approved list. Returns
    /// the specific rejection error when the package/version is not approved.
    pub fn validate_spec(&self, spec: &str) -> Result<(), PluginEnvironmentError> {
        let (name, version) = parse_spec(spec);
        let normalized = normalize_package_name(&name);
        let Some(allowlisted_versions) = self.allowlisted_versions(&normalized) else {
            return Err(PluginEnvironmentError::PackageNotAllowlisted {
                name: name.trim().to_string(),
            });
        };
        match version {
            None => Err(PluginEnvironmentError::PackageVersionRequired {
                name: name.trim().to_string(),
                example_version: allowlisted_versions
                    .iter()
                    .next()
                    .cloned()
                    .unwrap_or_default(),
            }),
            Some(version) if allowlisted_versions.contains(&version) => Ok(()),
            Some(version) => Err(PluginEnvironmentError::PackageVersionNotAllowlisted {
                name: name.trim().to_string(),
                requested: version,
                allowlisted: allowlisted_versions
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", "),
            }),
        }
    }

    /// Validate every spec, failing on the first rejected one.
    pub fn validate_all(&self, specs: &[String]) -> Result<(), PluginEnvironmentError> {
        for spec in specs {
            self.validate_spec(spec)?;
        }
        Ok(())
    }
}

/// The server-side policy applied to every pip install: an optional approved
/// package list, an optional constraints file, and whether to require hashes.
/// A [`PipManager`] holds one of these and consults it before shelling out to
/// pip.
#[derive(Debug, Clone, Default)]
pub struct PipPolicy {
    /// When present, `install_packages` specs are validated against this list.
    pub allowlist: Option<AllowlistedPackages>,
    /// When present, passed to pip as `-c <file>`.
    pub constraints_file: Option<PathBuf>,
    /// When true, pip is invoked with `--require-hashes`.
    pub require_hashes: bool,
}

impl PipPolicy {
    /// A policy that imposes no restrictions: no approved list, no constraints
    /// file, no hash requirement. Matches the historical behavior.
    pub fn unrestricted() -> Self {
        Self::default()
    }

    /// Build a policy from operator config. The approved-packages file is read
    /// and parsed early so a bad file fails fast at startup rather than on the
    /// first install request.
    ///
    /// `require_hashes` requires a `constraints_file`, since the hashes pip
    /// verifies must come from it; that combination is rejected here so a
    /// misconfiguration fails at startup rather than on the first install.
    pub fn from_config(
        allowlist_file: Option<&Path>,
        constraints_file: Option<&Path>,
        require_hashes: bool,
    ) -> Result<Self, PluginEnvironmentError> {
        if require_hashes && constraints_file.is_none() {
            return Err(PluginEnvironmentError::RequireHashesWithoutConstraints);
        }
        let allowlist = match allowlist_file {
            Some(path) => Some(AllowlistedPackages::from_file(path)?),
            None => None,
        };
        Ok(Self {
            allowlist,
            constraints_file: constraints_file.map(Path::to_path_buf),
            require_hashes,
        })
    }

    /// Whether an approved-packages allowlist is configured. Used to decide
    /// whether the requirements (`-r`) path must be validated.
    fn has_allowlist(&self) -> bool {
        self.allowlist.is_some()
    }

    /// Validate every entry of a requirements file against the allowlist. Called
    /// for the `-r requirements.txt` path so it cannot bypass the allowlist that
    /// `install_packages` enforces.
    ///
    /// Only simple pinned `name==version` requirement lines are permitted. Lines
    /// that pip would treat as something other than a plain pinned requirement —
    /// URLs, VCS refs, local paths, editable installs (`-e`), index options
    /// (`-i` / `--index-url` / `--extra-index-url`), nested requirement includes
    /// (`-r` / `-c`), and line continuations — are rejected, because they could
    /// pull code the allowlist never approved. Blank lines and `#` comments are
    /// skipped. Each surviving line is validated with
    /// [`AllowlistedPackages::validate_spec`], so version/pinning rules match the
    /// `install_packages` path exactly.
    fn validate_requirements_contents(
        &self,
        contents: &str,
    ) -> Result<(), PluginEnvironmentError> {
        let Some(approved) = &self.allowlist else {
            return Ok(());
        };
        for (index, raw_line) in contents.lines().enumerate() {
            let line_number = index + 1;
            // Drop inline comments (` #...`) and surrounding whitespace.
            let without_comment = match raw_line.split_once(" #") {
                Some((head, _)) => head,
                None => raw_line,
            };
            let line = without_comment.trim();
            if line.is_empty() || line.starts_with('#') {
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
            // allowlist; strip markers only after the pin is validated. Reject
            // for now to keep the gate strict.
            if line.contains(';') {
                return Err(reject("environment markers are not allowed"));
            }

            // Whatever remains must be a plain pinned `name==version` that the
            // allowlist approves.
            approved.validate_spec(line)?;
        }
        Ok(())
    }

    /// Validate install specs against the approved list, if one is configured.
    /// A policy with no approved list approves everything.
    fn validate_packages(&self, packages: &[String]) -> Result<(), PluginEnvironmentError> {
        match &self.allowlist {
            Some(approved) => approved.validate_all(packages),
            None => Ok(()),
        }
    }

    /// Validate a requirements file (by path) against the allowlist. Reads the
    /// file server-side so a client-supplied `-r` path cannot bypass the
    /// allowlist. A policy with no allowlist performs no check.
    fn validate_requirements_file(&self, path: &str) -> Result<(), PluginEnvironmentError> {
        if !self.has_allowlist() {
            return Ok(());
        }
        let contents = std::fs::read_to_string(path).map_err(|source| {
            PluginEnvironmentError::RequirementsFileUnreadable {
                path: path.to_string(),
                source,
            }
        })?;
        self.validate_requirements_contents(&contents)
    }

    /// The extra arguments to append to a `pip install` invocation for this
    /// policy: `-c <constraints_file>` and/or `--require-hashes`.
    fn pip_extra_args(&self) -> Vec<OsString> {
        let mut args = Vec::new();
        if let Some(constraints_file) = &self.constraints_file {
            args.push(OsString::from("-c"));
            args.push(constraints_file.clone().into_os_string());
        }
        if self.require_hashes {
            args.push(OsString::from("--require-hashes"));
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
    /// A `PipManager` with no restrictions (no approved list, constraints, or
    /// hash requirement). Matches the historical behavior.
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
        self.policy.validate_packages(&packages)?;

        let python_exe = find_python();
        let output = Command::new(python_exe)
            .arg("-m")
            .arg("pip")
            .arg("install")
            .args(&packages)
            .args(self.policy.pip_extra_args())
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
        self.policy.validate_requirements_file(&requirements_path)?;

        let python_exe = find_python();
        let output = Command::new(python_exe)
            .arg("-m")
            .arg("pip")
            .args(["install", "-r", &requirements_path])
            .args(self.policy.pip_extra_args())
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
        AllowlistedPackages::from_str_lines(contents, "test").expect("valid approved list")
    }

    #[test]
    fn approved_packages_parses_and_skips_blanks_and_comments() {
        let list = approved(
            "# a comment\n\nnumpy==2.1.3\nnumpy==2.0.0\n  # indented comment\nPandas==2.2.0\n",
        );
        // Two versions of numpy collapse under one normalized key.
        assert!(list.validate_spec("numpy==2.1.3").is_ok());
        assert!(list.validate_spec("numpy==2.0.0").is_ok());
        // Name normalization: `Pandas` approved as `pandas`.
        assert!(list.validate_spec("pandas==2.2.0").is_ok());
    }

    #[test]
    fn approved_packages_rejects_unpinned_entry() {
        let err = AllowlistedPackages::from_str_lines("numpy\n", "test").unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::AllowlistFileInvalidEntry { line_number: 1, .. }
        ));
    }

    #[test]
    fn approved_packages_rejects_range_operator_entry() {
        let err = AllowlistedPackages::from_str_lines("numpy>=2.0\n", "test").unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::AllowlistFileInvalidEntry { .. }
        ));
    }

    #[test]
    fn validate_spec_not_on_list() {
        let list = approved("numpy==2.1.3\n");
        let err = list.validate_spec("scipy==1.0.0").unwrap_err();
        match err {
            PluginEnvironmentError::PackageNotAllowlisted { name } => assert_eq!(name, "scipy"),
            other => panic!("expected PackageNotAllowlisted, got {other:?}"),
        }
    }

    #[test]
    fn validate_spec_wrong_version() {
        let list = approved("numpy==2.1.3\nnumpy==2.0.0\n");
        let err = list.validate_spec("numpy==1.0.0").unwrap_err();
        match err {
            PluginEnvironmentError::PackageVersionNotAllowlisted {
                name,
                requested,
                allowlisted,
            } => {
                assert_eq!(name, "numpy");
                assert_eq!(requested, "1.0.0");
                // BTreeSet ordering makes the allowlisted list deterministic.
                assert_eq!(allowlisted, "2.0.0, 2.1.3");
            }
            other => panic!("expected PackageVersionNotAllowlisted, got {other:?}"),
        }
    }

    #[test]
    fn validate_spec_missing_version() {
        let list = approved("numpy==2.1.3\n");
        let err = list.validate_spec("numpy").unwrap_err();
        match err {
            PluginEnvironmentError::PackageVersionRequired {
                name,
                example_version,
            } => {
                assert_eq!(name, "numpy");
                assert_eq!(example_version, "2.1.3");
            }
            other => panic!("expected PackageVersionRequired, got {other:?}"),
        }
    }

    #[test]
    fn validate_spec_normalizes_requested_name() {
        let list = approved("foo-bar==1.0.0\n");
        // Underscore/case variant of an approved package validates.
        assert!(list.validate_spec("Foo_Bar==1.0.0").is_ok());
    }

    #[test]
    fn validate_all_fails_on_first_bad_spec() {
        let list = approved("numpy==2.1.3\n");
        let err = list
            .validate_all(&["numpy==2.1.3".to_string(), "scipy==1.0.0".to_string()])
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
        assert!(policy.validate_packages(&["anything==9.9.9".to_string()]).is_ok());
        assert!(policy.pip_extra_args().is_empty());
    }

    #[test]
    fn pip_policy_from_config_reads_approved_list_and_builds_args() {
        let tmp = TempDir::new().unwrap();
        let approved_path = tmp.path().join("approved.txt");
        std::fs::write(&approved_path, "numpy==2.1.3\n").unwrap();
        let constraints_path = tmp.path().join("constraints.txt");
        std::fs::write(&constraints_path, "numpy==2.1.3 --hash=sha256:abc\n").unwrap();

        let policy = PipPolicy::from_config(
            Some(&approved_path),
            Some(&constraints_path),
            true,
        )
        .expect("policy builds from valid config");

        // Approved list is enforced.
        assert!(policy.validate_packages(&["numpy==2.1.3".to_string()]).is_ok());
        assert!(policy.validate_packages(&["scipy==1.0.0".to_string()]).is_err());

        // Extra args carry the constraints file and --require-hashes.
        let args = policy.pip_extra_args();
        assert_eq!(args[0], std::ffi::OsString::from("-c"));
        assert_eq!(args[1], constraints_path.clone().into_os_string());
        assert_eq!(args[2], std::ffi::OsString::from("--require-hashes"));
    }

    #[test]
    fn pip_policy_from_config_fails_fast_on_bad_approved_file() {
        let tmp = TempDir::new().unwrap();
        let approved_path = tmp.path().join("approved.txt");
        std::fs::write(&approved_path, "numpy\n").unwrap();
        let err = PipPolicy::from_config(Some(&approved_path), None, false).unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::AllowlistFileInvalidEntry { .. }
        ));
    }

    #[test]
    fn pip_policy_require_hashes_without_constraints_fails_fast() {
        let err = PipPolicy::from_config(None, None, true).unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::RequireHashesWithoutConstraints
        ));
    }

    #[test]
    fn pip_policy_require_hashes_with_constraints_ok() {
        let tmp = TempDir::new().unwrap();
        let constraints = tmp.path().join("constraints.txt");
        std::fs::write(&constraints, "numpy==2.1.3 --hash=sha256:abc\n").unwrap();
        let policy = PipPolicy::from_config(None, Some(&constraints), true)
            .expect("require-hashes with a constraints file is valid");
        let args = policy.pip_extra_args();
        assert!(args.contains(&std::ffi::OsString::from("--require-hashes")));
    }

    fn policy_with_allowlist(contents: &str) -> PipPolicy {
        PipPolicy {
            allowlist: Some(approved(contents)),
            constraints_file: None,
            require_hashes: false,
        }
    }

    #[test]
    fn requirements_allows_approved_pinned_entries() {
        let policy = policy_with_allowlist("numpy==2.1.3\npandas==2.2.0\n");
        let reqs = "# a comment\n\nnumpy==2.1.3\npandas==2.2.0  # inline comment\n";
        assert!(policy.validate_requirements_contents(reqs).is_ok());
    }

    #[test]
    fn requirements_rejects_unapproved_package() {
        let policy = policy_with_allowlist("numpy==2.1.3\n");
        let err = policy
            .validate_requirements_contents("scipy==1.11.0\n")
            .unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::PackageNotAllowlisted { .. }
        ));
    }

    #[test]
    fn requirements_rejects_wrong_version() {
        let policy = policy_with_allowlist("numpy==2.1.3\n");
        let err = policy
            .validate_requirements_contents("numpy==2.5.3\n")
            .unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::PackageVersionNotAllowlisted { .. }
        ));
    }

    #[test]
    fn requirements_rejects_unpinned_entry() {
        let policy = policy_with_allowlist("numpy==2.1.3\n");
        let err = policy
            .validate_requirements_contents("numpy\n")
            .unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::PackageVersionRequired { .. }
        ));
    }

    #[test]
    fn requirements_rejects_urls_vcs_paths_and_options() {
        let policy = policy_with_allowlist("numpy==2.1.3\n");
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
            let err = policy
                .validate_requirements_contents(&format!("{bad}\n"))
                .unwrap_err();
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
        // No allowlist configured => the requirements path is not gated here.
        let policy = PipPolicy::unrestricted();
        assert!(
            policy
                .validate_requirements_contents("anything==9.9.9\n-e .\n")
                .is_ok()
        );
    }

    #[test]
    fn requirements_file_unreadable_reports_error() {
        let policy = policy_with_allowlist("numpy==2.1.3\n");
        let err = policy
            .validate_requirements_file("/nonexistent/requirements.txt")
            .unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::RequirementsFileUnreadable { .. }
        ));
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
