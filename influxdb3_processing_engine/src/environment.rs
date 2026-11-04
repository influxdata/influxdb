use crate::environment::PluginEnvironmentError::PluginEnvironmentDisabled;

use crate::allowlist::{
    AllowlistMode, PackageAllowlist, logical_lines, parse_named_requirement,
    split_requirement_options,
};
use crate::virtualenv::{VenvError, find_python, initialize_venv};
use influxdb3_types::http::{BrokenDependent, ProcessingEngineUninstallPackagesResponse};
use observability_deps::tracing::{debug, info, warn};
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
    #[error("'{command}' failed: {stderr}")]
    PipCommandFailed { command: String, stderr: String },
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

    #[error(
        "'{spec}' is not allowed. Allowlisted version(s) of '{name}': {allowlisted}. Contact your \
         administrator to request a different version."
    )]
    PackageVersionNotAllowlisted {
        spec: String,
        name: String,
        allowlisted: String,
    },

    #[error("'{spec}' is not allowed: {reason}")]
    PackageSpecNotAllowed { spec: String, reason: String },

    #[error("'{name}' cannot be uninstalled: {reason}")]
    UninstallNotAllowed { name: String, reason: String },

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

    #[error("package allowlist file '{path}' lists no packages")]
    AllowlistEmpty { path: String },

    #[error(
        "package allowlist file '{path}' mixes modes: line {hashed_line} uses hashes, but line \
         {unhashed_line} has none ('{line}'). Use version ranges with no hashes, or hash every \
         entry (e.g. with 'uv pip compile --generate-hashes')."
    )]
    AllowlistMixedModes {
        path: String,
        hashed_line: usize,
        unhashed_line: usize,
        line: String,
    },

    #[error(
        "a hashed --plugin-package-allowlist requires pip 26.1 or later in the plugin \
         environment '{venv}' ({found}). Upgrade it with \
         '{python} -m pip install --upgrade \"pip>=26.1\"', or use an allowlist of version ranges."
    )]
    PipTooOldForHashedAllowlist {
        venv: String,
        python: String,
        found: String,
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
/// `-`, `_`, or `.` into a single `-`, so `Foo_Bar` and `foo-bar` are treated
/// as the same package.
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

/// Split a package spec into its name and optional pinned version.
///
/// Only the exact-pin form `name==version` is recognized as carrying a version;
/// any other spec is returned whole as the name with `version = None`. The
/// returned name is *not* normalized; callers normalize as needed.
pub fn parse_spec(spec: &str) -> (String, Option<String>) {
    let spec = spec.trim();
    if let Some((name, version)) = spec.split_once("==") {
        (name.trim().to_string(), Some(version.trim().to_string()))
    } else {
        (spec.to_string(), None)
    }
}

/// The server-side policy applied to every pip install: the operator's
/// optional package allowlist. A [`PipManager`] holds one of these and
/// consults it before shelling out to pip.
#[derive(Debug, Clone, Default)]
pub struct PipPolicy {
    /// When present, requests are checked against it and it is passed to pip
    /// as `-c <file>`.
    pub allowlist: Option<PackageAllowlist>,
}

impl PipPolicy {
    /// A policy that imposes no restrictions. Matches the historical behavior.
    pub fn unrestricted() -> Self {
        Self::default()
    }

    /// Build a policy from operator config. The allowlist file is read and
    /// validated here so an invalid allowlist fails startup rather than the
    /// first install request.
    pub fn from_config(allowlist_file: Option<&Path>) -> Result<Self, PluginEnvironmentError> {
        let allowlist = allowlist_file
            .map(PackageAllowlist::from_file)
            .transpose()?;
        if let Some(allowlist) = &allowlist {
            info!(
                path = %allowlist.path().display(),
                mode = %allowlist.mode(),
                packages = allowlist.package_count(),
                "plugin package allowlist loaded"
            );
        }
        Ok(Self { allowlist })
    }

    /// Whether the allowlist is hashed, so pip runs with `--require-hashes`
    /// and must be new enough to read hashes from `-c`.
    fn is_hashed(&self) -> bool {
        self.allowlist
            .as_ref()
            .is_some_and(|allowlist| allowlist.mode() == AllowlistMode::Hashed)
    }

    /// Check one user-supplied spec against the allowlist. The spec must be a
    /// plain PEP 508 requirement naming a package by version, so it cannot
    /// carry pip options or a URL that would change where pip fetches from,
    /// and the package must be allowed by the allowlist.
    fn validate_spec(
        allowlist: &PackageAllowlist,
        spec: &str,
    ) -> Result<(), PluginEnvironmentError> {
        let requirement = parse_named_requirement(spec).map_err(|reason| {
            PluginEnvironmentError::PackageSpecNotAllowed {
                spec: spec.trim().to_string(),
                reason,
            }
        })?;
        allowlist.check(&requirement, spec)
    }

    /// Validate install specs. A spec starting with `-` is always refused,
    /// because pip would read it as an option (`-r <file>`, `--target <dir>`,
    /// `--python <exe>`, …) rather than a package. With an allowlist
    /// configured, each spec must also be a plain PEP 508 requirement that the
    /// allowlist allows.
    fn validate_packages(&self, packages: &[String]) -> Result<(), PluginEnvironmentError> {
        for spec in packages {
            if spec.trim_start().starts_with('-') {
                return Err(PluginEnvironmentError::PackageSpecNotAllowed {
                    spec: spec.trim().to_string(),
                    reason: "pip options are not allowed".to_string(),
                });
            }
            if let Some(allowlist) = &self.allowlist {
                Self::validate_spec(allowlist, spec)?;
            }
        }
        Ok(())
    }

    /// Validate every entry of a user requirements file against the
    /// allowlist, so the `-r requirements.txt` path cannot bypass it.
    ///
    /// Only plain PEP 508 requirements are permitted. pip options of any kind
    /// (index options, nested `-r`/`-c`, `-e`, `--hash`, …), URLs, and local
    /// paths are rejected, because they could change where pip fetches from or
    /// pull code the allowlist never approved.
    fn validate_requirements_contents(&self, contents: &str) -> Result<(), PluginEnvironmentError> {
        let Some(allowlist) = &self.allowlist else {
            return Ok(());
        };
        for line in logical_lines(contents) {
            let reject = |reason: String| PluginEnvironmentError::RequirementsEntryNotAllowed {
                line_number: line.line_number,
                line: line.text.clone(),
                reason,
            };
            let (requirement_text, options) = split_requirement_options(&line.text);
            if requirement_text.is_empty() || !options.is_empty() {
                return Err(reject(
                    "pip options (-e, -i, --index-url, -r, -c, --hash, …) are not allowed"
                        .to_string(),
                ));
            }
            let requirement = parse_named_requirement(&requirement_text).map_err(reject)?;
            allowlist.check(&requirement, &requirement_text)?;
        }
        Ok(())
    }

    /// Validate an uninstall request. Each entry may be a bare name or a
    /// `name==version` spec; pip uninstalls whatever version is present.
    ///
    /// The allowlist only limits new installs, so it is not consulted here.
    /// Two entries are always refused:
    ///
    /// * `pip` itself: removing it leaves the venv unable to install anything
    ///   until it is rebuilt.
    /// * Entries starting with `-`, so a request cannot inject pip options
    ///   such as `-r <file>`.
    fn validate_uninstall(&self, packages: &[String]) -> Result<(), PluginEnvironmentError> {
        for spec in packages {
            let spec = spec.trim();
            let reject = |reason: &str| PluginEnvironmentError::UninstallNotAllowed {
                name: spec.to_string(),
                reason: reason.to_string(),
            };
            if spec.starts_with('-') {
                return Err(reject("pip options are not allowed"));
            }
            let (name, _version) = parse_spec(spec);
            if normalize_package_name(&name) == "pip" {
                return Err(reject("pip is required by the plugin environment"));
            }
        }
        Ok(())
    }

    /// Validate a requirements file (by path) against the allowlist. Reads the
    /// file server-side so a client-supplied `-r` path cannot bypass the
    /// allowlist. A policy with no allowlist performs no check.
    fn validate_requirements_file(&self, path: &str) -> Result<(), PluginEnvironmentError> {
        if self.allowlist.is_none() {
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

    /// Build the argument list that follows `pip install` for a request naming
    /// `specs` (already allowlist-validated).
    ///
    /// The user's specs drive the install; the allowlist only constrains it.
    /// It is passed as `-c <file>`, so pip applies its versions and its index
    /// and build options (`--index-url`, `--only-binary`, …) but installs
    /// nothing the user did not ask for or need.
    ///
    /// A hashed allowlist also adds `--require-hashes`. pip 26.1 and later
    /// take a requirement's hashes from a matching `-c` entry
    /// (pypa/pip#8792, pypa/pip#9243), so every package installed, including
    /// dependencies, must be in the allowlist with a matching hash.
    fn pip_install_args(&self, specs: &[String]) -> Vec<OsString> {
        let mut args: Vec<OsString> = specs.iter().map(OsString::from).collect();
        self.push_allowlist_args(&mut args);
        args
    }

    /// Build the argument list that follows `pip install` for the requirements
    /// (`-r`) path. Mirrors [`PipPolicy::pip_install_args`].
    fn pip_install_requirements_args(&self, user_requirements_path: &str) -> Vec<OsString> {
        let mut args = vec![OsString::from("-r"), OsString::from(user_requirements_path)];
        self.push_allowlist_args(&mut args);
        args
    }

    /// Append `-c <allowlist>` if configured, and `--require-hashes` for a
    /// hashed allowlist.
    fn push_allowlist_args(&self, args: &mut Vec<OsString>) {
        if let Some(allowlist) = &self.allowlist {
            args.push(OsString::from("-c"));
            args.push(allowlist.path().as_os_str().to_os_string());
        }
        if self.is_hashed() {
            args.push(OsString::from("--require-hashes"));
        }
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

    /// Uninstall `packages`. On success, reports installed packages that still
    /// require one of the removed packages, so the caller can warn that they
    /// need to be reinstalled.
    fn uninstall_packages(
        &self,
        packages: Vec<String>,
    ) -> Result<ProcessingEngineUninstallPackagesResponse, PluginEnvironmentError>;
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
        if self.policy.is_hashed() {
            check_pip_supports_constraint_hashes(&venv_path)?;
        }
        Ok(())
    }

    fn install_packages(&self, packages: Vec<String>) -> Result<(), PluginEnvironmentError> {
        // Enforce the allowlist before shelling out to pip so a package it does
        // not allow never reaches the network.
        self.policy.validate_packages(&packages)?;

        let python_exe = find_python();
        let output = Command::new(python_exe)
            .arg("-m")
            .arg("pip")
            .arg("install")
            .args(self.policy.pip_install_args(&packages))
            .output()?;
        if !output.status.success() {
            return Err(pip_command_failed("pip install", &output));
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
            .arg("install")
            .args(
                self.policy
                    .pip_install_requirements_args(&requirements_path),
            )
            .output()?;
        if !output.status.success() {
            return Err(pip_command_failed("pip install -r", &output));
        }
        Ok(())
    }

    fn uninstall_packages(
        &self,
        packages: Vec<String>,
    ) -> Result<ProcessingEngineUninstallPackagesResponse, PluginEnvironmentError> {
        // Refuse protected names and pip options before shelling out to pip.
        self.policy.validate_uninstall(&packages)?;
        // Treats an already-absent package as a warning with a zero exit
        // so uninstall is idempotent.
        let python_exe = find_python();
        let output = Command::new(&python_exe)
            .arg("-m")
            .arg("pip")
            .arg("uninstall")
            .arg("-y")
            .args(&packages)
            .output()?;
        if !output.status.success() {
            return Err(pip_command_failed("pip uninstall", &output));
        }

        // The uninstall succeeded; find installed packages that still need
        // what was removed. `pip check` exits non-zero when it finds any
        // problem, so its exit status is not an error here. A failure to run
        // it only loses the warning, so it is logged rather than returned.
        let removed: BTreeSet<String> = packages
            .iter()
            .map(|spec| normalize_package_name(&parse_spec(spec).0))
            .collect();
        let broken_dependents = match Command::new(&python_exe)
            .arg("-m")
            .arg("pip")
            .arg("check")
            .output()
        {
            Ok(check) => {
                broken_dependents_from_pip_check(&String::from_utf8_lossy(&check.stdout), &removed)
            }
            Err(error) => {
                warn!(%error, "failed to run pip check after uninstall");
                Vec::new()
            }
        };
        if !broken_dependents.is_empty() {
            warn!(
                ?broken_dependents,
                "uninstall left installed packages without a required dependency"
            );
        }
        Ok(ProcessingEngineUninstallPackagesResponse { broken_dependents })
    }
}

/// A failed pip invocation, with its stderr trimmed for the error message.
fn pip_command_failed(command: &str, output: &std::process::Output) -> PluginEnvironmentError {
    PluginEnvironmentError::PipCommandFailed {
        command: command.to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    }
}

/// Pick out the packages that `pip check` reports as missing one of the
/// `removed` packages. pip prints one line per problem, e.g.
/// `python-dateutil 2.9.0.post0 requires six, which is not installed.`; lines
/// about other problems (version conflicts, packages missing for unrelated
/// reasons) are ignored so the warning only covers this uninstall.
fn broken_dependents_from_pip_check(
    output: &str,
    removed: &BTreeSet<String>,
) -> Vec<BrokenDependent> {
    let mut by_package: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for line in output.lines() {
        let Some(rest) = line.trim().strip_suffix(", which is not installed.") else {
            continue;
        };
        let Some((dependent, missing)) = rest.split_once(" requires ") else {
            continue;
        };
        let missing = normalize_package_name(missing.trim());
        if !removed.contains(&missing) {
            continue;
        }
        // `dependent` is "<name> <version>"; report it as a reinstallable spec.
        let package = match dependent.trim().rsplit_once(' ') {
            Some((name, version)) => format!("{name}=={version}"),
            None => dependent.trim().to_string(),
        };
        by_package.entry(package).or_default().insert(missing);
    }
    by_package
        .into_iter()
        .map(|(package, missing)| BrokenDependent {
            package,
            missing: missing.into_iter().collect(),
        })
        .collect()
}

/// The oldest pip that reads hashes from a `-c` constraints file
/// (pip 26.1, pypa/pip#8792, pypa/pip#9243). Hashed allowlists depend on it.
const MIN_PIP_FOR_CONSTRAINT_HASHES: (u64, u64) = (26, 1);

/// The interpreter inside a venv. A venv always contains `bin/python`;
/// `python3` is only a symlink that may be absent.
fn venv_python(venv_path: &Path) -> PathBuf {
    cfg_select! {
        windows => venv_path.join("Scripts").join("python.exe"),
        _ => venv_path.join("bin").join("python"),
    }
}

/// Parse the major and minor version from `pip --version` output such as
/// `pip 26.2.2 from /path/site-packages/pip (python 3.12)`. Trailing
/// pre-release text on the minor (`26.1b1`) is ignored.
fn parse_pip_version(output: &str) -> Option<(u64, u64)> {
    let version = output
        .trim()
        .strip_prefix("pip ")?
        .split_whitespace()
        .next()?;
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor_text = parts.next().unwrap_or("0");
    let digits_end = minor_text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(minor_text.len());
    let minor = minor_text[..digits_end].parse().ok()?;
    Some((major, minor))
}

/// Refuse to start with a hashed allowlist unless the venv's pip is new enough
/// to verify hashes from a `-c` file. Older pip rejects every such
/// install, so this fails startup instead of every install request. A pip that
/// cannot be run or whose version cannot be read is treated as too old.
fn check_pip_supports_constraint_hashes(venv_path: &Path) -> Result<(), PluginEnvironmentError> {
    let python = venv_python(venv_path);
    let found = match Command::new(&python)
        .args(["-m", "pip", "--version"])
        .output()
    {
        Ok(output) if output.status.success() => {
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            match parse_pip_version(&stdout) {
                Some(version) if version >= MIN_PIP_FOR_CONSTRAINT_HASHES => return Ok(()),
                Some(_) => stdout,
                None => format!("unrecognized version output '{stdout}'"),
            }
        }
        Ok(output) => format!(
            "pip --version failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ),
        Err(error) => format!("could not run pip: {error}"),
    };
    Err(PluginEnvironmentError::PipTooOldForHashedAllowlist {
        venv: venv_path.display().to_string(),
        python: python.display().to_string(),
        found,
    })
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

    fn uninstall_packages(
        &self,
        _packages: Vec<String>,
    ) -> Result<ProcessingEngineUninstallPackagesResponse, PluginEnvironmentError> {
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

    fn uninstall_packages(
        &self,
        _packages: Vec<String>,
    ) -> Result<ProcessingEngineUninstallPackagesResponse, PluginEnvironmentError> {
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

    fn uninstall_packages(
        &self,
        _packages: Vec<String>,
    ) -> Result<ProcessingEngineUninstallPackagesResponse, PluginEnvironmentError> {
        // Always succeed for tests
        Ok(ProcessingEngineUninstallPackagesResponse::default())
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
        let venv_python = venv_python(&self.path);
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
        // A non-`==` operator is left attached to the name.
        assert_eq!(parse_spec("numpy>=2.0"), ("numpy>=2.0".to_string(), None));
        assert_eq!(
            parse_spec("  numpy == 2.1.3  "),
            ("numpy".to_string(), Some("2.1.3".to_string()))
        );
    }

    const RANGES: &str = "--index-url https://pypi.org/simple\nnumpy>=2,<3\npandas~=2.2\n";
    const HASHED: &str = "--only-binary :all:\nnumpy==2.2.6 \\\n    --hash=sha256:0386\nsix==1.17.0 --hash=sha256:abcd\n";

    /// Write an allowlist file into `dir` and build a policy from it.
    fn policy_from(dir: &Path, contents: &str) -> (PipPolicy, PathBuf) {
        let path = dir.join("allowlist.txt");
        std::fs::write(&path, contents).unwrap();
        let policy = PipPolicy::from_config(Some(&path)).expect("valid allowlist");
        (policy, path)
    }

    #[test]
    fn pip_policy_unrestricted_approves_everything() {
        let policy = PipPolicy::unrestricted();
        assert!(
            policy
                .validate_packages(&[
                    "anything==9.9.9".to_string(),
                    "git+https://example.com/pkg".to_string(),
                ])
                .is_ok()
        );
        assert!(policy.validate_requirements_contents("-e .\n").is_ok());
        // With no allowlist, the only install args are the user's own specs.
        assert_eq!(
            policy.pip_install_args(&["numpy==2.1.3".to_string()]),
            vec![OsString::from("numpy==2.1.3")]
        );
    }

    #[test]
    fn validate_packages_rejects_options_without_allowlist() {
        let policy = PipPolicy::unrestricted();
        for bad in ["-r", "--python", "  --target=/tmp/x", "-e."] {
            let err = policy
                .validate_packages(&["numpy".to_string(), bad.to_string()])
                .unwrap_err();
            assert!(
                matches!(err, PluginEnvironmentError::PackageSpecNotAllowed { .. }),
                "{bad:?}: {err:?}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn pip_command_failed_reports_command_and_stderr() {
        use std::os::unix::process::ExitStatusExt;
        let output = std::process::Output {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: Vec::new(),
            stderr: b"ERROR: No matching distribution found for nope\n".to_vec(),
        };
        assert_eq!(
            pip_command_failed("pip install", &output).to_string(),
            "'pip install' failed: ERROR: No matching distribution found for nope"
        );
    }

    #[test]
    fn pip_policy_from_config_fails_fast_on_invalid_allowlist() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("allowlist.txt");
        std::fs::write(&path, "numpy>=\n").unwrap();
        assert!(matches!(
            PipPolicy::from_config(Some(&path)).unwrap_err(),
            PluginEnvironmentError::AllowlistFileInvalidEntry { .. }
        ));
        assert!(matches!(
            PipPolicy::from_config(Some(&tmp.path().join("missing.txt"))).unwrap_err(),
            PluginEnvironmentError::AllowlistFileUnreadable { .. }
        ));
    }

    #[test]
    fn ranges_policy_passes_allowlist_as_constraints() {
        let tmp = TempDir::new().unwrap();
        let (policy, path) = policy_from(tmp.path(), RANGES);
        assert!(!policy.is_hashed());
        assert_eq!(
            policy.pip_install_args(&["numpy".to_string()]),
            vec![
                OsString::from("numpy"),
                OsString::from("-c"),
                path.clone().into_os_string(),
            ]
        );
        assert_eq!(
            policy.pip_install_requirements_args("/tmp/user-reqs.txt"),
            vec![
                OsString::from("-r"),
                OsString::from("/tmp/user-reqs.txt"),
                OsString::from("-c"),
                path.into_os_string(),
            ]
        );
    }

    #[test]
    fn hashed_policy_adds_require_hashes() {
        let tmp = TempDir::new().unwrap();
        let (policy, path) = policy_from(tmp.path(), HASHED);
        assert!(policy.is_hashed());
        assert_eq!(
            policy.pip_install_args(&["numpy".to_string()]),
            vec![
                OsString::from("numpy"),
                OsString::from("-c"),
                path.clone().into_os_string(),
                OsString::from("--require-hashes"),
            ]
        );
        assert_eq!(
            policy.pip_install_requirements_args("/tmp/user-reqs.txt"),
            vec![
                OsString::from("-r"),
                OsString::from("/tmp/user-reqs.txt"),
                OsString::from("-c"),
                path.into_os_string(),
                OsString::from("--require-hashes"),
            ]
        );
    }

    #[test]
    fn validate_packages_enforces_allowlist() {
        let tmp = TempDir::new().unwrap();
        let (policy, _) = policy_from(tmp.path(), RANGES);
        policy
            .validate_packages(&["numpy".to_string(), "Pandas==2.2.3".to_string()])
            .unwrap();
        // The first rejected spec fails the request.
        let err = policy
            .validate_packages(&["numpy".to_string(), "scipy".to_string()])
            .unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::PackageNotAllowlisted { .. }
        ));
        let err = policy
            .validate_packages(&["numpy==1.26.4".to_string()])
            .unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::PackageVersionNotAllowlisted { .. }
        ));
    }

    #[test]
    fn validate_packages_rejects_options_urls_and_paths() {
        let tmp = TempDir::new().unwrap();
        let (policy, _) = policy_from(tmp.path(), RANGES);
        for bad in [
            "-r /etc/requirements.txt",
            "--index-url=https://evil.example/simple",
            "-i",
            "numpy --index-url https://evil.example/simple",
            "numpy @ https://evil.example/numpy.whl",
            "https://evil.example/numpy.whl",
            "./numpy",
            "git+https://github.com/numpy/numpy",
            "",
        ] {
            let err = policy.validate_packages(&[bad.to_string()]).unwrap_err();
            assert!(
                matches!(err, PluginEnvironmentError::PackageSpecNotAllowed { .. }),
                "{bad:?}: {err:?}"
            );
        }
    }

    #[test]
    fn requirements_allows_allowlisted_entries() {
        let tmp = TempDir::new().unwrap();
        let (policy, _) = policy_from(tmp.path(), RANGES);
        let reqs = "# a comment\n\nnumpy>=2.1\npandas==2.2.3  # inline comment\nnumpy \\\n  <2.5\n";
        policy.validate_requirements_contents(reqs).unwrap();
    }

    #[test]
    fn requirements_rejects_unlisted_and_disallowed_versions() {
        let tmp = TempDir::new().unwrap();
        let (policy, _) = policy_from(tmp.path(), HASHED);
        assert!(matches!(
            policy
                .validate_requirements_contents("scipy==1.11.0\n")
                .unwrap_err(),
            PluginEnvironmentError::PackageNotAllowlisted { .. }
        ));
        assert!(matches!(
            policy
                .validate_requirements_contents("numpy==2.2.5\n")
                .unwrap_err(),
            PluginEnvironmentError::PackageVersionNotAllowlisted { .. }
        ));
    }

    #[test]
    fn requirements_rejects_urls_vcs_paths_and_options() {
        let tmp = TempDir::new().unwrap();
        let (policy, _) = policy_from(tmp.path(), RANGES);
        for bad in [
            "https://example.com/evil-1.0-py3-none-any.whl",
            "git+https://github.com/foo/bar.git",
            "-e .",
            "--index-url https://evil.example.com/simple",
            "--extra-index-url=https://evil.example.com/simple",
            "-r other-requirements.txt",
            "-c other-constraints.txt",
            "./local-package",
            "/abs/path/pkg",
            "numpy @ https://example.com/evil.whl",
            "numpy==2.2.6 --hash=sha256:abcd",
            "numpy==2.2.6 \\\n    --index-url https://evil.example.com/simple",
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
    fn requirements_file_unreadable_reports_error() {
        let tmp = TempDir::new().unwrap();
        let (policy, _) = policy_from(tmp.path(), RANGES);
        let err = policy
            .validate_requirements_file("/nonexistent/requirements.txt")
            .unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::RequirementsFileUnreadable { .. }
        ));
    }

    #[test]
    fn validate_uninstall_refuses_only_pip_and_options() {
        // The allowlist only limits new installs: packages that are not on it,
        // such as dependencies or packages installed before it changed, can
        // still be removed.
        let tmp = TempDir::new().unwrap();
        let (with_allowlist, _) = policy_from(tmp.path(), RANGES);
        for policy in [PipPolicy::unrestricted(), with_allowlist] {
            for ok in ["numpy", "six", "scipy==1.0", "Python_Dateutil"] {
                assert!(
                    policy.validate_uninstall(&[ok.to_string()]).is_ok(),
                    "{ok} should be allowed"
                );
            }
            for spec in ["pip", "PIP", "pip==26.2.2", "-r", "--yes"] {
                let err = policy.validate_uninstall(&[spec.to_string()]).unwrap_err();
                assert!(
                    matches!(err, PluginEnvironmentError::UninstallNotAllowed { .. }),
                    "{spec}: unexpected error {err:?}"
                );
            }
        }
    }

    #[test]
    fn broken_dependents_from_pip_check_reports_only_removed_packages() {
        let output = "\
python-dateutil 2.9.0.post0 requires six, which is not installed.
pandas 2.2.0 requires python-dateutil, which is not installed.
pandas 2.2.0 requires Six, which is not installed.
pandas 2.2.0 requires pytz, which is not installed.
foo 1.0 has requirement bar<2, but you have bar 3.0.
";
        let removed: BTreeSet<String> = ["six".to_string()].into_iter().collect();
        assert_eq!(
            broken_dependents_from_pip_check(output, &removed),
            vec![
                BrokenDependent {
                    package: "pandas==2.2.0".to_string(),
                    missing: vec!["six".to_string()],
                },
                BrokenDependent {
                    package: "python-dateutil==2.9.0.post0".to_string(),
                    missing: vec!["six".to_string()],
                },
            ]
        );
        assert!(
            broken_dependents_from_pip_check("No broken requirements found.\n", &removed)
                .is_empty()
        );
    }

    #[test]
    fn parse_pip_version_reads_major_minor() {
        let cases = [
            (
                "pip 26.2.2 from /v/site-packages/pip (python 3.12)",
                Some((26, 2)),
            ),
            (
                "pip 26.1 from /v/site-packages/pip (python 3.13)",
                Some((26, 1)),
            ),
            ("pip 26.1b1 from /v (python 3.13)", Some((26, 1))),
            ("pip 24.0 from /v (python 3.12)", Some((24, 0))),
            ("pip 27 from /v (python 3.13)", Some((27, 0))),
            ("not pip output", None),
            ("pip x.y from /v", None),
        ];
        for (output, expected) in cases {
            assert_eq!(parse_pip_version(output), expected, "{output}");
        }
        assert!(parse_pip_version("pip 26.0.1 from /v").unwrap() < MIN_PIP_FOR_CONSTRAINT_HASHES);
        assert!(parse_pip_version("pip 26.1 from /v").unwrap() >= MIN_PIP_FOR_CONSTRAINT_HASHES);
    }

    #[test]
    fn check_pip_supports_constraint_hashes_fails_closed_without_venv() {
        let tmp = TempDir::new().unwrap();
        let err = check_pip_supports_constraint_hashes(&tmp.path().join("no-venv")).unwrap_err();
        assert!(matches!(
            err,
            PluginEnvironmentError::PipTooOldForHashedAllowlist { .. }
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
