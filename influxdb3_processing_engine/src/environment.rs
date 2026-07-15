use crate::environment::PluginEnvironmentError::PluginEnvironmentDisabled;

use crate::virtualenv::{VenvError, find_python, initialize_venv};
use observability_deps::tracing::debug;
use pyo3::prelude::PyAnyMethods;
use pyo3::{PyResult, Python};
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
}

#[derive(Debug, Copy, Clone)]
pub struct PipManager;

#[derive(Debug, Copy, Clone)]
pub struct DisabledManager;

fn is_valid_venv(venv_path: &Path) -> bool {
    if cfg!(windows) {
        venv_path.join("Scripts").join("activate.bat").exists()
    } else {
        venv_path.join("bin").join("activate").exists()
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
        let python_exe = find_python();
        Command::new(python_exe)
            .arg("-m")
            .arg("pip")
            .arg("install")
            .args(&packages)
            .output()?;
        Ok(())
    }
    fn install_requirements(
        &self,
        requirements_path: String,
    ) -> Result<(), PluginEnvironmentError> {
        let python_exe = find_python();
        Command::new(python_exe)
            .arg("-m")
            .arg("pip")
            .args(["install", "-r", &requirements_path])
            .output()?;

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
    pub fn determine_package_manager(&self) -> Arc<dyn PythonEnvironmentManager> {
        // A venv always contains `bin/python`; `python3` is only a symlink that
        // may be absent.
        let venv_python = if cfg!(windows) {
            self.path.join("Scripts").join("python.exe")
        } else {
            self.path.join("bin").join("python")
        };
        debug!("Running: {} -m pip --version", venv_python.display());
        let pip_available = Command::new(&venv_python)
            .args(["-m", "pip", "--version"])
            .output()
            .is_ok_and(|output| output.status.success());
        if pip_available {
            Arc::new(PipManager)
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
        let manager = first_ready.determine_package_manager();
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
}
