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

/// The location of the one-per-process venv, set once by [`init_venv`].
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
/// seconds-long step; it runs once and is shared by every [`PendingVenv::ready`]
/// caller and the background thread spawned by [`init_venv`].
fn build_venv() -> Result<(), String> {
    let venv_path = VENV_PATH
        .get()
        .expect("init_venv sets VENV_PATH before the build runs");

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
/// with the rest of startup. Call [`PendingVenv::ready`] to wait for it.
pub fn init_venv(venv_path: PathBuf) -> PendingVenv {
    let _ = VENV_PATH.set(venv_path);
    let build = std::thread::spawn(|| {
        VENV_BUILD.get_or_init(build_venv);
    });
    PendingVenv { build }
}

/// The in-flight venv build started by [`init_venv`]. Holding the build thread's
/// handle makes this a move-only, [`init_venv`]-only handle.
#[derive(Debug)]
pub struct PendingVenv {
    build: std::thread::JoinHandle<()>,
}

impl PendingVenv {
    /// Block until the background venv build finishes.
    ///
    /// The returned [`ReadyVenv`] is the only handle to the built venv and the
    /// entry point for operating on it (e.g. [`ReadyVenv::determine_package_manager`]).
    /// Because nothing else can construct a `ReadyVenv`, any code that touches
    /// the venv must wait on the build first — the compiler enforces it.
    pub fn ready(self) -> Result<ReadyVenv, PluginEnvironmentError> {
        // Wait for the build thread; if it failed to run for some reason, fall
        // back to building inline (a no-op once the thread has populated it).
        let _ = self.build.join();
        VENV_BUILD
            .get_or_init(build_venv)
            .clone()
            .map_err(PluginEnvironmentError::PackageManagerNotFound)?;
        let path = VENV_PATH.get().expect("init_venv set VENV_PATH").clone();
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
