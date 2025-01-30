use crate::environment::PluginEnvironmentError::PluginEnvironmentDisabled;
#[cfg(feature = "system-py")]
use crate::virtualenv::{initialize_venv, VenvError};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::process::Command;
use observability_deps::tracing::info;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PluginEnvironmentError {
    #[error("Package manager not available: {0}")]
    PackageManagerNotFound(String),
    #[error("Required binary '{binary}' not found or not executable: {error}")]
    BinaryNotFound { binary: String, error: String },
    #[error("Installation failed: {0}")]
    InstallationFailed(String),
    #[error("Command execution failed: {command} - {error}")]
    CommandFailed { command: String, error: String },
    #[error("Plugin environment management is disabled")]
    PluginEnvironmentDisabled,
    #[cfg(feature = "system-py")]
    #[error("Virtual environment error: {0}")]
    VenvError(#[from] VenvError),
}

fn check_binary_available(binary: &str) -> Result<(), PluginEnvironmentError> {
    let output = Command::new(binary)
        .arg("--version")
        .output()
        .map_err(|e| PluginEnvironmentError::BinaryNotFound {
            binary: binary.to_string(),
            error: e.to_string(),
        })?;

    if !output.status.success() {
        return Err(PluginEnvironmentError::BinaryNotFound {
            binary: binary.to_string(),
            error: String::from_utf8_lossy(&output.stderr).to_string(),
        });
    }
    Ok(())
}

pub trait PythonEnvironmentManager: Debug + Send + Sync + 'static {
    fn init_pyenv(
        &self,
        plugin_dir: &Path,
        virtual_env_location: Option<&PathBuf>,
    ) -> Result<(), PluginEnvironmentError>;
    fn install_packages(&self, packages: Vec<String>) -> Result<(), PluginEnvironmentError>;

    fn install_requirements(&self, requirements_path: String)
        -> Result<(), PluginEnvironmentError>;
}

#[derive(Debug, Copy, Clone)]
pub struct UVManager;
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

impl PythonEnvironmentManager for UVManager {
    fn init_pyenv(
        &self,
        plugin_dir: &Path,
        virtual_env_location: Option<&PathBuf>,
    ) -> Result<(), PluginEnvironmentError> {
        info!("initializing environment");
        check_binary_available("uv")?;
        check_binary_available("python3")?;

        let venv_path = match virtual_env_location {
            Some(path) => path,
            None => &plugin_dir.join(".venv"),
        };

        if !is_valid_venv(venv_path) {
            let output = Command::new("uv")
                .arg("venv")
                .arg(venv_path)
                .output()
                .map_err(|e| PluginEnvironmentError::CommandFailed {
                    command: "uv venv".to_string(),
                    error: e.to_string(),
                })?;

            if !output.status.success() {
                return Err(PluginEnvironmentError::InstallationFailed(
                    format!("uv venv {:?}", venv_path)
                ));
            }
        }

        #[cfg(feature = "system-py")]
        initialize_venv(venv_path)?;
        Ok(())
    }

    fn install_packages(&self, packages: Vec<String>) -> Result<(), PluginEnvironmentError> {
        let output = Command::new("uv")
            .args(["pip", "install"])
            .args(&packages)
            .output()
            .map_err(|e| PluginEnvironmentError::CommandFailed {
            command: "uv pip install".to_string(),
            error: e.to_string(),
        })?;

        if !output.status.success() {
            return Err(PluginEnvironmentError::InstallationFailed(
                format!("uv pip install {:?} failed", packages),
            ));
        }

        Ok(())
    }

    fn install_requirements(
        &self,
        requirements_path: String,
    ) -> Result<(), PluginEnvironmentError> {
        let output = Command::new("uv")
            .args(["pip", "install", "-r", &requirements_path])
            .output()
            .map_err(|e| PluginEnvironmentError::CommandFailed {
                command: "uv pip install -r".to_string(),
                error: e.to_string(),
            })?;

        if !output.status.success() {
            return Err(PluginEnvironmentError::InstallationFailed(
                format!("uv pip install -r {:?} failed", requirements_path),
            ));
        }
        Ok(())
    }
}

impl PythonEnvironmentManager for PipManager {
    fn init_pyenv(
        &self,
        plugin_dir: &Path,
        virtual_env_location: Option<&PathBuf>,
    ) -> Result<(), PluginEnvironmentError> {
        check_binary_available("python3")?;
        check_binary_available("pip")?;

        let venv_path = match virtual_env_location {
            Some(path) => path,
            None => &plugin_dir.join(".venv"),
        };

        if !is_valid_venv(venv_path) {
            let output = Command::new("python3")
                .arg("-m")
                .arg("venv")
                .arg(venv_path)
                .output()
                .map_err(|e| PluginEnvironmentError::CommandFailed {
                    command: "python3 -m venv".to_string(),
                    error: e.to_string(),
                })?;

            if !output.status.success() {
                return Err(PluginEnvironmentError::InstallationFailed(
                    format!("python3 -m venv {:?} failed", venv_path),
                ));
            }
        }

        #[cfg(feature = "system-py")]
        initialize_venv(venv_path)?;
        Ok(())
    }
    fn install_packages(&self, packages: Vec<String>) -> Result<(), PluginEnvironmentError> {
        let output = Command::new("pip")
            .arg("install")
            .args(&packages)
            .output()
            .map_err(|e| PluginEnvironmentError::CommandFailed {
                command: "pip install".to_string(),
                error: e.to_string(),
            })?;

        if !output.status.success() {
            return Err(PluginEnvironmentError::InstallationFailed(
                format!("pip install {:?} failed", packages),
            ));
        }
        Ok(())
    }
    fn install_requirements(
        &self,
        requirements_path: String,
    ) -> Result<(), PluginEnvironmentError> {
        let output = Command::new("pip")
            .args(["install", "-r", &requirements_path])
            .output()
            .map_err(|e| PluginEnvironmentError::CommandFailed {
                command: "pip install -r".to_string(),
                error: e.to_string(),
            })?;

        if !output.status.success() {
            return Err(PluginEnvironmentError::InstallationFailed(
                format!("pip install {:?} failed", requirements_path),
            ));
        }
        Ok(())
    }
}

impl PythonEnvironmentManager for DisabledManager {
    fn init_pyenv(
        &self,
        _plugin_dir: &Path,
        _virtual_env_location: Option<&PathBuf>,
    ) -> Result<(), PluginEnvironmentError> {
        Ok(())
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
