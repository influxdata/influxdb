use crate::environment::PluginEnvironmentError::PluginEnvironmentDisabled;
use std::fmt::Debug;
use std::process::Command;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PluginEnvironmentError {
    #[error("Package manager not available: {0}")]
    PackageManagerNotFound(String),
    #[error("External call failed: {0}")]
    InstallationFailed(#[from] std::io::Error),
    #[error("Plugin environment management is disabled")]
    PluginEnvironmentDisabled,
}

pub trait PythonEnvironmentManager: Debug + Send + Sync + 'static {
    fn install_package(&self, package_name: String) -> Result<(), PluginEnvironmentError>;

    fn install_requirements(&self, requirements_path: String)
        -> Result<(), PluginEnvironmentError>;
}

#[derive(Debug, Copy, Clone)]
pub struct UVManager;
#[derive(Debug, Copy, Clone)]
pub struct PipManager;
#[derive(Debug, Copy, Clone)]
pub struct PipxManager;

#[derive(Debug, Copy, Clone)]
pub struct DisabledManager;

impl PythonEnvironmentManager for UVManager {
    fn install_package(&self, package: String) -> Result<(), PluginEnvironmentError> {
        Command::new("uv")
            .args(["pip", "install", &package])
            .output()?;
        Ok(())
    }

    fn install_requirements(
        &self,
        requirements_path: String,
    ) -> Result<(), PluginEnvironmentError> {
        Command::new("uv")
            .args(["pip", "install", "-r", &requirements_path])
            .output()?;
        Ok(())
    }
}

impl PythonEnvironmentManager for PipManager {
    fn install_package(&self, package: String) -> Result<(), PluginEnvironmentError> {
        Command::new("pip").args(["install", &package]).output()?;
        Ok(())
    }
    fn install_requirements(
        &self,
        requirements_path: String,
    ) -> Result<(), PluginEnvironmentError> {
        Command::new("pip")
            .args(["install", "-r", &requirements_path])
            .output()?;
        Ok(())
    }
}

impl PythonEnvironmentManager for PipxManager {
    fn install_package(&self, package: String) -> Result<(), PluginEnvironmentError> {
        Command::new("pipx").args(["install", &package]).output()?;
        Ok(())
    }
    fn install_requirements(
        &self,
        requirements_path: String,
    ) -> Result<(), PluginEnvironmentError> {
        Command::new("pipx")
            .args(["install", "-r", &requirements_path])
            .output()?;
        Ok(())
    }
}

impl PythonEnvironmentManager for DisabledManager {
    fn install_package(&self, _package: String) -> Result<(), PluginEnvironmentError> {
        Err(PluginEnvironmentDisabled)
    }

    fn install_requirements(
        &self,
        _requirements_path: String,
    ) -> Result<(), PluginEnvironmentError> {
        Err(PluginEnvironmentDisabled)
    }
}
