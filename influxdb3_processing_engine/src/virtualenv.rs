use observability_deps::tracing::{info, warn};
use std::path::Path;
use std::process::Command;
use std::sync::Once;
use thiserror::Error;

static PYTHON_INIT: Once = Once::new();

#[derive(Error, Debug)]
pub(crate) enum VenvError {
    #[error("Failed to initialize virtualenv: {0}")]
    InitError(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

fn get_python_version() -> Result<(u8, u8), std::io::Error> {
    let output = Command::new("python3")
        .args(["-c", "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"])
        .output()?;

    let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let mut parts = version.split('.');
    let major: u8 = parts.next().unwrap().parse().unwrap();
    let minor: u8 = parts.next().unwrap().parse().unwrap();

    Ok((major, minor))
}

#[cfg(unix)]
fn set_pythonpath(venv_dir: &Path) -> Result<(), std::io::Error> {
    let (major, minor) = get_python_version()?;
    let site_packages = venv_dir
        .join("lib")
        .join(format!("python{}.{}", major, minor))
        .join("site-packages");

    info!("site packages is {}", site_packages.to_string_lossy());

    if site_packages.exists() {
        std::env::set_var("PYTHONPATH", site_packages);
    }

    Ok(())
}

pub(crate) fn try_init_venv(venv_path: &Path) {
    PYTHON_INIT.call_once(|| {
        if let Err(err) = initialize_venv(venv_path) {
            warn!(
                "Failed to initialize virtualenv at {}: {}",
                venv_path.to_string_lossy(),
                err
            );
        }
        pyo3::prepare_freethreaded_python();
    })
}

#[cfg(unix)]
pub(crate) fn initialize_venv(venv_path: &Path) -> Result<(), VenvError> {
    use std::process::Command;

    let activate_script = venv_path.join("bin").join("activate");
    if !activate_script.exists() {
        return Err(VenvError::InitError(format!(
            "Activation script not found at {:?}",
            activate_script
        )));
    }

    let output = Command::new("bash")
        .arg("-c")
        .arg(format!(
            "source {} && env",
            activate_script.to_str().unwrap()
        ))
        .output()?;

    if !output.status.success() {
        return Err(VenvError::InitError(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }
    set_pythonpath(venv_path)?;

    // Apply environment changes
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_once('='))
        .for_each(|(key, value)| {
            println!("{}={}", key, value);
            std::env::set_var(key, value)});


    Ok(())
}
