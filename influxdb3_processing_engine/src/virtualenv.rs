use observability_deps::tracing::debug;
use std::path::Path;
use std::process::Command;
use std::sync::Once;
use thiserror::Error;

static PYTHON_INIT: Once = Once::new();

#[derive(Error, Debug)]
pub enum VenvError {
    #[error("Failed to initialize virtualenv: {0}")]
    InitError(String),
    #[error("Error shelling out: {0}")]
    CommandError(#[from] std::io::Error),
}

fn get_python_version() -> Result<(u8, u8), VenvError> {
    let output = Command::new("python3")
        .args([
            "-c",
            "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')",
        ])
        .output()
        .map_err(VenvError::CommandError)?;

    if !output.status.success() {
        return Err(VenvError::InitError(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }

    let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let mut parts = version.split('.');
    let major: u8 = parts
        .next()
        .ok_or_else(|| VenvError::InitError("Invalid version format".to_string()))?
        .parse()
        .map_err(|_| VenvError::InitError("Invalid major version".to_string()))?;
    let minor: u8 = parts
        .next()
        .ok_or_else(|| VenvError::InitError("Invalid version format".to_string()))?
        .parse()
        .map_err(|_| VenvError::InitError("Invalid minor version".to_string()))?;

    Ok((major, minor))
}

fn set_pythonpath(venv_dir: &Path) -> Result<(), std::io::Error> {
    let (major, minor) = get_python_version().unwrap();
    let site_packages = venv_dir
        .join("lib")
        .join(format!("python{}.{}", major, minor))
        .join("site-packages");

    print!("Setting PYTHONPATH to: {}", site_packages.to_string_lossy());
    std::env::set_var("PYTHONPATH", &site_packages);

    Ok(())
}

pub fn init_pyo3() {
    PYTHON_INIT.call_once(|| {
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
    set_pythonpath(venv_path).unwrap();

    let output = Command::new("bash")
        .arg("-c")
        .arg(format!(
            "source {} && env",
            activate_script.to_str().unwrap()
        ))
        .output()
        .expect("couldn't source");

    if !output.status.success() {
        return Err(VenvError::InitError(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }

    // Apply environment changes
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_once('='))
        .for_each(|(key, value)| std::env::set_var(key, value));

    Ok(())
}
