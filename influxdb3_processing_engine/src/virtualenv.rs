use observability_deps::tracing::debug;
use pyo3::Python;
use std::env;
use std::ffi::CString;
use std::path::{Path, PathBuf};
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

// Find the python installation location (not virtual env).
// XXX: use build flag?
fn find_python_install() -> PathBuf {
    let influxdb3_exe = env::current_exe().unwrap();
    let influxdb3_exe_dir = influxdb3_exe.parent().unwrap();
    let influxdb3_rel_dir = influxdb3_exe_dir.join("python");
    let influxdb3_linux_dir = influxdb3_exe_dir.join("../lib/influxdb3/python");

    let python_inst = if cfg!(target_os = "linux")
        && (influxdb3_exe_dir == Path::new("/usr/bin")
            || influxdb3_exe_dir == Path::new("/usr/local/bin"))
        && influxdb3_linux_dir.is_dir()
    {
        // Official Linux deb/rpm/docker builds are in /usr (but also allow for
        // /usr/local) so use runtime in /usr/[local/]lib/influxdb3/python/
        influxdb3_linux_dir
    } else if influxdb3_rel_dir.is_dir() {
        // Official tar/zip builds use runtime in python/ relative to executable
        influxdb3_rel_dir
    } else {
        // Could not find python-build-standalone installation
        PathBuf::new()
    };
    debug!(
        "Found python standalone installation: {}",
        python_inst.display()
    );
    python_inst
}

pub fn find_python() -> PathBuf {
    let python_exe_bn = if cfg!(windows) {
        "python.exe"
    } else {
        "python3"
    };

    let python_home = find_python_install();
    let python_exe = if let Ok(v) = env::var("VIRTUAL_ENV") {
        // After initialize_venv(), VIRTUAL_ENV is set, so honor it (thus
        // allowing package installs to be installed in the venv)
        let mut path = PathBuf::from(v);
        if cfg!(windows) {
            path.push("Scripts");
        } else {
            path.push("bin");
        }
        path.push(python_exe_bn);
        path
    } else if !python_home.as_os_str().is_empty() {
        // Prior to initialize_venv(), VIRTUAL_ENV is not set so we'll want to
        // look for where the python installation is, which allows
        // 'python -m venv' to work correctly
        let mut path = python_home;
        if !cfg!(windows) {
            path.push("bin");
        }
        path.push(python_exe_bn);
        path
    } else {
        // Fallback to searching PATH
        PathBuf::from(python_exe_bn)
    };
    debug!("Found python executable: {}", python_exe.display());
    python_exe
}

fn get_python_version() -> Result<(u8, u8), std::io::Error> {
    let python_exe = find_python();
    let output = Command::new(python_exe)
        .args([
            "-c",
            "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')",
        ])
        .output()?;

    let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let mut parts = version.split('.');
    let major: u8 = parts.next().unwrap().parse().unwrap();
    let minor: u8 = parts.next().unwrap().parse().unwrap();

    Ok((major, minor))
}

fn set_pythonpath(venv_dir: &Path) -> Result<(), std::io::Error> {
    let (major, minor) = get_python_version()?;
    let site_packages = if cfg!(target_os = "windows") {
        venv_dir.join("Lib").join("site-packages")
    } else {
        venv_dir
            .join("lib")
            .join(format!("python{}.{}", major, minor))
            .join("site-packages")
    };

    debug!("Setting PYTHONPATH to: {}", site_packages.to_string_lossy());
    unsafe {
        std::env::set_var("PYTHONPATH", &site_packages);
    }

    Ok(())
}

pub fn init_pyo3() {
    // PYO3 compiled against python-build-standalone needs PYTHONHOME set
    // to the installation location for initialization to work correctly
    let python_inst = find_python_install();
    let orig_pyhome_env = env::var("PYTHONHOME").ok();
    if !python_inst.as_os_str().is_empty() {
        // Found python-build-standalone installation
        debug!(
            "Temporarily setting PYTHONHOME during initialization to: {}",
            python_inst.to_string_lossy()
        );
        unsafe {
            env::set_var("PYTHONHOME", &python_inst);
        }
    }

    PYTHON_INIT.call_once(|| {
        pyo3::prepare_freethreaded_python();

        // This sets the signal handler fo SIGINT to be the default, allowing CTRL+C to work.
        // See https://github.com/PyO3/pyo3/issues/3218.
        Python::with_gil(|py| {
            py.run(
                &CString::new("import signal;signal.signal(signal.SIGINT, signal.SIG_DFL)")
                    .unwrap(),
                None,
                None,
            )
            .expect("should be able to set signal handler.");
        });
    });

    // now that we're initialized, restore/unset PYTHONHOME
    match orig_pyhome_env {
        Some(v) => {
            debug!("Restoring previous PYTHONHOME to: {}", v);
            unsafe { env::set_var("PYTHONHOME", v) }
        }
        None => {
            debug!("Unsetting temporary PYTHONHOME");
            unsafe { env::remove_var("PYTHONHOME") }
        }
    }
}

pub(crate) fn initialize_venv(venv_path: &Path) -> Result<(), VenvError> {
    use std::process::Command;

    let activate_script = if cfg!(target_os = "windows") {
        venv_path.join("Scripts").join("activate.bat")
    } else {
        venv_path.join("bin").join("activate")
    };

    if !activate_script.exists() {
        return Err(VenvError::InitError(format!(
            "Activation script not found at {:?}",
            activate_script
        )));
    }

    // Calling the activate script isn't enough to change our process' environment. Instead,
    // source/call the script, print the resulting environment, capture its output and
    // set all env vars found. This should future-proof us against changes to activate script
    // specifics.
    let output = if cfg!(target_os = "windows") {
        Command::new("cmd")
            .arg("/C")
            .arg(format!("{} && set", activate_script.to_str().unwrap()))
            .output()?
    } else {
        Command::new("bash")
            .arg("-c")
            .arg(format!(
                "source {} && env",
                activate_script.to_str().unwrap()
            ))
            .output()?
    };

    if !output.status.success() {
        return Err(VenvError::InitError(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }

    // Apply environment changes
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_once('='))
        .for_each(|(key, value)| unsafe { std::env::set_var(key, value) });

    if let Ok(v) = env::var("VIRTUAL_ENV") {
        debug!("VIRTUAL_ENV set to: {}", v);
    }

    set_pythonpath(venv_path)?;

    Ok(())
}
