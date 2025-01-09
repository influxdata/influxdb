use anyhow::{Context, Result};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() -> Result<()> {
    // Read configuration from environment or use defaults
    let python_version = env::var("PYTHON_VERSION").unwrap_or_else(|_| "3.12.8".to_string());
    let release_date = env::var("PYTHON_RELEASE_DATE").unwrap_or_else(|_| "20250106".to_string());
    let major_minor = python_version
        .split('.')
        .take(2)
        .collect::<Vec<_>>()
        .join(".");

    // Get target platform info
    let target_os = env::var("CARGO_CFG_TARGET_OS")?;
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH")?;

    // Get the manifest directory and set up paths
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let python_dir = manifest_dir.join("../python_embedded/python");
    let python_lib_dir = python_dir.join("lib");

    // Part 1: Download and install Python if needed
    if !python_dir.join("bin").exists() || env::var("FORCE_PYTHON_DOWNLOAD").is_ok() {
        let url = get_python_url(&python_version, &release_date, &target_os, &target_arch)?;
        println!("cargo:warning=Python download URL: {}", url);
        download_python(&url, &python_dir)?;
    }

    // Part 2: Set up linking configuration
    println!(
        "cargo:rustc-link-search=native={}",
        python_lib_dir.display()
    );

    match target_os.as_str() {
        "macos" => {
            println!("cargo:rustc-link-lib=python{}", major_minor);
            println!("cargo:rustc-link-lib=dylib=python{}", major_minor);

            let config_dir = python_lib_dir.join(format!(
                "python{}/config-{}-darwin",
                major_minor, major_minor
            ));
            println!("cargo:rustc-link-search=native={}", config_dir.display());

            println!(
                "cargo:rustc-link-arg=-Wl,-rpath,@loader_path/../../python_embedded/python/lib"
            );
        }
        "linux" => {
            println!("cargo:rustc-link-lib=python{}", major_minor);
            println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/../../python_embedded/python/lib");
        }
        "windows" => {
            println!("cargo:rustc-link-lib=python{}", major_minor);
            // TODO: Windows linking
        }
        _ => anyhow::bail!("Unsupported platform: {}", target_os),
    }

    // Make sure we rebuild if relevant environment variables change
    println!("cargo:rerun-if-env-changed=PYTHON_VERSION");
    println!("cargo:rerun-if-env-changed=PYTHON_RELEASE_DATE");
    println!("cargo:rerun-if-env-changed=FORCE_PYTHON_DOWNLOAD");

    Ok(())
}

fn get_python_url(version: &str, date: &str, os: &str, arch: &str) -> Result<String> {
    let (arch_str, os_str, platform_str) = match (os, arch) {
        ("macos", "aarch64") => ("aarch64", "apple", "darwin"),
        ("macos", "x86_64") => ("x86_64", "apple", "darwin"),
        ("linux", "aarch64") => ("aarch64", "unknown", "linux-gnu"),
        ("linux", "x86_64") => ("x86_64", "unknown", "linux-gnu"),
        ("windows", "x86_64") => ("x86_64", "pc", "windows-msvc"),
        ("windows", "x86") => ("x86", "pc", "windows-msvc"),
        _ => anyhow::bail!("Unsupported platform: {}-{}", os, arch),
    };

    Ok(format!(
        "https://github.com/astral-sh/python-build-standalone/releases/download/{}/cpython-{}+{}-{}-{}-{}-install_only.tar.gz",
        date, version, date, arch_str, os_str, platform_str
    ))
}

fn download_python(url: &str, target_dir: &Path) -> Result<()> {
    println!(
        "cargo:warning=Downloading Python to {}",
        target_dir.display()
    );

    // Download the archive
    let response =
        reqwest::blocking::get(url).with_context(|| format!("Failed to download from {}", url))?;
    let archive_bytes = response.bytes()?;

    // Extract the archive
    let tar_gz = flate2::read::GzDecoder::new(&archive_bytes[..]);
    let mut archive = tar::Archive::new(tar_gz);
    archive.unpack(target_dir.parent().unwrap())?;

    // Fix permissions on Unix systems
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let python_bin = target_dir.join("bin/python3");
        if python_bin.exists() {
            let mut perms = fs::metadata(&python_bin)?.permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&python_bin, perms)?;
        }
    }

    Ok(())
}
