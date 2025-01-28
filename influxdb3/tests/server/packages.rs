use crate::cli::run_with_confirmation;
use crate::{ConfigProvider, TestServer};
use anyhow::{bail, Result};
use serde_json::Value;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::{NamedTempFile, TempDir};

const TEST_PACKAGE: &str = "tablib";
const TEST_VERSION: &str = "3.8.0";

struct VenvTest {
    venv_dir: TempDir,
    plugin_file: NamedTempFile,
}

impl VenvTest {
    fn new() -> Result<Self> {
        let venv_dir = TempDir::new()?;
        let plugin_file = create_version_check_plugin()?;
        Ok(Self {
            venv_dir,
            plugin_file,
        })
    }

    fn venv_path(&self) -> PathBuf {
        self.venv_dir.path().to_path_buf()
    }

    fn plugin_dir(&self) -> String {
        self.plugin_file
            .path()
            .parent()
            .unwrap()
            .to_string_lossy()
            .to_string()
    }
}

fn create_version_check_plugin() -> Result<NamedTempFile> {
    let plugin_code = r#"
import pkg_resources

def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    try:
        version = pkg_resources.get_distribution('tablib').version
        influxdb3_local.info(version)
    except pkg_resources.DistributionNotFound:
        influxdb3_local.info("tablib is not installed")
"#;
    let mut plugin_file = NamedTempFile::new()?;
    plugin_file.write_all(plugin_code.as_bytes())?;
    Ok(plugin_file)
}

async fn run_version_check(server_addr: &str, plugin_path: &str) -> Result<Vec<String>> {
    let output = run_with_confirmation(&[
        "test",
        "schedule_plugin",
        "-H",
        server_addr,
        "-d",
        "version_check",
        plugin_path,
    ]);

    let json: Value = serde_json::from_str(&output)?;
    Ok(json["log_lines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|line| {
            line.as_str()
                .unwrap()
                .trim_start_matches("INFO: ")
                .to_string()
        })
        .collect())
}

fn setup_python_venv(venv_path: &Path) -> Result<()> {
    Command::new("python3")
        .args(["-m", "venv", venv_path.to_str().unwrap()])
        .status()?;
    Ok(())
}

fn setup_uv_venv(venv_path: &Path) -> Result<()> {
    let status = Command::new("uv")
        .args(["venv", venv_path.to_str().unwrap()])
        .status()?;
    if !status.success() {
        bail!("failed to execute venv");
    }
    Ok(())
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_python_venv_pip_install() -> Result<()> {
    let test = VenvTest::new()?;
    setup_python_venv(&test.venv_path())?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .spawn()
        .await;
    let server_addr = server.client_addr();

    run_with_confirmation(&[
        "create",
        "database",
        "--host",
        &server_addr,
        "version_check",
    ]);

    // Check package is not installed
    let logs = run_version_check(&server_addr, test.plugin_file.path().to_str().unwrap()).await?;
    assert_eq!(logs, vec!["tablib is not installed"]);

    // Install specific version
    run_with_confirmation(&[
        "install",
        "package",
        "--host",
        &server_addr,
        "--package-manager",
        "pip",
        &format!("{}=={}", TEST_PACKAGE, TEST_VERSION),
    ]);

    // Verify correct version installed
    let logs = run_version_check(&server_addr, test.plugin_file.path().to_str().unwrap()).await?;
    assert_eq!(logs, vec![TEST_VERSION]);

    Ok(())
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_uv_venv_uv_install() -> Result<()> {
    let test = VenvTest::new()?;
    setup_uv_venv(&test.venv_path())?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .with_package_manager("uv")
        .spawn()
        .await;
    let server_addr = server.client_addr();

    run_with_confirmation(&[
        "create",
        "database",
        "--host",
        &server_addr,
        "version_check",
    ]);

    // Check package is not installed
    let logs = run_version_check(&server_addr, test.plugin_file.path().to_str().unwrap()).await?;
    assert_eq!(vec!["tablib is not installed"], logs);

    // Install with UV
    run_with_confirmation(&[
        "install",
        "package",
        "--host",
        &server_addr,
        "--package-manager",
        "uv",
        TEST_PACKAGE,
    ]);

    // Verify package is installed
    let logs = run_version_check(&server_addr, test.plugin_file.path().to_str().unwrap()).await?;
    assert!(!logs[0].contains("not installed"));

    Ok(())
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_venv_requirements_install() -> Result<()> {
    let test = VenvTest::new()?;
    setup_python_venv(&test.venv_path())?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .spawn()
        .await;
    let server_addr = server.client_addr();

    run_with_confirmation(&[
        "create",
        "database",
        "--host",
        &server_addr,
        "version_check",
    ]);

    // Check package is not installed
    let logs = run_version_check(&server_addr, test.plugin_file.path().to_str().unwrap()).await?;
    assert_eq!(logs, vec!["tablib is not installed"]);

    // Create requirements.txt
    let mut requirements_file = NamedTempFile::new()?;
    writeln!(requirements_file, "{}=={}", TEST_PACKAGE, TEST_VERSION)?;

    // Install from requirements
    run_with_confirmation(&[
        "install",
        "package",
        "--host",
        &server_addr,
        "--requirements",
        requirements_file.path().to_str().unwrap(),
    ]);

    // Verify installation
    let logs = run_version_check(&server_addr, test.plugin_file.path().to_str().unwrap()).await?;
    assert_eq!(logs, vec![TEST_VERSION]);
    Ok(())
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_venv_remote_install() -> Result<()> {
    let test = VenvTest::new()?;
    setup_python_venv(&test.venv_path())?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .spawn()
        .await;
    let server_addr = server.client_addr();

    run_with_confirmation(&[
        "create",
        "database",
        "--host",
        &server_addr,
        "version_check",
    ]);

    // Check package is not installed
    let logs = run_version_check(&server_addr, test.plugin_file.path().to_str().unwrap()).await?;
    assert_eq!(logs, vec!["tablib is not installed"]);

    // Test remote installation
    run_with_confirmation(&["install", "package", "--host", &server_addr, TEST_PACKAGE]);

    // Verify installation
    let logs = run_version_check(&server_addr, test.plugin_file.path().to_str().unwrap()).await?;
    assert!(!logs[0].contains("not installed"));
    Ok(())
}
