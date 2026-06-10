use crate::server::{ConfigProvider, TestServer};
use anyhow::Result;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::{NamedTempFile, TempDir};

const TEST_PACKAGE: &str = "tablib";
const TEST_VERSION: &str = "3.8.0";
const TEST_DB: &str = "version_check";

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

    fn plugin_file_relative(&self) -> &str {
        self.plugin_file
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
    }
}

fn create_version_check_plugin() -> Result<NamedTempFile> {
    let plugin_code = r#"
import importlib.metadata

def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    try:
        version = importlib.metadata.version('tablib')
        influxdb3_local.info(f"VERSION: {version}")
    except importlib.metadata.PackageNotFoundError:
        influxdb3_local.info("VERSION: tablib is not installed")
"#;
    let mut plugin_file = NamedTempFile::new()?;
    plugin_file.write_all(plugin_code.as_bytes())?;
    Ok(plugin_file)
}

async fn run_version_check(test_server: &TestServer, plugin_path: &str) -> Result<Vec<String>> {
    let json = test_server
        .test_schedule_plugin(TEST_DB, plugin_path, "* * * * * *")
        .run()?;

    let errors = json["errors"].as_array().expect("is array");
    assert!(errors.is_empty(), "Errors:\n{errors:#?}");

    Ok(json["log_lines"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|line| {
            line.as_str()
                .expect("is string")
                .strip_prefix("INFO: VERSION: ")
                .map(|s| s.to_owned())
        })
        .collect())
}

fn setup_python_venv(venv_path: &Path) -> Result<()> {
    Command::new("python3")
        .args(["-m", "venv", venv_path.to_str().unwrap()])
        .status()?;
    Ok(())
}

async fn assert_tablib_telemetry(test_server: &TestServer, installed: bool) {
    let snapshot = test_server.telemetry_snapshot().await;
    let packages: Vec<_> = snapshot["installed_packages"]
        .as_array()
        .expect("installed_packages should be an array")
        .iter()
        .map(|package| package.as_str().expect("package name should be a string"))
        .collect();

    assert_eq!(
        installed,
        packages.contains(&TEST_PACKAGE),
        "unexpected telemetry package list: {packages:?}"
    );
}

#[test_log::test(tokio::test)]
#[ignore]
async fn test_python_venv_pip_install() -> Result<()> {
    let test = VenvTest::new()?;
    setup_python_venv(&test.venv_path())?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .with_test_mode()
        .spawn()
        .await;

    server.create_database(TEST_DB).run()?;

    // Check package is not installed
    let logs = run_version_check(&server, test.plugin_file_relative()).await?;
    assert_eq!(logs, vec!["tablib is not installed"]);
    assert_tablib_telemetry(&server, false).await;

    // Install specific version
    server
        .install_package()
        .add_package(format!("{TEST_PACKAGE}=={TEST_VERSION}"))
        .run()?;

    // Verify correct version installed
    let logs = run_version_check(&server, test.plugin_file_relative()).await?;
    assert_eq!(logs, vec![TEST_VERSION]);
    assert_tablib_telemetry(&server, true).await;
    // And that it isn't on the system python.
    assert_tablib_not_in_system_python();
    Ok(())
}

#[test_log::test(tokio::test)]
#[ignore]
async fn test_venv_requirements_install() -> Result<()> {
    let test = VenvTest::new()?;
    setup_python_venv(&test.venv_path())?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .spawn()
        .await;

    server.create_database(TEST_DB).run()?;

    // Check package is not installed
    let logs = run_version_check(&server, test.plugin_file_relative()).await?;
    assert_eq!(logs, vec!["tablib is not installed"]);

    // Create requirements.txt
    let mut requirements_file = NamedTempFile::new()?;
    writeln!(requirements_file, "{TEST_PACKAGE}=={TEST_VERSION}")?;

    // Install from requirements
    server
        .install_package()
        .with_requirements_file(requirements_file.path().to_str().unwrap())
        .run()?;

    // Verify installation
    let logs = run_version_check(&server, test.plugin_file_relative()).await?;
    assert_eq!(logs, vec![TEST_VERSION]);
    // And that it isn't on the system python.
    assert_tablib_not_in_system_python();
    Ok(())
}

#[test_log::test(tokio::test)]
#[ignore]
async fn test_venv_remote_install() -> Result<()> {
    let test = VenvTest::new()?;
    setup_python_venv(&test.venv_path())?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .spawn()
        .await;

    server.create_database(TEST_DB).run()?;

    // Check package is not installed
    let logs = run_version_check(&server, test.plugin_file_relative()).await?;
    assert_eq!(logs, vec!["tablib is not installed"]);

    // Test remote installation
    server
        .install_package()
        .with_requirements_file(TEST_PACKAGE)
        .run()?;

    // Verify installation
    let logs = run_version_check(&server, test.plugin_file_relative()).await?;
    assert!(!logs[0].contains("not installed"));
    // And that it isn't on the system python.
    assert_tablib_not_in_system_python();
    Ok(())
}

#[test_log::test(tokio::test)]
#[ignore]
async fn test_auto_venv_pip_install() -> Result<()> {
    let test = VenvTest::new()?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .with_package_manager("pip")
        .spawn()
        .await;

    server.create_database(TEST_DB).run()?;

    // Check package is not installed
    let logs = run_version_check(&server, test.plugin_file_relative()).await?;
    assert_eq!(logs, vec!["tablib is not installed"]);

    // Install specific version
    server
        .install_package()
        .add_package(format!("{TEST_PACKAGE}=={TEST_VERSION}"))
        .with_package_manager("pip")
        .run()?;

    // Verify correct version installed
    let logs = run_version_check(&server, test.plugin_file_relative()).await?;
    assert_eq!(logs, vec![TEST_VERSION]);
    // And that it isn't on the system python.
    assert_tablib_not_in_system_python();

    Ok(())
}

fn assert_tablib_not_in_system_python() {
    let output = Command::new("python3")
        .args([
            "-c",
            "import pkg_resources; pkg_resources.get_distribution('tablib')",
        ])
        .output()
        .map_err(|e| anyhow::anyhow!("Failed to run python: {}", e))
        .unwrap();

    if output.status.success() {
        panic!(
            "tablib is installed in system Python. In OS X this can cause dependency conflicts to tests, as pyo3 doesn't fully leverage the virtualenv paths."
        );
    }
}
