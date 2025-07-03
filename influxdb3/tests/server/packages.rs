use crate::server::{ConfigProvider, TestServer};
use anyhow::{Result, bail};
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

async fn run_version_check(test_server: &TestServer, plugin_path: &str) -> Result<Vec<String>> {
    let json = test_server
        .test_schedule_plugin("version_test", plugin_path, "* * * * * *")
        .run()?;
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

#[test_log::test(tokio::test)]
#[ignore]
async fn test_python_venv_pip_install() -> Result<()> {
    let test = VenvTest::new()?;
    setup_python_venv(&test.venv_path())?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .spawn()
        .await;

    server.create_database("version_check").run()?;

    // Check package is not installed
    let logs = run_version_check(&server, test.plugin_file.path().to_str().unwrap()).await?;
    assert_eq!(logs, vec!["tablib is not installed"]);

    // Install specific version
    server
        .install_package()
        .add_package(format!("{TEST_PACKAGE}=={TEST_VERSION}"))
        .run()?;

    // Verify correct version installed
    let logs = run_version_check(&server, test.plugin_file.path().to_str().unwrap()).await?;
    assert_eq!(logs, vec![TEST_VERSION]);
    // And that it isn't on the system python.
    assert_tablib_not_in_system_python();
    Ok(())
}

#[test_log::test(tokio::test)]
#[ignore]
async fn test_uv_venv_uv_install() -> Result<()> {
    let test = VenvTest::new()?;
    setup_uv_venv(&test.venv_path())?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .with_package_manager("uv")
        .spawn()
        .await;

    server.create_database("version_check").run()?;

    // Check package is not installed
    let logs = run_version_check(&server, test.plugin_file.path().to_str().unwrap()).await?;
    assert_eq!(vec!["tablib is not installed"], logs);

    // Install with UV
    server.install_package().add_package(TEST_PACKAGE).run()?;

    // Verify package is installed
    let logs = run_version_check(&server, test.plugin_file.path().to_str().unwrap()).await?;
    assert!(!logs[0].contains("not installed"));
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

    server.create_database("version_check").run()?;

    // Check package is not installed
    let logs = run_version_check(&server, test.plugin_file.path().to_str().unwrap()).await?;
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
    let logs = run_version_check(&server, test.plugin_file.path().to_str().unwrap()).await?;
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

    server.create_database("version_check").run()?;

    // Check package is not installed
    let logs = run_version_check(&server, test.plugin_file.path().to_str().unwrap()).await?;
    assert_eq!(logs, vec!["tablib is not installed"]);

    // Test remote installation
    server
        .install_package()
        .with_requirements_file(TEST_PACKAGE)
        .run()?;

    // Verify installation
    let logs = run_version_check(&server, test.plugin_file.path().to_str().unwrap()).await?;
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

    server.create_database("version_check").run()?;

    // Check package is not installed
    let logs = run_version_check(&server, test.plugin_file.path().to_str().unwrap()).await?;
    assert_eq!(logs, vec!["tablib is not installed"]);

    // Install specific version
    server
        .install_package()
        .add_package(format!("{TEST_PACKAGE}=={TEST_VERSION}"))
        .with_package_manager("pip")
        .run()?;

    // Verify correct version installed
    let logs = run_version_check(&server, test.plugin_file.path().to_str().unwrap()).await?;
    assert_eq!(logs, vec![TEST_VERSION]);
    // And that it isn't on the system python.
    assert_tablib_not_in_system_python();

    Ok(())
}

#[test_log::test(tokio::test)]
#[ignore]
async fn test_auto_venv_uv_install() -> Result<()> {
    let test = VenvTest::new()?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .with_package_manager("uv")
        .spawn()
        .await;

    server.create_database("version_check").run()?;

    // Check package is not installed
    let logs = run_version_check(&server, test.plugin_file.path().to_str().unwrap()).await?;
    assert_eq!(vec!["tablib is not installed"], logs);

    // Install with UV
    server.install_package().add_package(TEST_PACKAGE).run()?;

    // Verify package is installed
    let logs = run_version_check(&server, test.plugin_file.path().to_str().unwrap()).await?;
    assert!(!logs[0].contains("not installed"));
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
