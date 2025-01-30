use std::fs;
use std::fs::create_dir;
use crate::cli::run_with_confirmation;
use crate::{ConfigProvider, TestServer};
use anyhow::{bail, Result};
use serde_json::Value;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use libc::mkdir;
use observability_deps::tracing::info;
use tempfile::{NamedTempFile, TempDir};

const TEST_PACKAGE: &str = "tablib";
const TEST_VERSION: &str = "3.8.0";

struct VenvTest {
    venv_dir: PathBuf,
    plugin_file: NamedTempFile,
    parent_dir: TempDir,
}

impl VenvTest {
    fn new(name :&str) -> Result<Self> {
        let parent_dir = TempDir::new()?;
        // make a dir named `name`
        let subdir = parent_dir.path().join(name);
        create_dir(subdir.clone())?;
        let venv_dir = subdir.join(".venv");
        println!("venv_dir: {:?}", venv_dir);
        let plugin_file = create_version_check_plugin(&subdir).expect("Cannot create plugin");

        Ok(Self {
            venv_dir,
            plugin_file,
            parent_dir
        })
    }

    fn venv_path(&self) -> PathBuf {
        self.venv_dir.clone()
    }

    fn plugin_dir(&self) -> String {
        self.venv_dir
            .parent()
            .unwrap()
            .to_string_lossy()
            .to_string()
    }
}

fn create_version_check_plugin(dir: &Path) -> Result<NamedTempFile> {
    let plugin_code = r#"
from importlib.metadata import version, PackageNotFoundError

def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    try:
        package_version = version('tablib')
        influxdb3_local.info(package_version)
    except PackageNotFoundError:
        influxdb3_local.info("tablib is not installed")
"#;
    let mut plugin_file = NamedTempFile::new_in(dir)?;
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
    let errors = &json["errors"] .as_array().unwrap();
    if !errors.is_empty() {
        bail!("failed with errors: {:?}", errors);
    }
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
    let test = VenvTest::new("test_python_venv_pip_install")?;
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
        &format!("{}=={}", TEST_PACKAGE, TEST_VERSION),
    ]);

    // Verify correct version installed
    let logs = run_version_check(&server_addr, test.plugin_file.path().to_str().unwrap()).await?;
    assert_eq!(logs, vec![TEST_VERSION]);
    // And that it isn't on the system python.
    assert_tablib_not_in_system_python();
    Ok(())
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_uv_venv_uv_install() -> Result<()> {
    let test = VenvTest::new("test_uv_venv_uv_install")?;
    setup_uv_venv(&test.venv_path())?;

    let mut server = TestServer::configure()
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
    let output = run_with_confirmation(&["install", "package", "--host", &server_addr, TEST_PACKAGE]);
    println!("install output: {:?}", output);

    // Verify package is installed
    let logs = run_version_check(&server_addr, test.plugin_file.path().to_str().unwrap()).await?;
    if !logs[0].contains("not installed") {
        let reader = BufReader::new(server.server_process.stdout.take().unwrap());
        for line in reader.lines() {
            println!("stdout:{}", line.unwrap());
        }
    }
    assert!(!logs[0].contains("not installed"));
    // And that it isn't on the system python.
    assert_tablib_not_in_system_python();
    Ok(())
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_venv_requirements_install() -> Result<()> {
    let test = VenvTest::new("test_venv_requirements_install")?;
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
    // And that it isn't on the system python.
    assert_tablib_not_in_system_python();
    Ok(())
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_venv_remote_install() -> Result<()> {
    let test = VenvTest::new("test_venv_remote_install")?;
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
    // And that it isn't on the system python.
    assert_tablib_not_in_system_python();
    Ok(())
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_auto_venv_pip_install() -> Result<()> {
    let test = VenvTest::new("test_auto_venv_pip_install")?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .with_package_manager("pip")
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
    // And that it isn't on the system python.
    assert_tablib_not_in_system_python();

    Ok(())
}

#[cfg(feature = "system-py")]
#[test_log::test(tokio::test)]
async fn test_auto_venv_uv_install() -> Result<()> {
    let test = VenvTest::new("test_auto_venv_uv_install")?;

    println!("plugin dir is {:?}", test.plugin_dir());
    println!("test file is {:?}", test.plugin_file.path());

    // list out contents of plugin_dir
    for path in fs::read_dir(test.plugin_dir())? {
        let entry = path?;
        let metadata = entry.metadata()?;
        println!("{} {} bytes",
                 entry.path().display(),
                 metadata.len()
        );
    }
    let plugin_contents = fs::read_to_string(test.plugin_file.path())?;
    println!("plugin file is {}", test.plugin_file.path().to_string_lossy());
    println!("plugin contents is {}", plugin_contents);

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
      //  .with_virtual_env(test.venv_path().to_string_lossy())
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

    let venv_dir = test.plugin_file.path().parent().unwrap().join(".venv");
    println!("venv_dir is {:?}", venv_dir);
    // list out contents of plugin_dir
    for path in fs::read_dir(venv_dir)? {
        let entry = path?;
        let metadata = entry.metadata()?;
        println!("{} {} bytes",
                 entry.path().display(),
                 metadata.len()
        );
    }

    // Check package is not installed
    let logs = run_version_check(&server_addr, test.plugin_file.path().to_str().unwrap()).await?;
    assert_eq!(vec!["tablib is not installed"], logs);

    // Install with UV
    let install_result = run_with_confirmation(&["install", "package", "--host", &server_addr, TEST_PACKAGE]);
    println!("install_result: {:?}", install_result);

    // Verify package is installed
    let logs = run_version_check(&server_addr, test.plugin_file.path().to_str().unwrap()).await?;
    println!("logs:{:?}", logs);
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
        panic!("tablib is installed in system Python. In OS X this can cause dependency conflicts to tests, as pyo3 doesn't fully leverage the virtualenv paths.");
    }
}
