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

fn python_bin() -> PathBuf {
    // In CircleCI use the same standalone Python configured for PyO3.
    if let Ok(pyo3_config) = std::env::var("PYO3_CONFIG_FILE")
        && let Some(path) = PathBuf::from(pyo3_config).parent()
    {
        path.join("python").join("bin").join("python3")
    } else {
        PathBuf::from("python3")
    }
}

fn setup_python_venv(venv_path: &Path) -> Result<()> {
    let output = Command::new(python_bin())
        .args(["-m", "venv", venv_path.to_str().unwrap()])
        .output()?;
    if !output.status.success() {
        anyhow::bail!(
            "venv setup failed:\n\nCode:\n{}\n\nStdout:\n{}\n\nStderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
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

// Server-side message surfaced (via the CLI's stderr) when package management is off.
const PACKAGE_DISABLED_MSG: &str = "Package installation has been disabled";

fn assert_package_management_disabled(result: &Result<String>) {
    let err = result
        .as_ref()
        .expect_err("install must be rejected when package management is disabled");
    let msg = err.to_string();
    assert!(
        msg.contains(PACKAGE_DISABLED_MSG),
        "expected {PACKAGE_DISABLED_MSG:?} in rejection, got: {msg}"
    );
}

const DISABLE_PM_PLUGIN: &str = r#"
import importlib.metadata

def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    try:
        version = importlib.metadata.version('tablib')
        influxdb3_local.info(f"VERSION: {version}")
    except importlib.metadata.PackageNotFoundError:
        influxdb3_local.info("VERSION: tablib is not installed")
"#;

#[test_log::test(tokio::test)]
async fn test_disable_package_management() -> Result<()> {
    // Isolated plugin dir so any venv would be <plugin_dir>/.venv.
    let plugin_dir = TempDir::new()?;
    std::fs::write(plugin_dir.path().join("check.py"), DISABLE_PM_PLUGIN)?;

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.path().to_string_lossy())
        .with_disable_package_management()
        .spawn()
        .await;

    server.create_database(TEST_DB).run()?;

    // Triggers still run (stdlib-only plugin, no venv packages needed).
    let logs = run_version_check(&server, "check.py").await?;
    assert_eq!(logs, vec!["tablib is not installed"]);

    // Both install endpoints are rejected with the disabled message.
    assert_package_management_disabled(&server.install_package().add_package(TEST_PACKAGE).run());

    let mut requirements_file = NamedTempFile::new()?;
    writeln!(requirements_file, "{TEST_PACKAGE}")?;
    assert_package_management_disabled(
        &server
            .install_package()
            .with_requirements_file(requirements_file.path().to_str().unwrap())
            .run(),
    );

    // No venv was created.
    let venv = plugin_dir.path().join(".venv");
    assert!(
        !venv.exists(),
        "--disable-package-management must not create a .venv, found: {}",
        venv.display()
    );

    Ok(())
}

const VENV_IMPORT_MARKER: &str = "venv-import-ok";

// Imports a module that exists only in the venv's site-packages. The import
// succeeds only if the server put the correct site-packages path on sys.path,
// which depends on the Python version init_pyo3 reads in-process.
const IMPORT_PLUGIN: &str = r#"
def process_scheduled_call(influxdb3_local, schedule_time, args=None):
    import dpm_probe
    influxdb3_local.info(f"IMPORT: {dpm_probe.MARKER}")
"#;

/// The `lib/pythonX.Y/site-packages` directory of a unix venv.
fn venv_site_packages(venv: &Path) -> PathBuf {
    let py_lib = std::fs::read_dir(venv.join("lib"))
        .expect("venv lib dir")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("python"))
        })
        .expect("venv pythonX.Y dir");
    py_lib.join("site-packages")
}

async fn run_import_check(server: &TestServer, plugin_path: &str) -> Result<String> {
    let json = server
        .test_schedule_plugin(TEST_DB, plugin_path, "* * * * * *")
        .run()?;
    let errors = json["errors"].as_array().expect("is array");
    assert!(errors.is_empty(), "Errors:\n{errors:#?}");
    Ok(json["log_lines"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|line| line.as_str().and_then(|s| s.strip_prefix("INFO: IMPORT: ")))
        .expect("plugin should import the venv module")
        .to_owned())
}

// A user managing their own venv (as the flag requires) keeps working: a package
// installed in the venv imports, installs are still rejected, and the venv is
// untouched. Drives the flag via INFLUXDB3_DISABLE_PACKAGE_MANAGEMENT to cover
// env parsing.
#[test_log::test(tokio::test)]
async fn test_disable_package_management_preserves_existing_venv() -> Result<()> {
    let plugin_dir = TempDir::new()?;
    std::fs::write(plugin_dir.path().join("import_check.py"), IMPORT_PLUGIN)?;

    // A pre-existing, user-managed venv with a module already in its site-packages.
    let venv_dir = TempDir::new()?;
    setup_python_venv(venv_dir.path())?;
    let activate = venv_dir.path().join("bin").join("activate");
    assert!(activate.exists(), "precondition: venv created");
    let site_packages = venv_site_packages(venv_dir.path());
    std::fs::write(
        site_packages.join("dpm_probe.py"),
        format!("MARKER = \"{VENV_IMPORT_MARKER}\"\n"),
    )?;

    let server = TestServer::configure()
        .with_plugin_dir(plugin_dir.path().to_string_lossy())
        .with_env_var("INFLUXDB3_DISABLE_PACKAGE_MANAGEMENT", "true")
        .with_env_var("VIRTUAL_ENV", venv_dir.path().to_string_lossy())
        .spawn()
        .await;
    server.create_database(TEST_DB).run()?;

    // The existing venv is usable: a module in its site-packages imports, which
    // only works if the correct site-packages path (right Python version) is set.
    let marker = run_import_check(&server, "import_check.py").await?;
    assert_eq!(marker, VENV_IMPORT_MARKER);

    // Installs are still rejected while the flag is set via env.
    assert_package_management_disabled(&server.install_package().add_package(TEST_PACKAGE).run());

    // The existing venv is untouched.
    assert!(
        activate.exists(),
        "existing venv must survive startup and a rejected install"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Plugin package allowlist
// ---------------------------------------------------------------------------

/// `uv pip compile --generate-hashes --emit-index-url` output for
/// `tablib==3.8.0`. tablib has no required dependencies, so this is the whole
/// closure, and a released version's hashes never change.
const TABLIB_HASHED_ALLOWLIST: &str = "\
# This file was autogenerated by uv via the following command:
#    uv pip compile allowlist.txt --generate-hashes --python-version 3.12 --emit-index-url
--index-url https://pypi.org/simple

tablib==3.8.0 \\
    --hash=sha256:35bdb9d4ec7052232f8803908f9c7a9c3c65807188b70618fa7a7d8ccd560b4d \\
    --hash=sha256:94d8bcdc65a715a0024a6d5b701a5f31e45bd159269e62c73731de79f048db2b
    # via -r allowlist.txt
";

/// Write an allowlist file and return it; the file lives as long as the handle.
fn write_allowlist(contents: &str) -> Result<NamedTempFile> {
    let mut file = NamedTempFile::new()?;
    file.write_all(contents.as_bytes())?;
    Ok(file)
}

/// Upgrade pip in `venv_path` to a release that reads hashes from `-c`
/// constraints files, which a hashed allowlist requires.
fn upgrade_venv_pip(venv_path: &Path) -> Result<()> {
    let output = Command::new(venv_path.join("bin").join("python"))
        .args(["-m", "pip", "install", "--upgrade", "pip>=26.1"])
        .output()?;
    if !output.status.success() {
        anyhow::bail!(
            "pip upgrade failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

/// Assert an install or uninstall was refused by the allowlist (HTTP 403)
/// with a message containing `expected`.
fn assert_forbidden(result: &Result<String>, expected: &str) {
    let msg = result
        .as_ref()
        .expect_err("request should be rejected")
        .to_string();
    assert!(msg.contains("403 Forbidden"), "expected a 403, got: {msg}");
    assert!(
        msg.contains(expected),
        "expected {expected:?} in rejection, got: {msg}"
    );
}

fn uninstall_package(server: &TestServer, package: &str) -> Result<String> {
    server.run(
        vec!["uninstall", "package"],
        &[package, "--tls-ca", "../testing-certs/rootCA.pem"],
    )
}

/// A ranges allowlist refuses unlisted packages, out-of-range versions, and
/// pip options with a 403 before pip runs, and allows an in-range install.
/// Uninstall is not limited by the allowlist, so the package can be removed
/// and the plugin sees it gone.
#[test_log::test(tokio::test)]
#[ignore]
async fn test_allowlist_ranges_install_uninstall_round_trip() -> Result<()> {
    let test = VenvTest::new()?;
    setup_python_venv(&test.venv_path())?;
    let allowlist = write_allowlist(
        "--index-url https://pypi.org/simple\n--only-binary :all:\n\ntablib>=3.8,<3.9\n",
    )?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .with_plugin_package_allowlist(allowlist.path().to_string_lossy())
        .spawn()
        .await;
    server.create_database(TEST_DB).run()?;

    assert_forbidden(
        &server.install_package().add_package("requests").run(),
        "'requests' is not in the package allowlist",
    );
    assert_forbidden(
        &server.install_package().add_package("tablib==3.7.0").run(),
        "'tablib==3.7.0' is not allowed. Allowlisted version(s) of 'tablib': >=3.8, <3.9",
    );
    assert_forbidden(
        &server
            .install_package()
            .add_package("tablib @ https://example.invalid/tablib-3.8.0-py3-none-any.whl")
            .run(),
        "direct URL references",
    );
    let mut requirements_file = NamedTempFile::new()?;
    writeln!(
        requirements_file,
        "--extra-index-url https://example.invalid/simple"
    )?;
    writeln!(requirements_file, "{TEST_PACKAGE}=={TEST_VERSION}")?;
    assert_forbidden(
        &server
            .install_package()
            .with_requirements_file(requirements_file.path().to_str().unwrap())
            .run(),
        "requirements file entry on line 1 is not allowed",
    );
    let logs = run_version_check(&server, test.plugin_file_relative()).await?;
    assert_eq!(logs, vec!["tablib is not installed"]);

    server
        .install_package()
        .add_package(format!("{TEST_PACKAGE}=={TEST_VERSION}"))
        .run()?;
    let logs = run_version_check(&server, test.plugin_file_relative()).await?;
    assert_eq!(logs, vec![TEST_VERSION]);

    uninstall_package(&server, TEST_PACKAGE)?;
    let logs = run_version_check(&server, test.plugin_file_relative()).await?;
    assert_eq!(logs, vec!["tablib is not installed"]);
    assert_tablib_not_in_system_python();
    Ok(())
}

/// A hashed allowlist installs the requested package with pip verifying its
/// hash from the allowlist (`-c <allowlist> --require-hashes`), refuses
/// versions it does not pin, and fails the install when the downloaded
/// artifact does not match a listed hash.
#[test_log::test(tokio::test)]
#[ignore]
async fn test_allowlist_hashed_tablib() -> Result<()> {
    let test = VenvTest::new()?;
    setup_python_venv(&test.venv_path())?;
    upgrade_venv_pip(&test.venv_path())?;
    let allowlist = write_allowlist(TABLIB_HASHED_ALLOWLIST)?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .with_plugin_package_allowlist(allowlist.path().to_string_lossy())
        .spawn()
        .await;
    server.create_database(TEST_DB).run()?;

    assert_forbidden(
        &server.install_package().add_package("tablib==3.7.0").run(),
        "Allowlisted version(s) of 'tablib': ==3.8.0",
    );
    assert_forbidden(
        &server.install_package().add_package("requests").run(),
        "'requests' is not in the package allowlist",
    );
    // Ranges the pinned version does not satisfy are refused before pip runs.
    for spec in [
        "tablib<3.8",
        "tablib>3.8.0",
        "tablib!=3.8.0",
        "tablib==3.9.*",
    ] {
        assert_forbidden(
            &server.install_package().add_package(spec).run(),
            "Allowlisted version(s) of 'tablib': ==3.8.0",
        );
    }

    // A range the pin satisfies installs the pinned version, hash-verified.
    server.install_package().add_package("tablib>=3.8").run()?;
    let logs = run_version_check(&server, test.plugin_file_relative()).await?;
    assert_eq!(logs, vec![TEST_VERSION]);

    // So does a bare name.
    uninstall_package(&server, TEST_PACKAGE)?;
    server.install_package().add_package(TEST_PACKAGE).run()?;
    let logs = run_version_check(&server, test.plugin_file_relative()).await?;
    assert_eq!(logs, vec![TEST_VERSION]);
    assert_tablib_not_in_system_python();
    Ok(())
}

/// Range requests in ranges mode. The server checks only exact `==` pins
/// against the allowlist; other specifiers go to pip, which applies the
/// allowlist as `-c` constraints. A request that overlaps the allowed range
/// installs within it; one that does not fails in pip's resolver (500 with
/// pip's error), not with a 403.
#[test_log::test(tokio::test)]
#[ignore]
async fn test_allowlist_ranges_version_specifiers() -> Result<()> {
    let test = VenvTest::new()?;
    setup_python_venv(&test.venv_path())?;
    // Excludes 3.9.0 and later, which an unconstrained `tablib>=3.8` would
    // pick, so getting 3.8.0 shows pip applied the allowlist.
    let allowlist = write_allowlist(
        "--index-url https://pypi.org/simple\n--only-binary :all:\n\ntablib>=3.8,<3.9\n",
    )?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .with_plugin_package_allowlist(allowlist.path().to_string_lossy())
        .spawn()
        .await;
    server.create_database(TEST_DB).run()?;

    // pip picks the newest version allowed by both the request and the
    // allowlist.
    for spec in [
        "tablib>=3.8",
        "tablib>3.7,<=3.9",
        "tablib~=3.0",
        "tablib!=3.9.0",
    ] {
        server.install_package().add_package(spec).run()?;
        let logs = run_version_check(&server, test.plugin_file_relative()).await?;
        assert_eq!(logs, vec![TEST_VERSION], "{spec}");
        uninstall_package(&server, TEST_PACKAGE)?;
    }

    // No overlap with the allowlist: pip cannot resolve it.
    for spec in ["tablib<3.8", "tablib>=3.9", "tablib==3.9.*"] {
        let msg = server
            .install_package()
            .add_package(spec)
            .run()
            .expect_err("pip should fail to resolve")
            .to_string();
        assert!(msg.contains("500 Internal Server Error"), "{spec}: {msg}");
        assert!(msg.contains("'pip install' failed"), "{spec}: {msg}");
        assert!(msg.contains("ResolutionImpossible"), "{spec}: {msg}");
    }
    let logs = run_version_check(&server, test.plugin_file_relative()).await?;
    assert_eq!(logs, vec!["tablib is not installed"]);
    assert_tablib_not_in_system_python();
    Ok(())
}

/// pip rejects an artifact whose hash is not in the allowlist, and the error
/// names the failed pip command rather than a missing package manager.
#[test_log::test(tokio::test)]
#[ignore]
async fn test_allowlist_hashed_rejects_hash_mismatch() -> Result<()> {
    let test = VenvTest::new()?;
    setup_python_venv(&test.venv_path())?;
    upgrade_venv_pip(&test.venv_path())?;
    // Same file with both hashes altered.
    let tampered = TABLIB_HASHED_ALLOWLIST
        .replace("sha256:35bdb9d4", "sha256:00000000")
        .replace("sha256:94d8bcdc", "sha256:00000000");
    let allowlist = write_allowlist(&tampered)?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .with_plugin_package_allowlist(allowlist.path().to_string_lossy())
        .spawn()
        .await;
    server.create_database(TEST_DB).run()?;

    let msg = server
        .install_package()
        .add_package(TEST_PACKAGE)
        .run()
        .expect_err("pip should reject the mismatched hash")
        .to_string();
    assert!(msg.contains("'pip install' failed"), "got: {msg}");
    assert!(
        msg.contains("THESE PACKAGES DO NOT MATCH THE HASHES"),
        "got: {msg}"
    );
    assert!(!msg.contains("Package manager not available"), "got: {msg}");

    let logs = run_version_check(&server, test.plugin_file_relative()).await?;
    assert_eq!(logs, vec!["tablib is not installed"]);
    Ok(())
}

/// Install specs that pip would parse as options are refused even with no
/// allowlist configured. Needs no network: the request never reaches pip.
#[test_log::test(tokio::test)]
async fn test_install_rejects_pip_options_without_allowlist() -> Result<()> {
    let test = VenvTest::new()?;
    setup_python_venv(&test.venv_path())?;

    let server = TestServer::configure()
        .with_plugin_dir(test.plugin_dir())
        .with_virtual_env(test.venv_path().to_string_lossy())
        .spawn()
        .await;

    // `--` ends CLI option parsing so the spec reaches the server as written.
    for spec in ["--python=/tmp/x", "-r/etc/passwd"] {
        let result = server.run(
            vec!["install", "package"],
            &[
                "--tls-ca",
                "../testing-certs/rootCA.pem",
                "--",
                spec,
                "numpy",
            ],
        );
        assert_forbidden(&result, "pip options are not allowed");
    }
    Ok(())
}
