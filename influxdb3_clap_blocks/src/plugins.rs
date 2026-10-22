use std::num::NonZeroUsize;
use std::path::PathBuf;

// Specifies the behavior of the Processing Engine.
// Currently used to determine the plugin directory and which tooling to use to initialize python,
// but will expand for other settings, such as error behavior.
#[derive(Debug, clap::Parser, Clone)]
pub struct ProcessingEngineConfig {
    /// Location of the plugins
    #[clap(long = "plugin-dir", env = "INFLUXDB3_PLUGIN_DIR")]
    pub plugin_dir: Option<PathBuf>,
    #[clap(long = "virtual-env-location", env = "VIRTUAL_ENV")]
    pub virtual_env_location: Option<PathBuf>,
    /// Deprecated. Python and pip are bundled with InfluxDB; this option does not
    /// need to be set because pip is always used for environment setup. `disabled`
    /// only keeps blocking plugin package install API calls for compatibility.
    #[clap(
        long = "package-manager",
        env = "INFLUXDB3_PACKAGE_MANAGER",
        default_value = "discover"
    )]
    pub package_manager: PackageManager,
    /// Disable Processing Engine package management: the server never creates or
    /// touches a virtual environment or invokes `pip`, and package-install API calls
    /// are rejected. Takes precedence over `--package-manager`.
    #[clap(
        long = "disable-package-management",
        env = "INFLUXDB3_DISABLE_PACKAGE_MANAGEMENT"
    )]
    pub disable_package_management: bool,
    #[clap(long = "plugin-repo", env = "INFLUXDB3_PLUGIN_REPO")]
    pub plugin_repo: Option<String>,
    /// Restrict plugin triggers to the provided trigger type(s).
    #[clap(
        long = "restrict-plugin-triggers-to",
        env = "INFLUXDB3_RESTRICT_PLUGIN_TRIGGERS_TO",
        value_enum,
        value_delimiter = ',',
        num_args = 1..
    )]
    pub restrict_plugin_triggers_to: Vec<PluginTriggerType>,
    /// Maximum number of concurrent invocations per asynchronous trigger
    /// (`run_asynchronous`). Defaults to unlimited. Synchronous triggers always
    /// run one invocation at a time. Must be greater than zero.
    #[clap(
        long = "async-trigger-concurrency-limit",
        env = "INFLUXDB3_ASYNC_TRIGGER_CONCURRENCY_LIMIT",
        default_value_t = NonZeroUsize::MAX
    )]
    pub async_trigger_concurrency_limit: NonZeroUsize,
    /// Path to a file listing the packages end users are allowed to install via
    /// the Processing Engine package-install API (the allowlist). Each non-blank,
    /// non-comment line must be a pinned `name==version` spec (multiple versions
    /// of the same package are allowed on separate lines). When set,
    /// `install_packages` requests are validated against the allowlist and
    /// rejected with HTTP 403 if the exact `name==version` is not allowlisted.
    /// When unset, no allowlist check is applied.
    #[clap(
        long = "plugin-package-allowlist",
        env = "INFLUXDB3_PLUGIN_PACKAGE_ALLOWLIST"
    )]
    pub plugin_package_allowlist: Option<PathBuf>,
    /// Path to a server-controlled package constraints file passed to every
    /// package-install invocation as `-c <file>`. Used to pin exact versions
    /// and, with `--plugin-package-constraints-mode require-hashes`, to enforce
    /// integrity via hashes the server owns (generate with
    /// `pip-compile --generate-hashes`). When unset, no constraints file is
    /// passed.
    #[clap(
        long = "plugin-package-constraints",
        env = "INFLUXDB3_PLUGIN_PACKAGE_CONSTRAINTS"
    )]
    pub plugin_package_constraints: Option<PathBuf>,
    /// How the constraints file is enforced. `pin` (the default) uses it only to
    /// pin versions. `require-hashes` additionally passes `--require-hashes`,
    /// forcing a hash to be verified for every package in the resolved
    /// dependency closure; the hashes must be present in the constraints file.
    /// `require-hashes` requires `--plugin-package-constraints` to be set, or the
    /// server fails to start.
    #[clap(
        long = "plugin-package-constraints-mode",
        env = "INFLUXDB3_PLUGIN_PACKAGE_CONSTRAINTS_MODE",
        value_enum,
        default_value = "pin"
    )]
    pub plugin_package_constraints_mode: PipConstraintsMode,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, clap::ValueEnum)]
pub enum PluginTriggerType {
    #[value(alias = "wal_flush")]
    Wal,
    Schedule,
    Request,
}

#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum PackageManager {
    #[default]
    Discover,
    Pip,
    UV,
    Disabled,
}

/// How a configured pip constraints file is enforced.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum PipConstraintsMode {
    /// Use the constraints file to pin versions only.
    #[default]
    Pin,
    /// Pin versions and pass `--require-hashes`, forcing pip to verify a hash
    /// for every package in the resolved dependency closure.
    RequireHashes,
}

#[cfg(test)]
mod tests;
