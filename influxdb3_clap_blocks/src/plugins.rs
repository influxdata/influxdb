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
    /// Path to the plugin package allowlist: a pip requirements file listing
    /// the packages users may install with the Processing Engine
    /// package-install APIs, the versions they may install, and the package
    /// index pip uses (`--index-url`, `--extra-index-url`, `--only-binary`, …).
    ///
    /// The file uses one of two modes and cannot mix them:
    ///
    /// * Version ranges: every entry is a PEP 508 dependency specifier, such as
    ///   `numpy>=2,<3`.
    /// * Pinned hashes: every entry is pinned with `==` and has `--hash`
    ///   options, covering the packages and all their dependencies (for
    ///   example the output of `uv pip compile --generate-hashes`). Requires
    ///   pip 26.1 or later in the plugin environment.
    ///
    /// The server refuses to start with an invalid allowlist and refuses
    /// installs of packages or versions it does not allow. The allowlist is
    /// passed to pip as a constraints file, so it only limits new installs; it
    /// never installs, removes, or changes a package. Changing it requires a
    /// restart. When unset, installs are not restricted.
    #[clap(
        long = "plugin-package-allowlist",
        env = "INFLUXDB3_PLUGIN_PACKAGE_ALLOWLIST"
    )]
    pub plugin_package_allowlist: Option<PathBuf>,
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

#[cfg(test)]
mod tests;
