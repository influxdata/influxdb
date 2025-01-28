use std::path::PathBuf;

// Specifies the behavior of the Processing Engine.
// Currently used to determine the plugin directory and which tooling to use to initialize python,
// but will expand for other settings, such as error behavior.
#[derive(Debug, clap::Parser, Clone)]
pub struct ProcessingEngineConfig {
    /// Location of the plugins
    #[clap(long = "plugin-dir")]
    pub plugin_dir: Option<PathBuf>,
    #[clap(long = "virtual-env-location", env = "VIRTUAL_ENV")]
    pub virtual_env_location: Option<PathBuf>,
    #[clap(long = "package-manager", default_value = "discover")]
    pub package_manager: PackageManager,
}

#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
pub enum PackageManager {
    #[default]
    Discover,
    Pip,
    UV,
}
