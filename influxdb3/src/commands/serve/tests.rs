#[test]
fn disable_package_management_builds_no_venv() {
    use influxdb3_clap_blocks::plugins::{PackageManager, ProcessingEngineConfig};
    use tempfile::TempDir;

    let plugin_dir = TempDir::new().unwrap();

    // The flag forces DisabledPackageManager and builds no venv even for package
    // managers that would otherwise build or probe one.
    for package_manager in [PackageManager::Discover, PackageManager::Pip] {
        let config = ProcessingEngineConfig {
            plugin_dir: Some(plugin_dir.path().to_path_buf()),
            virtual_env_location: None,
            package_manager,
            disable_package_management: true,
            plugin_repo: None,
            restrict_plugin_triggers_to: Vec::new(),
            async_trigger_concurrency_limit: std::num::NonZeroUsize::MAX,
        };

        let env = super::setup_processing_engine_env_manager(&config);
        let manager_debug = format!("{:?}", env.package_manager);
        assert!(
            manager_debug.contains("DisabledPackageManager"),
            "expected DisabledPackageManager, got: {manager_debug}"
        );
        assert!(
            !plugin_dir.path().join(".venv").exists(),
            "--disable-package-management must not create a .venv"
        );
    }
}
