// --hard-delete-default-duration was removed as dead in 3.11 (#4064) but is
// exposed by a third-party reseller's 3.10 configurations, so it must keep
// parsing (as an inert, warned-about arg) instead of failing startup.
#[test]
fn test_hard_delete_default_duration_parses_as_inert() {
    use clap::Parser as _;

    let config = super::Config::try_parse_from([
        "cmd",
        "--node-id",
        "test",
        "--hard-delete-default-duration",
        "30d",
    ])
    .expect("removed flag must still parse");
    assert_eq!(config.hard_delete_default_duration.as_deref(), Some("30d"));

    let config = super::Config::try_parse_from(["cmd", "--node-id", "test"])
        .expect("serve config should parse");
    assert!(config.hard_delete_default_duration.is_none());
}

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

// The legacy spellings of the renamed size options must keep accepting the
// pre-3.11 value format (bare numbers), and --max-http-request-size must
// keep accepting bare bytes: these four options are exposed by a
// third-party reseller's 3.9/3.10 configurations, which must not fail
// startup after an upgrade.
#[test]
fn test_legacy_size_option_spellings_accept_pre_311_values() {
    use clap::Parser as _;
    use std::collections::HashMap;

    let mut config = super::Config::try_parse_from([
        "cmd",
        "--node-id",
        "test",
        "--parquet-mem-cache-size",
        "500",
        "--exec-mem-pool-bytes",
        "8192",
        "--force-snapshot-mem-threshold",
        "1000",
        "--max-http-request-size",
        "10485760",
    ])
    .expect("a pre-3.11 configuration must parse");

    super::resolve_legacy_size_options(&mut config, &HashMap::new());

    // Bare numbers on the legacy spellings mean megabytes, as in 3.10.
    assert_eq!(config.file_cache_size.as_num_bytes(), 500 * 1024 * 1024);
    assert_eq!(config.exec_mem_pool_size.as_num_bytes(), 8192 * 1024 * 1024);
    assert_eq!(
        config.force_snapshot_mem_size.as_num_bytes(),
        1000 * 1024 * 1024
    );
    // A bare number for --max-http-request-size means bytes, as in 3.10.
    assert_eq!(config.max_http_request_size.as_num_bytes(), 10485760);
    assert!(config.max_http_request_size.is_bare());
}

#[test]
fn test_new_size_option_names_reject_bare_numbers() {
    use clap::Parser as _;

    for (flag, value) in [
        ("--file-cache-size", "500"),
        ("--exec-mem-pool-size", "8192"),
        ("--force-snapshot-mem-size", "1000"),
    ] {
        let result = super::Config::try_parse_from(["cmd", "--node-id", "test", flag, value]);
        assert!(result.is_err(), "{flag} must reject bare numbers");
    }
}

#[test]
fn test_explicitly_set_new_size_spelling_wins_over_legacy() {
    use clap::Parser as _;
    use std::collections::HashMap;

    let mut config = super::Config::try_parse_from([
        "cmd",
        "--node-id",
        "test",
        "--file-cache-size",
        "1gb",
        "--parquet-mem-cache-size",
        "500",
    ])
    .expect("serve config should parse");

    let user_params = HashMap::from([
        ("file-cache-size".to_string(), "1gb".to_string()),
        ("parquet-mem-cache-size".to_string(), "500".to_string()),
    ]);
    super::resolve_legacy_size_options(&mut config, &user_params);

    assert_eq!(config.file_cache_size.as_num_bytes(), 1024 * 1024 * 1024);
}

// Env-var twins of the three tests above. The env route is separately
// wired: the legacy env names bind directly to the hidden legacy args
// rather than being copied onto the new names by an ENV_ALIASES pass.
//
// This test mutates the process environment. nextest's process-per-test
// execution isolates it; the vars are cleared before and after each case
// regardless so a plain `cargo test` run is left clean.
#[test]
fn test_size_option_env_vars_mirror_cli_behavior() {
    use clap::{CommandFactory as _, FromArgMatches as _, Parser as _, parser::ValueSource};
    use std::collections::HashMap;

    const VARS: &[&str] = &[
        "INFLUXDB3_PARQUET_MEM_CACHE_SIZE",
        "INFLUXDB3_EXEC_MEM_POOL_BYTES",
        "INFLUXDB3_FORCE_SNAPSHOT_MEM_THRESHOLD",
        "INFLUXDB3_MAX_HTTP_REQUEST_SIZE",
        "INFLUXDB3_FILE_CACHE_SIZE",
        "INFLUXDB3_EXEC_MEM_POOL_SIZE",
        "INFLUXDB3_FORCE_SNAPSHOT_MEM_SIZE",
    ];
    let clear = || {
        for name in VARS {
            unsafe { std::env::remove_var(name) };
        }
    };
    let base_args = ["cmd", "--node-id", "test"];

    // The legacy env names accept the pre-3.11 bare-number format.
    clear();
    unsafe {
        std::env::set_var("INFLUXDB3_PARQUET_MEM_CACHE_SIZE", "500");
        std::env::set_var("INFLUXDB3_EXEC_MEM_POOL_BYTES", "8192");
        std::env::set_var("INFLUXDB3_FORCE_SNAPSHOT_MEM_THRESHOLD", "1000");
        std::env::set_var("INFLUXDB3_MAX_HTTP_REQUEST_SIZE", "10485760");
    }
    // Run the startup alias pass as production does: the legacy size names
    // must NOT be copied onto the strict new names — a re-added ENV_ALIASES
    // entry would route the bare value into the strict parser and fail
    // startup.
    influxdb3_startup::env_compat::copy_env_aliases(influxdb3_startup::env_compat::ENV_ALIASES);
    for var in [
        "INFLUXDB3_FILE_CACHE_SIZE",
        "INFLUXDB3_EXEC_MEM_POOL_SIZE",
        "INFLUXDB3_FORCE_SNAPSHOT_MEM_SIZE",
    ] {
        assert!(
            std::env::var(var).is_err(),
            "{var} must not be populated from a legacy env spelling"
        );
    }
    let mut config =
        super::Config::try_parse_from(base_args).expect("a pre-3.11 env configuration must parse");
    super::resolve_legacy_size_options(&mut config, &HashMap::new());
    assert_eq!(config.file_cache_size.as_num_bytes(), 500 * 1024 * 1024);
    assert_eq!(config.exec_mem_pool_size.as_num_bytes(), 8192 * 1024 * 1024);
    assert_eq!(
        config.force_snapshot_mem_size.as_num_bytes(),
        1000 * 1024 * 1024
    );
    assert_eq!(config.max_http_request_size.as_num_bytes(), 10485760);
    assert!(config.max_http_request_size.is_bare());

    // The new env names route into the strict parsers: bare numbers fail.
    for (var, value) in [
        ("INFLUXDB3_FILE_CACHE_SIZE", "500"),
        ("INFLUXDB3_EXEC_MEM_POOL_SIZE", "8192"),
        ("INFLUXDB3_FORCE_SNAPSHOT_MEM_SIZE", "1000"),
    ] {
        clear();
        unsafe { std::env::set_var(var, value) };
        assert!(
            super::Config::try_parse_from(base_args).is_err(),
            "{var} must reject bare numbers"
        );
    }

    // An env-set new spelling wins over an env-set legacy one. clap reports
    // env-set options as ValueSource::EnvVariable, which
    // `user_provided_value_source` counts as explicitly set, so
    // `extract_user_params` puts the new name in the user-params map just
    // as it does for a CLI arg — asserted here so a change to how clap
    // sources env values would fail this test rather than silently flip
    // the precedence.
    clear();
    unsafe {
        std::env::set_var("INFLUXDB3_FILE_CACHE_SIZE", "1gb");
        std::env::set_var("INFLUXDB3_PARQUET_MEM_CACHE_SIZE", "500");
    }
    let matches = super::Config::command()
        .try_get_matches_from(base_args)
        .expect("serve config should parse");
    assert_eq!(
        matches.value_source("file_cache_size"),
        Some(ValueSource::EnvVariable)
    );
    let mut config =
        super::Config::from_arg_matches(&matches).expect("config should build from matches");
    let user_params = HashMap::from([
        ("file-cache-size".to_string(), "1gb".to_string()),
        ("parquet-mem-cache-size".to_string(), "500".to_string()),
    ]);
    super::resolve_legacy_size_options(&mut config, &user_params);
    assert_eq!(config.file_cache_size.as_num_bytes(), 1024 * 1024 * 1024);

    clear();
}
