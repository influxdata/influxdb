use super::*;
use clap::Parser;

#[test]
fn disable_package_management_defaults_to_false() {
    let config = ProcessingEngineConfig::try_parse_from(["test"]).unwrap();
    assert!(!config.disable_package_management);
}

#[test]
fn disable_package_management_flag_sets_true() {
    let config =
        ProcessingEngineConfig::try_parse_from(["test", "--disable-package-management"]).unwrap();
    assert!(config.disable_package_management);
}
