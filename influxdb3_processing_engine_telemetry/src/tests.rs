use super::*;
use std::{sync::Arc, time::Duration};

#[test]
fn normalize_plugin_name_handles_supported_plugin_reference_forms() {
    let test_cases = [
        ("gh:influxdata/downsampler/downsampler.py", "downsampler"),
        ("plugin.py", "plugin"),
        ("my_multifile_plugin", "my_multifile_plugin"),
        ("nested/local/paths/plugin.py", "plugin"),
        (r"nested\local\paths\plugin.py", "plugin"),
        ("nested/my_plugin/", "my_plugin"),
        (r"nested\my_plugin\", "my_plugin"),
        ("my.plugin.py", "my.plugin"),
        ("my.plugin", "my.plugin"),
        ("gh:org/repo/plugin", "plugin"),
        ("gh:org/repo/plugin/", "plugin"),
        ("__init__.py", "__init__"),
        ("", UNKNOWN_PLUGIN_NAME),
        ("   ", UNKNOWN_PLUGIN_NAME),
        ("/", UNKNOWN_PLUGIN_NAME),
        (r"\", UNKNOWN_PLUGIN_NAME),
        ("nested//plugin.py", "plugin"),
        (r"nested\\plugin.py", "plugin"),
    ];

    for (raw_plugin_filename, expected_plugin_name) in test_cases {
        assert_eq!(
            normalize_plugin_name(raw_plugin_filename).as_ref(),
            expected_plugin_name,
            "unexpected plugin name for {raw_plugin_filename:?}",
        );
    }
}

#[test]
fn plugin_trigger_entrypoint_maps_catalog_trigger_specs_to_python_entrypoints() {
    let test_cases = [
        (
            TriggerSpecificationDefinition::AllTablesWalWrite,
            PluginTriggerEntrypoint::Writes,
        ),
        (
            TriggerSpecificationDefinition::SingleTableWalWrite {
                table_name: "cpu".to_owned(),
            },
            PluginTriggerEntrypoint::Writes,
        ),
        (
            TriggerSpecificationDefinition::Schedule {
                schedule: "* * * * * *".to_owned(),
            },
            PluginTriggerEntrypoint::ScheduledCall,
        ),
        (
            TriggerSpecificationDefinition::Every {
                duration: Duration::from_secs(1),
            },
            PluginTriggerEntrypoint::ScheduledCall,
        ),
        (
            TriggerSpecificationDefinition::RequestPath {
                path: "run".to_owned(),
            },
            PluginTriggerEntrypoint::Request,
        ),
    ];

    for (spec, expected_entrypoint) in test_cases {
        assert_eq!(
            PluginTriggerEntrypoint::from_spec(&spec),
            expected_entrypoint
        );
    }
}

#[test]
fn plugin_trigger_invocation_registry_aggregates_sorts_truncates_and_resets() {
    let registry = PluginTriggerInvocationRegistry::default();
    let request_b = PluginTriggerInvocationKey::new(
        Arc::from("db_b"),
        Arc::from("trigger_b"),
        "plugin_b.py",
        PluginTriggerEntrypoint::Request,
    );
    let request_b_other_db = PluginTriggerInvocationKey::new(
        Arc::from("db_c"),
        Arc::from("trigger_b"),
        "plugin_b.py",
        PluginTriggerEntrypoint::Request,
    );
    let request_a = PluginTriggerInvocationKey::new(
        Arc::from("db_a"),
        Arc::from("trigger_a"),
        "plugin_a.py",
        PluginTriggerEntrypoint::Request,
    );
    let writes_b = PluginTriggerInvocationKey::new(
        Arc::from("db_b"),
        Arc::from("trigger_b"),
        "plugin_b.py",
        PluginTriggerEntrypoint::Writes,
    );

    registry.record_invocation(&request_b);
    registry.record_invocation(&request_b);
    registry.record_invocation(&request_b_other_db);
    registry.record_invocation(&request_a);
    registry.record_invocation(&writes_b);

    let snapshot = registry.snapshot();
    assert_eq!(
        snapshot,
        vec![
            PluginTriggerInvocationSnapshot {
                database_name: "db_b".to_owned(),
                trigger_name: "trigger_b".to_owned(),
                plugin_name: "plugin_b".to_owned(),
                trigger_type: PluginTriggerEntrypoint::Request.as_str(),
                invocation_count: 2,
            },
            PluginTriggerInvocationSnapshot {
                database_name: "db_a".to_owned(),
                trigger_name: "trigger_a".to_owned(),
                plugin_name: "plugin_a".to_owned(),
                trigger_type: PluginTriggerEntrypoint::Request.as_str(),
                invocation_count: 1,
            },
            PluginTriggerInvocationSnapshot {
                database_name: "db_b".to_owned(),
                trigger_name: "trigger_b".to_owned(),
                plugin_name: "plugin_b".to_owned(),
                trigger_type: PluginTriggerEntrypoint::Writes.as_str(),
                invocation_count: 1,
            },
            PluginTriggerInvocationSnapshot {
                database_name: "db_c".to_owned(),
                trigger_name: "trigger_b".to_owned(),
                plugin_name: "plugin_b".to_owned(),
                trigger_type: PluginTriggerEntrypoint::Request.as_str(),
                invocation_count: 1,
            },
        ]
    );

    for index in 0..=MAX_PLUGIN_TRIGGER_INVOCATION_TELEMETRY_ENTRIES {
        let key = PluginTriggerInvocationKey::new(
            Arc::from("tail_db"),
            Arc::from(format!("tail_{index:03}")),
            "plugin.py",
            PluginTriggerEntrypoint::ScheduledCall,
        );
        registry.record_invocation(&key);
    }

    assert_eq!(
        registry.snapshot().len(),
        MAX_PLUGIN_TRIGGER_INVOCATION_TELEMETRY_ENTRIES
    );

    registry.reset();
    assert!(registry.snapshot().is_empty());
}
