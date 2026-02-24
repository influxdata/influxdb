use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use influxdb3_catalog::log::{ErrorBehavior, TriggerSpecificationDefinition};

#[test]
fn parse_args_create_last_cache() {
    let args = super::Config::parse_from([
        "create",
        "last_cache",
        "--database",
        "bar",
        "--table",
        "foo",
        "--key-columns",
        "tag1,tag2,tag3",
        "--value-columns",
        "field1,field2,field3",
        "--ttl",
        "1 hour",
        "--count",
        "15",
        "bar",
    ]);
    let super::SubCommand::LastCache(super::LastCacheConfig {
        table,
        cache_name,
        key_columns,
        value_columns,
        count,
        ttl,
        influxdb3_config: crate::commands::common::InfluxDb3Config { database_name, .. },
        ..
    }) = args.cmd
    else {
        panic!("Did not parse args correctly: {args:#?}")
    };
    assert_eq!("bar", database_name);
    assert_eq!("foo", table);
    assert!(cache_name.is_some_and(|n| n == "bar"));
    assert!(key_columns.is_some_and(|keys| keys == ["tag1", "tag2", "tag3"]));
    assert!(value_columns.is_some_and(|vals| vals == ["field1", "field2", "field3"]));
    assert!(count.is_some_and(|c| c == 15));
    assert!(ttl.is_some_and(|t| t.as_secs() == 3600));
}

#[test]
fn parse_args_create_trigger_arguments() {
    let args = super::Config::parse_from([
        "create",
        "trigger",
        "--trigger-spec",
        "every:10s",
        "--path",
        "plugin.py",
        "--database",
        "test",
        "--trigger-arguments",
        "query_path=/metrics?format=json,whatever=hello",
        "test-trigger",
    ]);
    let super::SubCommand::Trigger(super::TriggerConfig {
        trigger_name,
        trigger_arguments,
        trigger_specification,
        path,
        disabled,
        run_asynchronous,
        error_behavior,
        influxdb3_config: crate::commands::common::InfluxDb3Config { database_name, .. },
        ..
    }) = args.cmd
    else {
        panic!("Did not parse args correctly: {args:#?}")
    };
    assert_eq!("test", database_name);
    assert_eq!("test-trigger", trigger_name);
    assert_eq!(Some(PathBuf::from("plugin.py")), path);
    assert_eq!(
        TriggerSpecificationDefinition::Every {
            duration: Duration::from_secs(10)
        },
        trigger_specification
    );
    assert!(!disabled);
    assert!(!run_asynchronous);
    assert_eq!(ErrorBehavior::Log, error_behavior);

    let trigger_arguments = trigger_arguments.expect("args must include trigger arguments");

    assert_eq!(2, trigger_arguments.len());

    let query_path = trigger_arguments
        .into_iter()
        .find(|v| v.0.0 == "query_path")
        .expect("must include query_path trigger argument");

    assert_eq!("/metrics?format=json", query_path.0.1);
}
