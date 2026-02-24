use super::*;
use datafusion::assert_batches_sorted_eq;
use influxdb3_catalog::log::{TriggerDefinition, TriggerSettings, TriggerSpecificationDefinition};
use influxdb3_id::TriggerId;

#[tokio::test]
async fn test_trigger_arguments_table_empty() {
    let table = ProcessingEngineTriggerArgumentsTable::new(vec![]);
    let batch = table.scan(None, None).await.unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[tokio::test]
async fn test_trigger_arguments_table_with_triggers_no_args() {
    let trigger = Arc::new(TriggerDefinition {
        trigger_id: TriggerId::new(1),
        trigger_name: "test_trigger".into(),
        plugin_filename: "test_plugin.py".to_string(),
        database_name: "mydb".into(),
        node_id: Default::default(),
        trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
        trigger_settings: TriggerSettings::default(),
        trigger_arguments: None,
        disabled: false,
    });

    let table = ProcessingEngineTriggerArgumentsTable::new(vec![trigger]);
    let batch = table.scan(None, None).await.unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[tokio::test]
async fn test_trigger_arguments_table_with_arguments() {
    let mut args = hashbrown::HashMap::new();
    args.insert("key1".to_string(), "value1".to_string());
    args.insert("key2".to_string(), "value2".to_string());

    let trigger = Arc::new(TriggerDefinition {
        trigger_id: TriggerId::new(1),
        trigger_name: "test_trigger".into(),
        plugin_filename: "test_plugin.py".to_string(),
        database_name: "mydb".into(),
        node_id: Default::default(),
        trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
        trigger_settings: TriggerSettings::default(),
        trigger_arguments: Some(args),
        disabled: false,
    });

    let table = ProcessingEngineTriggerArgumentsTable::new(vec![trigger]);
    let batch = table.scan(None, None).await.unwrap();

    // Use assert_batches_sorted_eq to check the output
    assert_batches_sorted_eq!(
        [
            "+--------------+--------------+----------------+",
            "| trigger_name | argument_key | argument_value |",
            "+--------------+--------------+----------------+",
            "| test_trigger | key1         | value1         |",
            "| test_trigger | key2         | value2         |",
            "+--------------+--------------+----------------+",
        ],
        &[batch]
    );
}

#[tokio::test]
async fn test_trigger_arguments_table_multiple_triggers() {
    let mut args1 = hashbrown::HashMap::new();
    args1.insert("arg1".to_string(), "val1".to_string());

    let mut args2 = hashbrown::HashMap::new();
    args2.insert("arg2".to_string(), "val2".to_string());
    args2.insert("arg3".to_string(), "val3".to_string());

    let trigger1 = Arc::new(TriggerDefinition {
        trigger_id: TriggerId::new(1),
        trigger_name: "trigger1".into(),
        plugin_filename: "plugin1.py".to_string(),
        database_name: "db1".into(),
        node_id: Default::default(),
        trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
        trigger_settings: TriggerSettings::default(),
        trigger_arguments: Some(args1),
        disabled: false,
    });

    let trigger2 = Arc::new(TriggerDefinition {
        trigger_id: TriggerId::new(2),
        trigger_name: "trigger2".into(),
        plugin_filename: "plugin2.py".to_string(),
        database_name: "db2".into(),
        node_id: Default::default(),
        trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
        trigger_settings: TriggerSettings::default(),
        trigger_arguments: Some(args2),
        disabled: false,
    });

    let table = ProcessingEngineTriggerArgumentsTable::new(vec![trigger1, trigger2]);
    let batch = table.scan(None, None).await.unwrap();

    // Use assert_batches_sorted_eq to check the output
    assert_batches_sorted_eq!(
        [
            "+--------------+--------------+----------------+",
            "| trigger_name | argument_key | argument_value |",
            "+--------------+--------------+----------------+",
            "| trigger1     | arg1         | val1           |",
            "| trigger2     | arg2         | val2           |",
            "| trigger2     | arg3         | val3           |",
            "+--------------+--------------+----------------+",
        ],
        &[batch]
    );
}
