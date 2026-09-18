use influxdb3_catalog::catalog::{
    ErrorBehavior, NodeSpec, TriggerSettings, TriggerSpecificationDefinition,
};
use influxdb3_id::TriggerId;

use super::*;

#[tokio::test]
async fn test_trigger_table_includes_error_behavior() {
    let triggers = vec![
        Arc::new(TriggerDefinition {
            trigger_id: TriggerId::new(1),
            trigger_name: "log_trigger".into(),
            plugin_filename: "log.py".to_string(),
            database_name: "mydb".into(),
            node_spec: NodeSpec::All,
            trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
            trigger_settings: TriggerSettings {
                error_behavior: ErrorBehavior::Log,
                ..Default::default()
            },
            trigger_arguments: None,
            disabled: false,
        }),
        Arc::new(TriggerDefinition {
            trigger_id: TriggerId::new(2),
            trigger_name: "retry_trigger".into(),
            plugin_filename: "retry.py".to_string(),
            database_name: "mydb".into(),
            node_spec: NodeSpec::All,
            trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
            trigger_settings: TriggerSettings {
                error_behavior: ErrorBehavior::Retry,
                ..Default::default()
            },
            trigger_arguments: None,
            disabled: true,
        }),
        Arc::new(TriggerDefinition {
            trigger_id: TriggerId::new(3),
            trigger_name: "disable_trigger".into(),
            plugin_filename: "disable.py".to_string(),
            database_name: "mydb".into(),
            node_spec: NodeSpec::All,
            trigger: TriggerSpecificationDefinition::AllTablesWalWrite,
            trigger_settings: TriggerSettings {
                error_behavior: ErrorBehavior::Disable,
                ..Default::default()
            },
            trigger_arguments: None,
            disabled: false,
        }),
    ];

    let table = ProcessingEngineTriggerTable::new(triggers);
    let batch = table.scan(None, None).await.unwrap();

    datafusion::assert_batches_sorted_eq!(
        [
            "+-----------------+-----------------+------------------------+----------+----------------+",
            "| trigger_name    | plugin_filename | trigger_specification  | disabled | error_behavior |",
            "+-----------------+-----------------+------------------------+----------+----------------+",
            "| disable_trigger | disable.py      | \"all_tables_wal_write\" | false    | disable        |",
            "| log_trigger     | log.py          | \"all_tables_wal_write\" | false    | log            |",
            "| retry_trigger   | retry.py        | \"all_tables_wal_write\" | true     | retry          |",
            "+-----------------+-----------------+------------------------+----------+----------------+",
        ],
        &[batch]
    );
}
