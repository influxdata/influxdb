use influxdb3_authz::{CrudActions, DatabaseActions, SystemActions};

use super::*;

#[test]
fn node_mode_encodings() {
    assert_encoding_stable!(NodeMode::Core, "00");
    assert_encoding_stable!(NodeMode::Query, "01");
    assert_encoding_stable!(NodeMode::Ingest, "02");
    assert_encoding_stable!(NodeMode::Compact, "03");
    assert_encoding_stable!(NodeMode::Process, "04");
    assert_encoding_stable!(NodeMode::All, "05");
}

#[test]
fn node_spec_encodings() {
    assert_encoding_stable!(NodeSpec::All, "00");
    assert_encoding_stable!(NodeSpec::Nodes(vec![1, 2, 3]), "0103040639");
}

#[test]
fn cache_source_encodings() {
    assert_encoding_stable!(CacheSource::User, "00");
    assert_encoding_stable!(CacheSource::Auto, "01");
}

#[test]
fn last_cache_value_columns_def_encodings() {
    assert_encoding_stable!(
        LastCacheValueColumnsDef::Explicit(vec![ColumnIdentifier::Tag(1)]),
        "0001010100"
    );
    assert_encoding_stable!(LastCacheValueColumnsDef::AllNonKeyColumns, "01");
}

#[test]
fn column_identifier_encodings() {
    assert_encoding_stable!(ColumnIdentifier::Timestamp, "00");
    assert_encoding_stable!(ColumnIdentifier::Tag(5), "010500");
    assert_encoding_stable!(
        ColumnIdentifier::Field(FieldIdentifier {
            family_id: 0,
            field_id: 1,
        }),
        "0200000100"
    );
}

#[test]
fn column_definition_encodings() {
    assert_encoding_stable!(
        ColumnDefinition::Timestamp(TimestampColumn {
            column_id: Some(0),
            name: "time".to_string(),
        }),
        "000100000474696d65"
    );
    assert_encoding_stable!(
        ColumnDefinition::Tag(TagColumn {
            id: 1,
            column_id: Some(1),
            name: "host".to_string(),
        }),
        "01010001010004686f7374"
    );
    assert_encoding_stable!(
        ColumnDefinition::Field(FieldColumn {
            id: FieldIdentifier {
                field_id: 1,
                family_id: 0,
            },
            column_id: Some(2),
            name: "value".to_string(),
            data_type: FieldDataType::Float,
        }),
        "02000001000102000576616c756503"
    );
    // A column whose legacy ordinal space was exhausted (PachaTree mode).
    assert_encoding_stable!(
        ColumnDefinition::Field(FieldColumn {
            id: FieldIdentifier {
                field_id: 9,
                family_id: 0,
            },
            column_id: None,
            name: "overflow".to_string(),
            data_type: FieldDataType::Integer,
        }),
        "020000090000086f766572666c6f7701"
    );
}

#[test]
fn field_data_type_encodings() {
    assert_encoding_stable!(FieldDataType::Integer, "01");
    assert_encoding_stable!(FieldDataType::UInteger, "02");
    assert_encoding_stable!(FieldDataType::Float, "03");
    assert_encoding_stable!(FieldDataType::String, "00");
    assert_encoding_stable!(FieldDataType::Boolean, "04");
}

#[test]
fn field_family_name_encodings() {
    assert_encoding_stable!(
        FieldFamilyName::User("my_family".to_string()),
        "00096d795f66616d696c79"
    );
    assert_encoding_stable!(FieldFamilyName::Auto(3), "010300");
}

#[test]
fn field_family_mode_encodings() {
    assert_encoding_stable!(FieldFamilyMode::Aware, "00");
    assert_encoding_stable!(FieldFamilyMode::Auto, "01");
}

#[test]
fn trigger_spec_encodings() {
    assert_encoding_stable!(
        TriggerSpec::SingleTableWalWrite {
            table_name: "cpu".to_string(),
        },
        "0003637075"
    );
    assert_encoding_stable!(TriggerSpec::AllTablesWalWrite, "01");
    assert_encoding_stable!(
        TriggerSpec::Schedule {
            schedule: "0 * * * *".to_string(),
        },
        "020930202a202a202a202a"
    );
    assert_encoding_stable!(
        TriggerSpec::RequestPath {
            path: "/api/v1".to_string(),
        },
        "03072f6170692f7631"
    );
    assert_encoding_stable!(
        TriggerSpec::Every {
            duration_ns: 60_000_000_000,
        },
        "0400005847f80d000000"
    );
}

#[test]
fn error_behavior_encodings() {
    assert_encoding_stable!(ErrorBehavior::Log, "00");
    assert_encoding_stable!(ErrorBehavior::Retry, "01");
    assert_encoding_stable!(ErrorBehavior::Disable, "02");
}

#[test]
fn retention_period_encodings() {
    assert_encoding_stable!(RetentionPeriod::Indefinite, "00");
    assert_encoding_stable!(
        RetentionPeriod::Duration {
            duration_secs: 3600,
        },
        "0104100e"
    );
}

#[test]
fn storage_mode_encodings() {
    assert_encoding_stable!(StorageMode::Parquet, "00");
    assert_encoding_stable!(StorageMode::PachaTree, "01");
    assert_encoding_stable!(StorageMode::ParquetAndPachaTree, "02");
}

#[test]
fn deletion_scope_encodings() {
    assert_encoding_stable!(DeletionScope::DataAndCatalog, "00");
    assert_encoding_stable!(DeletionScope::DataOnlyRemoveTables, "01");
    assert_encoding_stable!(DeletionScope::DataOnlyKeepResources, "02");
}

#[test]
fn resource_type_encodings() {
    assert_encoding_stable!(ResourceType::Database, "00");
    assert_encoding_stable!(ResourceType::Token, "01");
    assert_encoding_stable!(ResourceType::System, "02");
    assert_encoding_stable!(ResourceType::Wildcard, "03");
}

#[test]
fn resource_identifier_encodings() {
    assert_encoding_stable!(ResourceIdentifier::Database(vec![1, 2]), "0002040102");
    assert_encoding_stable!(ResourceIdentifier::Token(vec![3]), "01010603");
    assert_encoding_stable!(ResourceIdentifier::System(vec![0, 1]), "0202020001");
    assert_encoding_stable!(ResourceIdentifier::Wildcard, "03");
}

#[test]
fn actions_encodings() {
    assert_encoding_stable!(Actions::Database(DatabaseActions::CREATE), "000100");
    assert_encoding_stable!(Actions::Token(CrudActions::CREATE), "010100");
    assert_encoding_stable!(Actions::System(SystemActions::READ), "020100");
    assert_encoding_stable!(Actions::Wildcard, "03");
}

#[test]
fn permission_encoding() {
    assert_encoding_stable!(
        Permission {
            resource_type: ResourceType::Database,
            resource_identifier: ResourceIdentifier::Database(vec![7]),
            actions: Actions::Database(DatabaseActions::CREATE),
            resource_names: vec![],
        },
        "000001040700010000"
    );
    assert_encoding_stable!(
        Permission {
            resource_type: ResourceType::Database,
            resource_identifier: ResourceIdentifier::Database(vec![7, 9]),
            actions: Actions::Database(DatabaseActions::CREATE),
            resource_names: vec![
                ResourceNameEntry {
                    resource_id: "7".to_string(),
                    name: "live_db".to_string(),
                    deleted: false,
                },
                ResourceNameEntry {
                    resource_id: "9".to_string(),
                    name: "gone_db".to_string(),
                    deleted: true,
                },
            ],
        },
        "000002040709000100020101373907076c6976655f6462676f6e655f646202"
    );
}

#[test]
fn role_database_action_encodings() {
    assert_encoding_stable!(RoleDatabaseAction::Describe, "00");
    assert_encoding_stable!(RoleDatabaseAction::Read, "01");
    assert_encoding_stable!(RoleDatabaseAction::Write, "02");
    assert_encoding_stable!(RoleDatabaseAction::Create, "03");
    assert_encoding_stable!(RoleDatabaseAction::Delete, "04");
    // Retained so older catalogs still decode. Do not remove.
    assert_encoding_stable!(RoleDatabaseAction::GrantUsage, "05");
}

#[test]
fn role_token_action_encodings() {
    assert_encoding_stable!(RoleTokenAction::Read, "00");
    assert_encoding_stable!(RoleTokenAction::Create, "01");
    assert_encoding_stable!(RoleTokenAction::Delete, "02");
    // Retained so older catalogs still decode. Do not remove.
    assert_encoding_stable!(RoleTokenAction::GrantUsage, "03");
}

#[test]
fn role_user_action_encodings() {
    assert_encoding_stable!(RoleUserAction::Read, "00");
    assert_encoding_stable!(RoleUserAction::Create, "01");
    assert_encoding_stable!(RoleUserAction::Update, "02");
    assert_encoding_stable!(RoleUserAction::Delete, "03");
    // Retained so older catalogs still decode. Do not remove.
    assert_encoding_stable!(RoleUserAction::GrantUsage, "04");
}

#[test]
fn role_role_action_encodings() {
    assert_encoding_stable!(RoleRoleAction::Read, "00");
    assert_encoding_stable!(RoleRoleAction::Create, "01");
    assert_encoding_stable!(RoleRoleAction::Update, "02");
    assert_encoding_stable!(RoleRoleAction::Delete, "03");
}

#[test]
fn role_admin_token_action_encodings() {
    assert_encoding_stable!(RoleAdminTokenAction::Create, "00");
    assert_encoding_stable!(RoleAdminTokenAction::Delete, "01");
}

#[test]
fn role_database_resource_encodings() {
    assert_encoding_stable!(RoleDatabaseResource::All, "00");
    assert_encoding_stable!(RoleDatabaseResource::Identifier(42), "01042a");
}

#[test]
fn role_permission_grant_encodings() {
    assert_encoding_stable!(RolePermissionGrant::AccountAdminAll, "00");
    assert_encoding_stable!(
        RolePermissionGrant::Database(RoleDatabasePermission {
            action: RoleDatabaseAction::Write,
            resource: RoleDatabaseResource::Identifier(7),
        }),
        "0102010407"
    );
    assert_encoding_stable!(
        RolePermissionGrant::Token(RoleTokenPermission {
            action: RoleTokenAction::Read,
        }),
        "0200"
    );
    assert_encoding_stable!(
        RolePermissionGrant::User(RoleUserPermission {
            action: RoleUserAction::Update,
        }),
        "0302"
    );
    assert_encoding_stable!(
        RolePermissionGrant::Role(RoleRolePermission {
            action: RoleRoleAction::Read,
        }),
        "0400"
    );
    assert_encoding_stable!(
        RolePermissionGrant::AdminToken(RoleAdminTokenPermission {
            action: RoleAdminTokenAction::Delete,
        }),
        "0501"
    );
}
