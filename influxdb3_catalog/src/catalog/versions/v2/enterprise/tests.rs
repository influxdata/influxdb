use std::sync::Arc;

use indexmap::IndexMap;
use influxdb3_process::{ProcessUuidGetter, ProcessUuidWrapper};
use influxdb3_shutdown::ShutdownManager;
use iox_time::{MockProvider, Time};
use object_store::memory::InMemory;

use super::super::Catalog;
use crate::{catalog::versions::v2::CatalogLimits, log::versions::v4::NodeMode};

#[test_log::test(tokio::test)]
async fn test_node_register_and_stop() {
    let object_store = Arc::new(InMemory::new());
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let shutdown_manager = ShutdownManager::new_testing();
    let process_uuid_getter: Arc<dyn ProcessUuidGetter> = Arc::new(ProcessUuidWrapper::new());
    let catalog = Catalog::new_enterprise_with_shutdown(
        "node",
        "cluster",
        Arc::clone(&object_store) as _,
        Arc::clone(&time_provider) as _,
        Default::default(),
        shutdown_manager.register("test"),
        Arc::new(CatalogLimits::default()),
        Default::default(),
        Arc::clone(&process_uuid_getter),
    )
    .await
    .unwrap();
    catalog
        .register_node(
            "node",
            2,
            vec![NodeMode::Ingest],
            Arc::clone(&process_uuid_getter),
            Arc::from("test-instance-node"),
            None,
            None,
            0,
        )
        .await
        .unwrap();
    insta::assert_json_snapshot!(catalog.snapshot(), {
        ".catalog_uuid" => "[uuid]",
        ".nodes.repo[][1].instance_id" => "[uuid]",
        ".nodes.repo[][1].process_uuids[]" => "[uuid]"
    });
    shutdown_manager.shutdown();
    shutdown_manager.join().await;
    insta::assert_json_snapshot!(catalog.snapshot(), {
        ".catalog_uuid" => "[uuid]",
        ".nodes.repo[][1].instance_id" => "[uuid]",
        ".nodes.repo[][1].process_uuids[]" => "[uuid]"
    });
}

#[test_log::test(tokio::test)]
async fn test_stop_node_frees_licensed_cores() {
    let object_store = Arc::new(InMemory::new());
    let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
    let shutdown_manager = ShutdownManager::new_testing();
    let process_uuid_getter: Arc<dyn ProcessUuidGetter> = Arc::new(ProcessUuidWrapper::new());

    let catalog = Catalog::new_enterprise_with_shutdown(
        "test-node",
        "test-cluster",
        Arc::clone(&object_store) as _,
        Arc::clone(&time_provider) as _,
        Default::default(),
        shutdown_manager.register("test"),
        Arc::new(CatalogLimits::default()),
        Default::default(),
        Arc::clone(&process_uuid_getter),
    )
    .await
    .unwrap();

    // Register node1 with 4 cores
    catalog
        .register_node(
            "node1",
            4,
            vec![NodeMode::Ingest],
            Arc::clone(&process_uuid_getter),
            Arc::from("instance-node1"),
            None,
            None,
            0,
        )
        .await
        .unwrap();

    // Register node2 with 6 cores
    catalog
        .register_node(
            "node2",
            6,
            vec![NodeMode::Query],
            Arc::clone(&process_uuid_getter),
            Arc::from("instance-node2"),
            None,
            None,
            0,
        )
        .await
        .unwrap();

    // Verify initial total cores in use (should be 10 = 4 + 6)
    assert_eq!(catalog.total_active_cores_in_use_by_all_nodes(), 10);

    // Stop node1
    catalog.stop_node("node1").await.unwrap();

    // Verify cores after stopping node1 (should be 6, only node2's cores)
    assert_eq!(catalog.total_active_cores_in_use_by_all_nodes(), 6);

    // Stop node2
    catalog.stop_node("node2").await.unwrap();

    // Verify cores after stopping both nodes (should be 0)
    assert_eq!(catalog.total_active_cores_in_use_by_all_nodes(), 0);

    // Register node3 with 8 cores to verify cores are truly available for reuse
    catalog
        .register_node(
            "node3",
            8,
            vec![NodeMode::Compact],
            Arc::clone(&process_uuid_getter),
            Arc::from("instance-node3"),
            None,
            None,
            0,
        )
        .await
        .unwrap();

    // Verify node3's cores are counted
    assert_eq!(catalog.total_active_cores_in_use_by_all_nodes(), 8);
}

#[test]
fn test_delete_database_captures_resource_names_in_tokens() {
    use influxdb3_authz::{
        Actions, DatabaseActions, Permission, ResourceIdentifier, ResourceType, TokenInfo,
    };
    use influxdb3_id::{DbId, TokenId};
    use uuid::Uuid;

    use super::super::{DatabaseSchema, InnerCatalog};
    use crate::log::versions::v4::{DeleteBatch, DeleteOp};

    // Create InnerCatalog
    let mut catalog = InnerCatalog::new(Arc::from("test-catalog"), Uuid::new_v4());

    // Add a database
    let db_id = DbId::new(1);
    let db_name = Arc::from("test_db");
    let db_schema = DatabaseSchema::new(db_id, Arc::clone(&db_name));
    catalog.databases.insert(db_id, db_schema).unwrap();

    // Add another database that won't be deleted
    let db_id_2 = DbId::new(2);
    let db_name_2 = Arc::from("other_db");
    let db_schema_2 = DatabaseSchema::new(db_id_2, Arc::clone(&db_name_2));
    catalog.databases.insert(db_id_2, db_schema_2).unwrap();

    // Create Token 1: Permission for test_db WITHOUT resource_names
    let token_id_1 = TokenId::new(1);
    let mut token_1 = TokenInfo::new(
        token_id_1,
        Arc::from("token-1"),
        "hash1".as_bytes().to_vec(),
        1234567890,
        None,
    );
    token_1.set_permissions(vec![Permission {
        resource_type: ResourceType::Database,
        resource_identifier: ResourceIdentifier::Database(vec![db_id]),
        actions: Actions::Database(DatabaseActions::from(DatabaseActions::READ)),
        resource_names: None, // No resource names initially
    }]);
    catalog.tokens.add_token(token_id_1, token_1).unwrap();

    // Create Token 2: Permission for test_db WITH partial resource_names
    let token_id_2 = TokenId::new(2);
    let mut token_2 = TokenInfo::new(
        token_id_2,
        Arc::from("token-2"),
        "hash2".as_bytes().to_vec(),
        1234567890,
        None,
    );
    let mut existing_names = IndexMap::new();
    existing_names.insert(
        "999".to_string(),
        influxdb3_authz::ResourceMetadata {
            name: "some_other_db".to_string(),
            deleted: false,
        },
    );
    token_2.set_permissions(vec![Permission {
        resource_type: ResourceType::Database,
        resource_identifier: ResourceIdentifier::Database(vec![db_id, DbId::new(999)]),
        actions: Actions::Database(DatabaseActions::from(DatabaseActions::READ)),
        resource_names: Some(existing_names), // Has some existing resource names
    }]);
    catalog.tokens.add_token(token_id_2, token_2).unwrap();

    // Create Token 3: Permission for other_db (shouldn't be modified)
    let token_id_3 = TokenId::new(3);
    let mut token_3 = TokenInfo::new(
        token_id_3,
        Arc::from("token-3"),
        "hash3".as_bytes().to_vec(),
        1234567890,
        None,
    );
    token_3.set_permissions(vec![Permission {
        resource_type: ResourceType::Database,
        resource_identifier: ResourceIdentifier::Database(vec![db_id_2]),
        actions: Actions::Database(DatabaseActions::from(DatabaseActions::READ)),
        resource_names: None,
    }]);
    catalog.tokens.add_token(token_id_3, token_3).unwrap();

    // Apply delete batch for test_db
    let delete_batch = DeleteBatch {
        time_ns: 0,
        ops: vec![DeleteOp::DeleteDatabase(db_id)],
    };
    let result = catalog.apply_delete_batch(&delete_batch).unwrap();
    assert!(
        result,
        "Delete batch should return true when changes are made"
    );

    // Verify Token 1 now has resource_names captured
    let updated_token_1 = catalog.tokens.repo().get_by_id(&token_id_1).unwrap();
    let perm_1 = &updated_token_1.permissions[0];
    assert!(
        perm_1.resource_names.is_some(),
        "Token 1 should now have resource_names"
    );
    let resource_names_1 = perm_1.resource_names.as_ref().unwrap();
    assert_eq!(
        resource_names_1.get(&db_id.to_string()).map(|m| &m.name),
        Some(&"test_db".to_string()),
        "Token 1 should have captured the database name"
    );

    // Verify Token 2 still has its original resource_names plus the new one
    let updated_token_2 = catalog.tokens.repo().get_by_id(&token_id_2).unwrap();
    let perm_2 = &updated_token_2.permissions[0];
    assert!(
        perm_2.resource_names.is_some(),
        "Token 2 should still have resource_names"
    );
    let resource_names_2 = perm_2.resource_names.as_ref().unwrap();
    assert_eq!(
        resource_names_2.get("999").map(|m| &m.name),
        Some(&"some_other_db".to_string()),
        "Token 2 should still have its original resource name"
    );
    assert_eq!(
        resource_names_2.get(&db_id.to_string()).map(|m| &m.name),
        Some(&"test_db".to_string()),
        "Token 2 should have captured the deleted database name"
    );

    // Verify Token 3 is unchanged (no resource_names)
    let updated_token_3 = catalog.tokens.repo().get_by_id(&token_id_3).unwrap();
    let perm_3 = &updated_token_3.permissions[0];
    assert!(
        perm_3.resource_names.is_none(),
        "Token 3 should not have resource_names as it doesn't reference the deleted database"
    );

    // Verify the database was actually deleted
    assert!(
        catalog.databases.get_by_id(&db_id).is_none(),
        "test_db should be deleted from the catalog"
    );
    assert!(
        catalog.databases.get_by_id(&db_id_2).is_some(),
        "other_db should still exist in the catalog"
    );
}

#[test]
fn test_soft_delete_database_captures_resource_names_in_tokens() {
    use influxdb3_authz::{
        Actions, DatabaseActions, Permission, ResourceIdentifier, ResourceType, TokenInfo,
    };
    use influxdb3_id::{DbId, TokenId};
    use iox_time::Time;
    use uuid::Uuid;

    use super::super::{DatabaseSchema, InnerCatalog};
    use crate::log::versions::v4::{DatabaseBatch, DatabaseCatalogOp, SoftDeleteDatabaseLog};

    // Create InnerCatalog
    let mut catalog = InnerCatalog::new(Arc::from("test-catalog"), Uuid::new_v4());

    // Add a database
    let db_id = DbId::new(1);
    let db_name = Arc::from("foodb");
    let db_schema = DatabaseSchema::new(db_id, Arc::clone(&db_name));
    catalog.databases.insert(db_id, db_schema).unwrap();

    // Create Token 1: Permission for foodb WITHOUT resource_names
    let token_id_1 = TokenId::new(1);
    let mut token_1 = TokenInfo::new(
        token_id_1,
        Arc::from("token-1"),
        "hash1".as_bytes().to_vec(),
        1234567890,
        None,
    );
    token_1.set_permissions(vec![Permission {
        resource_type: ResourceType::Database,
        resource_identifier: ResourceIdentifier::Database(vec![db_id]),
        actions: Actions::Database(DatabaseActions::from(DatabaseActions::READ)),
        resource_names: None, // No resource names initially
    }]);
    catalog.tokens.add_token(token_id_1, token_1).unwrap();

    // Create Token 2: Permission for foodb WITH existing resource_names
    let token_id_2 = TokenId::new(2);
    let mut token_2 = TokenInfo::new(
        token_id_2,
        Arc::from("token-2"),
        "hash2".as_bytes().to_vec(),
        1234567890,
        None,
    );
    let mut existing_names = IndexMap::new();
    existing_names.insert(
        "999".to_string(),
        influxdb3_authz::ResourceMetadata {
            name: "some_other_db".to_string(),
            deleted: false,
        },
    );
    token_2.set_permissions(vec![Permission {
        resource_type: ResourceType::Database,
        resource_identifier: ResourceIdentifier::Database(vec![db_id, DbId::new(999)]),
        actions: Actions::Database(DatabaseActions::from(DatabaseActions::WRITE)),
        resource_names: Some(existing_names), // Has some existing resource names
    }]);
    catalog.tokens.add_token(token_id_2, token_2).unwrap();

    // Apply soft delete batch for foodb
    let deletion_time = Time::from_timestamp_nanos(1234567890000000000);
    let database_batch = DatabaseBatch {
        time_ns: deletion_time.timestamp_nanos(),
        database_id: db_id,
        database_name: Arc::clone(&db_name),
        ops: vec![DatabaseCatalogOp::SoftDeleteDatabase(
            SoftDeleteDatabaseLog {
                database_id: db_id,
                database_name: Arc::clone(&db_name),
                deletion_time: deletion_time.timestamp_nanos(),
                hard_deletion_time: None,
                hard_delete_scope: None,
            },
        )],
    };

    let result = catalog.apply_database_batch(&database_batch).unwrap();
    assert!(
        result,
        "Database batch with soft delete should return true when changes are made"
    );

    // Verify the database has been renamed with timestamp
    let updated_db = catalog.databases.get_by_id(&db_id).unwrap();
    assert!(
        updated_db.name.contains("-20090213T233130"),
        "Database name should contain deletion timestamp, but got: {}",
        updated_db.name
    );
    assert!(updated_db.deleted, "Database should be marked as deleted");

    // Verify Token 1 now has resource_names captured with original name
    let updated_token_1 = catalog.tokens.repo().get_by_id(&token_id_1).unwrap();
    let perm_1 = &updated_token_1.permissions[0];
    assert!(
        perm_1.resource_names.is_some(),
        "Token 1 should now have resource_names"
    );
    let resource_names_1 = perm_1.resource_names.as_ref().unwrap();
    assert_eq!(
        resource_names_1.get(&db_id.to_string()).map(|m| &m.name),
        Some(&"foodb".to_string()),
        "Token 1 should have captured the original database name 'foodb', not the renamed version"
    );

    // Verify Token 2 still has its original resource_names plus the new one
    let updated_token_2 = catalog.tokens.repo().get_by_id(&token_id_2).unwrap();
    let perm_2 = &updated_token_2.permissions[0];
    assert!(
        perm_2.resource_names.is_some(),
        "Token 2 should still have resource_names"
    );
    let resource_names_2 = perm_2.resource_names.as_ref().unwrap();
    assert_eq!(
        resource_names_2.get("999").map(|m| &m.name),
        Some(&"some_other_db".to_string()),
        "Token 2 should still have its original resource name"
    );
    assert_eq!(
        resource_names_2.get(&db_id.to_string()).map(|m| &m.name),
        Some(&"foodb".to_string()),
        "Token 2 should have captured the original database name 'foodb', not the renamed version"
    );
}

// Unit tests for EnterpriseCatalogLimiter
mod enterprise_catalog_limiter_tests {
    use super::super::super::Catalog;
    use super::super::{CatalogLimiter, CurrentCatalogUsage, MaximumColumnCountLimiter};

    fn create_usage(
        total_db_count: u32,
        total_table_count: u32,
        total_column_count: u32,
    ) -> CurrentCatalogUsage {
        CurrentCatalogUsage::new(
            total_db_count as usize,
            total_table_count as usize,
            total_column_count as usize,
        )
    }

    #[test]
    fn test_default_creation() {
        let limiter = MaximumColumnCountLimiter::default();
        let usage = create_usage(0, 0, 0);

        // Default should use MAX_TOTAL_COLUMNS
        assert_eq!(limiter.max_total_columns, Catalog::MAX_TOTAL_COLUMNS);
        // With 0 columns (under limit), should return remaining capacity
        assert_eq!(
            limiter.database_count_limit(&usage),
            Catalog::MAX_TOTAL_COLUMNS as usize,
        );
        assert_eq!(
            limiter.table_count_limit(&usage),
            Catalog::MAX_TOTAL_COLUMNS as usize,
        );
        assert_eq!(
            limiter.column_per_table_limit(&usage),
            Catalog::MAX_TOTAL_COLUMNS as usize,
        );
    }

    #[test]
    fn test_custom_creation() {
        let limiter = MaximumColumnCountLimiter::new(1000);
        let usage = create_usage(10, 50, 500);

        assert_eq!(limiter.max_total_columns, 1000);
        assert_eq!(limiter.database_count_limit(&usage), 510);
        assert_eq!(limiter.table_count_limit(&usage), 550);
        assert_eq!(limiter.column_per_table_limit(&usage), 500);
    }

    #[test]
    fn test_boundary_conditions() {
        let limiter = MaximumColumnCountLimiter::new(1000);

        // Test under limit (total_column_count < max_total_columns)
        let usage_under = create_usage(5, 20, 999);
        assert_eq!(
            limiter.database_count_limit(&usage_under),
            6,
            "should return remaining capacity when under limit"
        );
        assert_eq!(
            limiter.table_count_limit(&usage_under),
            21,
            "should return remaining capacity when under limit"
        );
        assert_eq!(
            limiter.column_per_table_limit(&usage_under),
            1,
            "should return remaining capacity when under limit"
        );

        // Test at limit (total_column_count == max_total_columns)
        let usage_at = create_usage(5, 20, 1000);
        assert_eq!(
            limiter.database_count_limit(&usage_at),
            5,
            "should return current when at limit"
        );
        assert_eq!(
            limiter.table_count_limit(&usage_at),
            20,
            "should return current when at limit"
        );
        assert_eq!(
            limiter.column_per_table_limit(&usage_at),
            0,
            "should return 0 when at limit"
        );

        // Test over limit (total_column_count > max_total_columns)
        let usage_over = create_usage(5, 20, 1001);
        assert_eq!(
            limiter.database_count_limit(&usage_over),
            5,
            "should return current when over limit"
        );
        assert_eq!(
            limiter.table_count_limit(&usage_over),
            20,
            "should return current when over limit"
        );
        assert_eq!(
            limiter.column_per_table_limit(&usage_over),
            0,
            "should return 0 when over limit"
        );
    }

    #[test]
    fn test_small_limits() {
        // Test with limit of 0
        let limiter_zero = MaximumColumnCountLimiter::new(0);
        let usage_zero = create_usage(0, 0, 0);
        let usage_nonzero = create_usage(1, 1, 1);

        // With zero usage and zero limit, at limit (not under)
        assert_eq!(limiter_zero.database_count_limit(&usage_zero), 0);
        assert_eq!(limiter_zero.table_count_limit(&usage_zero), 0);
        assert_eq!(limiter_zero.column_per_table_limit(&usage_zero), 0);

        // With non-zero usage and zero limit, over limit
        assert_eq!(limiter_zero.database_count_limit(&usage_nonzero), 1);
        assert_eq!(limiter_zero.table_count_limit(&usage_nonzero), 1);
        assert_eq!(limiter_zero.column_per_table_limit(&usage_nonzero), 0);

        // Test with limit of 1
        let limiter_one = MaximumColumnCountLimiter::new(1);
        let usage_one = create_usage(1, 1, 1);
        let usage_two = create_usage(2, 2, 2);

        // Zero columns, under limit (0 < 1), returns remaining capacity (1 - 0 = 1)
        assert_eq!(limiter_one.database_count_limit(&usage_zero), 1);
        assert_eq!(limiter_one.table_count_limit(&usage_zero), 1);
        assert_eq!(limiter_one.column_per_table_limit(&usage_zero), 1);

        // One column, at limit (not under)
        assert_eq!(limiter_one.database_count_limit(&usage_one), 1);
        assert_eq!(limiter_one.table_count_limit(&usage_one), 1);
        assert_eq!(limiter_one.column_per_table_limit(&usage_one), 0);

        // Two columns, over limit
        assert_eq!(limiter_one.database_count_limit(&usage_two), 2);
        assert_eq!(limiter_one.table_count_limit(&usage_two), 2);
        assert_eq!(limiter_one.column_per_table_limit(&usage_two), 0);
    }

    #[test]
    fn test_large_values() {
        // Choose u32::MAX/2 because every table must have at least one field, which means
        // that we can only have u32::MAX/2 tables if we have at least u32::MAX/2 columns
        let limiter = MaximumColumnCountLimiter::new(u32::MAX / 2);

        let usage_under = create_usage(100, 1000, u32::MAX / 2 - 2);
        let usage_at = create_usage(100, 1000, u32::MAX / 2);
        let usage_over = create_usage(100, 1000, u32::MAX / 2 + 1);

        // Under limit, return remaining capacity + 2.
        assert_eq!(limiter.database_count_limit(&usage_under), 100 + 2);
        assert_eq!(limiter.table_count_limit(&usage_under), 1000 + 2);
        assert_eq!(limiter.column_per_table_limit(&usage_under), 2);

        // At limit, return current
        assert_eq!(limiter.database_count_limit(&usage_at), 100);
        assert_eq!(limiter.table_count_limit(&usage_at), 1000);
        assert_eq!(limiter.column_per_table_limit(&usage_at), 0,);

        // Over limit, return current
        assert_eq!(limiter.database_count_limit(&usage_over), 100);
        assert_eq!(limiter.table_count_limit(&usage_over), 1000);
        assert_eq!(limiter.column_per_table_limit(&usage_over), 0);

        let usage_absurd_table_count = create_usage(100, u32::MAX / 2, u32::MAX / 2);
        assert_eq!(
            limiter.table_count_limit(&usage_absurd_table_count),
            (u32::MAX / 2) as usize
        );
    }
}
