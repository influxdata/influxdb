use std::sync::Arc;

use uuid::Uuid;

use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::format::CatalogRecord;
use crate::format::records::assert_roundtrip;
use crate::format::records::cache::{
    CreateDistinctCache, CreateLastCache, DeleteDistinctCache, DeleteLastCache,
};
use crate::format::records::database::CreateDatabase;
use crate::format::records::table::CreateTable;
use crate::format::records::types::{
    CacheSource, ColumnIdentifier, FieldFamilyMode, LastCacheValueColumnsDef, NodeSpec,
    RetentionPeriod,
};

/// Helper to create a test catalog.
fn test_catalog() -> InnerCatalog {
    InnerCatalog::new(Arc::from("test"), Uuid::nil())
}

/// Helper: create a database + empty table for cache tests.
fn setup_db_and_table(catalog: &mut InnerCatalog) {
    CreateDatabase {
        database_id: 1,
        database_name: "mydb".to_string(),
        retention_period: RetentionPeriod::Indefinite,
    }
    .apply(catalog)
    .unwrap();
    CreateTable {
        database_id: 1,
        database_name: "mydb".to_string(),
        table_name: "cpu".to_string(),
        table_id: 10,
        retention_period: RetentionPeriod::Indefinite,
        field_family_mode: FieldFamilyMode::Auto,
    }
    .apply(catalog)
    .unwrap();
}

#[test]
fn create_distinct_cache_round_trip() {
    assert_roundtrip!(
        CreateDistinctCache {
            db_id: 1,
            table_id: 2,
            node_spec: NodeSpec::All,
            cache_id: 1,
            cache_name: "dc1".to_string(),
            column_ids: vec![ColumnIdentifier::Tag(5)],
            max_cardinality: 100_000,
            max_age_seconds: 86400,
            source: CacheSource::User,
            lookback_seconds: None,
            refresh_interval_seconds: None,
        },
        "04010402000100036463310101050002a08601000280510100000000"
    );
}

#[test]
fn delete_distinct_cache_round_trip() {
    assert_roundtrip!(
        DeleteDistinctCache {
            db_id: 1,
            table_id: 2,
            cache_id: 1,
        },
        "040104020100"
    );
}

#[test]
fn create_last_cache_round_trip() {
    assert_roundtrip!(
        CreateLastCache {
            db_id: 1,
            table_id: 2,
            id: 1,
            node_spec: NodeSpec::All,
            name: "lc1".to_string(),
            key_columns: vec![ColumnIdentifier::Tag(3)],
            value_columns: LastCacheValueColumnsDef::AllNonKeyColumns,
            count: 1,
            ttl_seconds: 14400,
        },
        "04010402010000036c633101010300010601044038"
    );
}

#[test]
fn delete_last_cache_round_trip() {
    assert_roundtrip!(
        DeleteLastCache {
            db_id: 1,
            table_id: 2,
            cache_id: 1,
        },
        "040104020100"
    );
}

#[test]
fn record_ids() {
    assert_eq!(CreateDistinctCache::ID.raw(), 9);
    assert_eq!(DeleteDistinctCache::ID.raw(), 10);
    assert_eq!(CreateLastCache::ID.raw(), 11);
    assert_eq!(DeleteLastCache::ID.raw(), 12);

    assert!(!CreateDistinctCache::FLAGS.is_upgrade_safe());
    assert!(!DeleteDistinctCache::FLAGS.is_upgrade_safe());
    assert!(!CreateLastCache::FLAGS.is_upgrade_safe());
    assert!(!DeleteLastCache::FLAGS.is_upgrade_safe());
}

// ---------------------------------------------------------------------------
// Apply tests
// ---------------------------------------------------------------------------

#[test]
fn apply_create_and_delete_last_cache() {
    let mut catalog = test_catalog();
    setup_db_and_table(&mut catalog);

    let table = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap()
        .table_definition_by_id(&influxdb3_id::TableId::new(10))
        .unwrap();
    assert_eq!(table.last_caches.len(), 0);

    CreateLastCache {
        db_id: 1,
        table_id: 10,
        id: 1,
        node_spec: NodeSpec::All,
        name: "my_cache".to_string(),
        key_columns: vec![],
        value_columns: LastCacheValueColumnsDef::AllNonKeyColumns,
        count: 1,
        ttl_seconds: 3600,
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    let table = db
        .table_definition_by_id(&influxdb3_id::TableId::new(10))
        .unwrap();
    assert_eq!(table.last_caches.len(), 1);

    DeleteLastCache {
        db_id: 1,
        table_id: 10,
        cache_id: 1,
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    let table = db
        .table_definition_by_id(&influxdb3_id::TableId::new(10))
        .unwrap();
    assert_eq!(table.last_caches.len(), 0);
}

#[test]
fn apply_create_and_delete_distinct_cache() {
    let mut catalog = test_catalog();
    setup_db_and_table(&mut catalog);

    let table = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap()
        .table_definition_by_id(&influxdb3_id::TableId::new(10))
        .unwrap();
    assert_eq!(table.distinct_caches.len(), 0);

    CreateDistinctCache {
        db_id: 1,
        table_id: 10,
        node_spec: NodeSpec::All,
        cache_id: 1,
        cache_name: "dc1".to_string(),
        column_ids: vec![],
        max_cardinality: 10000,
        max_age_seconds: 3600,
        source: CacheSource::User,
        lookback_seconds: None,
        refresh_interval_seconds: None,
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    let table = db
        .table_definition_by_id(&influxdb3_id::TableId::new(10))
        .unwrap();
    assert_eq!(table.distinct_caches.len(), 1);

    DeleteDistinctCache {
        db_id: 1,
        table_id: 10,
        cache_id: 1,
    }
    .apply(&mut catalog)
    .unwrap();

    let db = catalog
        .databases
        .get_by_id(&influxdb3_id::DbId::new(1))
        .unwrap();
    let table = db
        .table_definition_by_id(&influxdb3_id::TableId::new(10))
        .unwrap();
    assert_eq!(table.distinct_caches.len(), 0);
}
