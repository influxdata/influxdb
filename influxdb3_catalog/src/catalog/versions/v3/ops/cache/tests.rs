use crate::CatalogError;
use crate::catalog::versions::v3::inner::InnerCatalog;
use crate::catalog::versions::v3::ops::CatalogOp;
use crate::catalog::versions::v3::ops::test_util::{
    apply_batch, create_db, create_table, test_catalog,
};
use crate::catalog::versions::v3::schema::cache::{
    LastCacheSize, LastCacheTtl, MaxAge, MaxCardinality,
};
use crate::catalog::versions::v3::schema::node::NodeSpec;
use crate::format::RecordBatch;

use super::{
    CreateDistinctCacheArgs, CreateDistinctCacheOp, CreateLastCacheArgs, CreateLastCacheOp,
    DeleteDistinctCacheArgs, DeleteDistinctCacheOp, DeleteLastCacheArgs, DeleteLastCacheOp,
};

/// Create a database + table with a tag and field column for cache tests.
fn setup_db_and_table(catalog: &mut InnerCatalog) {
    create_db(catalog, "mydb");
    create_table(catalog, "mydb", "cpu");
}

// ---------------------------------------------------------------------------
// Distinct cache
// ---------------------------------------------------------------------------

#[test]
fn prepare_create_distinct_cache_user() {
    let mut catalog = test_catalog();
    setup_db_and_table(&mut catalog);

    let mut batch = RecordBatch::new(1);
    let op = CreateDistinctCacheOp::prepare(
        &CreateDistinctCacheArgs::User {
            db_name: "mydb".to_string(),
            table_name: "cpu".to_string(),
            node_spec: NodeSpec::All,
            cache_name: Some("my_dc".to_string()),
            columns: vec!["host".to_string()],
            max_cardinality: MaxCardinality::from_usize_unchecked(10000),
            max_age: MaxAge::from_secs(3600),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    assert_eq!(batch.len(), 1);

    apply_batch(&batch, &mut catalog);

    let cache = op.output(&catalog);
    assert_eq!(cache.cache_name.as_ref(), "my_dc");
}

#[test]
fn prepare_create_distinct_cache_invalid_column_type() {
    let mut catalog = test_catalog();
    setup_db_and_table(&mut catalog);

    let mut batch = RecordBatch::new(1);
    let result = CreateDistinctCacheOp::prepare(
        &CreateDistinctCacheArgs::User {
            db_name: "mydb".to_string(),
            table_name: "cpu".to_string(),
            node_spec: NodeSpec::All,
            cache_name: Some("bad_dc".to_string()),
            columns: vec!["usage".to_string()], // float — not valid for distinct cache
            max_cardinality: MaxCardinality::from_usize_unchecked(10000),
            max_age: MaxAge::from_secs(3600),
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(
        result,
        Err(CatalogError::InvalidDistinctCacheColumnType)
    ));
}

#[test]
fn prepare_create_distinct_cache_duplicate() {
    let mut catalog = test_catalog();
    setup_db_and_table(&mut catalog);

    let mut batch = RecordBatch::new(1);
    CreateDistinctCacheOp::prepare(
        &CreateDistinctCacheArgs::User {
            db_name: "mydb".to_string(),
            table_name: "cpu".to_string(),
            node_spec: NodeSpec::All,
            cache_name: Some("my_dc".to_string()),
            columns: vec!["host".to_string()],
            max_cardinality: MaxCardinality::from_usize_unchecked(10000),
            max_age: MaxAge::from_secs(3600),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let result = CreateDistinctCacheOp::prepare(
        &CreateDistinctCacheArgs::User {
            db_name: "mydb".to_string(),
            table_name: "cpu".to_string(),
            node_spec: NodeSpec::All,
            cache_name: Some("my_dc".to_string()),
            columns: vec!["host".to_string()],
            max_cardinality: MaxCardinality::from_usize_unchecked(10000),
            max_age: MaxAge::from_secs(3600),
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(result, Err(CatalogError::AlreadyExists)));
}

#[test]
fn prepare_delete_distinct_cache() {
    let mut catalog = test_catalog();
    setup_db_and_table(&mut catalog);

    let mut batch = RecordBatch::new(1);
    CreateDistinctCacheOp::prepare(
        &CreateDistinctCacheArgs::User {
            db_name: "mydb".to_string(),
            table_name: "cpu".to_string(),
            node_spec: NodeSpec::All,
            cache_name: Some("my_dc".to_string()),
            columns: vec!["host".to_string()],
            max_cardinality: MaxCardinality::from_usize_unchecked(10000),
            max_age: MaxAge::from_secs(3600),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let op = DeleteDistinctCacheOp::prepare(
        &DeleteDistinctCacheArgs {
            db_name: "mydb".to_string(),
            table_name: "cpu".to_string(),
            cache_name: "my_dc".to_string(),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    assert_eq!(batch.len(), 1);
    apply_batch(&batch, &mut catalog);

    let deleted = op.output(&catalog);
    assert_eq!(deleted.cache_name.as_ref(), "my_dc");
}

// ---------------------------------------------------------------------------
// Last cache
// ---------------------------------------------------------------------------

#[test]
fn prepare_create_last_cache() {
    let mut catalog = test_catalog();
    setup_db_and_table(&mut catalog);

    let mut batch = RecordBatch::new(1);
    let op = CreateLastCacheOp::prepare(
        &CreateLastCacheArgs {
            db_name: "mydb".to_string(),
            table_name: "cpu".to_string(),
            node_spec: NodeSpec::All,
            cache_name: Some("my_lc".to_string()),
            key_columns: Some(vec!["host".to_string()]),
            value_columns: None,
            count: LastCacheSize::new(1).unwrap(),
            ttl: LastCacheTtl::from_secs(3600),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    assert_eq!(batch.len(), 1);

    apply_batch(&batch, &mut catalog);

    let cache = op.output(&catalog);
    assert_eq!(cache.name.as_ref(), "my_lc");
}

#[test]
fn prepare_create_last_cache_duplicate() {
    let mut catalog = test_catalog();
    setup_db_and_table(&mut catalog);

    let args = CreateLastCacheArgs {
        db_name: "mydb".to_string(),
        table_name: "cpu".to_string(),
        node_spec: NodeSpec::All,
        cache_name: Some("my_lc".to_string()),
        key_columns: Some(vec!["host".to_string()]),
        value_columns: None,
        count: LastCacheSize::new(1).unwrap(),
        ttl: LastCacheTtl::from_secs(3600),
    };

    let mut batch = RecordBatch::new(1);
    CreateLastCacheOp::prepare(&args, &catalog, &mut batch).unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let result = CreateLastCacheOp::prepare(&args, &catalog, &mut batch);
    assert!(matches!(result, Err(CatalogError::AlreadyExists)));
}

#[test]
fn prepare_delete_last_cache() {
    let mut catalog = test_catalog();
    setup_db_and_table(&mut catalog);

    let mut batch = RecordBatch::new(1);
    CreateLastCacheOp::prepare(
        &CreateLastCacheArgs {
            db_name: "mydb".to_string(),
            table_name: "cpu".to_string(),
            node_spec: NodeSpec::All,
            cache_name: Some("my_lc".to_string()),
            key_columns: Some(vec!["host".to_string()]),
            value_columns: None,
            count: LastCacheSize::new(1).unwrap(),
            ttl: LastCacheTtl::from_secs(3600),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    apply_batch(&batch, &mut catalog);

    batch = RecordBatch::new(1);
    let op = DeleteLastCacheOp::prepare(
        &DeleteLastCacheArgs {
            db_name: "mydb".to_string(),
            table_name: "cpu".to_string(),
            cache_name: "my_lc".to_string(),
        },
        &catalog,
        &mut batch,
    )
    .unwrap();
    assert_eq!(batch.len(), 1);
    apply_batch(&batch, &mut catalog);

    let deleted = op.output(&catalog);
    assert_eq!(deleted.name.as_ref(), "my_lc");
}

#[test]
fn prepare_delete_last_cache_not_found() {
    let mut catalog = test_catalog();
    setup_db_and_table(&mut catalog);

    let mut batch = RecordBatch::new(1);
    let result = DeleteLastCacheOp::prepare(
        &DeleteLastCacheArgs {
            db_name: "mydb".to_string(),
            table_name: "cpu".to_string(),
            cache_name: "nonexistent".to_string(),
        },
        &catalog,
        &mut batch,
    );
    assert!(matches!(result, Err(CatalogError::NotFound(_))));
}
