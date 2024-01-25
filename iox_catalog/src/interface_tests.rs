//! Abstract tests of the catalog interface w/o relying on the actual implementation.
use crate::{
    interface::{
        CasFailure, Catalog, Error, ParquetFileRepoExt, PartitionRepoExt, RepoCollection,
        SoftDeletedRows,
    },
    test_helpers::{arbitrary_namespace, arbitrary_parquet_file_params, arbitrary_table},
    util::{list_schemas, validate_or_insert_schema},
};

use ::test_helpers::assert_error;
use assert_matches::assert_matches;
use async_trait::async_trait;
use data_types::snapshot::table::TableSnapshot;
use data_types::{
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    ColumnId, ColumnType, CompactionLevel, MaxColumnsPerTable, MaxTables, Namespace, NamespaceId,
    NamespaceName, NamespaceSchema, ObjectStoreId, ParquetFile, ParquetFileId, ParquetFileParams,
    PartitionId, SortKeyIds, TableId, Timestamp,
};
use data_types::{snapshot::partition::PartitionSnapshot, Column, PartitionHashId, PartitionKey};
use futures::{Future, StreamExt};
use generated_types::influxdata::iox::partition_template::v1 as proto;
use iox_time::TimeProvider;
use metric::{Attributes, DurationHistogram, Metric};
use parking_lot::Mutex;
use std::{any::Any, fmt::Display};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    ops::DerefMut,
    sync::Arc,
    time::Duration,
};

pub(crate) async fn test_catalog<R, F>(clean_state: R)
where
    R: Fn() -> F + Send + Sync,
    F: Future<Output = Arc<dyn Catalog>> + Send,
{
    test_setup(clean_state().await).await;
    test_namespace_soft_deletion(clean_state().await).await;
    test_partitions_new_file_between(clean_state().await).await;
    test_column(clean_state().await).await;
    test_partition(clean_state().await).await;
    test_parquet_file(clean_state().await).await;
    test_parquet_file_delete_broken(clean_state().await).await;
    test_update_to_compaction_level_1(clean_state().await).await;
    test_list_by_partiton_not_to_delete(clean_state().await).await;
    test_list_schemas(clean_state().await).await;
    test_list_schemas_soft_deleted_rows(clean_state().await).await;
    test_delete_namespace(clean_state().await).await;

    let catalog = clean_state().await;
    test_namespace(Arc::clone(&catalog)).await;
    assert_metric_hit(&catalog.metrics(), "namespace_create");

    let catalog = clean_state().await;
    test_table(Arc::clone(&catalog)).await;
    assert_metric_hit(&catalog.metrics(), "table_create");

    let catalog = clean_state().await;
    test_column(Arc::clone(&catalog)).await;
    assert_metric_hit(&catalog.metrics(), "column_create_or_get");

    let catalog = clean_state().await;
    test_partition(Arc::clone(&catalog)).await;
    assert_metric_hit(&catalog.metrics(), "partition_create_or_get");

    let catalog = clean_state().await;
    test_parquet_file(Arc::clone(&catalog)).await;
    assert_metric_hit(&catalog.metrics(), "parquet_create_upgrade_delete");

    test_two_repos(clean_state().await).await;
    test_partition_create_or_get_idempotent(clean_state().await).await;
    test_column_create_or_get_many_unchecked(clean_state).await;
}

async fn test_setup(catalog: Arc<dyn Catalog>) {
    catalog.setup().await.expect("first catalog setup");
    catalog.setup().await.expect("second catalog setup");
}

async fn test_namespace(catalog: Arc<dyn Catalog>) {
    let mut repos = catalog.repositories();
    let namespace_name = NamespaceName::new("test_namespace").unwrap();
    let namespace = repos
        .namespaces()
        .create(&namespace_name, None, None, None)
        .await
        .unwrap();
    assert!(namespace.id > NamespaceId::new(0));
    assert_eq!(namespace.name, namespace_name.as_str());
    assert_eq!(
        namespace.partition_template,
        NamespacePartitionTemplateOverride::default()
    );
    let lookup_namespace = repos
        .namespaces()
        .get_by_name(&namespace_name, SoftDeletedRows::ExcludeDeleted)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(namespace, lookup_namespace);

    // Assert default values for service protection limits.
    assert_eq!(namespace.max_tables, MaxTables::default());
    assert_eq!(
        namespace.max_columns_per_table,
        MaxColumnsPerTable::default()
    );

    let conflict = repos
        .namespaces()
        .create(&namespace_name, None, None, None)
        .await;
    assert!(matches!(conflict.unwrap_err(), Error::AlreadyExists { .. }));

    let found = repos
        .namespaces()
        .get_by_id(namespace.id, SoftDeletedRows::ExcludeDeleted)
        .await
        .unwrap()
        .expect("namespace should be there");
    assert_eq!(namespace, found);

    let not_found = repos
        .namespaces()
        .get_by_id(NamespaceId::new(i64::MAX), SoftDeletedRows::ExcludeDeleted)
        .await
        .unwrap();
    assert!(not_found.is_none());

    let found = repos
        .namespaces()
        .get_by_name(&namespace_name, SoftDeletedRows::ExcludeDeleted)
        .await
        .unwrap()
        .expect("namespace should be there");
    assert_eq!(namespace, found);

    let not_found = repos
        .namespaces()
        .get_by_name("does_not_exist", SoftDeletedRows::ExcludeDeleted)
        .await
        .unwrap();
    assert!(not_found.is_none());

    let namespace2 = arbitrary_namespace(&mut *repos, "test_namespace2").await;
    let mut namespaces = repos
        .namespaces()
        .list(SoftDeletedRows::ExcludeDeleted)
        .await
        .unwrap();
    namespaces.sort_by_key(|ns| ns.name.clone());
    assert_eq!(namespaces, vec![namespace, namespace2]);

    let new_table_limit = MaxTables::try_from(15_000).unwrap();
    let modified = repos
        .namespaces()
        .update_table_limit(namespace_name.as_str(), new_table_limit)
        .await
        .expect("namespace should be updateable");
    assert_eq!(new_table_limit, modified.max_tables);

    let new_column_limit = MaxColumnsPerTable::try_from(1_500).unwrap();
    let modified = repos
        .namespaces()
        .update_column_limit(namespace_name.as_str(), new_column_limit)
        .await
        .expect("namespace should be updateable");
    assert_eq!(new_column_limit, modified.max_columns_per_table);

    const NEW_RETENTION_PERIOD_NS: i64 = 5 * 60 * 60 * 1000 * 1000 * 1000;
    let modified = repos
        .namespaces()
        .update_retention_period(namespace_name.as_str(), Some(NEW_RETENTION_PERIOD_NS))
        .await
        .expect("namespace should be updateable");
    assert_eq!(
        NEW_RETENTION_PERIOD_NS,
        modified.retention_period_ns.unwrap()
    );

    let modified = repos
        .namespaces()
        .update_retention_period(namespace_name.as_str(), None)
        .await
        .expect("namespace should be updateable");
    assert!(modified.retention_period_ns.is_none());

    // create namespace with retention period NULL (the default)
    let namespace3 = arbitrary_namespace(&mut *repos, "test_namespace3").await;
    assert!(namespace3.retention_period_ns.is_none());

    // create namespace with retention period
    let namespace4_name = NamespaceName::new("test_namespace4").unwrap();
    let namespace4 = repos
        .namespaces()
        .create(&namespace4_name, None, Some(NEW_RETENTION_PERIOD_NS), None)
        .await
        .expect("namespace with 5-hour retention should be created");
    assert_eq!(
        NEW_RETENTION_PERIOD_NS,
        namespace4.retention_period_ns.unwrap()
    );
    // reset retention period to NULL to avoid affecting later tests
    repos
        .namespaces()
        .update_retention_period(&namespace4_name, None)
        .await
        .expect("namespace should be updateable");

    // create a namespace with a PartitionTemplate other than the default
    let tag_partition_template =
        NamespacePartitionTemplateOverride::try_from(proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TagValue("tag1".into())),
            }],
        })
        .unwrap();
    let namespace5_name = NamespaceName::new("test_namespace5").unwrap();
    let namespace5 = repos
        .namespaces()
        .create(
            &namespace5_name,
            Some(tag_partition_template.clone()),
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(namespace5.partition_template, tag_partition_template);
    let lookup_namespace5 = repos
        .namespaces()
        .get_by_name(&namespace5_name, SoftDeletedRows::ExcludeDeleted)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(namespace5, lookup_namespace5);

    // remove namespace to avoid it from affecting later tests
    repos
        .namespaces()
        .soft_delete("test_namespace")
        .await
        .expect("delete namespace should succeed");
    repos
        .namespaces()
        .soft_delete("test_namespace2")
        .await
        .expect("delete namespace should succeed");
    repos
        .namespaces()
        .soft_delete("test_namespace3")
        .await
        .expect("delete namespace should succeed");
    repos
        .namespaces()
        .soft_delete("test_namespace4")
        .await
        .expect("delete namespace should succeed");
}

/// Construct a set of two namespaces:
///
///  * deleted-ns: marked as soft-deleted
///  * active-ns: not marked as deleted
///
/// And assert the expected "soft delete" semantics / correctly filter out
/// the expected rows for all three states of [`SoftDeletedRows`].
async fn test_namespace_soft_deletion(catalog: Arc<dyn Catalog>) {
    let mut repos = catalog.repositories();

    let deleted_ns = arbitrary_namespace(&mut *repos, "deleted-ns").await;
    let active_ns = arbitrary_namespace(&mut *repos, "active-ns").await;

    // Mark "deleted-ns" as soft-deleted.
    repos.namespaces().soft_delete("deleted-ns").await.unwrap();

    // Which should be idempotent (ignoring the timestamp change - when
    // changing this to "soft delete" it was idempotent, so I am preserving
    // that).
    repos.namespaces().soft_delete("deleted-ns").await.unwrap();

    // Listing should respect soft deletion.
    let got = repos
        .namespaces()
        .list(SoftDeletedRows::AllRows)
        .await
        .unwrap()
        .into_iter()
        .map(|v| v.name);
    assert_string_set_eq(got, ["deleted-ns", "active-ns"]);

    let got = repos
        .namespaces()
        .list(SoftDeletedRows::OnlyDeleted)
        .await
        .unwrap()
        .into_iter()
        .map(|v| v.name);
    assert_string_set_eq(got, ["deleted-ns"]);

    let got = repos
        .namespaces()
        .list(SoftDeletedRows::ExcludeDeleted)
        .await
        .unwrap()
        .into_iter()
        .map(|v| v.name);
    assert_string_set_eq(got, ["active-ns"]);

    // As should get by ID
    let got = repos
        .namespaces()
        .get_by_id(deleted_ns.id, SoftDeletedRows::AllRows)
        .await
        .unwrap()
        .into_iter()
        .map(|v| v.name);
    assert_string_set_eq(got, ["deleted-ns"]);
    let got = repos
        .namespaces()
        .get_by_id(deleted_ns.id, SoftDeletedRows::OnlyDeleted)
        .await
        .unwrap()
        .into_iter()
        .map(|v| {
            assert!(v.deleted_at.is_some());
            v.name
        });
    assert_string_set_eq(got, ["deleted-ns"]);
    let got = repos
        .namespaces()
        .get_by_id(deleted_ns.id, SoftDeletedRows::ExcludeDeleted)
        .await
        .unwrap();
    assert!(got.is_none());
    let got = repos
        .namespaces()
        .get_by_id(active_ns.id, SoftDeletedRows::AllRows)
        .await
        .unwrap()
        .into_iter()
        .map(|v| v.name);
    assert_string_set_eq(got, ["active-ns"]);
    let got = repos
        .namespaces()
        .get_by_id(active_ns.id, SoftDeletedRows::OnlyDeleted)
        .await
        .unwrap();
    assert!(got.is_none());
    let got = repos
        .namespaces()
        .get_by_id(active_ns.id, SoftDeletedRows::ExcludeDeleted)
        .await
        .unwrap()
        .into_iter()
        .map(|v| v.name);
    assert_string_set_eq(got, ["active-ns"]);

    // And get by name
    let got = repos
        .namespaces()
        .get_by_name(&deleted_ns.name, SoftDeletedRows::AllRows)
        .await
        .unwrap()
        .into_iter()
        .map(|v| v.name);
    assert_string_set_eq(got, ["deleted-ns"]);
    let got = repos
        .namespaces()
        .get_by_name(&deleted_ns.name, SoftDeletedRows::OnlyDeleted)
        .await
        .unwrap()
        .into_iter()
        .map(|v| {
            assert!(v.deleted_at.is_some());
            v.name
        });
    assert_string_set_eq(got, ["deleted-ns"]);
    let got = repos
        .namespaces()
        .get_by_name(&deleted_ns.name, SoftDeletedRows::ExcludeDeleted)
        .await
        .unwrap();
    assert!(got.is_none());
    let got = repos
        .namespaces()
        .get_by_name(&active_ns.name, SoftDeletedRows::AllRows)
        .await
        .unwrap()
        .into_iter()
        .map(|v| v.name);
    assert_string_set_eq(got, ["active-ns"]);
    let got = repos
        .namespaces()
        .get_by_name(&active_ns.name, SoftDeletedRows::OnlyDeleted)
        .await
        .unwrap();
    assert!(got.is_none());
    let got = repos
        .namespaces()
        .get_by_name(&active_ns.name, SoftDeletedRows::ExcludeDeleted)
        .await
        .unwrap()
        .into_iter()
        .map(|v| v.name);
    assert_string_set_eq(got, ["active-ns"]);
}

// Assert the set of strings "a" is equal to the set "b", tolerating
// duplicates.
#[track_caller]
fn assert_string_set_eq<T, U>(a: impl IntoIterator<Item = T>, b: impl IntoIterator<Item = U>)
where
    T: Into<String>,
    U: Into<String>,
{
    let mut a = a.into_iter().map(Into::into).collect::<Vec<String>>();
    a.sort_unstable();
    let mut b = b.into_iter().map(Into::into).collect::<Vec<String>>();
    b.sort_unstable();
    assert_eq!(a, b);
}

async fn test_table(catalog: Arc<dyn Catalog>) {
    let mut repos = catalog.repositories();
    let namespace = arbitrary_namespace(&mut *repos, "namespace_table_test").await;

    // test we can create a table
    let t = arbitrary_table(&mut *repos, "test_table", &namespace).await;
    assert!(t.id > TableId::new(0));
    assert_eq!(
        t.partition_template,
        TablePartitionTemplateOverride::default()
    );

    // The default template doesn't use any tag values, so no columns need to be created.
    let table_columns = repos.columns().list_by_table_id(t.id).await.unwrap();
    assert!(table_columns.is_empty());

    // test we get an error if we try to create it again
    let err = repos
        .tables()
        .create(
            "test_table",
            TablePartitionTemplateOverride::try_new(None, &namespace.partition_template).unwrap(),
            namespace.id,
        )
        .await;
    assert_error!(
        err,
        Error::AlreadyExists { ref descr }
            if descr == &format!("table 'test_table' in namespace {}", namespace.id)
    );

    // get by id
    assert_eq!(t, repos.tables().get_by_id(t.id).await.unwrap().unwrap());
    assert!(repos
        .tables()
        .get_by_id(TableId::new(i64::MAX))
        .await
        .unwrap()
        .is_none());

    let tables = repos
        .tables()
        .list_by_namespace_id(namespace.id)
        .await
        .unwrap();
    assert_eq!(vec![t.clone()], tables);

    // test we can create a table of the same name in a different namespace
    let namespace2 = arbitrary_namespace(&mut *repos, "two").await;
    assert_ne!(namespace, namespace2);
    let test_table = arbitrary_table(&mut *repos, "test_table", &namespace2).await;
    assert_ne!(t.id, test_table.id);
    assert_eq!(test_table.namespace_id, namespace2.id);

    // test get by namespace and name
    let foo_table = arbitrary_table(&mut *repos, "foo", &namespace2).await;
    assert_eq!(
        repos
            .tables()
            .get_by_namespace_and_name(NamespaceId::new(i64::MAX), "test_table")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        repos
            .tables()
            .get_by_namespace_and_name(namespace.id, "not_existing")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        repos
            .tables()
            .get_by_namespace_and_name(namespace.id, "test_table")
            .await
            .unwrap(),
        Some(t.clone())
    );
    assert_eq!(
        repos
            .tables()
            .get_by_namespace_and_name(namespace2.id, "test_table")
            .await
            .unwrap()
            .as_ref(),
        Some(&test_table)
    );
    assert_eq!(
        repos
            .tables()
            .get_by_namespace_and_name(namespace2.id, "foo")
            .await
            .unwrap()
            .as_ref(),
        Some(&foo_table)
    );

    // All tables should be returned by list(), regardless of namespace
    let mut list = repos.tables().list().await.unwrap();
    list.sort_by_key(|t| t.id);
    let mut expected = [t, test_table, foo_table];
    expected.sort_by_key(|t| t.id);
    assert_eq!(&list, &expected);

    // test per-namespace table limits
    let latest = repos
        .namespaces()
        .update_table_limit("namespace_table_test", MaxTables::try_from(1).unwrap())
        .await
        .expect("namespace should be updateable");
    let err = repos
        .tables()
        .create(
            "definitely_unique",
            TablePartitionTemplateOverride::try_new(None, &latest.partition_template).unwrap(),
            latest.id,
        )
        .await
        .expect_err("should error with table create limit error");
    assert!(matches!(err, Error::LimitExceeded { .. }));

    // Create a table with a partition template other than the default
    let custom_table_template = TablePartitionTemplateOverride::try_new(
        Some(proto::PartitionTemplate {
            parts: vec![
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("tag1".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("tag2".into())),
                },
            ],
        }),
        &namespace2.partition_template,
    )
    .unwrap();
    let templated = repos
        .tables()
        .create(
            "use_a_template",
            custom_table_template.clone(),
            namespace2.id,
        )
        .await
        .unwrap();
    assert_eq!(templated.partition_template, custom_table_template);

    // Tag columns should be created for tags used in the template
    let table_columns = repos
        .columns()
        .list_by_table_id(templated.id)
        .await
        .unwrap();
    assert_eq!(table_columns.len(), 2);
    assert!(table_columns.iter().all(|c| c.is_tag()));
    let mut column_names: Vec<_> = table_columns.iter().map(|c| &c.name).collect();
    column_names.sort();
    assert_eq!(column_names, &["tag1", "tag2"]);

    let lookup_templated = repos
        .tables()
        .get_by_namespace_and_name(namespace2.id, "use_a_template")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(templated, lookup_templated);

    // Create a namespace with a partition template other than the default
    let custom_namespace_template =
        NamespacePartitionTemplateOverride::try_from(proto::PartitionTemplate {
            parts: vec![
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("zzz".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("aaa".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                },
            ],
        })
        .unwrap();
    let custom_namespace_name = NamespaceName::new("custom_namespace").unwrap();
    let custom_namespace = repos
        .namespaces()
        .create(
            &custom_namespace_name,
            Some(custom_namespace_template.clone()),
            None,
            None,
        )
        .await
        .unwrap();
    // Create a table without specifying the partition template
    let custom_table_template =
        TablePartitionTemplateOverride::try_new(None, &custom_namespace.partition_template)
            .unwrap();
    let table_templated_by_namespace = repos
        .tables()
        .create(
            "use_namespace_template",
            custom_table_template,
            custom_namespace.id,
        )
        .await
        .unwrap();
    assert_eq!(
        table_templated_by_namespace.partition_template,
        TablePartitionTemplateOverride::try_new(None, &custom_namespace_template).unwrap()
    );

    // Tag columns should be created for tags used in the template
    let table_columns = repos
        .columns()
        .list_by_table_id(table_templated_by_namespace.id)
        .await
        .unwrap();
    assert_eq!(table_columns.len(), 2);
    assert!(table_columns.iter().all(|c| c.is_tag()));
    let mut column_names: Vec<_> = table_columns.iter().map(|c| &c.name).collect();
    column_names.sort();
    assert_eq!(column_names, &["aaa", "zzz"]);

    repos
        .namespaces()
        .soft_delete("namespace_table_test")
        .await
        .expect("delete namespace should succeed");
    repos
        .namespaces()
        .soft_delete("two")
        .await
        .expect("delete namespace should succeed");
}

async fn test_column(catalog: Arc<dyn Catalog>) {
    let mut repos = catalog.repositories();
    let namespace = arbitrary_namespace(&mut *repos, "namespace_column_test").await;
    let table = arbitrary_table(&mut *repos, "test_table", &namespace).await;
    assert_eq!(table.namespace_id, namespace.id);

    // test we can create or get a column
    let c = repos
        .columns()
        .create_or_get("column_test", table.id, ColumnType::Tag)
        .await
        .unwrap();

    let ts1 = repos.tables().snapshot(table.id).await.unwrap();
    validate_table_snapshot(repos.as_mut(), &ts1).await;

    let cc = repos
        .columns()
        .create_or_get("column_test", table.id, ColumnType::Tag)
        .await
        .unwrap();
    assert!(c.id > ColumnId::new(0));
    assert_eq!(c, cc);

    let ts2 = repos.tables().snapshot(table.id).await.unwrap();
    validate_table_snapshot(repos.as_mut(), &ts2).await;

    assert_gt(ts2.generation(), ts1.generation());

    // test that attempting to create an already defined column of a different type returns
    // error
    let err = repos
        .columns()
        .create_or_get("column_test", table.id, ColumnType::U64)
        .await
        .expect_err("should error with wrong column type");
    assert!(matches!(err, Error::AlreadyExists { .. }));

    // test that we can create a column of the same name under a different table
    let table2 = arbitrary_table(&mut *repos, "test_table_2", &namespace).await;
    let ccc = repos
        .columns()
        .create_or_get("column_test", table2.id, ColumnType::U64)
        .await
        .unwrap();
    assert_ne!(c, ccc);

    let columns = repos
        .columns()
        .list_by_namespace_id(namespace.id)
        .await
        .unwrap();

    let ts3 = repos.tables().snapshot(table2.id).await.unwrap();
    validate_table_snapshot(repos.as_mut(), &ts3).await;

    let mut want = vec![c.clone(), ccc];
    assert_eq!(want, columns);

    let columns = repos.columns().list_by_table_id(table.id).await.unwrap();

    let want2 = vec![c];
    assert_eq!(want2, columns);

    // Add another tag column into table2
    let c3 = repos
        .columns()
        .create_or_get("b", table2.id, ColumnType::Tag)
        .await
        .unwrap();

    let ts4 = repos.tables().snapshot(table2.id).await.unwrap();
    validate_table_snapshot(repos.as_mut(), &ts4).await;

    assert_gt(ts4.generation(), ts3.generation());

    // Listing columns should return all columns in the catalog
    let list = repos.columns().list().await.unwrap();
    want.extend([c3]);
    assert_eq!(list, want);

    // test create_or_get_many_unchecked, below column limit
    let mut columns = HashMap::new();
    columns.insert("column_test", ColumnType::Tag);
    columns.insert("new_column", ColumnType::Tag);
    let table1_columns = repos
        .columns()
        .create_or_get_many_unchecked(table.id, columns)
        .await
        .unwrap();
    let mut table1_column_names: Vec<_> = table1_columns.iter().map(|c| &c.name).collect();
    table1_column_names.sort();
    assert_eq!(table1_column_names, vec!["column_test", "new_column"]);

    // test per-namespace column limits
    repos
        .namespaces()
        .update_column_limit(
            "namespace_column_test",
            MaxColumnsPerTable::try_from(1).unwrap(),
        )
        .await
        .expect("namespace should be updateable");
    let err = repos
        .columns()
        .create_or_get("definitely unique", table.id, ColumnType::Tag)
        .await
        .expect_err("should error with table create limit error");
    assert!(matches!(err, Error::LimitExceeded { .. }));

    // test per-namespace column limits are NOT enforced with create_or_get_many_unchecked
    let table3 = arbitrary_table(&mut *repos, "test_table_3", &namespace).await;
    let mut columns = HashMap::new();
    columns.insert("apples", ColumnType::Tag);
    columns.insert("oranges", ColumnType::Tag);
    let table3_columns = repos
        .columns()
        .create_or_get_many_unchecked(table3.id, columns)
        .await
        .unwrap();
    let mut table3_column_names: Vec<_> = table3_columns.iter().map(|c| &c.name).collect();
    table3_column_names.sort();
    assert_eq!(table3_column_names, vec!["apples", "oranges"]);

    repos
        .namespaces()
        .soft_delete("namespace_column_test")
        .await
        .expect("delete namespace should succeed");
}

async fn test_partition(catalog: Arc<dyn Catalog>) {
    let mut repos = catalog.repositories();
    let namespace = arbitrary_namespace(&mut *repos, "namespace_partition_test").await;
    let table = arbitrary_table(&mut *repos, "test_table", &namespace).await;

    let mut created = BTreeMap::new();
    // partition to use
    let partition = repos
        .partitions()
        .create_or_get("foo".into(), table.id)
        .await
        .expect("failed to create partition");
    // Test: sort_key_ids from create_or_get
    assert!(partition.sort_key_ids().is_none());
    created.insert(partition.id, partition.clone());
    // partition to use
    let partition_bar = repos
        .partitions()
        .create_or_get("bar".into(), table.id)
        .await
        .expect("failed to create partition");
    created.insert(partition_bar.id, partition_bar);
    // partition to be skipped later
    let to_skip_partition = repos
        .partitions()
        .create_or_get("asdf".into(), table.id)
        .await
        .unwrap();
    created.insert(to_skip_partition.id, to_skip_partition.clone());
    // partition to be skipped later
    let to_skip_partition_too = repos
        .partitions()
        .create_or_get("asdf too".into(), table.id)
        .await
        .unwrap();
    created.insert(to_skip_partition_too.id, to_skip_partition_too.clone());

    // partitions can be retrieved easily
    let mut created_sorted = created.values().cloned().collect::<Vec<_>>();
    created_sorted.sort_by_key(|p| p.id);
    assert_eq!(
        to_skip_partition,
        repos
            .partitions()
            .get_by_id_batch(&[to_skip_partition.id])
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    );
    let non_existing_partition_id = PartitionId::new(i64::MAX);
    assert!(repos
        .partitions()
        .get_by_id_batch(&[non_existing_partition_id])
        .await
        .unwrap()
        .is_empty());
    let mut batch = repos
        .partitions()
        .get_by_id_batch(
            &created
                .keys()
                .cloned()
                // non-existing entries are ignored
                .chain([non_existing_partition_id])
                // duplicates are ignored
                .chain(created.keys().cloned())
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
    batch.sort_by_key(|p| p.id);
    assert_eq!(created_sorted, batch);
    // Test: sort_key_ids from get_by_id_batch
    assert!(batch.iter().all(|p| p.sort_key_ids().is_none()));

    assert_eq!(created_sorted, batch);

    let s1 = repos.tables().snapshot(table.id).await.unwrap();
    validate_table_snapshot(repos.as_mut(), &s1).await;

    let listed = repos
        .partitions()
        .list_by_table_id(table.id)
        .await
        .expect("failed to list partitions")
        .into_iter()
        .map(|v| (v.id, v))
        .collect::<BTreeMap<_, _>>();
    // Test: sort_key_ids from list_by_table_id
    assert!(listed.values().all(|p| p.sort_key_ids().is_none()));

    assert_eq!(created, listed);

    let listed = repos
        .partitions()
        .list_ids()
        .await
        .expect("failed to list partitions")
        .into_iter()
        .collect::<BTreeSet<_>>();

    assert_eq!(created.keys().copied().collect::<BTreeSet<_>>(), listed);

    // The code no longer supports creating old-style partitions, so this list is always empty
    // in these tests. See each catalog implementation for tests that insert old-style
    // partitions directly and verify they're returned.
    let old_style = repos.partitions().list_old_style().await.unwrap();
    assert!(
        old_style.is_empty(),
        "Expected no old-style partitions, got {old_style:?}"
    );

    // sort key should be unset on creation
    assert!(to_skip_partition.sort_key_ids().is_none());

    let s1 = repos
        .partitions()
        .snapshot(to_skip_partition.id)
        .await
        .unwrap();
    validate_partition_snapshot(repos.as_mut(), &s1).await;

    // test that updates sort key from None to Some
    let updated_partition = repos
        .partitions()
        .cas_sort_key(to_skip_partition.id, None, &SortKeyIds::from([2, 1, 3]))
        .await
        .unwrap();

    // verify sort key is updated correctly
    assert_eq!(
        updated_partition.sort_key_ids().unwrap(),
        &SortKeyIds::from([2, 1, 3])
    );

    let s2 = repos
        .partitions()
        .snapshot(to_skip_partition.id)
        .await
        .unwrap();
    assert_gt(s2.generation(), s1.generation());
    validate_partition_snapshot(repos.as_mut(), &s2).await;

    // test that provides value of old_sort_key_ids but it do not match the existing one
    // --> the new sort key will not be updated
    let err = repos
        .partitions()
        .cas_sort_key(
            to_skip_partition.id,
            Some(&SortKeyIds::from([1])),
            &SortKeyIds::from([1, 2, 3, 4]),
        )
        .await
        .expect_err("CAS with incorrect value should fail");
    // verify the sort key is not updated
    assert_matches!(err, CasFailure::ValueMismatch(old_sort_key_ids) => {
        assert_eq!(old_sort_key_ids, SortKeyIds::from([2, 1, 3]));
    });

    // test that provides same length but not-matched old_sort_key_ids
    // --> the new sort key will not be updated
    let err = repos
        .partitions()
        .cas_sort_key(
            to_skip_partition.id,
            Some(&SortKeyIds::from([1, 5, 10])),
            &SortKeyIds::from([1, 2, 3, 4]),
        )
        .await
        .expect_err("CAS with incorrect value should fail");
    // verify the sort key is not updated
    assert_matches!(err, CasFailure::ValueMismatch(old_sort_key_ids) => {
        assert_eq!(old_sort_key_ids, SortKeyIds::from([2, 1, 3]));
    });

    // test that provide None sort_key_ids that do not match with existing values that are not None
    // --> the new sort key will not be updated
    let err = repos
        .partitions()
        .cas_sort_key(to_skip_partition.id, None, &SortKeyIds::from([1, 2, 3, 4]))
        .await
        .expect_err("CAS with incorrect value should fail");
    assert_matches!(err, CasFailure::ValueMismatch(old_sort_key_ids) => {
        assert_eq!(old_sort_key_ids, SortKeyIds::from([2, 1, 3]));
    });

    // test getting partition from partition id and verify values of sort_key and sort_key_ids
    let updated_other_partition = repos
        .partitions()
        .get_by_id_batch(&[to_skip_partition.id])
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    // still has the old sort key
    assert_eq!(
        updated_other_partition.sort_key_ids().unwrap(),
        &SortKeyIds::from([2, 1, 3])
    );

    // test that updates sort_key_ids from Some matching value to Some other value
    let updated_partition = repos
        .partitions()
        .cas_sort_key(
            to_skip_partition.id,
            Some(&SortKeyIds::from([2, 1, 3])),
            &SortKeyIds::from([2, 1, 4, 3]),
        )
        .await
        .unwrap();
    // verify the new values are updated
    assert_eq!(
        updated_partition.sort_key_ids().unwrap(),
        &SortKeyIds::from([2, 1, 4, 3])
    );

    // test getting the new sort key from partition id
    let updated_partition = repos
        .partitions()
        .get_by_id_batch(&[to_skip_partition.id])
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(
        updated_partition.sort_key_ids().unwrap(),
        &SortKeyIds::from([2, 1, 4, 3])
    );

    // use to_skip_partition_too to update sort key from empty old values
    // first make sure the old sort key is unset
    assert!(to_skip_partition_too.sort_key_ids().is_none());

    // test that provides empty old_sort_key_ids
    // --> the new sort key will be updated
    let updated_to_skip_partition_too = repos
        .partitions()
        .cas_sort_key(to_skip_partition_too.id, None, &SortKeyIds::from([3, 4]))
        .await
        .unwrap();
    // verify the new values are updated
    assert_eq!(
        updated_to_skip_partition_too.sort_key_ids().unwrap(),
        &SortKeyIds::from([3, 4])
    );

    let s3 = repos
        .partitions()
        .snapshot(to_skip_partition.id)
        .await
        .unwrap();
    assert_gt(s3.generation(), s2.generation());
    validate_partition_snapshot(repos.as_mut(), &s3).await;

    // The compactor can log why compaction was skipped
    let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
    assert!(
        skipped_compactions.is_empty(),
        "Expected no skipped compactions, got: {skipped_compactions:?}"
    );
    repos
        .partitions()
        .record_skipped_compaction(to_skip_partition.id, "I am le tired", 1, 2, 4, 10, 20)
        .await
        .unwrap();
    let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
    assert_eq!(skipped_compactions.len(), 1);
    assert_eq!(skipped_compactions[0].partition_id, to_skip_partition.id);
    assert_eq!(skipped_compactions[0].reason, "I am le tired");
    assert_eq!(skipped_compactions[0].num_files, 1);
    assert_eq!(skipped_compactions[0].limit_num_files, 2);
    assert_eq!(skipped_compactions[0].estimated_bytes, 10);
    assert_eq!(skipped_compactions[0].limit_bytes, 20);
    //
    let skipped_partition_records = repos
        .partitions()
        .get_in_skipped_compactions(&[
            to_skip_partition.id,
            PartitionId::new(i64::MAX),
            to_skip_partition.id,
        ])
        .await
        .unwrap();
    assert_eq!(
        skipped_partition_records[0].partition_id,
        to_skip_partition.id
    );
    assert_eq!(skipped_partition_records[0].reason, "I am le tired");

    let s4 = repos
        .partitions()
        .snapshot(to_skip_partition.id)
        .await
        .unwrap();
    assert_gt(s4.generation(), s3.generation());
    validate_partition_snapshot(repos.as_mut(), &s4).await;

    // Only save the last reason that any particular partition was skipped (really if the
    // partition appears in the skipped compactions, it shouldn't become a compaction candidate
    // again, but race conditions and all that)
    repos
        .partitions()
        .record_skipped_compaction(to_skip_partition.id, "I'm on fire", 11, 12, 24, 110, 120)
        .await
        .unwrap();
    let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
    assert_eq!(skipped_compactions.len(), 1);
    assert_eq!(skipped_compactions[0].partition_id, to_skip_partition.id);
    assert_eq!(skipped_compactions[0].reason, "I'm on fire");
    assert_eq!(skipped_compactions[0].num_files, 11);
    assert_eq!(skipped_compactions[0].limit_num_files, 12);
    assert_eq!(skipped_compactions[0].estimated_bytes, 110);
    assert_eq!(skipped_compactions[0].limit_bytes, 120);
    //
    let skipped_partition_records = repos
        .partitions()
        .get_in_skipped_compactions(&[to_skip_partition.id])
        .await
        .unwrap();
    assert_eq!(
        skipped_partition_records[0].partition_id,
        to_skip_partition.id
    );
    assert_eq!(skipped_partition_records[0].reason, "I'm on fire");

    // Can receive multiple skipped compactions for different partitions
    repos
        .partitions()
        .record_skipped_compaction(
            to_skip_partition_too.id,
            "I am le tired too",
            1,
            2,
            4,
            10,
            20,
        )
        .await
        .unwrap();
    let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
    assert_eq!(skipped_compactions.len(), 2);
    assert_eq!(skipped_compactions[0].partition_id, to_skip_partition.id);
    assert_eq!(
        skipped_compactions[1].partition_id,
        to_skip_partition_too.id
    );
    // confirm can fetch subset of skipped compactions (a.k.a. have two, only fetch 1)
    let skipped_partition_records = repos
        .partitions()
        .get_in_skipped_compactions(&[to_skip_partition.id])
        .await
        .unwrap();
    assert_eq!(skipped_partition_records.len(), 1);
    assert_eq!(skipped_compactions[0].partition_id, to_skip_partition.id);
    let skipped_partition_records = repos
        .partitions()
        .get_in_skipped_compactions(&[to_skip_partition_too.id])
        .await
        .unwrap();
    assert_eq!(skipped_partition_records.len(), 1);
    assert_eq!(
        skipped_partition_records[0].partition_id,
        to_skip_partition_too.id
    );
    // confirm can fetch both skipped compactions, and not the unskipped one
    // also confirm will not error on non-existing partition
    let non_existing_partition_id = PartitionId::new(9999);
    let skipped_partition_records = repos
        .partitions()
        .get_in_skipped_compactions(&[
            partition.id,
            to_skip_partition.id,
            to_skip_partition_too.id,
            non_existing_partition_id,
        ])
        .await
        .unwrap();
    assert_eq!(skipped_partition_records.len(), 2);
    assert_eq!(
        skipped_partition_records[0].partition_id,
        to_skip_partition.id
    );
    assert_eq!(
        skipped_partition_records[1].partition_id,
        to_skip_partition_too.id
    );

    // Delete the skipped compactions
    let deleted_skipped_compaction = repos
        .partitions()
        .delete_skipped_compactions(to_skip_partition.id)
        .await
        .unwrap()
        .expect("The skipped compaction should have been returned");
    assert_eq!(
        deleted_skipped_compaction.partition_id,
        to_skip_partition.id
    );
    assert_eq!(deleted_skipped_compaction.reason, "I'm on fire");
    assert_eq!(deleted_skipped_compaction.num_files, 11);
    assert_eq!(deleted_skipped_compaction.limit_num_files, 12);
    assert_eq!(deleted_skipped_compaction.estimated_bytes, 110);
    assert_eq!(deleted_skipped_compaction.limit_bytes, 120);
    //
    let deleted_skipped_compaction = repos
        .partitions()
        .delete_skipped_compactions(to_skip_partition_too.id)
        .await
        .unwrap()
        .expect("The skipped compaction should have been returned");
    assert_eq!(
        deleted_skipped_compaction.partition_id,
        to_skip_partition_too.id
    );
    assert_eq!(deleted_skipped_compaction.reason, "I am le tired too");
    //
    let skipped_partition_records = repos
        .partitions()
        .get_in_skipped_compactions(&[to_skip_partition.id])
        .await
        .unwrap();
    assert!(skipped_partition_records.is_empty());

    let not_deleted_skipped_compaction = repos
        .partitions()
        .delete_skipped_compactions(to_skip_partition.id)
        .await
        .unwrap();

    assert!(
        not_deleted_skipped_compaction.is_none(),
        "There should be no skipped compation",
    );

    let skipped_compactions = repos.partitions().list_skipped_compactions().await.unwrap();
    assert!(
        skipped_compactions.is_empty(),
        "Expected no skipped compactions, got: {skipped_compactions:?}"
    );

    let recent = repos
        .partitions()
        .most_recent_n(10)
        .await
        .expect("should list most recent");
    assert_eq!(recent.len(), 4);

    // Test: sort_key_ids from most_recent_n
    // Only the first two partitions (represent to_skip_partition_too and to_skip_partition) have vallues, the others are empty
    assert_eq!(
        recent[0].sort_key_ids().unwrap(),
        &SortKeyIds::from(vec![3, 4])
    );
    assert_eq!(
        recent[1].sort_key_ids().unwrap(),
        &SortKeyIds::from(vec![2, 1, 4, 3])
    );
    assert!(recent[2].sort_key_ids().is_none());
    assert!(recent[3].sort_key_ids().is_none());

    let recent = repos
        .partitions()
        .most_recent_n(4)
        .await
        .expect("should list most recent");
    assert_eq!(recent.len(), 4); // no off by one error

    let recent = repos
        .partitions()
        .most_recent_n(2)
        .await
        .expect("should list most recent");
    assert_eq!(recent.len(), 2);

    repos
        .namespaces()
        .soft_delete("namespace_partition_test")
        .await
        .expect("delete namespace should succeed");
}

async fn validate_partition_snapshot(repos: &mut dyn RepoCollection, snapshot: &PartitionSnapshot) {
    // compare files
    let mut expected = repos
        .parquet_files()
        .list_by_partition_not_to_delete_batch(vec![snapshot.partition_id()])
        .await
        .unwrap();
    expected.sort_unstable_by_key(|x| x.id);
    let mut actual = snapshot.files().collect::<Result<Vec<_>, _>>().unwrap();
    actual.sort_unstable_by_key(|x| x.id);
    assert_eq!(expected, actual);

    // compare skipped partition
    let expected = repos
        .partitions()
        .get_in_skipped_compactions(&[snapshot.partition_id()])
        .await
        .unwrap()
        .into_iter()
        .next();
    let actual = snapshot.skipped_compaction();
    assert_eq!(actual, expected);

    // compare partition itself
    let actual = snapshot.partition().unwrap();
    let expected = repos
        .partitions()
        .get_by_id(snapshot.partition_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual, expected);
}

async fn validate_table_snapshot(repos: &mut dyn RepoCollection, snapshot: &TableSnapshot) {
    let table = snapshot.table().unwrap();

    let expected = repos.tables().get_by_id(table.id).await.unwrap().unwrap();
    assert_eq!(table, expected);

    // compare columns
    let mut expected = repos.columns().list_by_table_id(table.id).await.unwrap();
    expected.sort_unstable_by_key(|x| x.id);
    let mut actual = snapshot.columns().collect::<Result<Vec<_>, _>>().unwrap();
    actual.sort_unstable_by_key(|x| x.id);
    assert_eq!(expected, actual);

    // compare partitions
    let mut expected = repos.partitions().list_by_table_id(table.id).await.unwrap();
    expected.sort_unstable_by_key(|x| x.id);
    let mut actual = snapshot
        .partitions()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    actual.sort_unstable_by_key(|x| x.id());
    assert_eq!(expected.len(), actual.len());

    let eq = expected
        .iter()
        .zip(&actual)
        .all(|(l, r)| l.id == r.id() && l.partition_key.as_bytes() == r.key());
    assert!(eq, "expected {expected:?} got {actual:?}");
}

/// List all parquet files in given namespace.
async fn list_parquet_files_by_namespace_not_to_delete(
    catalog: Arc<dyn Catalog>,
    namespace_id: NamespaceId,
) -> Vec<ParquetFile> {
    let partitions = futures::stream::iter(
        catalog
            .repositories()
            .tables()
            .list_by_namespace_id(namespace_id)
            .await
            .unwrap(),
    )
    .then(|t| {
        let catalog = Arc::clone(&catalog);
        async move {
            futures::stream::iter(
                catalog
                    .repositories()
                    .partitions()
                    .list_by_table_id(t.id)
                    .await
                    .unwrap(),
            )
        }
    })
    .flatten()
    .map(|p| p.id)
    .collect::<Vec<_>>()
    .await;

    catalog
        .repositories()
        .parquet_files()
        .list_by_partition_not_to_delete_batch(partitions)
        .await
        .unwrap()
}

/// tests many interactions with the catalog and parquet files. See the individual conditions
/// herein
async fn test_parquet_file(catalog: Arc<dyn Catalog>) {
    let mut repos = catalog.repositories();
    let namespace = arbitrary_namespace(&mut *repos, "namespace_parquet_file_test").await;
    let table = arbitrary_table(&mut *repos, "test_table", &namespace).await;
    let other_table = arbitrary_table(&mut *repos, "other", &namespace).await;
    let partition = repos
        .partitions()
        .create_or_get("one".into(), table.id)
        .await
        .unwrap();
    let other_partition = repos
        .partitions()
        .create_or_get("one".into(), other_table.id)
        .await
        .unwrap();

    let ts1 = repos.tables().snapshot(table.id).await.unwrap();
    validate_table_snapshot(repos.as_mut(), &ts1).await;

    let ts2 = repos.tables().snapshot(other_table.id).await.unwrap();
    validate_table_snapshot(repos.as_mut(), &ts2).await;

    let parquet_file_params = arbitrary_parquet_file_params(&namespace, &table, &partition);
    let parquet_file = repos
        .parquet_files()
        .create(parquet_file_params.clone())
        .await
        .unwrap();

    // verify we can get it by its object store id
    let pfg = repos
        .parquet_files()
        .get_by_object_store_id(parquet_file.object_store_id)
        .await
        .unwrap();
    assert_eq!(parquet_file, pfg.unwrap());

    // verify that trying to create a file with the same UUID throws an error
    let err = repos
        .parquet_files()
        .create(parquet_file_params.clone())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists { .. }));

    let other_params = ParquetFileParams {
        table_id: other_partition.table_id,
        partition_id: other_partition.id,
        partition_hash_id: other_partition.hash_id().cloned(),
        object_store_id: ObjectStoreId::new(),
        min_time: Timestamp::new(50),
        max_time: Timestamp::new(60),
        ..parquet_file_params.clone()
    };
    let other_file = repos.parquet_files().create(other_params).await.unwrap();

    let exist_id = parquet_file.id;
    let non_exist_id = ParquetFileId::new(other_file.id.get() + 10);
    // make sure exists_id != non_exist_id
    assert_ne!(exist_id, non_exist_id);

    // verify that to_delete is initially set to null and the file does not get deleted
    assert!(parquet_file.to_delete.is_none());
    let older_than = Timestamp::new(
        (catalog.time_provider().now() + Duration::from_secs(100)).timestamp_nanos(),
    );
    let deleted = repos
        .parquet_files()
        .delete_old_ids_only(older_than)
        .await
        .unwrap();
    assert!(deleted.is_empty());

    // test list_all that includes soft-deleted file
    // at this time the file is not soft-deleted yet and will be included in the returned list
    let files =
        list_parquet_files_by_namespace_not_to_delete(Arc::clone(&catalog), namespace.id).await;
    assert_eq!(files.len(), 2);

    // verify to_delete can be updated to a timestamp
    repos
        .parquet_files()
        .create_upgrade_delete(
            parquet_file.partition_id,
            &[parquet_file.object_store_id],
            &[],
            &[],
            CompactionLevel::Initial,
        )
        .await
        .unwrap();

    // test list_all that includes soft-deleted file
    // at this time the file is soft-deleted and will be NOT included in the returned list
    let files =
        list_parquet_files_by_namespace_not_to_delete(Arc::clone(&catalog), namespace.id).await;
    assert_eq!(files.len(), 1);

    // the deleted file can still be retrieved by UUID though
    repos
        .parquet_files()
        .get_by_object_store_id(parquet_file.object_store_id)
        .await
        .unwrap()
        .unwrap();

    // File is not deleted if it was marked to be deleted after the specified time
    let before_deleted = Timestamp::new(
        (catalog.time_provider().now() - Duration::from_secs(100)).timestamp_nanos(),
    );
    let deleted = repos
        .parquet_files()
        .delete_old_ids_only(before_deleted)
        .await
        .unwrap();
    assert!(deleted.is_empty());

    // not hard-deleted yet
    repos
        .parquet_files()
        .get_by_object_store_id(parquet_file.object_store_id)
        .await
        .unwrap()
        .unwrap();

    // File is deleted if it was marked to be deleted before the specified time
    let deleted = repos
        .parquet_files()
        .delete_old_ids_only(older_than)
        .await
        .unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(parquet_file.object_store_id, deleted[0]);

    // test list_all that includes soft-deleted file
    // at this time the file is hard deleted -> the returned list is empty
    assert!(repos
        .parquet_files()
        .get_by_object_store_id(parquet_file.object_store_id)
        .await
        .unwrap()
        .is_none());

    // test list
    let files =
        list_parquet_files_by_namespace_not_to_delete(Arc::clone(&catalog), namespace.id).await;
    assert_eq!(vec![other_file.clone()], files);

    // test list_by_namespace_not_to_delete
    let namespace2 = arbitrary_namespace(&mut *repos, "namespace_parquet_file_test1").await;
    let table2 = arbitrary_table(&mut *repos, "test_table2", &namespace2).await;
    let partition2 = repos
        .partitions()
        .create_or_get("foo".into(), table2.id)
        .await
        .unwrap();
    let files =
        list_parquet_files_by_namespace_not_to_delete(Arc::clone(&catalog), namespace2.id).await;
    assert!(files.is_empty());

    let ts3 = repos.tables().snapshot(table2.id).await.unwrap();
    validate_table_snapshot(repos.as_mut(), &ts3).await;

    let f1_params = ParquetFileParams {
        table_id: partition2.table_id,
        partition_id: partition2.id,
        partition_hash_id: partition2.hash_id().cloned(),
        namespace_id: namespace2.id,
        object_store_id: ObjectStoreId::new(),
        min_time: Timestamp::new(1),
        max_time: Timestamp::new(10),
        ..parquet_file_params
    };
    let f1 = repos
        .parquet_files()
        .create(f1_params.clone())
        .await
        .unwrap();

    let f2_params = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        min_time: Timestamp::new(50),
        max_time: Timestamp::new(60),
        ..f1_params.clone()
    };
    let f2 = repos
        .parquet_files()
        .create(f2_params.clone())
        .await
        .unwrap();
    let files =
        list_parquet_files_by_namespace_not_to_delete(Arc::clone(&catalog), namespace2.id).await;
    assert_eq!(vec![f1.clone(), f2.clone()], files);

    let f3_params = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        min_time: Timestamp::new(50),
        max_time: Timestamp::new(60),
        ..f2_params
    };
    let f3 = repos
        .parquet_files()
        .create(f3_params.clone())
        .await
        .unwrap();
    let files =
        list_parquet_files_by_namespace_not_to_delete(Arc::clone(&catalog), namespace2.id).await;
    assert_eq!(vec![f1.clone(), f2.clone(), f3.clone()], files);

    let s1 = repos.partitions().snapshot(partition2.id).await.unwrap();
    validate_partition_snapshot(repos.as_mut(), &s1).await;

    repos
        .parquet_files()
        .create_upgrade_delete(
            f2.partition_id,
            &[f2.object_store_id],
            &[],
            &[],
            CompactionLevel::Initial,
        )
        .await
        .unwrap();
    let files =
        list_parquet_files_by_namespace_not_to_delete(Arc::clone(&catalog), namespace2.id).await;
    assert_eq!(vec![f1.clone(), f3.clone()], files);

    // Cannot delete file twice
    let err = repos
        .parquet_files()
        .create_upgrade_delete(
            partition2.id,
            &[f2.object_store_id, f3.object_store_id],
            &[],
            &[],
            CompactionLevel::Initial,
        )
        .await
        .unwrap_err();
    assert_matches!(err, Error::NotFound { .. });

    let err = repos
        .parquet_files()
        .create_upgrade_delete(
            partition2.id,
            &[f2.object_store_id],
            &[f3.object_store_id],
            &[],
            CompactionLevel::Initial,
        )
        .await
        .unwrap_err();
    assert_matches!(err, Error::NotFound { .. });

    // Cannot upgrade deleted file
    let err = repos
        .parquet_files()
        .create_upgrade_delete(
            partition2.id,
            &[f3.object_store_id],
            &[f2.object_store_id],
            &[],
            CompactionLevel::Initial,
        )
        .await
        .unwrap_err();
    assert_matches!(err, Error::NotFound { .. });

    // Failed transactions don't modify
    let files =
        list_parquet_files_by_namespace_not_to_delete(Arc::clone(&catalog), namespace2.id).await;
    assert_eq!(vec![f1.clone(), f3.clone()], files);

    let s2 = repos.partitions().snapshot(partition2.id).await.unwrap();
    assert_gt(s2.generation(), s1.generation());
    validate_partition_snapshot(repos.as_mut(), &s2).await;

    let files = list_parquet_files_by_namespace_not_to_delete(
        Arc::clone(&catalog),
        NamespaceId::new(i64::MAX),
    )
    .await;
    assert!(files.is_empty());

    // test delete_old_ids_only
    let older_than = Timestamp::new(
        (catalog.time_provider().now() + Duration::from_secs(100)).timestamp_nanos(),
    );
    let ids = repos
        .parquet_files()
        .delete_old_ids_only(older_than)
        .await
        .unwrap();
    assert_eq!(ids.len(), 1);

    let s3 = repos.partitions().snapshot(partition2.id).await.unwrap();
    assert_ge(s3.generation(), s2.generation()); // no new snapshot required, but some backends will generate a new one
    validate_partition_snapshot(repos.as_mut(), &s3).await;

    // test retention-based flagging for deletion
    // Since mem catalog has default retention 1 hour, let us first set it to 0 means infinite
    let namespaces = repos
        .namespaces()
        .list(SoftDeletedRows::AllRows)
        .await
        .expect("listing namespaces");
    for namespace in namespaces {
        repos
            .namespaces()
            .update_retention_period(&namespace.name, None) // infinite
            .await
            .unwrap();
    }

    // 1. with no retention period set on the ns, nothing should get flagged
    let ids = repos
        .parquet_files()
        .flag_for_delete_by_retention()
        .await
        .unwrap();
    assert!(ids.is_empty());
    // 2. set ns retention period to one hour then create some files before and after and
    //    ensure correct files get deleted
    repos
        .namespaces()
        .update_retention_period(&namespace2.name, Some(60 * 60 * 1_000_000_000)) // 1 hour
        .await
        .unwrap();
    let f4_params = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        max_time: Timestamp::new(
            // a bit over an hour ago
            (catalog.time_provider().now() - Duration::from_secs(60 * 65)).timestamp_nanos(),
        ),
        ..f3_params
    };
    let f4 = repos
        .parquet_files()
        .create(f4_params.clone())
        .await
        .unwrap();
    let f5_params = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        max_time: Timestamp::new(
            // a bit under an hour ago
            (catalog.time_provider().now() - Duration::from_secs(60 * 55)).timestamp_nanos(),
        ),
        ..f4_params
    };
    let f5 = repos
        .parquet_files()
        .create(f5_params.clone())
        .await
        .unwrap();
    let ids = repos
        .parquet_files()
        .flag_for_delete_by_retention()
        .await
        .unwrap();
    assert!(ids.len() > 1); // it's also going to flag f1, f2 & f3 because they have low max
                            // timestamps but i don't want this test to be brittle if those
                            // values change so i'm not asserting len == 4
    let f4 = repos
        .parquet_files()
        .get_by_object_store_id(f4.object_store_id)
        .await
        .unwrap()
        .unwrap();
    assert_matches!(f4.to_delete, Some(_)); // f4 is > 1hr old
    let f5 = repos
        .parquet_files()
        .get_by_object_store_id(f5.object_store_id)
        .await
        .unwrap()
        .unwrap();
    assert_matches!(f5.to_delete, None); // f5 is < 1hr old

    let s4 = repos.partitions().snapshot(partition2.id).await.unwrap();
    assert_gt(s4.generation(), s3.generation());
    validate_partition_snapshot(repos.as_mut(), &s4).await;

    // call flag_for_delete_by_retention() again and nothing should be flagged because they've
    // already been flagged
    let ids = repos
        .parquet_files()
        .flag_for_delete_by_retention()
        .await
        .unwrap();
    assert!(ids.is_empty());

    // test that flag_for_delete_by_retention respects UPDATE LIMIT
    // create limit + the meaning of life parquet files that are all older than the retention (>1hr)
    const LIMIT: usize = 1000;
    const MOL: usize = 42;
    let now = catalog.time_provider().now();
    let params = (0..LIMIT + MOL)
        .map(|_| {
            ParquetFileParams {
                object_store_id: ObjectStoreId::new(),
                max_time: Timestamp::new(
                    // a bit over an hour ago
                    (now - Duration::from_secs(60 * 65)).timestamp_nanos(),
                ),
                ..f1_params.clone()
            }
        })
        .collect::<Vec<_>>();
    repos
        .parquet_files()
        .create_upgrade_delete(
            f1_params.partition_id,
            &[],
            &[],
            &params,
            CompactionLevel::Initial,
        )
        .await
        .unwrap();
    let ids = repos
        .parquet_files()
        .flag_for_delete_by_retention()
        .await
        .unwrap();
    assert_eq!(ids.len(), LIMIT);
    let ids = repos
        .parquet_files()
        .flag_for_delete_by_retention()
        .await
        .unwrap();
    assert_eq!(ids.len(), MOL); // second call took remainder
    let ids = repos
        .parquet_files()
        .flag_for_delete_by_retention()
        .await
        .unwrap();
    assert_eq!(ids.len(), 0); // none left

    // test create_update_delete
    let f6_params = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        ..f5_params
    };
    let f6 = repos
        .parquet_files()
        .create(f6_params.clone())
        .await
        .unwrap();

    let f7_params = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        ..f6_params
    };
    let f1_uuid = f1.object_store_id;
    let f6_uuid = f6.object_store_id;
    let f5_uuid = f5.object_store_id;
    let cud = repos
        .parquet_files()
        .create_upgrade_delete(
            f5.partition_id,
            &[f5.object_store_id],
            &[f6.object_store_id],
            &[f7_params.clone()],
            CompactionLevel::Final,
        )
        .await
        .unwrap();

    assert_eq!(cud.len(), 1);
    let f5_delete = repos
        .parquet_files()
        .get_by_object_store_id(f5_uuid)
        .await
        .unwrap()
        .unwrap();
    assert_matches!(f5_delete.to_delete, Some(_));

    let f6_compaction_level = repos
        .parquet_files()
        .get_by_object_store_id(f6_uuid)
        .await
        .unwrap()
        .unwrap();

    assert_matches!(f6_compaction_level.compaction_level, CompactionLevel::Final);

    let f7 = repos
        .parquet_files()
        .get_by_object_store_id(f7_params.object_store_id)
        .await
        .unwrap()
        .unwrap();

    let f7_uuid = f7.object_store_id;

    // test create_update_delete transaction (rollback because f7 already exists)
    let cud = repos
        .parquet_files()
        .create_upgrade_delete(
            partition2.id,
            &[],
            &[],
            &[f7_params.clone()],
            CompactionLevel::Final,
        )
        .await;

    assert_matches!(
        cud,
        Err(Error::AlreadyExists {
            descr
        }) if descr == f7_params.object_store_id.to_string()
    );

    let f1_to_delete = repos
        .parquet_files()
        .get_by_object_store_id(f1_uuid)
        .await
        .unwrap()
        .unwrap();
    assert_matches!(f1_to_delete.to_delete, Some(_));

    let f7_not_delete = repos
        .parquet_files()
        .get_by_object_store_id(f7_uuid)
        .await
        .unwrap()
        .unwrap();
    assert_matches!(f7_not_delete.to_delete, None);

    // test exists_by_object_store_id_batch returns parquet files by object store id
    let does_not_exist = ObjectStoreId::new();
    let mut present = repos
        .parquet_files()
        .exists_by_object_store_id_batch(vec![f1_uuid, f7_uuid, does_not_exist])
        .await
        .unwrap();
    let mut expected = vec![f1_uuid, f7_uuid];
    present.sort();
    expected.sort();
    assert_eq!(present, expected);

    let s5 = repos.partitions().snapshot(partition2.id).await.unwrap();
    assert_gt(s5.generation(), s4.generation());
    validate_partition_snapshot(repos.as_mut(), &s5).await;

    // Cannot mix partition IDs
    let partition3 = repos
        .partitions()
        .create_or_get("three".into(), table.id)
        .await
        .unwrap();

    let ts4 = repos.tables().snapshot(table.id).await.unwrap();
    validate_table_snapshot(repos.as_mut(), &ts4).await;
    assert_gt(ts4.generation(), ts1.generation());

    let f8_params = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        partition_id: partition3.id,
        ..f7_params
    };
    let err = repos
        .parquet_files()
        .create_upgrade_delete(
            partition2.id,
            &[f7_uuid],
            &[],
            &[f8_params.clone()],
            CompactionLevel::Final,
        )
        .await
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("Inconsistent ParquetFileParams, expected PartitionId"),
        "{err}"
    );

    let list = repos
        .parquet_files()
        .list_by_partition_not_to_delete_batch(vec![partition2.id])
        .await
        .unwrap();
    assert_eq!(list.len(), 2);

    repos
        .parquet_files()
        .create_upgrade_delete(
            partition3.id,
            &[],
            &[],
            &[f8_params.clone()],
            CompactionLevel::Final,
        )
        .await
        .unwrap();

    let files = repos
        .parquet_files()
        .list_by_partition_not_to_delete_batch(vec![partition3.id])
        .await
        .unwrap();
    assert_eq!(files.len(), 1);
    let f8_uuid = files[0].object_store_id;

    let files = repos
        .parquet_files()
        .list_by_partition_not_to_delete_batch(vec![])
        .await
        .unwrap();
    assert_eq!(files.len(), 0);
    let files = repos
        .parquet_files()
        .list_by_partition_not_to_delete_batch(vec![partition2.id, partition3.id])
        .await
        .unwrap();
    assert_eq!(files.len(), 3);
    let files = repos
        .parquet_files()
        .list_by_partition_not_to_delete_batch(vec![
            partition2.id,
            PartitionId::new(i64::MAX),
            partition3.id,
            partition2.id,
        ])
        .await
        .unwrap();
    assert_eq!(files.len(), 3);

    let err = repos
        .parquet_files()
        .create_upgrade_delete(partition2.id, &[f8_uuid], &[], &[], CompactionLevel::Final)
        .await
        .unwrap_err();

    assert_matches!(err, Error::NotFound { .. });

    let err = repos
        .parquet_files()
        .create_upgrade_delete(partition2.id, &[], &[f8_uuid], &[], CompactionLevel::Final)
        .await
        .unwrap_err();

    assert_matches!(err, Error::NotFound { .. });

    repos
        .parquet_files()
        .create_upgrade_delete(partition3.id, &[f8_uuid], &[], &[], CompactionLevel::Final)
        .await
        .unwrap();

    // take snapshot of unknown partition
    let err = repos
        .partitions()
        .snapshot(PartitionId::new(i64::MAX))
        .await
        .unwrap_err();
    assert_matches!(err, Error::NotFound { .. });
}

async fn test_parquet_file_delete_broken(catalog: Arc<dyn Catalog>) {
    let mut repos = catalog.repositories();
    let namespace_1 = arbitrary_namespace(&mut *repos, "retention_broken_1").await;
    let namespace_2 = repos
        .namespaces()
        .create(
            &NamespaceName::new("retention_broken_2").unwrap(),
            None,
            Some(1),
            None,
        )
        .await
        .unwrap();
    let table_1 = arbitrary_table(&mut *repos, "test_table", &namespace_1).await;
    let table_2 = arbitrary_table(&mut *repos, "test_table", &namespace_2).await;
    let partition_1 = repos
        .partitions()
        .create_or_get("one".into(), table_1.id)
        .await
        .unwrap();
    let partition_2 = repos
        .partitions()
        .create_or_get("one".into(), table_2.id)
        .await
        .unwrap();

    let parquet_file_params_1 = arbitrary_parquet_file_params(&namespace_1, &table_1, &partition_1);
    let parquet_file_params_2 = arbitrary_parquet_file_params(&namespace_2, &table_2, &partition_2);
    let _parquet_file_1 = repos
        .parquet_files()
        .create(parquet_file_params_1)
        .await
        .unwrap();
    let parquet_file_2 = repos
        .parquet_files()
        .create(parquet_file_params_2)
        .await
        .unwrap();

    let ids = repos
        .parquet_files()
        .flag_for_delete_by_retention()
        .await
        .unwrap();
    assert_eq!(
        ids,
        vec![(parquet_file_2.partition_id, parquet_file_2.object_store_id)]
    );
}

async fn test_partitions_new_file_between(catalog: Arc<dyn Catalog>) {
    let mut repos = catalog.repositories();
    let namespace = arbitrary_namespace(&mut *repos, "test_partitions_new_file_between").await;
    let table = arbitrary_table(&mut *repos, "test_table_for_new_file_between", &namespace).await;

    // param for the tests
    let time_now = Timestamp::from(catalog.time_provider().now());
    let time_one_hour_ago = Timestamp::from(catalog.time_provider().hours_ago(1));
    let time_two_hour_ago = Timestamp::from(catalog.time_provider().hours_ago(2));
    let time_three_hour_ago = Timestamp::from(catalog.time_provider().hours_ago(3));
    let time_five_hour_ago = Timestamp::from(catalog.time_provider().hours_ago(5));
    let time_six_hour_ago = Timestamp::from(catalog.time_provider().hours_ago(6));

    // Db has no partitions
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_two_hour_ago, None)
        .await
        .unwrap();
    assert!(partitions.is_empty());

    // -----------------
    // PARTITION one
    // The DB has 1 partition but it does not have any file
    let partition1 = repos
        .partitions()
        .create_or_get("one".into(), table.id)
        .await
        .unwrap();
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_two_hour_ago, None)
        .await
        .unwrap();
    assert!(partitions.is_empty());

    // create files for partition one
    let parquet_file_params = arbitrary_parquet_file_params(&namespace, &table, &partition1);

    // create a deleted L0 file that was created 3 hours ago
    let delete_l0_file = repos
        .parquet_files()
        .create(parquet_file_params.clone())
        .await
        .unwrap();
    repos
        .parquet_files()
        .create_upgrade_delete(
            delete_l0_file.partition_id,
            &[delete_l0_file.object_store_id],
            &[],
            &[],
            CompactionLevel::Initial,
        )
        .await
        .unwrap();
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_two_hour_ago, None)
        .await
        .unwrap();
    assert!(partitions.is_empty());
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_two_hour_ago, Some(time_one_hour_ago))
        .await
        .unwrap();
    assert!(partitions.is_empty());
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_three_hour_ago, Some(time_one_hour_ago))
        .await
        .unwrap();
    assert!(partitions.is_empty());

    // create a deleted L0 file that was created 1 hour ago
    let l0_one_hour_ago_file_params = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        created_at: time_one_hour_ago,
        ..parquet_file_params.clone()
    };
    repos
        .parquet_files()
        .create(l0_one_hour_ago_file_params.clone())
        .await
        .unwrap();
    // partition one should be returned
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_two_hour_ago, None)
        .await
        .unwrap();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0], partition1.id);
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_two_hour_ago, Some(time_now))
        .await
        .unwrap();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0], partition1.id);
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_three_hour_ago, Some(time_now))
        .await
        .unwrap();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0], partition1.id);
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_three_hour_ago, Some(time_two_hour_ago))
        .await
        .unwrap();
    assert!(partitions.is_empty());

    // -----------------
    // PARTITION two
    // Partition two without any file
    let partition2 = repos
        .partitions()
        .create_or_get("two".into(), table.id)
        .await
        .unwrap();
    // should return partition one only
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_two_hour_ago, None)
        .await
        .unwrap();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0], partition1.id);
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_three_hour_ago, Some(time_now))
        .await
        .unwrap();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0], partition1.id);

    // Add a L0 file created 5 hours ago
    let l0_five_hour_ago_file_params = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        created_at: time_five_hour_ago,
        partition_id: partition2.id,
        partition_hash_id: partition2.hash_id().cloned(),
        ..parquet_file_params.clone()
    };
    repos
        .parquet_files()
        .create(l0_five_hour_ago_file_params.clone())
        .await
        .unwrap();
    // still return partition one only
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_two_hour_ago, None)
        .await
        .unwrap();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0], partition1.id);
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_three_hour_ago, Some(time_now))
        .await
        .unwrap();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0], partition1.id);
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_three_hour_ago, Some(time_now))
        .await
        .unwrap();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0], partition1.id);
    // Between six and three hours ago, return only partition 2
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_six_hour_ago, Some(time_three_hour_ago))
        .await
        .unwrap();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0], partition2.id);

    // Add an L1 file created just now
    let l1_file_params = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        created_at: time_now,
        partition_id: partition2.id,
        partition_hash_id: partition2.hash_id().cloned(),
        compaction_level: CompactionLevel::FileNonOverlapped,
        ..parquet_file_params.clone()
    };
    repos
        .parquet_files()
        .create(l1_file_params.clone())
        .await
        .unwrap();
    // should return both partitions
    let mut partitions = repos
        .partitions()
        .partitions_new_file_between(time_two_hour_ago, None)
        .await
        .unwrap();
    assert_eq!(partitions.len(), 2);
    partitions.sort();
    assert_eq!(partitions[0], partition1.id);
    assert_eq!(partitions[1], partition2.id);
    // Only return partition1: the creation time must be strictly less than the maximum time,
    // not equal
    let mut partitions = repos
        .partitions()
        .partitions_new_file_between(time_three_hour_ago, Some(time_now))
        .await
        .unwrap();
    assert_eq!(partitions.len(), 1);
    partitions.sort();
    assert_eq!(partitions[0], partition1.id);
    // Between six and three hours ago, return none
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_six_hour_ago, Some(time_three_hour_ago))
        .await
        .unwrap();
    assert!(partitions.is_empty());

    // -----------------
    // PARTITION three
    // Partition three without any file
    let partition3 = repos
        .partitions()
        .create_or_get("three".into(), table.id)
        .await
        .unwrap();
    // should return partition one and two only
    let mut partitions = repos
        .partitions()
        .partitions_new_file_between(time_two_hour_ago, None)
        .await
        .unwrap();
    assert_eq!(partitions.len(), 2);
    partitions.sort();
    assert_eq!(partitions[0], partition1.id);
    assert_eq!(partitions[1], partition2.id);
    // Only return partition1: the creation time must be strictly less than the maximum time,
    // not equal
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_three_hour_ago, Some(time_now))
        .await
        .unwrap();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0], partition1.id);
    // When the maximum time is greater than the creation time of partition2, return it
    let mut partitions = repos
        .partitions()
        .partitions_new_file_between(time_three_hour_ago, Some(time_now + 1))
        .await
        .unwrap();
    assert_eq!(partitions.len(), 2);
    partitions.sort();
    assert_eq!(partitions[0], partition1.id);
    assert_eq!(partitions[1], partition2.id);
    // Between six and three hours ago, return none
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_six_hour_ago, Some(time_three_hour_ago))
        .await
        .unwrap();
    assert!(partitions.is_empty());

    // Add an L2 file created just now for partition three
    let l2_file_params = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        created_at: time_now,
        partition_id: partition3.id,
        partition_hash_id: partition3.hash_id().cloned(),
        compaction_level: CompactionLevel::Final,
        ..parquet_file_params.clone()
    };
    repos
        .parquet_files()
        .create(l2_file_params.clone())
        .await
        .unwrap();
    // now should return partition one two and three
    let mut partitions = repos
        .partitions()
        .partitions_new_file_between(time_two_hour_ago, None)
        .await
        .unwrap();
    assert_eq!(partitions.len(), 3);
    partitions.sort();
    assert_eq!(partitions[0], partition1.id);
    assert_eq!(partitions[1], partition2.id);
    assert_eq!(partitions[2], partition3.id);
    // Only return partition1: the creation time must be strictly less than the maximum time,
    // not equal
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_three_hour_ago, Some(time_now))
        .await
        .unwrap();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0], partition1.id);
    // Between six and three hours ago, return none
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_six_hour_ago, Some(time_three_hour_ago))
        .await
        .unwrap();
    assert!(partitions.is_empty());

    // add an L0 file created one hour ago for partition three
    let l0_one_hour_ago_file_params = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        created_at: time_one_hour_ago,
        partition_id: partition3.id,
        partition_hash_id: partition3.hash_id().cloned(),
        ..parquet_file_params.clone()
    };
    repos
        .parquet_files()
        .create(l0_one_hour_ago_file_params.clone())
        .await
        .unwrap();
    // should return all partitions
    let mut partitions = repos
        .partitions()
        .partitions_new_file_between(time_two_hour_ago, None)
        .await
        .unwrap();
    assert_eq!(partitions.len(), 3);
    partitions.sort();
    assert_eq!(partitions[0], partition1.id);
    assert_eq!(partitions[1], partition2.id);
    assert_eq!(partitions[2], partition3.id);
    // Only return partitions 1 and 3; 2 was created just now
    let mut partitions = repos
        .partitions()
        .partitions_new_file_between(time_three_hour_ago, Some(time_now))
        .await
        .unwrap();
    assert_eq!(partitions.len(), 2);
    partitions.sort();
    assert_eq!(partitions[0], partition1.id);
    assert_eq!(partitions[1], partition3.id);
    // Between six and three hours ago, return none
    let partitions = repos
        .partitions()
        .partitions_new_file_between(time_six_hour_ago, Some(time_three_hour_ago))
        .await
        .unwrap();
    assert!(partitions.is_empty());
}

async fn test_list_by_partiton_not_to_delete(catalog: Arc<dyn Catalog>) {
    let mut repos = catalog.repositories();
    let namespace = arbitrary_namespace(
        &mut *repos,
        "namespace_parquet_file_test_list_by_partiton_not_to_delete",
    )
    .await;
    let table = arbitrary_table(&mut *repos, "test_table", &namespace).await;

    let partition = repos
        .partitions()
        .create_or_get("test_list_by_partiton_not_to_delete_one".into(), table.id)
        .await
        .unwrap();
    let partition2 = repos
        .partitions()
        .create_or_get("test_list_by_partiton_not_to_delete_two".into(), table.id)
        .await
        .unwrap();

    let parquet_file_params = arbitrary_parquet_file_params(&namespace, &table, &partition);

    let parquet_file = repos
        .parquet_files()
        .create(parquet_file_params.clone())
        .await
        .unwrap();
    let delete_file_params = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        ..parquet_file_params.clone()
    };
    let delete_file = repos
        .parquet_files()
        .create(delete_file_params)
        .await
        .unwrap();
    repos
        .parquet_files()
        .create_upgrade_delete(
            partition.id,
            &[delete_file.object_store_id],
            &[],
            &[],
            CompactionLevel::Initial,
        )
        .await
        .unwrap();
    let level1_file_params = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        ..parquet_file_params.clone()
    };
    let mut level1_file = repos
        .parquet_files()
        .create(level1_file_params)
        .await
        .unwrap();
    repos
        .parquet_files()
        .create_upgrade_delete(
            partition.id,
            &[],
            &[level1_file.object_store_id],
            &[],
            CompactionLevel::FileNonOverlapped,
        )
        .await
        .unwrap();
    level1_file.compaction_level = CompactionLevel::FileNonOverlapped;

    let other_partition_params = ParquetFileParams {
        partition_id: partition2.id,
        partition_hash_id: partition2.hash_id().cloned(),
        object_store_id: ObjectStoreId::new(),
        ..parquet_file_params.clone()
    };
    let _partition2_file = repos
        .parquet_files()
        .create(other_partition_params)
        .await
        .unwrap();

    let files = repos
        .parquet_files()
        .list_by_partition_not_to_delete_batch(vec![partition.id])
        .await
        .unwrap();
    assert_eq!(files.len(), 2);

    let mut file_ids: Vec<_> = files.into_iter().map(|f| f.id).collect();
    file_ids.sort();
    let mut expected_ids = vec![parquet_file.id, level1_file.id];
    expected_ids.sort();
    assert_eq!(file_ids, expected_ids);

    // Using the catalog partition ID should return the same files, even if the Parquet file
    // records don't have the partition ID on them (which is the default now)
    let files = repos
        .parquet_files()
        .list_by_partition_not_to_delete_batch(vec![partition.id])
        .await
        .unwrap();
    assert_eq!(files.len(), 2);

    let mut file_ids: Vec<_> = files.into_iter().map(|f| f.id).collect();
    file_ids.sort();
    let mut expected_ids = vec![parquet_file.id, level1_file.id];
    expected_ids.sort();
    assert_eq!(file_ids, expected_ids);
}

async fn test_update_to_compaction_level_1(catalog: Arc<dyn Catalog>) {
    let mut repos = catalog.repositories();
    let namespace =
        arbitrary_namespace(&mut *repos, "namespace_update_to_compaction_level_1_test").await;
    let table = arbitrary_table(&mut *repos, "update_table", &namespace).await;
    let partition = repos
        .partitions()
        .create_or_get("test_update_to_compaction_level_1_one".into(), table.id)
        .await
        .unwrap();

    // Set up the window of times we're interested in level 1 files for
    let query_min_time = Timestamp::new(5);
    let query_max_time = Timestamp::new(10);

    // Create a file with times entirely within the window
    let mut parquet_file_params = arbitrary_parquet_file_params(&namespace, &table, &partition);
    parquet_file_params.min_time = query_min_time + 1;
    parquet_file_params.max_time = query_max_time - 1;
    let parquet_file = repos
        .parquet_files()
        .create(parquet_file_params.clone())
        .await
        .unwrap();

    // Create a file that will remain as level 0
    let level_0_params = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        ..parquet_file_params.clone()
    };
    repos.parquet_files().create(level_0_params).await.unwrap();

    // Make parquet_file compaction level 1
    let created = repos
        .parquet_files()
        .create_upgrade_delete(
            parquet_file.partition_id,
            &[],
            &[parquet_file.object_store_id],
            &[],
            CompactionLevel::FileNonOverlapped,
        )
        .await
        .unwrap();
    assert_eq!(created, vec![]);

    // remove namespace to avoid it from affecting later tests
    repos
        .namespaces()
        .soft_delete("namespace_update_to_compaction_level_1_test")
        .await
        .expect("delete namespace should succeed");
}

/// Assert that a namespace deletion does NOT cascade to the tables/schema
/// items/parquet files/etc.
///
/// Removal of this entities breaks the invariant that once created, a row
/// always exists for the lifetime of an IOx process, and causes the system
/// to panic in multiple components. It's also ineffective, because most
/// components maintain a cache of at least one of these entities.
///
/// Instead soft deleted namespaces should have their files GC'd like a
/// normal parquet file deletion, removing the rows once they're no longer
/// being actively used by the system. This is done by waiting a long time
/// before deleting records, and whilst isn't perfect, it is largely
/// effective.
async fn test_delete_namespace(catalog: Arc<dyn Catalog>) {
    let mut repos = catalog.repositories();
    let namespace_1 = arbitrary_namespace(&mut *repos, "namespace_test_delete_namespace_1").await;
    let table_1 = arbitrary_table(&mut *repos, "test_table_1", &namespace_1).await;
    let _c = repos
        .columns()
        .create_or_get("column_test_1", table_1.id, ColumnType::Tag)
        .await
        .unwrap();
    let partition_1 = repos
        .partitions()
        .create_or_get("test_delete_namespace_one".into(), table_1.id)
        .await
        .unwrap();

    // parquet files
    let parquet_file_params = arbitrary_parquet_file_params(&namespace_1, &table_1, &partition_1);
    repos
        .parquet_files()
        .create(parquet_file_params.clone())
        .await
        .unwrap();
    let parquet_file_params_2 = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        min_time: Timestamp::new(200),
        max_time: Timestamp::new(300),
        ..parquet_file_params
    };
    repos
        .parquet_files()
        .create(parquet_file_params_2.clone())
        .await
        .unwrap();

    // we've now created a namespace with a table and parquet files. before we test deleting
    // it, let's create another so we can ensure that doesn't get deleted.
    let namespace_2 = arbitrary_namespace(&mut *repos, "namespace_test_delete_namespace_2").await;
    let table_2 = arbitrary_table(&mut *repos, "test_table_2", &namespace_2).await;
    let _c = repos
        .columns()
        .create_or_get("column_test_2", table_2.id, ColumnType::Tag)
        .await
        .unwrap();
    let partition_2 = repos
        .partitions()
        .create_or_get("test_delete_namespace_two".into(), table_2.id)
        .await
        .unwrap();

    // parquet files
    let parquet_file_params = arbitrary_parquet_file_params(&namespace_2, &table_2, &partition_2);
    repos
        .parquet_files()
        .create(parquet_file_params.clone())
        .await
        .unwrap();
    let parquet_file_params_2 = ParquetFileParams {
        object_store_id: ObjectStoreId::new(),
        min_time: Timestamp::new(200),
        max_time: Timestamp::new(300),
        ..parquet_file_params
    };
    repos
        .parquet_files()
        .create(parquet_file_params_2.clone())
        .await
        .unwrap();

    // now delete namespace_1 and assert it's all gone and none of
    // namespace_2 is gone
    repos
        .namespaces()
        .soft_delete("namespace_test_delete_namespace_1")
        .await
        .expect("delete namespace should succeed");
    // assert that namespace is soft-deleted, but the table, column, and parquet files are all
    // still there.
    assert!(repos
        .namespaces()
        .get_by_id(namespace_1.id, SoftDeletedRows::ExcludeDeleted)
        .await
        .expect("get namespace should succeed")
        .is_none());
    assert_eq!(
        repos
            .namespaces()
            .get_by_id(namespace_1.id, SoftDeletedRows::AllRows)
            .await
            .expect("get namespace should succeed")
            .map(|mut v| {
                // The only change after soft-deletion should be the deleted_at
                // field being set - this block normalises that field, so that
                // the before/after can be asserted as equal.
                v.deleted_at = None;
                v
            })
            .expect("should see soft-deleted row"),
        namespace_1
    );
    assert_eq!(
        repos
            .tables()
            .get_by_id(table_1.id)
            .await
            .expect("get table should succeed")
            .expect("should return row"),
        table_1
    );
    assert_eq!(
        repos
            .columns()
            .list_by_namespace_id(namespace_1.id)
            .await
            .expect("listing columns should succeed")
            .len(),
        1
    );
    assert_eq!(
        repos
            .columns()
            .list_by_table_id(table_1.id)
            .await
            .expect("listing columns should succeed")
            .len(),
        1
    );

    // partition's get_by_id should succeed
    repos
        .partitions()
        .get_by_id_batch(&[partition_1.id])
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    // assert that the namespace, table, column, and parquet files for namespace_2 are still
    // there
    assert!(repos
        .namespaces()
        .get_by_id(namespace_2.id, SoftDeletedRows::ExcludeDeleted)
        .await
        .expect("get namespace should succeed")
        .is_some());

    assert!(repos
        .tables()
        .get_by_id(table_2.id)
        .await
        .expect("get table should succeed")
        .is_some());
    assert_eq!(
        repos
            .columns()
            .list_by_namespace_id(namespace_2.id)
            .await
            .expect("listing columns should succeed")
            .len(),
        1
    );
    assert_eq!(
        repos
            .columns()
            .list_by_table_id(table_2.id)
            .await
            .expect("listing columns should succeed")
            .len(),
        1
    );

    // partition's get_by_id should succeed
    repos
        .partitions()
        .get_by_id_batch(&[partition_2.id])
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
}

/// Upsert a namespace called `namespace_name` and write `lines` to it.
async fn populate_namespace<R>(
    repos: &mut R,
    namespace_name: &str,
    lines: &str,
) -> (Namespace, NamespaceSchema)
where
    R: RepoCollection + ?Sized,
{
    let namespace = repos
        .namespaces()
        .create(
            &NamespaceName::new(namespace_name).unwrap(),
            None,
            None,
            None,
        )
        .await;

    let namespace = match namespace {
        Ok(v) => v,
        Err(Error::AlreadyExists { .. }) => repos
            .namespaces()
            .get_by_name(namespace_name, SoftDeletedRows::AllRows)
            .await
            .unwrap()
            .unwrap(),
        e @ Err(_) => e.unwrap(),
    };

    let batches = mutable_batch_lp::lines_to_batches(lines, 42).unwrap();
    let batches = batches.iter().map(|(table, batch)| (table.as_str(), batch));
    let ns = NamespaceSchema::new_empty_from(&namespace);

    let schema = validate_or_insert_schema(batches, &ns, repos)
        .await
        .expect("validate schema failed")
        .unwrap_or(ns);

    (namespace, schema)
}

async fn test_list_schemas(catalog: Arc<dyn Catalog>) {
    let mut repos = catalog.repositories();

    let ns1 = populate_namespace(
        repos.deref_mut(),
        "ns1",
        "cpu,tag=1 field=1i\nanother,tag=1 field=1.0",
    )
    .await;
    let ns2 = populate_namespace(
        repos.deref_mut(),
        "ns2",
        "cpu,tag=1 field=1i\nsomethingelse field=1u",
    )
    .await;

    // Otherwise the in-mem catalog deadlocks.... (but not postgres)
    drop(repos);

    let got = list_schemas(&*catalog)
        .await
        .expect("should be able to list the schemas")
        .collect::<Vec<_>>();

    assert!(got.contains(&ns1), "{:#?}\n\nwant{:#?}", got, &ns1);
    assert!(got.contains(&ns2), "{:#?}\n\nwant{:#?}", got, &ns2);
}

async fn test_list_schemas_soft_deleted_rows(catalog: Arc<dyn Catalog>) {
    let mut repos = catalog.repositories();

    let ns1 = populate_namespace(
        repos.deref_mut(),
        "ns1",
        "cpu,tag=1 field=1i\nanother,tag=1 field=1.0",
    )
    .await;
    let ns2 = populate_namespace(
        repos.deref_mut(),
        "ns2",
        "cpu,tag=1 field=1i\nsomethingelse field=1u",
    )
    .await;

    repos
        .namespaces()
        .soft_delete(&ns2.0.name)
        .await
        .expect("failed to soft delete namespace");

    // Otherwise the in-mem catalog deadlocks.... (but not postgres)
    drop(repos);

    let got = list_schemas(&*catalog)
        .await
        .expect("should be able to list the schemas")
        .collect::<Vec<_>>();

    assert!(got.contains(&ns1), "{:#?}\n\nwant{:#?}", got, &ns1);
    assert!(!got.contains(&ns2), "{:#?}\n\n do not want{:#?}", got, &ns2);
}

/// Ensure that we can create two repo objects and that they instantly share their state.
///
/// This is a regression test for <https://github.com/influxdata/influxdb_iox/issues/3859>.
async fn test_two_repos(catalog: Arc<dyn Catalog>) {
    let mut repos_1 = catalog.repositories();
    let mut repos_2 = catalog.repositories();
    let repo_1 = repos_1.namespaces();
    let repo_2 = repos_2.namespaces();

    let namespace_name = NamespaceName::new("test_namespace").unwrap();
    repo_1
        .create(&namespace_name, None, None, None)
        .await
        .unwrap();

    repo_2
        .get_by_name(&namespace_name, SoftDeletedRows::AllRows)
        .await
        .unwrap()
        .unwrap();
}

async fn test_partition_create_or_get_idempotent(catalog: Arc<dyn Catalog>) {
    let mut repos = catalog.repositories();

    let namespace = arbitrary_namespace(&mut *repos, "ns4").await;
    let table_id = arbitrary_table(&mut *repos, "table", &namespace).await.id;

    let key = PartitionKey::from("bananas");

    let hash_id = PartitionHashId::new(table_id, &key);

    let a = repos
        .partitions()
        .create_or_get(key.clone(), table_id)
        .await
        .expect("should create OK");

    assert_eq!(a.hash_id().unwrap(), &hash_id);
    // Test: sort_key_ids from partition_create_or_get_idempotent
    assert!(a.sort_key_ids().is_none());

    // Call create_or_get for the same (key, table_id) pair, to ensure the write is idempotent.
    let b = repos
        .partitions()
        .create_or_get(key.clone(), table_id)
        .await
        .expect("idempotent write should succeed");

    assert_eq!(a, b);

    // Check that the hash_id is saved in the database and is returned when queried.
    let table_partitions = repos.partitions().list_by_table_id(table_id).await.unwrap();
    assert_eq!(table_partitions.len(), 1);
    assert_eq!(table_partitions[0].hash_id().unwrap(), &hash_id);

    // Test: sort_key_ids from partition_create_or_get_idempotent
    assert!(table_partitions[0].sort_key_ids().is_none());
}

#[track_caller]
fn assert_metric_hit(metrics: &metric::Registry, name: &'static str) {
    let histogram = metrics
        .get_instrument::<Metric<DurationHistogram>>("catalog_op_duration")
        .expect("failed to read metric")
        .get_observer(&Attributes::from(&[("op", name), ("result", "success")]))
        .expect("failed to get observer")
        .fetch();

    let hit_count = histogram.sample_count();
    assert!(hit_count > 0, "metric did not record any calls");
}

async fn test_column_create_or_get_many_unchecked<R, F>(clean_state: R)
where
    R: Fn() -> F + Send + Sync,
    F: Future<Output = Arc<dyn Catalog>> + Send,
{
    // Issue a few calls to create_or_get_many that contain distinct columns and
    // covers the full set of column types.
    test_column_create_or_get_many_unchecked_sub(
        clean_state().await,
        &[
            &[
                ("test1", ColumnType::I64),
                ("test2", ColumnType::U64),
                ("test3", ColumnType::F64),
                ("test4", ColumnType::Bool),
                ("test5", ColumnType::String),
                ("test6", ColumnType::Time),
                ("test7", ColumnType::Tag),
            ],
            &[("test8", ColumnType::String), ("test9", ColumnType::Bool)],
        ],
        |res| assert_matches!(res, Ok(_)),
    )
    .await;

    // Issue two calls with overlapping columns - request should succeed (upsert
    // semantics).
    test_column_create_or_get_many_unchecked_sub(
        clean_state().await,
        &[
            &[
                ("test1", ColumnType::I64),
                ("test2", ColumnType::U64),
                ("test3", ColumnType::F64),
                ("test4", ColumnType::Bool),
            ],
            &[
                ("test1", ColumnType::I64),
                ("test2", ColumnType::U64),
                ("test3", ColumnType::F64),
                ("test4", ColumnType::Bool),
                ("test5", ColumnType::String),
                ("test6", ColumnType::Time),
                ("test7", ColumnType::Tag),
                ("test8", ColumnType::String),
            ],
        ],
        |res| assert_matches!(res, Ok(_)),
    )
    .await;

    // Issue two calls with the same columns and types.
    test_column_create_or_get_many_unchecked_sub(
        clean_state().await,
        &[
            &[
                ("test1", ColumnType::I64),
                ("test2", ColumnType::U64),
                ("test3", ColumnType::F64),
                ("test4", ColumnType::Bool),
            ],
            &[
                ("test1", ColumnType::I64),
                ("test2", ColumnType::U64),
                ("test3", ColumnType::F64),
                ("test4", ColumnType::Bool),
            ],
        ],
        |res| assert_matches!(res, Ok(_)),
    )
    .await;

    // Issue two calls with overlapping columns with conflicting types and
    // observe a correctly populated ColumnTypeMismatch error.
    test_column_create_or_get_many_unchecked_sub(
        clean_state().await,
        &[
            &[
                ("test1", ColumnType::String),
                ("test2", ColumnType::String),
                ("test3", ColumnType::String),
                ("test4", ColumnType::String),
            ],
            &[
                ("test1", ColumnType::String),
                ("test2", ColumnType::Bool), // This one differs
                ("test3", ColumnType::String),
                // 4 is missing.
                ("test5", ColumnType::String),
                ("test6", ColumnType::Time),
                ("test7", ColumnType::Tag),
                ("test8", ColumnType::String),
            ],
        ],
        |res| assert_matches!(res, Err(e) => {
            assert_matches!(e, Error::AlreadyExists { descr } => {
                assert_eq!(descr, "column test2 is type string but schema update has type bool");
            })
        }),
    ).await;
}

async fn test_column_create_or_get_many_unchecked_sub<F>(
    catalog: Arc<dyn Catalog>,
    calls: &[&[(&'static str, ColumnType)]],
    want: F,
) where
    F: FnOnce(Result<Vec<Column>, Error>) + Send,
{
    let mut repos = catalog.repositories();

    let namespace = arbitrary_namespace(&mut *repos, "ns4").await;
    let table_id = arbitrary_table(&mut *repos, "table", &namespace).await.id;

    let mut last_got = None;
    for insert in calls {
        let insert = insert
            .iter()
            .map(|(n, t)| (*n, *t))
            .collect::<HashMap<_, _>>();

        let got = repos
            .columns()
            .create_or_get_many_unchecked(table_id, insert.clone())
            .await;

        // The returned columns MUST always match the requested
        // column values if successful.
        if let Ok(got) = &got {
            assert_eq!(insert.len(), got.len());

            for got in got {
                assert_eq!(table_id, got.table_id);
                let requested_column_type = insert
                    .get(got.name.as_str())
                    .expect("Should have gotten back a column that was inserted");
                assert_eq!(*requested_column_type, got.column_type,);
            }

            assert_metric_hit(&catalog.metrics(), "column_create_or_get_many_unchecked");
        }

        last_got = Some(got);
    }

    want(last_got.unwrap());
}

/// [`Catalog`] wrapper that is helpful for testing.
#[derive(Debug)]
pub(crate) struct TestCatalog {
    hold_onto: Mutex<Vec<Box<dyn Any + Send>>>,
    inner: Arc<dyn Catalog>,
}

impl TestCatalog {
    /// Create new test catalog.
    pub(crate) fn new(inner: Arc<dyn Catalog>) -> Self {
        Self {
            hold_onto: Mutex::new(vec![]),
            inner,
        }
    }

    /// Hold onto given value til dropped.
    pub(crate) fn hold_onto<T>(&self, o: T)
    where
        T: Send + 'static,
    {
        self.hold_onto.lock().push(Box::new(o) as _)
    }
}

impl Display for TestCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "test({})", self.inner)
    }
}

#[async_trait]
impl Catalog for TestCatalog {
    async fn setup(&self) -> Result<(), Error> {
        self.inner.setup().await
    }

    fn repositories(&self) -> Box<dyn RepoCollection> {
        self.inner.repositories()
    }

    fn metrics(&self) -> Arc<metric::Registry> {
        self.inner.metrics()
    }

    fn time_provider(&self) -> Arc<dyn TimeProvider> {
        self.inner.time_provider()
    }
}

#[track_caller]
fn assert_gt<T>(a: T, b: T)
where
    T: Display + PartialOrd,
{
    assert!(a > b, "failed: {a} > {b}",);
}

#[track_caller]
fn assert_ge<T>(a: T, b: T)
where
    T: Display + PartialOrd,
{
    assert!(a >= b, "failed: {a} >= {b}",);
}
