//! Catalog helper functions for creation of catalog objects
use data_types::{
    partition_template::TablePartitionTemplateOverride, ColumnId, ColumnSet, CompactionLevel,
    Namespace, NamespaceName, ObjectStoreId, ParquetFileParams, Partition, Table, TableSchema,
    Timestamp,
};

use crate::interface::RepoCollection;

/// When the details of the namespace don't matter; the test just needs *a* catalog namespace
/// with a particular name.
///
/// Use [`NamespaceRepo::create`] directly if:
///
/// - The values of the parameters to `create` need to be different than what's here
/// - The values of the parameters to `create` are relevant to the behavior under test
/// - You expect namespace creation to fail in the test
///
/// [`NamespaceRepo::create`]: crate::interface::NamespaceRepo::create
pub async fn arbitrary_namespace<R: RepoCollection + ?Sized>(
    repos: &mut R,
    name: &str,
) -> Namespace {
    let namespace_name = NamespaceName::new(name).unwrap();
    repos
        .namespaces()
        .create(&namespace_name, None, None, None)
        .await
        .unwrap()
}

/// When the details of the table don't matter; the test just needs *a* catalog table
/// with a particular name in a particular namespace.
///
/// Use [`TableRepo::create`] directly if:
///
/// - The values of the parameters to `create_or_get` need to be different than what's here
/// - The values of the parameters to `create_or_get` are relevant to the behavior under test
/// - You expect table creation to fail in the test
///
/// [`TableRepo::create`]: crate::interface::TableRepo::create
pub async fn arbitrary_table<R: RepoCollection + ?Sized>(
    repos: &mut R,
    name: &str,
    namespace: &Namespace,
) -> Table {
    repos
        .tables()
        .create(
            name,
            TablePartitionTemplateOverride::try_new(None, &namespace.partition_template).unwrap(),
            namespace.id,
        )
        .await
        .unwrap()
}

/// Load or create an arbitrary table schema in the same way that a write implicitly creates a
/// table, that is, with a time column.
pub async fn arbitrary_table_schema_load_or_create<R: RepoCollection + ?Sized>(
    repos: &mut R,
    name: &str,
    namespace: &Namespace,
) -> TableSchema {
    crate::util::table_load_or_create(repos, namespace.id, &namespace.partition_template, name)
        .await
        .unwrap()
}

/// When the details of a Parquet file record don't matter, the test just needs *a* Parquet
/// file record in a particular namespace+table+partition.
pub fn arbitrary_parquet_file_params(
    namespace: &Namespace,
    table: &Table,
    partition: &Partition,
) -> ParquetFileParams {
    ParquetFileParams {
        namespace_id: namespace.id,
        table_id: table.id,
        partition_id: partition.id,
        partition_hash_id: partition.hash_id().cloned(),
        object_store_id: ObjectStoreId::new(),
        min_time: Timestamp::new(1),
        max_time: Timestamp::new(10),
        file_size_bytes: 1337,
        row_count: 0,
        compaction_level: CompactionLevel::Initial,
        created_at: Timestamp::new(1),
        column_set: ColumnSet::new([ColumnId::new(1), ColumnId::new(2)]),
        max_l0_created_at: Timestamp::new(1),
    }
}
