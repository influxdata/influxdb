//! The IOx catalog keeps track of the namespaces, tables, columns, parquet files,
//! and deletes in the system. Configuration information for distributing ingest, query
//! and compaction is also stored here.
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use crate::interface::{ColumnTypeMismatchSnafu, Error, RepoCollection, Result};
use data_types::{
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    ColumnType, NamespaceId, NamespaceSchema, Partition, TableSchema, TransitionPartitionId,
};
use mutable_batch::MutableBatch;
use std::{borrow::Cow, collections::HashMap};
use thiserror::Error;

const TIME_COLUMN: &str = "time";

/// Default per-namespace table count service protection limit.
pub const DEFAULT_MAX_TABLES: i32 = 500;
/// Default per-table column count service protection limit.
pub const DEFAULT_MAX_COLUMNS_PER_TABLE: i32 = 200;
/// Default retention period for data in the catalog.
pub const DEFAULT_RETENTION_PERIOD: Option<i64> = None;

pub mod interface;
pub(crate) mod kafkaless_transition;
pub mod mem;
pub mod metrics;
pub mod migrate;
pub mod postgres;
pub mod sqlite;

/// An [`crate::interface::Error`] scoped to a single table for schema validation errors.
#[derive(Debug, Error)]
#[error("table {}, {}", .0, .1)]
pub struct TableScopedError(String, Error);

impl TableScopedError {
    /// Return the table name for this error.
    pub fn table(&self) -> &str {
        &self.0
    }

    /// Return a reference to the error.
    pub fn err(&self) -> &Error {
        &self.1
    }

    /// Return ownership of the error, discarding the table name.
    pub fn into_err(self) -> Error {
        self.1
    }
}

/// Look up a partition in the catalog by either database-assigned ID or deterministic hash ID.
///
/// The existence of this function should be temporary; it can be removed once all partition lookup
/// is happening with only the deterministic hash ID.
pub async fn partition_lookup<R>(
    repos: &mut R,
    id: &TransitionPartitionId,
) -> Result<Option<Partition>, Error>
where
    R: RepoCollection + ?Sized,
{
    match id {
        TransitionPartitionId::Deprecated(partition_id) => {
            repos.partitions().get_by_id(*partition_id).await
        }
        TransitionPartitionId::Deterministic(partition_hash_id) => {
            repos.partitions().get_by_hash_id(partition_hash_id).await
        }
    }
}

/// Look up multiple partitions in the catalog by either database-assigned ID or deterministic hash ID.
///
/// The output only contains existing partitions, the order is undefined.
///
/// The existence of this function should be temporary; it can be removed once all partition lookup
/// is happening with only the deterministic hash ID.
pub async fn partition_lookup_batch<R>(
    repos: &mut R,
    ids: &[&TransitionPartitionId],
) -> Result<Vec<Partition>, Error>
where
    R: RepoCollection + ?Sized,
{
    let mut partition_ids = Vec::with_capacity(ids.len());
    let mut partition_hash_ids = Vec::with_capacity(ids.len());

    for id in ids {
        match id {
            TransitionPartitionId::Deprecated(partition_id) => {
                partition_ids.push(*partition_id);
            }
            TransitionPartitionId::Deterministic(partition_hash_id) => {
                partition_hash_ids.push(partition_hash_id);
            }
        }
    }

    let mut out = Vec::with_capacity(partition_ids.len() + partition_hash_ids.len());
    if !partition_ids.is_empty() {
        let mut partitions = repos.partitions().get_by_id_batch(partition_ids).await?;
        out.append(&mut partitions);
    }
    if !partition_hash_ids.is_empty() {
        let mut partitions = repos
            .partitions()
            .get_by_hash_id_batch(&partition_hash_ids)
            .await?;
        out.append(&mut partitions);
    }
    Ok(out)
}

/// Given an iterator of `(table_name, batch)` to validate, this function
/// ensures all the columns within `batch` match the existing schema for
/// `table_name` in `schema`. If the column does not already exist in `schema`,
/// it is created and an updated [`NamespaceSchema`] is returned.
///
/// This function pushes schema additions through to the backend catalog, and
/// relies on the catalog to serialize concurrent additions of a given column,
/// ensuring only one type is ever accepted per column.
pub async fn validate_or_insert_schema<'a, T, U, R>(
    tables: T,
    schema: &NamespaceSchema,
    repos: &mut R,
) -> Result<Option<NamespaceSchema>, TableScopedError>
where
    T: IntoIterator<IntoIter = U, Item = (&'a str, &'a MutableBatch)> + Send + Sync,
    U: Iterator<Item = T::Item> + Send,
    R: RepoCollection + ?Sized,
{
    let tables = tables.into_iter();

    // The (potentially updated) NamespaceSchema to return to the caller.
    let mut schema = Cow::Borrowed(schema);

    for (table_name, batch) in tables {
        validate_mutable_batch(batch, table_name, &mut schema, repos)
            .await
            .map_err(|e| TableScopedError(table_name.to_string(), e))?;
    }

    match schema {
        Cow::Owned(v) => Ok(Some(v)),
        Cow::Borrowed(_) => Ok(None),
    }
}

// &mut Cow is used to avoid a copy, so allow it
#[allow(clippy::ptr_arg)]
async fn validate_mutable_batch<R>(
    mb: &MutableBatch,
    table_name: &str,
    schema: &mut Cow<'_, NamespaceSchema>,
    repos: &mut R,
) -> Result<()>
where
    R: RepoCollection + ?Sized,
{
    // Check if the table exists in the schema.
    //
    // Because the entry API requires &mut it is not used to avoid a premature
    // clone of the Cow.
    let mut table = match schema.tables.get(table_name) {
        Some(t) => Cow::Borrowed(t),
        None => {
            // The table does not exist in the cached schema.
            //
            // Attempt to load an existing table from the catalog or create a new table in the
            // catalog to populate the cache.
            let table =
                table_load_or_create(repos, schema.id, &schema.partition_template, table_name)
                    .await?;

            assert!(schema
                .to_mut()
                .tables
                .insert(table_name.to_string(), table)
                .is_none());

            Cow::Borrowed(schema.tables.get(table_name).unwrap())
        }
    };

    // The table is now in the schema (either by virtue of it already existing,
    // or through adding it above).
    //
    // If the table itself needs to be updated during column validation it
    // becomes a Cow::owned() copy and the modified copy should be inserted into
    // the schema before returning.
    let mut column_batch: HashMap<&str, ColumnType> = HashMap::new();

    for (name, col) in mb.columns() {
        // Check if the column exists in the cached schema.
        //
        // If it does, validate it. If it does not exist, create it and insert
        // it into the cached schema.

        match table.columns.get(name.as_str()) {
            Some(existing) if existing.matches_type(col.influx_type()) => {
                // No action is needed as the column matches the existing column
                // schema.
            }
            Some(existing) => {
                // The column schema, and the column in the mutable batch are of
                // different types.
                return ColumnTypeMismatchSnafu {
                    name,
                    existing: existing.column_type,
                    new: col.influx_type(),
                }
                .fail();
            }
            None => {
                // The column does not exist in the cache, add it to the column
                // batch to be bulk inserted later.
                let old = column_batch.insert(name.as_str(), ColumnType::from(col.influx_type()));
                assert!(
                    old.is_none(),
                    "duplicate column name `{name}` in new column batch shouldn't be possible"
                );
            }
        }
    }

    if !column_batch.is_empty() {
        repos
            .columns()
            .create_or_get_many_unchecked(table.id, column_batch)
            .await?
            .into_iter()
            .for_each(|c| table.to_mut().add_column(c));
    }

    if let Cow::Owned(table) = table {
        // The table schema was mutated and needs inserting into the namespace
        // schema to make the changes visible to the caller.
        assert!(schema
            .to_mut()
            .tables
            .insert(table_name.to_string(), table)
            .is_some());
    }

    Ok(())
}

async fn table_load_or_create<R>(
    repos: &mut R,
    namespace_id: NamespaceId,
    namespace_partition_template: &NamespacePartitionTemplateOverride,
    table_name: &str,
) -> Result<TableSchema>
where
    R: RepoCollection + ?Sized,
{
    let table = match repos
        .tables()
        .get_by_namespace_and_name(namespace_id, table_name)
        .await?
    {
        Some(table) => table,
        None => {
            // There is a possibility of a race condition here, if another request has also
            // created this table after the `get_by_namespace_and_name` call but before
            // this `create` call. In that (hopefully) rare case, do an additional fetch
            // from the catalog for the record that should now exist.
            let create_result = repos
                .tables()
                .create(
                    table_name,
                    // This table is being created implicitly by this write, so there's no
                    // possibility of a user-supplied partition template here, which is why there's
                    // a hardcoded `None`. If there is a namespace template, it must be valid because
                    // validity was checked during its creation, so that's why there's an `expect`.
                    TablePartitionTemplateOverride::try_new(None, namespace_partition_template)
                        .expect("no table partition template; namespace partition template has been validated"),
                    namespace_id,
                )
                .await;
            if let Err(Error::TableNameExists { .. }) = create_result {
                repos
                    .tables()
                    .get_by_namespace_and_name(namespace_id, table_name)
                    // Propagate any `Err` returned by the catalog
                    .await?
                    // Getting `Ok(None)` should be impossible if we're in this code path because
                    // the `create` request just said the table exists
                    .expect(
                        "Table creation failed because the table exists, so looking up the table \
                        should return `Some(table)`, but it returned `None`",
                    )
            } else {
                create_result?
            }
        }
    };

    let mut table = TableSchema::new_empty_from(&table);

    // Always add a time column to all new tables.
    let time_col = repos
        .columns()
        .create_or_get(TIME_COLUMN, table.id, ColumnType::Time)
        .await?;

    table.add_column(time_col);

    Ok(table)
}

/// Catalog helper functions for creation of catalog objects
pub mod test_helpers {
    use crate::RepoCollection;
    use data_types::{
        partition_template::TablePartitionTemplateOverride, ColumnId, ColumnSet, CompactionLevel,
        Namespace, NamespaceName, ParquetFileParams, Partition, Table, Timestamp,
    };
    use uuid::Uuid;

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
                TablePartitionTemplateOverride::try_new(None, &namespace.partition_template)
                    .unwrap(),
                namespace.id,
            )
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
            partition_id: partition.transition_partition_id(),
            object_store_id: Uuid::new_v4(),
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
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use super::*;
    use crate::{
        interface::{get_schema_by_name, SoftDeletedRows},
        mem::MemCatalog,
    };

    // Generate a test that simulates multiple, sequential writes in `lp` and
    // asserts the resulting schema.
    //
    // This test asserts the cached schema and the database entry are always in
    // sync.
    macro_rules! test_validate_schema {
        (
            $name:ident,
            lp = [$($lp:literal,)+],                                // An array of multi-line LP writes
            want_observe_conflict = $want_observe_conflict:literal, // true if a schema validation error should be observed at some point
            want_schema = {$($want_schema:tt) +}                    // The expected resulting schema after all writes complete.
        ) => {
            paste::paste! {
                #[allow(clippy::bool_assert_comparison)]
                #[tokio::test]
                async fn [<test_validate_schema_ $name>]() {
                    use crate::{interface::Catalog, test_helpers::arbitrary_namespace};
                    use std::ops::DerefMut;
                    use pretty_assertions::assert_eq;
                    const NAMESPACE_NAME: &str = "bananas";

                    let metrics = Arc::new(metric::Registry::default());
                    let repo = MemCatalog::new(metrics);
                    let mut txn = repo.repositories().await;

                    let namespace = arbitrary_namespace(&mut *txn, NAMESPACE_NAME)
                        .await;
                    let schema = NamespaceSchema::new_empty_from(&namespace);

                    // Apply all the lp literals as individual writes, feeding
                    // the result of one validation into the next to drive
                    // incremental construction of the schemas.
                    let mut observed_conflict = false;
                    $(
                        let schema = {
                            let lp: String = $lp.to_string();

                            let writes = mutable_batch_lp::lines_to_batches(lp.as_str(), 42)
                                .expect("failed to build test writes from LP");

                            let got = validate_or_insert_schema(writes.iter().map(|(k, v)| (k.as_str(), v)), &schema, txn.deref_mut())
                                .await;

                            match got {
                                Err(TableScopedError(_, Error::ColumnTypeMismatch{ .. })) => {
                                    observed_conflict = true;
                                    schema
                                },
                                Err(e) => panic!("unexpected error: {}", e),
                                Ok(Some(new_schema)) => new_schema,
                                Ok(None) => schema,
                            }
                        };
                    )+

                    assert_eq!($want_observe_conflict, observed_conflict, "should error mismatch");

                    // Invariant: in absence of concurrency, the schema within
                    // the database must always match the incrementally built
                    // cached schema.
                    let db_schema = get_schema_by_name(NAMESPACE_NAME, txn.deref_mut(), SoftDeletedRows::ExcludeDeleted)
                        .await
                        .expect("database failed to query for namespace schema");
                    assert_eq!(schema, db_schema, "schema in DB and cached schema differ");

                    // Generate the map of tables => desired column types
                    let want_tables: BTreeMap<String, BTreeMap<String, ColumnType>> = test_validate_schema!(@table, $($want_schema)+);

                    // Generate a similarly structured map from the actual
                    // schema
                    let actual_tables: BTreeMap<String, BTreeMap<String, ColumnType>> = schema
                        .tables
                        .iter()
                        .map(|(table, table_schema)| {
                            let desired_cols = table_schema
                                .columns
                                .iter()
                                .map(|(column, column_schema)| (column.clone(), column_schema.column_type))
                                .collect::<BTreeMap<_, _>>();

                            (table.clone(), desired_cols)
                        })
                        .collect();

                    // Assert the actual namespace contents matches the desired
                    // table schemas in the test args.
                    assert_eq!(want_tables, actual_tables, "cached schema and desired schema differ");
                }
            }
        };
        // Generate a map of table names => column map (below)
        //
        // out: BTreeMap<String, BTreeMap<ColName, ColumnType>>
        (@table, $($table_name:literal: [$($columns:tt) +],)*) => {{
            let mut tables = BTreeMap::new();
            $(
                let want_cols = test_validate_schema!(@column, $($columns)+);
                assert!(tables.insert($table_name.to_string(), want_cols).is_none());
            )*
            tables
        }};
        // Generate a map of column names => ColumnType
        //
        // out: BTreeMap<ColName, ColumnType>
        (@column, $($col_name:literal => $col_type:expr,)+) => {{
            let mut cols = BTreeMap::new();
            $(
                assert!(cols.insert($col_name.to_string(), $col_type).is_none());
            )*
            cols
        }};
    }

    test_validate_schema!(
        one_write_multiple_tables,
        lp = [
            "
                m1,t1=a,t2=b f1=2i,f2=2.0 1\n\
                m1,t1=a f1=3i 2\n\
                m2,t3=b f1=true 1\n\
            ",
        ],
        want_observe_conflict = false,
        want_schema = {
            "m1": [
                "t1" => ColumnType::Tag,
                "t2" => ColumnType::Tag,
                "f1" => ColumnType::I64,
                "f2" => ColumnType::F64,
                "time" => ColumnType::Time,
            ],
            "m2": [
                "f1" => ColumnType::Bool,
                "t3" => ColumnType::Tag,
                "time" => ColumnType::Time,
            ],
        }
    );

    // test that a new table will be created
    test_validate_schema!(
        two_writes_incremental_new_table,
        lp = [
            "
                m1,t1=a,t2=b f1=2i,f2=2.0 1\n\
                m1,t1=a f1=3i 2\n\
                m2,t3=b f1=true 1\n\
            ",
            "
                m1,t1=c f1=1i 2\n\
                new_measurement,t9=a f10=true 1\n\
            ",
        ],
        want_observe_conflict = false,
        want_schema = {
            "m1": [
                "t1" => ColumnType::Tag,
                "t2" => ColumnType::Tag,
                "f1" => ColumnType::I64,
                "f2" => ColumnType::F64,
                "time" => ColumnType::Time,
            ],
            "m2": [
                "f1" => ColumnType::Bool,
                "t3" => ColumnType::Tag,
                "time" => ColumnType::Time,
                ],
            "new_measurement": [
                "t9" => ColumnType::Tag,
                "f10" => ColumnType::Bool,
                "time" => ColumnType::Time,
            ],
        }
    );

    // test that a new column for an existing table will be created
    test_validate_schema!(
        two_writes_incremental_new_column,
        lp = [
            "
                m1,t1=a,t2=b f1=2i,f2=2.0 1\n\
                m1,t1=a f1=3i 2\n\
                m2,t3=b f1=true 1\n\
            ",
            "m1,new_tag=c new_field=1i 2",
        ],
        want_observe_conflict = false,
        want_schema = {
            "m1": [
                "t1" => ColumnType::Tag,
                "t2" => ColumnType::Tag,
                "f1" => ColumnType::I64,
                "f2" => ColumnType::F64,
                "time" => ColumnType::Time,
                // These are the incremental additions:
                "new_tag" => ColumnType::Tag,
                "new_field" => ColumnType::I64,
            ],
            "m2": [
                "f1" => ColumnType::Bool,
                "t3" => ColumnType::Tag,
                "time" => ColumnType::Time,
            ],
        }
    );

    test_validate_schema!(
        table_always_has_time_column,
        lp = [
            "m1,t1=a f1=2i",
        ],
        want_observe_conflict = false,
        want_schema = {
            "m1": [
                "t1" => ColumnType::Tag,
                "f1" => ColumnType::I64,
                "time" => ColumnType::Time,
            ],
        }
    );

    test_validate_schema!(
        two_writes_conflicting_column_types,
        lp = [
            "m1,t1=a f1=2i",
            // Second write has conflicting type for f1.
            "m1,t1=a f1=2.0",
        ],
        want_observe_conflict = true,
        want_schema = {
            "m1": [
                "t1" => ColumnType::Tag,
                "f1" => ColumnType::I64,
                "time" => ColumnType::Time,
            ],
        }
    );

    test_validate_schema!(
        two_writes_tag_field_transposition,
        lp = [
            // x is a tag
            "m1,t1=a,x=t f1=2i",
            // x is a field
            "m1,t1=a x=t,f1=2i",
        ],
        want_observe_conflict = true,
        want_schema = {
            "m1": [
                "t1" => ColumnType::Tag,
                "x" => ColumnType::Tag,
                "f1" => ColumnType::I64,
                "time" => ColumnType::Time,
            ],
        }
    );
}
