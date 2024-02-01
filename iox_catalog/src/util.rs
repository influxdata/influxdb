//! Helper methods to simplify catalog work.
//!
//! They all use the public [`Catalog`] interface and have no special access to internals, so in theory they can be
//! implement downstream as well.

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use data_types::{
    partition_template::{NamespacePartitionTemplateOverride, TablePartitionTemplateOverride},
    ColumnType, ColumnsByName, Namespace, NamespaceId, NamespaceSchema, PartitionId, SortKeyIds,
    TableId, TableSchema,
};
use mutable_batch::MutableBatch;
use thiserror::Error;

use crate::{
    constants::TIME_COLUMN,
    interface::{CasFailure, Catalog, Error, RepoCollection, SoftDeletedRows},
};

/// Gets the namespace schema including all tables and columns.
pub async fn get_schema_by_id<R>(
    id: NamespaceId,
    repos: &mut R,
    deleted: SoftDeletedRows,
) -> Result<Option<NamespaceSchema>, crate::interface::Error>
where
    R: RepoCollection + ?Sized,
{
    let Some(namespace) = repos.namespaces().get_by_id(id, deleted).await? else {
        return Ok(None);
    };

    Ok(Some(get_schema_internal(namespace, repos).await?))
}

/// Gets the namespace schema including all tables and columns.
pub async fn get_schema_by_name<R>(
    name: &str,
    repos: &mut R,
    deleted: SoftDeletedRows,
) -> Result<Option<NamespaceSchema>, crate::interface::Error>
where
    R: RepoCollection + ?Sized,
{
    let Some(namespace) = repos.namespaces().get_by_name(name, deleted).await? else {
        return Ok(None);
    };

    Ok(Some(get_schema_internal(namespace, repos).await?))
}

async fn get_schema_internal<R>(
    namespace: Namespace,
    repos: &mut R,
) -> Result<NamespaceSchema, crate::interface::Error>
where
    R: RepoCollection + ?Sized,
{
    // get the columns first just in case someone else is creating schema while we're doing this.
    let columns = repos.columns().list_by_namespace_id(namespace.id).await?;
    let tables = repos.tables().list_by_namespace_id(namespace.id).await?;

    let mut namespace = NamespaceSchema::new_empty_from(&namespace);

    let mut table_id_to_schema = BTreeMap::new();
    for t in tables {
        let table_schema = TableSchema::new_empty_from(&t);
        table_id_to_schema.insert(t.id, (t.name, table_schema));
    }

    for c in columns {
        let (_, t) = table_id_to_schema.get_mut(&c.table_id).unwrap();
        t.add_column(c);
    }

    for (_, (table_name, schema)) in table_id_to_schema {
        namespace.tables.insert(table_name, schema);
    }

    Ok(namespace)
}

/// Gets the schema for one particular table in a namespace.
pub async fn get_schema_by_namespace_and_table<R>(
    name: &str,
    table_name: &str,
    repos: &mut R,
    deleted: SoftDeletedRows,
) -> Result<Option<NamespaceSchema>, crate::interface::Error>
where
    R: RepoCollection + ?Sized,
{
    let Some(namespace) = repos.namespaces().get_by_name(name, deleted).await? else {
        return Ok(None);
    };

    let Some(table) = repos
        .tables()
        .get_by_namespace_and_name(namespace.id, table_name)
        .await?
    else {
        return Ok(None);
    };

    let mut table_schema = TableSchema::new_empty_from(&table);

    let columns = repos.columns().list_by_table_id(table.id).await?;
    for c in columns {
        table_schema.add_column(c);
    }

    let mut namespace = NamespaceSchema::new_empty_from(&namespace);
    namespace
        .tables
        .insert(table_name.to_string(), table_schema);

    Ok(Some(namespace))
}

/// Gets all the table's columns.
pub async fn get_table_columns_by_id<R>(
    id: TableId,
    repos: &mut R,
) -> Result<ColumnsByName, crate::interface::Error>
where
    R: RepoCollection + ?Sized,
{
    let columns = repos.columns().list_by_table_id(id).await?;

    Ok(ColumnsByName::new(columns))
}

/// Fetch all [`NamespaceSchema`] in the catalog.
///
/// This method performs the minimal number of queries needed to build the
/// result set. No table lock is obtained, nor are queries executed within a
/// transaction, but this method does return a point-in-time snapshot of the
/// catalog state.
///
/// # Soft Deletion
///
/// No schemas for soft-deleted namespaces are returned.
pub async fn list_schemas(
    catalog: &dyn Catalog,
) -> Result<impl Iterator<Item = (Namespace, NamespaceSchema)>, crate::interface::Error> {
    let mut repos = catalog.repositories();

    // In order to obtain a point-in-time snapshot, first fetch the columns,
    // then the tables, and then resolve the namespace IDs to Namespace in order
    // to construct the schemas.
    //
    // The set of columns returned forms the state snapshot, with the subsequent
    // queries resolving only what is needed to construct schemas for the
    // retrieved columns (ignoring any newly added tables/namespaces since the
    // column snapshot was taken).
    //
    // This approach also tolerates concurrently deleted namespaces, which are
    // simply ignored at the end when joining to the namespace query result.

    // First fetch all the columns - this is the state snapshot of the catalog
    // schemas.
    let columns = repos.columns().list().await?;

    // Construct the set of table IDs these columns belong to.
    let retain_table_ids = columns.iter().map(|c| c.table_id).collect::<HashSet<_>>();

    // Fetch all tables, and filter for those that are needed to construct
    // schemas for "columns" only.
    //
    // Discard any tables that have no columns or have been created since
    // the "columns" snapshot was retrieved, and construct a map of ID->Table.
    let tables = repos
        .tables()
        .list()
        .await?
        .into_iter()
        .filter_map(|t| {
            if !retain_table_ids.contains(&t.id) {
                return None;
            }

            Some((t.id, t))
        })
        .collect::<HashMap<_, _>>();

    // Drop the table ID set as it will not be referenced again.
    drop(retain_table_ids);

    // Do all the I/O to fetch the namespaces in the background, while this
    // thread constructs the NamespaceId->TableSchema map below.
    let namespaces = tokio::spawn(async move {
        repos
            .namespaces()
            .list(SoftDeletedRows::ExcludeDeleted)
            .await
    });

    // A set of tables within a single namespace.
    type NamespaceTables = BTreeMap<String, TableSchema>;

    let mut joined = HashMap::<NamespaceId, NamespaceTables>::default();
    for column in columns {
        // Resolve the table this column references
        let table = tables.get(&column.table_id).expect("no table for column");

        let table_schema = joined
            // Find or create a record in the joined <NamespaceId, Tables> map
            // for this namespace ID.
            .entry(table.namespace_id)
            .or_default()
            // Fetch the schema record for this table, or create an empty one.
            .entry(table.name.clone())
            .or_insert_with(|| TableSchema::new_empty_from(table));

        table_schema.add_column(column);
    }

    // The table map is no longer needed - immediately reclaim the memory.
    drop(tables);

    // Convert the Namespace instances into NamespaceSchema instances.
    let iter = namespaces
        .await
        .expect("namespace list task panicked")?
        .into_iter()
        // Ignore any namespaces that did not exist when the "columns" snapshot
        // was created, or have no tables/columns (and therefore have no entry
        // in "joined").
        .filter_map(move |v| {
            // The catalog call explicitly asked for no soft deleted records.
            assert!(v.deleted_at.is_none());

            let mut ns = NamespaceSchema::new_empty_from(&v);

            ns.tables = joined.remove(&v.id)?;
            Some((v, ns))
        });

    Ok(iter)
}

/// In a backoff loop, retry calling the compare-and-swap sort key catalog function if the catalog
/// returns a query error unrelated to the CAS operation.
///
/// Returns with a value of `Ok` containing the new sort key if:
///
/// - No concurrent updates were detected
/// - A concurrent update was detected, but the other update resulted in the same value this update
///   was attempting to set
///
/// Returns with a value of `Err(newly_observed_value)` if a concurrent, conflicting update was
/// detected. It is expected that callers of this function will take the returned value into
/// account (in whatever manner is appropriate) before calling this function again.
///
/// NOTE: it is expected that ONLY processes that ingest data (currently only the ingesters or the
/// bulk ingest API) update sort keys for existing partitions. Consider how calling this function
/// from new processes will interact with the existing calls.
pub async fn retry_cas_sort_key(
    old_sort_key_ids: Option<&SortKeyIds>,
    new_sort_key_ids: &SortKeyIds,
    partition_id: PartitionId,
    catalog: Arc<dyn Catalog>,
) -> Result<SortKeyIds, SortKeyIds> {
    use backoff::Backoff;
    use observability_deps::tracing::{info, warn};
    use std::ops::ControlFlow;

    Backoff::new(&Default::default())
        .retry_with_backoff("cas_sort_key", || {
            let new_sort_key_ids = new_sort_key_ids.clone();
            let catalog = Arc::clone(&catalog);
            async move {
                let mut repos = catalog.repositories();
                match repos
                    .partitions()
                    .cas_sort_key(partition_id, old_sort_key_ids, &new_sort_key_ids)
                    .await
                {
                    Ok(_) => ControlFlow::Break(Ok(new_sort_key_ids)),
                    Err(CasFailure::QueryError(e)) => ControlFlow::Continue(e),
                    Err(CasFailure::ValueMismatch(observed_sort_key_ids))
                        if observed_sort_key_ids == new_sort_key_ids =>
                    {
                        // A CAS failure occurred because of a concurrent
                        // sort key update, however the new catalog sort key
                        // exactly matches the sort key this node wants to
                        // commit.
                        //
                        // This is the sad-happy path, and this task can
                        // continue.
                        info!(
                            %partition_id,
                            ?old_sort_key_ids,
                            ?observed_sort_key_ids,
                            update_sort_key_ids=?new_sort_key_ids,
                            "detected matching concurrent sort key update"
                        );
                        ControlFlow::Break(Ok(new_sort_key_ids))
                    }
                    Err(CasFailure::ValueMismatch(observed_sort_key_ids)) => {
                        // Another ingester concurrently updated the sort
                        // key.
                        //
                        // This breaks a sort-key update invariant - sort
                        // key updates MUST be serialised. This operation must
                        // be retried.
                        //
                        // See:
                        //   https://github.com/influxdata/influxdb_iox/issues/6439
                        //
                        warn!(
                            %partition_id,
                            ?old_sort_key_ids,
                            ?observed_sort_key_ids,
                            update_sort_key_ids=?new_sort_key_ids,
                            "detected concurrent sort key update"
                        );
                        // Stop the retry loop with an error containing the
                        // newly observed sort key.
                        ControlFlow::Break(Err(observed_sort_key_ids))
                    }
                }
            }
        })
        .await
        .expect("retry forever")
}

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
        validate_mutable_batch(batch, table_name, &mut schema, repos).await?;
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
) -> Result<(), TableScopedError>
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
                    .await
                    .map_err(|e| TableScopedError(table_name.to_string(), e))?;

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
    validate_and_insert_columns(
        mb.columns()
            .map(|(name, col)| (name, col.influx_type().into())),
        table_name,
        &mut table,
        repos,
    )
    .await?;

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

/// Given an iterator of `(column_name, column_type)` to validate, this function ensures all the
/// columns match the existing `TableSchema` in `table`. If the column does not already exist in
/// `table`, it is created and the `table` is changed to the `Cow::Owned` variant.
///
/// This function pushes schema additions through to the backend catalog, and relies on the catalog
/// to serialize concurrent additions of a given column, ensuring only one type is ever accepted
/// per column.
// &mut Cow is used to avoid a copy, so allow it
#[allow(clippy::ptr_arg)]
pub async fn validate_and_insert_columns<R>(
    columns: impl Iterator<Item = (&String, ColumnType)> + Send,
    table_name: &str,
    table: &mut Cow<'_, TableSchema>,
    repos: &mut R,
) -> Result<(), TableScopedError>
where
    R: RepoCollection + ?Sized,
{
    let mut column_batch: HashMap<&str, ColumnType> = HashMap::new();

    for (name, column_type) in columns {
        // Check if the column exists in the cached schema.
        //
        // If it does, validate it. If it does not exist, create it and insert
        // it into the cached schema.

        match table.columns.get(name.as_str()) {
            Some(existing) if existing.column_type == column_type => {
                // No action is needed as the column matches the existing column
                // schema.
            }
            Some(existing) => {
                // The column schema and the column in the schema change are of
                // different types.
                return Err(TableScopedError(
                    table_name.to_string(),
                    Error::AlreadyExists {
                        descr: format!(
                            "column {} is type {} but schema update has type {}",
                            name, existing.column_type, column_type
                        ),
                    },
                ));
            }
            None => {
                // The column does not exist in the cache, add it to the column
                // batch to be bulk inserted later.
                let old = column_batch.insert(name.as_str(), column_type);
                assert!(
                    old.is_none(),
                    "duplicate column name `{name}` in new column schema shouldn't be possible"
                );
            }
        }
    }

    if !column_batch.is_empty() {
        repos
            .columns()
            .create_or_get_many_unchecked(table.id, column_batch)
            .await
            .map_err(|e| TableScopedError(table_name.to_string(), e))?
            .into_iter()
            .for_each(|c| table.to_mut().add_column(c));
    }

    Ok(())
}

/// Load or create table.
pub async fn table_load_or_create<R>(
    repos: &mut R,
    namespace_id: NamespaceId,
    namespace_partition_template: &NamespacePartitionTemplateOverride,
    table_name: &str,
) -> Result<TableSchema, Error>
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
            if let Err(Error::AlreadyExists { .. }) = create_result {
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

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use super::*;
    use crate::{interface::SoftDeletedRows, mem::MemCatalog, util::get_schema_by_name};

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
                    let time_provider = Arc::new(iox_time::SystemProvider::new());
                    let repo = MemCatalog::new(metrics, time_provider);
                    let mut txn = repo.repositories();

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
                                Err(TableScopedError(_, Error::AlreadyExists{ .. })) => {
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
                        .expect("database failed to query for namespace schema")
                        .expect("namespace exists");
                    assert_eq!(schema, db_schema, "schema in DB and cached schema differ");

                    // Generate the map of tables => desired column types
                    let want_tables: BTreeMap<String, BTreeMap<Arc<str>, ColumnType>> = test_validate_schema!(@table, $($want_schema)+);

                    // Generate a similarly structured map from the actual
                    // schema
                    let actual_tables: BTreeMap<String, BTreeMap<Arc<str>, ColumnType>> = schema
                        .tables
                        .iter()
                        .map(|(table, table_schema)| {
                            let desired_cols = table_schema
                                .columns
                                .iter()
                                .map(|(column, column_schema)| (Arc::clone(&column), column_schema.column_type))
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
                assert!(cols.insert(Arc::from($col_name), $col_type).is_none());
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

    #[tokio::test]
    async fn validate_table_create_race_doesnt_get_all_columns() {
        use crate::{interface::Catalog, test_helpers::arbitrary_namespace};
        use std::{collections::BTreeSet, ops::DerefMut};
        const NAMESPACE_NAME: &str = "bananas";

        let repo = MemCatalog::new(
            Default::default(),
            Arc::new(iox_time::SystemProvider::new()),
        );
        let mut txn = repo.repositories();
        let namespace = arbitrary_namespace(&mut *txn, NAMESPACE_NAME).await;

        // One cached schema has no tables.
        let empty_schema = NamespaceSchema::new_empty_from(&namespace);

        // Another cached schema gets a write that creates a table with some columns.
        let schema_with_table = empty_schema.clone();
        let writes = mutable_batch_lp::lines_to_batches("m1,t1=a f1=2i", 42).unwrap();
        validate_or_insert_schema(
            writes.iter().map(|(k, v)| (k.as_str(), v)),
            &schema_with_table,
            txn.deref_mut(),
        )
        .await
        .unwrap();

        // then the empty schema adds the same table with some different columns
        let other_writes = mutable_batch_lp::lines_to_batches("m1,t2=a f2=2i", 43).unwrap();
        let formerly_empty_schema = validate_or_insert_schema(
            other_writes.iter().map(|(k, v)| (k.as_str(), v)),
            &empty_schema,
            txn.deref_mut(),
        )
        .await
        .unwrap()
        .unwrap();

        // the formerly-empty schema should NOT have all the columns; schema convergence is handled
        // at a higher level by the namespace cache/gossip system
        let table = formerly_empty_schema.tables.get("m1").unwrap();
        assert_eq!(table.columns.names(), BTreeSet::from(["t2", "f2", "time"]));
    }
}
