use std::{ops::DerefMut, sync::Arc};

use async_trait::async_trait;
use data_types::{DeletePredicate, NamespaceId, NamespaceName, NamespaceSchema, TableId};
use hashbrown::HashMap;
use iox_catalog::{
    interface::{get_schema_by_name, Catalog, Error as CatalogError, SoftDeletedRows},
    validate_or_insert_schema,
};
use metric::U64Counter;
use mutable_batch::MutableBatch;
use observability_deps::tracing::*;
use thiserror::Error;
use trace::ctx::SpanContext;

use super::DmlHandler;
use crate::namespace_cache::{metrics::InstrumentedCache, MemoryNamespaceCache, NamespaceCache};

/// Errors emitted during schema validation.
#[derive(Debug, Error)]
pub enum SchemaError {
    /// The requested namespace could not be found in the catalog.
    #[error("failed to read namespace schema from catalog: {0}")]
    NamespaceLookup(iox_catalog::interface::Error),

    /// The user has hit their column/table limit.
    #[error("service limit reached: {0}")]
    ServiceLimit(Box<dyn std::error::Error + Send + Sync + 'static>),

    /// The request schema conflicts with the existing namespace schema.
    #[error("schema conflict: {0}")]
    Conflict(iox_catalog::TableScopedError),

    /// A catalog error during schema validation.
    ///
    /// NOTE: this may be due to transient I/O errors while interrogating the
    /// global catalog - the caller should inspect the inner error to determine
    /// the failure reason.
    #[error(transparent)]
    UnexpectedCatalogError(iox_catalog::interface::Error),
}

/// A [`SchemaValidator`] checks the schema of incoming writes against a
/// centralised schema store, maintaining an in-memory cache of all observed
/// schemas.
///
/// # Caching
///
/// This validator attempts to incrementally build an in-memory cache of all
/// table schemas it observes (never evicting the schemas).
///
/// All schema operations are scoped to a single namespace.
///
/// When a request contains columns that do not exist in the cached schema, the
/// catalog is queried for an existing column by the same name and (atomically)
/// the column is created if it does not exist. If the requested column type
/// conflicts with the catalog column, the request is rejected and the cached
/// schema is not updated.
///
/// Any successful write that adds new columns causes the new schema to be
/// cached.
///
/// To minimise locking, this cache is designed to allow (and tolerate) spurious
/// cache "updates" racing with each other and overwriting newer schemas with
/// older schemas. This is acceptable due to the incremental, additive schema
/// creation never allowing a column to change or be removed, therefore columns
/// lost by racy schema cache overwrites are "discovered" in subsequent
/// requests. This overwriting is scoped to the namespace, and is expected to be
/// relatively rare - it results in additional requests being made to the
/// catalog until the cached schema converges to match the catalog schema.
///
/// Note that the namespace-wide limit of the number of columns allowed per table
/// is also cached, which has two implications:
///
/// 1. If the namespace's column limit is updated in the catalog, the new limit
///    will not be enforced until the whole namespace is recached, likely only
///    on startup. In other words, updating the namespace's column limit requires
///    both a catalog update and service restart.
/// 2. There's a race condition that can result in a table ending up with more
///    columns than the namespace limit should allow. When multiple concurrent
///    writes come in to different service instances that each have their own
///    cache, and each of those writes add a disjoint set of new columns, the
///    requests will all succeed because when considered separately, they do
///    not exceed the number of columns in the cache. Once all the writes have
///    completed, the total set of columns in the table will be some multiple
///    of the limit.
///
/// # Correctness
///
/// The correct functioning of this schema validator relies on the catalog
/// implementation correctly validating new column requests against any existing
/// (but uncached) columns, returning an error if the requested type does not
/// match.
///
/// The catalog must serialize column creation to avoid `set(a=tag)` overwriting
/// a prior `set(a=int)` write to the catalog (returning an error that `a` is an
/// `int` for the second request).
///
/// Because each column is set incrementally and independently of other new
/// columns in the request, racing multiple requests for the same table can
/// produce incorrect schemas ([#3573]).
///
/// [#3573]: https://github.com/influxdata/influxdb_iox/issues/3573
#[derive(Debug)]
pub struct SchemaValidator<C = Arc<InstrumentedCache<MemoryNamespaceCache>>> {
    catalog: Arc<dyn Catalog>,
    cache: C,

    service_limit_hit_tables: U64Counter,
    service_limit_hit_columns: U64Counter,
    schema_conflict: U64Counter,
}

impl<C> SchemaValidator<C> {
    /// Initialise a new [`SchemaValidator`] decorator, loading schemas from
    /// `catalog`.
    ///
    /// Schemas are cached in `ns_cache`.
    pub fn new(catalog: Arc<dyn Catalog>, ns_cache: C, metrics: &metric::Registry) -> Self {
        let service_limit_hit = metrics.register_metric::<U64Counter>(
            "schema_validation_service_limit_reached",
            "number of requests that have hit the namespace table/column limit",
        );
        let service_limit_hit_tables = service_limit_hit.recorder(&[("limit", "table")]);
        let service_limit_hit_columns = service_limit_hit.recorder(&[("limit", "column")]);

        let schema_conflict = metrics
            .register_metric::<U64Counter>(
                "schema_validation_schema_conflict",
                "number of requests that fail due to a schema conflict",
            )
            .recorder(&[]);

        Self {
            catalog,
            cache: ns_cache,
            service_limit_hit_tables,
            service_limit_hit_columns,
            schema_conflict,
        }
    }
}

#[async_trait]
impl<C> DmlHandler for SchemaValidator<C>
where
    C: NamespaceCache,
{
    type WriteError = SchemaError;
    type DeleteError = SchemaError;

    // Accepts a map of TableName -> MutableBatch
    type WriteInput = HashMap<String, MutableBatch>;
    // And returns a map of TableId -> (TableName, MutableBatch)
    type WriteOutput = HashMap<TableId, (String, MutableBatch)>;

    /// Validate the schema of all the writes in `batches`.
    ///
    /// # Errors
    ///
    /// If `namespace` does not exist, [`SchemaError::NamespaceLookup`] is
    /// returned.
    ///
    /// If the schema validation fails due to a schema conflict in the request,
    /// [`SchemaError::Conflict`] is returned.
    ///
    /// If the schema validation fails due to a service limit being reached,
    /// [`SchemaError::ServiceLimit`] is returned.
    ///
    /// A request that fails validation on one or more tables fails the request
    /// as a whole - calling this method has "all or nothing" semantics.
    async fn write(
        &self,
        namespace: &NamespaceName<'static>,
        namespace_id: NamespaceId,
        batches: Self::WriteInput,
        _span_ctx: Option<SpanContext>,
    ) -> Result<Self::WriteOutput, Self::WriteError> {
        let mut repos = self.catalog.repositories().await;

        // Load the namespace schema from the cache, falling back to pulling it
        // from the global catalog (if it exists).
        let schema = self.cache.get_schema(namespace);
        let schema = match schema {
            Some(v) => v,
            None => {
                // Pull the schema from the global catalog or error if it does
                // not exist.
                let schema = get_schema_by_name(
                    namespace,
                    repos.deref_mut(),
                    SoftDeletedRows::ExcludeDeleted,
                )
                .await
                .map_err(|e| {
                    warn!(
                        error=%e,
                        %namespace,
                        %namespace_id,
                        "failed to retrieve namespace schema"
                    );
                    SchemaError::NamespaceLookup(e)
                })
                .map(Arc::new)?;

                self.cache
                    .put_schema(namespace.clone(), Arc::clone(&schema));

                trace!(%namespace, "schema cache populated");
                schema
            }
        };

        validate_schema_limits(&batches, &schema).map_err(|e| {
            match &e {
                CachedServiceProtectionLimit::Column {
                    table_name,
                    existing_column_count,
                    merged_column_count,
                    max_columns_per_table,
                } => {
                    warn!(
                        %table_name,
                        %existing_column_count,
                        %merged_column_count,
                        %max_columns_per_table,
                        %namespace,
                        %namespace_id,
                        "service protection limit reached (columns)"
                    );
                    self.service_limit_hit_columns.inc(1);
                }
                CachedServiceProtectionLimit::Table {
                    existing_table_count,
                    merged_table_count,
                    table_count_limit,
                } => {
                    warn!(
                        %existing_table_count,
                        %merged_table_count,
                        %table_count_limit,
                        %namespace,
                        %namespace_id,
                        "service protection limit reached (tables)"
                    );
                    self.service_limit_hit_tables.inc(1);
                }
            }
            SchemaError::ServiceLimit(Box::new(e))
        })?;

        let maybe_new_schema = validate_or_insert_schema(
            batches.iter().map(|(k, v)| (k.as_str(), v)),
            &schema,
            repos.deref_mut(),
        )
        .await
        .map_err(|e| {
            match e.err() {
                // Schema conflicts
                CatalogError::ColumnTypeMismatch {
                    ref name,
                    ref existing,
                    ref new,
                } => {
                    warn!(
                        %namespace,
                        %namespace_id,
                        column_name=%name,
                        existing_column_type=%existing,
                        request_column_type=%new,
                        table_name=%e.table(),
                        "schema conflict"
                    );
                    self.schema_conflict.inc(1);
                    SchemaError::Conflict(e)
                }
                // Service limits
                CatalogError::ColumnCreateLimitError { table_id, .. } => {
                    warn!(
                        %namespace,
                        %namespace_id,
                        %table_id,
                        error=%e,
                        "service protection limit reached (columns)"
                    );
                    self.service_limit_hit_columns.inc(1);
                    SchemaError::ServiceLimit(Box::new(e.into_err()))
                }
                CatalogError::TableCreateLimitError { .. } => {
                    warn!(
                        %namespace,
                        %namespace_id,
                        error=%e,
                        "service protection limit reached (tables)"
                    );
                    self.service_limit_hit_tables.inc(1);
                    SchemaError::ServiceLimit(Box::new(e.into_err()))
                }
                _ => {
                    error!(
                        %namespace,
                        %namespace_id,
                        error=%e,
                        "schema validation failed"
                    );
                    SchemaError::UnexpectedCatalogError(e.into_err())
                }
            }
        })?
        .map(Arc::new);

        trace!(%namespace, "schema validation complete");

        // If the schema has been updated, immediately add it to the cache
        // (before passing through the write) in order to allow subsequent,
        // parallel requests to use it while waiting on this request to
        // complete.
        let latest_schema = match maybe_new_schema {
            Some(v) => {
                // This call MAY overwrite a more-up-to-date cache entry if
                // racing with another request for the same namespace, but the
                // cache will eventually converge in subsequent requests.
                self.cache.put_schema(namespace.clone(), Arc::clone(&v));
                trace!(%namespace, "schema cache updated");
                v
            }
            None => {
                trace!(%namespace, "schema unchanged");
                schema
            }
        };

        // Map the "TableName -> Data" into "TableId -> (TableName, Data)" for
        // downstream handlers.
        let batches = batches
            .into_iter()
            .map(|(name, data)| {
                let id = latest_schema.tables.get(&name).unwrap().id;

                (id, (name, data))
            })
            .collect();

        Ok(batches)
    }

    /// This call is passed through to `D` - no schema validation is performed
    /// on deletes.
    async fn delete(
        &self,
        _namespace: &NamespaceName<'static>,
        _namespace_id: NamespaceId,
        _table_name: &str,
        _predicate: &DeletePredicate,
        _span_ctx: Option<SpanContext>,
    ) -> Result<(), Self::DeleteError> {
        Ok(())
    }
}

/// An error returned by schema limit evaluation against a cached
/// [`NamespaceSchema`].
#[derive(Debug, Error)]
pub enum CachedServiceProtectionLimit {
    /// The number of columns would exceed the table column limit cached in the
    /// [`NamespaceSchema`].
    #[error(
        "couldn't create columns in table `{table_name}`; table contains \
     {existing_column_count} existing columns, applying this write would result \
     in {merged_column_count} columns, limit is {max_columns_per_table}"
    )]
    Column {
        /// The table that exceeds the column limit.
        table_name: String,
        /// Number of columns already in the table.
        existing_column_count: usize,
        /// Number of resultant columns after merging the write with existing
        /// columns.
        merged_column_count: usize,
        /// The configured limit.
        max_columns_per_table: usize,
    },

    /// The number of table would exceed the table limit cached in the
    /// [`NamespaceSchema`].
    #[error(
        "couldn't create new table; namespace contains {existing_table_count} \
        existing tables, applying this write would result in \
        {merged_table_count} tables, limit is {table_count_limit}"
    )]
    Table {
        /// Number of tables already in the namespace.
        existing_table_count: usize,
        /// Number of resultant tables after merging the write with existing
        /// tables.
        merged_table_count: usize,
        /// The configured limit.
        table_count_limit: usize,
    },
}

/// Evaluate the number of columns/tables that would result if `batches` was
/// applied to `schema`, and ensure the column/table count does not exceed the
/// maximum permitted amount cached in the [`NamespaceSchema`].
fn validate_schema_limits(
    batches: &HashMap<String, MutableBatch>,
    schema: &NamespaceSchema,
) -> Result<(), CachedServiceProtectionLimit> {
    // Maintain a counter tracking the number of tables in `batches` that do not
    // exist in `schema`.
    //
    // This number of tables would be newly created when accepting the write.
    let mut new_tables = 0;

    for (table_name, batch) in batches {
        // Get the column set for this table from the schema.
        let mut existing_columns = match schema.tables.get(table_name) {
            Some(v) => v.column_names(),
            None if batch.columns().len() > schema.max_columns_per_table => {
                // The table does not exist, therefore all the columns in this
                // write must be created - there's no need to perform a set
                // union to discover the distinct column count.
                return Err(CachedServiceProtectionLimit::Column {
                    table_name: table_name.into(),
                    merged_column_count: batch.columns().len(),
                    existing_column_count: 0,
                    max_columns_per_table: schema.max_columns_per_table,
                });
            }
            None => {
                // The table must be created.
                new_tables += 1;

                // At least one new table will be created, ensure this does not
                // exceed the configured maximum.
                //
                // Enforcing the check here ensures table limits are validated
                // only when new tables are being created - this ensures
                // existing tables do not become unusable if the limit is
                // lowered, or because multiple writes were concurrently
                // submitted to multiple router instances, exceeding the schema
                // limit by some degree (eventual enforcement).
                let merged_table_count = schema.tables.len() + new_tables;
                if merged_table_count > schema.max_tables {
                    return Err(CachedServiceProtectionLimit::Table {
                        existing_table_count: schema.tables.len(),
                        merged_table_count,
                        table_count_limit: schema.max_tables,
                    });
                }

                // Therefore all the columns in this write are new, and they are
                // less than the maximum permitted number of columns.
                continue;
            }
        };

        // The union of existing columns and new columns in this write must be
        // calculated to derive the total distinct column count for this table
        // after this write applied.
        let existing_column_count = existing_columns.len();

        let merged_column_count = {
            existing_columns.append(&mut batch.column_names());
            existing_columns.len()
        };

        // If the table is currently over the column limit but this write only
        // includes existing columns and doesn't exceed the limit more, this is
        // allowed.
        let columns_were_added_in_this_batch = merged_column_count > existing_column_count;
        let column_limit_exceeded = merged_column_count > schema.max_columns_per_table;

        if columns_were_added_in_this_batch && column_limit_exceeded {
            return Err(CachedServiceProtectionLimit::Column {
                table_name: table_name.into(),
                merged_column_count,
                existing_column_count,
                max_columns_per_table: schema.max_columns_per_table,
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use data_types::{ColumnType, TimestampRange};
    use iox_tests::{TestCatalog, TestNamespace};
    use once_cell::sync::Lazy;

    use super::*;

    static NAMESPACE: Lazy<NamespaceName<'static>> = Lazy::new(|| "bananas".try_into().unwrap());

    #[tokio::test]
    async fn test_validate_column_limits() {
        let (catalog, namespace) = test_setup().await;

        namespace.update_column_limit(3).await;

        // Table not found in schema,
        {
            let schema = namespace.schema().await;
            // Columns under the limit is ok
            let batches = lp_to_writes("nonexistent val=42i 123456");
            assert!(validate_schema_limits(&batches, &schema).is_ok());
            // Columns over the limit is an error
            let batches = lp_to_writes("nonexistent,tag1=A,tag2=B val=42i 123456");
            assert_matches!(
                validate_schema_limits(&batches, &schema),
                Err(CachedServiceProtectionLimit::Column {
                    table_name: _,
                    existing_column_count: 0,
                    merged_column_count: 4,
                    max_columns_per_table: 3,
                })
            );
        }

        // Table exists but no columns in schema,
        {
            namespace.create_table("no_columns_in_schema").await;
            let schema = namespace.schema().await;
            // Columns under the limit is ok
            let batches = lp_to_writes("no_columns_in_schema val=42i 123456");
            assert!(validate_schema_limits(&batches, &schema).is_ok());
            // Columns over the limit is an error
            let batches = lp_to_writes("no_columns_in_schema,tag1=A,tag2=B val=42i 123456");
            assert_matches!(
                validate_schema_limits(&batches, &schema),
                Err(CachedServiceProtectionLimit::Column {
                    table_name: _,
                    existing_column_count: 0,
                    merged_column_count: 4,
                    max_columns_per_table: 3,
                })
            );
        }

        // Table exists with a column in the schema,
        {
            let table = namespace.create_table("i_got_columns").await;
            table.create_column("i_got_music", ColumnType::I64).await;
            let schema = namespace.schema().await;
            // Columns already existing is ok
            let batches = lp_to_writes("i_got_columns i_got_music=42i 123456");
            assert!(validate_schema_limits(&batches, &schema).is_ok());
            // Adding columns under the limit is ok
            let batches = lp_to_writes("i_got_columns,tag1=A i_got_music=42i 123456");
            assert!(validate_schema_limits(&batches, &schema).is_ok());
            // Adding columns over the limit is an error
            let batches = lp_to_writes("i_got_columns,tag1=A,tag2=B i_got_music=42i 123456");
            assert_matches!(
                validate_schema_limits(&batches, &schema),
                Err(CachedServiceProtectionLimit::Column {
                    table_name: _,
                    existing_column_count: 1,
                    merged_column_count: 4,
                    max_columns_per_table: 3,
                })
            );
        }

        // Table exists and is at the column limit,
        {
            let table = namespace.create_table("bananas").await;
            table.create_column("greatness", ColumnType::I64).await;
            table.create_column("tastiness", ColumnType::I64).await;
            table
                .create_column(schema::TIME_COLUMN_NAME, ColumnType::Time)
                .await;
            let schema = namespace.schema().await;
            // Columns already existing is allowed
            let batches = lp_to_writes("bananas greatness=42i 123456");
            assert!(validate_schema_limits(&batches, &schema).is_ok());
            // Adding columns over the limit is an error
            let batches = lp_to_writes("bananas i_got_music=42i 123456");
            assert_matches!(
                validate_schema_limits(&batches, &schema),
                Err(CachedServiceProtectionLimit::Column {
                    table_name: _,
                    existing_column_count: 3,
                    merged_column_count: 4,
                    max_columns_per_table: 3,
                })
            );
        }

        // Table exists and is over the column limit because of the race condition,
        {
            // Make two schema validator instances each with their own cache
            let handler1 = SchemaValidator::new(
                catalog.catalog(),
                Arc::new(MemoryNamespaceCache::default()),
                &catalog.metric_registry,
            );
            let handler2 = SchemaValidator::new(
                catalog.catalog(),
                Arc::new(MemoryNamespaceCache::default()),
                &catalog.metric_registry,
            );

            // Make a valid write with one column + timestamp through each validator so the
            // namespace schema gets cached
            let writes1_valid = lp_to_writes("dragonfruit val=42i 123456");
            handler1
                .write(&NAMESPACE, NamespaceId::new(42), writes1_valid, None)
                .await
                .expect("request should succeed");
            let writes2_valid = lp_to_writes("dragonfruit val=43i 123457");
            handler2
                .write(&NAMESPACE, NamespaceId::new(42), writes2_valid, None)
                .await
                .expect("request should succeed");

            // Make "valid" writes through each validator that each add a different column, thus
            // putting the table over the limit
            let writes1_add_column = lp_to_writes("dragonfruit,tag1=A val=42i 123456");
            handler1
                .write(&NAMESPACE, NamespaceId::new(42), writes1_add_column, None)
                .await
                .expect("request should succeed");
            let writes2_add_column = lp_to_writes("dragonfruit,tag2=B val=43i 123457");
            handler2
                .write(&NAMESPACE, NamespaceId::new(42), writes2_add_column, None)
                .await
                .expect("request should succeed");

            let schema = namespace.schema().await;

            // Columns already existing is allowed
            let batches = lp_to_writes("dragonfruit val=42i 123456");
            assert!(validate_schema_limits(&batches, &schema).is_ok());
            // Adding more columns over the limit is an error
            let batches = lp_to_writes("dragonfruit i_got_music=42i 123456");
            assert_matches!(
                validate_schema_limits(&batches, &schema),
                Err(CachedServiceProtectionLimit::Column {
                    table_name: _,
                    existing_column_count: 4,
                    merged_column_count: 5,
                    max_columns_per_table: 3,
                })
            );
        }
    }

    #[tokio::test]
    async fn test_validate_table_limits() {
        let (_catalog, namespace) = test_setup().await;

        namespace.update_table_limit(2).await;

        // Creating a table in an empty namespace is OK
        {
            let schema = namespace.schema().await;
            let batches = lp_to_writes("nonexistent val=42i 123456");
            assert!(validate_schema_limits(&batches, &schema).is_ok());
        }

        // Creating two tables (the limit) is OK
        {
            let schema = namespace.schema().await;
            let batches = lp_to_writes("nonexistent val=42i 123456\nbananas val=2 42");
            assert!(validate_schema_limits(&batches, &schema).is_ok());
        }

        // Creating three tables (above the limit) fails
        {
            let schema = namespace.schema().await;
            let batches =
                lp_to_writes("nonexistent val=42i 123456\nbananas val=2 42\nplatanos val=2 42");
            assert_matches!(
                validate_schema_limits(&batches, &schema),
                Err(CachedServiceProtectionLimit::Table {
                    existing_table_count: 0,
                    merged_table_count: 3,
                    table_count_limit: 2
                })
            );
        }

        // Create a table to cover non-empty namespaces
        namespace.create_table("bananas").await;

        // Adding a second table is OK
        {
            let schema = namespace.schema().await;
            let batches = lp_to_writes("bananas val=2 42\nplatanos val=2 42");
            assert!(validate_schema_limits(&batches, &schema).is_ok());
        }

        // Adding a third table is rejected OK
        {
            let schema = namespace.schema().await;
            let batches = lp_to_writes("bananas val=2 42\nplatanos val=2 42\nnope v=2 42");
            assert_matches!(
                validate_schema_limits(&batches, &schema),
                Err(CachedServiceProtectionLimit::Table {
                    existing_table_count: 1,
                    merged_table_count: 3,
                    table_count_limit: 2
                })
            );
        }

        // Create another table and reduce the table limit to be less than the
        // current number of tables.
        //
        // Multiple router instances can race to populate the catalog with new
        // tables/columns, therefore all existing tables MUST be accepted to
        // ensure deterministic enforcement once all caches have converged.
        namespace.create_table("platanos").await;
        namespace.update_table_limit(1).await;

        // The existing tables are accepted, even though this single write
        // exceeds the new table limit.
        {
            let schema = namespace.schema().await;
            assert_eq!(schema.tables.len(), 2);
            assert_eq!(schema.max_tables, 1);

            let batches = lp_to_writes("bananas val=2 42\nplatanos val=2 42");
            assert_matches!(validate_schema_limits(&batches, &schema), Ok(()));
        }

        // A new table is always rejected.
        {
            let schema = namespace.schema().await;
            let batches = lp_to_writes("bananas val=2 42\nplatanos val=2 42\nnope v=1 42");
            assert_matches!(
                validate_schema_limits(&batches, &schema),
                Err(CachedServiceProtectionLimit::Table {
                    existing_table_count: 2,
                    merged_table_count: 3,
                    table_count_limit: 1,
                })
            );
        }
        {
            let schema = namespace.schema().await;
            let batches = lp_to_writes("bananas val=2 42\nnope v=1 42");
            assert_matches!(
                validate_schema_limits(&batches, &schema),
                Err(CachedServiceProtectionLimit::Table {
                    existing_table_count: 2,
                    merged_table_count: 3,
                    table_count_limit: 1,
                })
            );
        }
        {
            let schema = namespace.schema().await;
            let batches = lp_to_writes("nope v=1 42");
            assert_matches!(
                validate_schema_limits(&batches, &schema),
                Err(CachedServiceProtectionLimit::Table {
                    existing_table_count: 2,
                    merged_table_count: 3,
                    table_count_limit: 1,
                })
            );
        }
    }

    // Parse `lp` into a table-keyed MutableBatch map.
    fn lp_to_writes(lp: &str) -> HashMap<String, MutableBatch> {
        let (writes, _) = mutable_batch_lp::lines_to_batches_stats(lp, 42)
            .expect("failed to build test writes from LP");
        writes
    }

    /// Initialise an in-memory [`MemCatalog`] and create a single namespace
    /// named [`NAMESPACE`].
    async fn test_setup() -> (Arc<TestCatalog>, Arc<TestNamespace>) {
        let catalog = TestCatalog::new();
        let namespace = catalog.create_namespace_1hr_retention(&NAMESPACE).await;

        (catalog, namespace)
    }

    fn assert_cache<C>(handler: &SchemaValidator<C>, table: &str, col: &str, want: ColumnType)
    where
        C: NamespaceCache,
    {
        // The cache should be populated.
        let ns = handler
            .cache
            .get_schema(&NAMESPACE)
            .expect("cache should be populated");
        let table = ns.tables.get(table).expect("table should exist in cache");
        assert_eq!(
            table
                .columns
                .get(col)
                .expect("column not cached")
                .column_type,
            want
        );
    }

    #[tokio::test]
    async fn test_write_ok() {
        let (catalog, namespace) = test_setup().await;

        // Create the table so that there is a known ID that must be returned.
        let want_id = namespace.create_table("bananas").await.table.id;

        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(
            catalog.catalog(),
            Arc::new(MemoryNamespaceCache::default()),
            &metrics,
        );

        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456");
        let got = handler
            .write(&NAMESPACE, NamespaceId::new(42), writes, None)
            .await
            .expect("request should succeed");

        // The cache should be populated.
        assert_cache(&handler, "bananas", "tag1", ColumnType::Tag);
        assert_cache(&handler, "bananas", "tag2", ColumnType::Tag);
        assert_cache(&handler, "bananas", "val", ColumnType::I64);
        assert_cache(&handler, "bananas", "time", ColumnType::Time);

        // Validate the table ID mapping.
        let (name, _data) = got.get(&want_id).expect("table not in output");
        assert_eq!(name, "bananas");
    }

    #[tokio::test]
    async fn test_write_schema_not_found() {
        let (catalog, _namespace) = test_setup().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(
            catalog.catalog(),
            Arc::new(MemoryNamespaceCache::default()),
            &metrics,
        );

        let ns = NamespaceName::try_from("A_DIFFERENT_NAMESPACE").unwrap();

        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456");
        let err = handler
            .write(&ns, NamespaceId::new(42), writes, None)
            .await
            .expect_err("request should fail");

        assert_matches!(err, SchemaError::NamespaceLookup(_));

        // The cache should not have retained the schema.
        assert!(handler.cache.get_schema(&ns).is_none());
    }

    #[tokio::test]
    async fn test_write_validation_failure() {
        let (catalog, _namespace) = test_setup().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(
            catalog.catalog(),
            Arc::new(MemoryNamespaceCache::default()),
            &metrics,
        );

        // First write sets the schema
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456"); // val=i64
        let got = handler
            .write(&NAMESPACE, NamespaceId::new(42), writes.clone(), None)
            .await
            .expect("request should succeed");
        assert_eq!(writes.len(), got.len());

        // Second write attempts to violate it causing an error
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42.0 123456"); // val=float
        let err = handler
            .write(&NAMESPACE, NamespaceId::new(42), writes, None)
            .await
            .expect_err("request should fail");

        assert_matches!(err, SchemaError::Conflict(e) => {
            assert_eq!(e.table(), "bananas");
        });

        // The cache should retain the original schema.
        assert_cache(&handler, "bananas", "tag1", ColumnType::Tag);
        assert_cache(&handler, "bananas", "tag2", ColumnType::Tag);
        assert_cache(&handler, "bananas", "val", ColumnType::I64); // original type
        assert_cache(&handler, "bananas", "time", ColumnType::Time);

        assert_eq!(1, handler.schema_conflict.fetch());
    }

    #[tokio::test]
    async fn test_write_table_service_limit() {
        let (catalog, _namespace) = test_setup().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(
            catalog.catalog(),
            Arc::new(MemoryNamespaceCache::default()),
            &metrics,
        );

        // First write sets the schema
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456");
        let got = handler
            .write(&NAMESPACE, NamespaceId::new(42), writes.clone(), None)
            .await
            .expect("request should succeed");
        assert_eq!(writes.len(), got.len());

        // Configure the service limit to be hit next request
        catalog
            .catalog()
            .repositories()
            .await
            .namespaces()
            .update_table_limit(NAMESPACE.as_str(), 1)
            .await
            .expect("failed to set table limit");

        // Second write attempts to violate limits, causing an error
        let writes = lp_to_writes("bananas2,tag1=A,tag2=B val=42i 123456");
        let err = handler
            .write(&NAMESPACE, NamespaceId::new(42), writes, None)
            .await
            .expect_err("request should fail");

        assert_matches!(err, SchemaError::ServiceLimit(_));
        assert_eq!(1, handler.service_limit_hit_tables.fetch());
    }

    #[tokio::test]
    async fn test_write_column_service_limit() {
        let (catalog, namespace) = test_setup().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(
            catalog.catalog(),
            Arc::new(MemoryNamespaceCache::default()),
            &metrics,
        );

        // First write sets the schema
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i 123456");
        let got = handler
            .write(&NAMESPACE, NamespaceId::new(42), writes.clone(), None)
            .await
            .expect("request should succeed");
        assert_eq!(writes.len(), got.len());

        // Configure the service limit to be hit next request
        namespace.update_column_limit(1).await;
        let handler = SchemaValidator::new(
            catalog.catalog(),
            Arc::new(MemoryNamespaceCache::default()),
            &metrics,
        );

        // Second write attempts to violate limits, causing an error
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i,val2=42i 123456");
        let err = handler
            .write(&NAMESPACE, NamespaceId::new(42), writes, None)
            .await
            .expect_err("request should fail");

        assert_matches!(err, SchemaError::ServiceLimit(_));
        assert_eq!(1, handler.service_limit_hit_columns.fetch());
    }

    #[tokio::test]
    async fn test_first_write_many_columns_service_limit() {
        let (catalog, _namespace) = test_setup().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(
            catalog.catalog(),
            Arc::new(MemoryNamespaceCache::default()),
            &metrics,
        );

        // Configure the service limit to be hit next request
        catalog
            .catalog()
            .repositories()
            .await
            .namespaces()
            .update_column_limit(NAMESPACE.as_str(), 3)
            .await
            .expect("failed to set column limit");

        // First write attempts to add columns over the limit, causing an error
        let writes = lp_to_writes("bananas,tag1=A,tag2=B val=42i,val2=42i 123456");
        let err = handler
            .write(&NAMESPACE, NamespaceId::new(42), writes, None)
            .await
            .expect_err("request should fail");

        assert_matches!(err, SchemaError::ServiceLimit(_));
        assert_eq!(1, handler.service_limit_hit_columns.fetch());
    }

    #[tokio::test]
    async fn test_write_delete_passthrough_ok() {
        const NAMESPACE: &str = "NAMESPACE_IS_NOT_VALIDATED";
        const TABLE: &str = "bananas";

        let (catalog, _namespace) = test_setup().await;
        let metrics = Arc::new(metric::Registry::default());
        let handler = SchemaValidator::new(
            catalog.catalog(),
            Arc::new(MemoryNamespaceCache::default()),
            &metrics,
        );

        let predicate = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };

        let ns = NamespaceName::try_from(NAMESPACE).unwrap();

        handler
            .delete(&ns, NamespaceId::new(42), TABLE, &predicate, None)
            .await
            .expect("request should succeed");

        // Deletes have no effect on the cache.
        assert!(handler.cache.get_schema(&ns).is_none());
    }
}
