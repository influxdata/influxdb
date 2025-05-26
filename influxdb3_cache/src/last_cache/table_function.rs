use std::{any::Any, sync::Arc};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableFunctionImpl, TableProvider},
    common::{DFSchema, internal_err, plan_err},
    datasource::TableType,
    error::DataFusionError,
    execution::context::ExecutionProps,
    logical_expr::TableProviderFilterPushDown,
    physical_expr::{
        create_physical_expr,
        utils::{Guarantee, LiteralGuarantee},
    },
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, memory::MemoryExec},
    prelude::Expr,
    scalar::ScalarValue,
};
use indexmap::{IndexMap, IndexSet};
use influxdb3_catalog::catalog::{DatabaseSchema, TableDefinition};
use influxdb3_id::{ColumnId, DbId, LastCacheId};
use schema::{InfluxColumnType, InfluxFieldType};

use super::{
    LastCacheProvider,
    cache::{KeyValue, Predicate},
};

/// The name of the function that is called to query the last cache
pub const LAST_CACHE_UDTF_NAME: &str = "last_cache";

/// Implementor of the [`TableProvider`] trait that is produced with a call to the
/// [`LastCacheFunction`]
#[derive(Debug)]
struct LastCacheFunctionProvider {
    /// The database schema that the query is associated with
    db_schema: Arc<DatabaseSchema>,
    /// The table definition that the cache being called is associated with
    table_def: Arc<TableDefinition>,
    /// The id of the cache
    cache_id: LastCacheId,
    /// Reference to the cache's schema
    schema: SchemaRef,
    /// Forwarded reference of the [`LastCacheProvider`], which is used to get the `LastCache`
    /// for the query using the `db_id` and `table_def`.
    provider: Arc<LastCacheProvider>,
}

#[async_trait]
impl TableProvider for LastCacheFunctionProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut recorder = self
            .provider
            .metrics
            .query_duration_recorder(self.db_schema.name.to_string());
        let read = self.provider.cache_map.read();
        let (predicates, batches) = if let Some(cache) = read
            .get(&self.db_schema.id)
            .and_then(|db| db.get(&self.table_def.table_id))
            .and_then(|tbl| tbl.get(&self.cache_id))
        {
            let predicates = convert_filter_exprs(
                self.table_def.as_ref(),
                cache.key_column_ids.as_ref(),
                Arc::clone(&self.schema),
                filters,
            )?;
            let batches = cache.to_record_batches(Arc::clone(&self.table_def), &predicates)?;
            ((!predicates.is_empty()).then_some(predicates), batches)
        } else {
            // If there is no cache, it means that it was removed, in which case, we just return
            // an empty set of record batches.
            (None, vec![])
        };
        drop(read);
        let mut exec = LastCacheExec::try_new(
            predicates,
            Arc::clone(&self.table_def),
            &[batches],
            self.schema(),
            projection.cloned(),
        )?;
        recorder.set_success();

        let show_sizes = ctx.config_options().explain.show_sizes;
        exec = exec.with_show_sizes(show_sizes);

        Ok(Arc::new(exec))
    }
}

/// Convert the given list of filter expresions `filters` to a map of [`ColumnId`] to [`Predicate`]
///
/// The resulting map is an [`IndexMap`] to ensure consistent ordering of entries in the map, which
/// makes testing the filter conversions easier via `EXPLAIN` query plans.
fn convert_filter_exprs(
    table_def: &TableDefinition,
    cache_key_column_ids: &IndexSet<ColumnId>,
    cache_schema: SchemaRef,
    filters: &[Expr],
) -> Result<IndexMap<ColumnId, Predicate>, DataFusionError> {
    let mut predicate_map: IndexMap<ColumnId, Option<Predicate>> = IndexMap::new();

    // used by `create_physical_expr` in the loop below:
    let schema: DFSchema = cache_schema.try_into()?;
    let props = ExecutionProps::new();

    // The set of `filters` that are passed in from DataFusion varies: 1) based on how they are
    // defined in the query, and 2) based on some decisions that DataFusion makes when parsing the
    // query into the `Expr` syntax tree. For example, the predicate:
    //
    // WHERE foo IN ('bar', 'baz')
    //
    // instead of being expressed as an `InList`, would be simplified to the following `Expr` tree:
    //
    // [
    //     BinaryExpr {
    //         left: BinaryExpr { left: "foo", op: Eq, right: "bar" },
    //         op: Or,
    //         right: BinaryExpr { left: "foo", op: Eq, right: "baz" }
    //     }
    // ]
    //
    // while the predicate:
    //
    // WHERE foo = 'bar' OR foo = 'baz' OR foo = 'bop' OR foo = 'bla'
    //
    // instead of being expressed as a tree of `BinaryExpr`s, is expressed as an `InList` with four
    // entries:
    //
    // [
    //     InList { col: "foo", values: ["bar", "baz", "bop", "bla"], negated: false }
    // ]
    //
    // Instead of handling all the combinations of `Expr`s that may be passed by the caller of
    // `TableProider::scan`, we can use the cache's schema to convert each `Expr` to a `PhysicalExpr`
    // and analyze it using DataFusion's `LiteralGuarantee`.
    //
    // This will distill the provided set of `Expr`s down to either an IN list, or a NOT IN list
    // which we can convert to the `Predicate` type for the lastcache.
    //
    // Special handling is taken for the case where multiple literal guarantees are encountered for
    // a given column. This would happen for clauses split with an AND conjunction. From the tests
    // run thusfar, this happens when a query contains a WHERE clause, e.g.,
    //
    // WHERE a != 'foo' AND a != 'bar'
    //
    // or,
    //
    // WHERE a NOT IN ('foo', 'bar')
    //
    // which DataFusion simplifies to the previous clause that uses an AND binary expression.

    for expr in filters {
        let physical_expr = create_physical_expr(expr, &schema, &props)?;
        let literal_guarantees = LiteralGuarantee::analyze(&physical_expr);
        for LiteralGuarantee {
            column,
            guarantee,
            literals,
        } in literal_guarantees
        {
            let Some(column_def) = table_def.column_definition(column.name()) else {
                return plan_err!(
                    "invalid column name in filter expression: {}",
                    column.name()
                );
            };
            // do not handle predicates on non-key columns, let datafusion do that:
            if !cache_key_column_ids.contains(&column_def.id) {
                continue;
            }
            // convert the literal values from the query into `KeyValue`s for the last cache
            // predicate, and also validate that the literal type is compatible with the column
            // being predicated.
            let value_set = literals
                .into_iter()
                .map(|literal| match (literal, column_def.data_type) {
                    (
                        ScalarValue::Boolean(Some(b)),
                        InfluxColumnType::Field(InfluxFieldType::Boolean),
                    ) => Ok(KeyValue::Bool(b)),
                    (
                        ScalarValue::Int64(Some(i)),
                        InfluxColumnType::Field(InfluxFieldType::Integer),
                    ) => Ok(KeyValue::Int(i)),
                    (
                        ScalarValue::UInt64(Some(u)),
                        InfluxColumnType::Field(InfluxFieldType::UInteger),
                    ) => Ok(KeyValue::UInt(u)),
                    (
                        ScalarValue::Utf8(Some(s))
                        | ScalarValue::Utf8View(Some(s))
                        | ScalarValue::LargeUtf8(Some(s)),
                        InfluxColumnType::Tag | InfluxColumnType::Field(InfluxFieldType::String),
                    ) => Ok(KeyValue::String(s)),
                    // TODO: handle Dictionary here?
                    (other_literal, column_data_type) => {
                        plan_err!(
                            "incompatible literal applied in predicate to column, \
                            column: {}, \
                            literal: {other_literal}, \
                            column type: {column_data_type}",
                            column.name()
                        )
                    }
                })
                .collect::<Result<_, DataFusionError>>()?;
            let mut predicate = match guarantee {
                Guarantee::In => Predicate::In(value_set),
                Guarantee::NotIn => Predicate::NotIn(value_set),
            };
            // place the predicate into the map, handling the case for a column already encountered
            predicate_map
                .entry(column_def.id)
                .and_modify(|e| {
                    if let Some(existing) = e {
                        match (existing, &mut predicate) {
                            // if we encounter a IN predicate on a column for which we already have
                            // a IN guarantee, we take their intersection, i.e.,
                            //
                            // a IN (1, 2) AND a IN (2, 3)
                            //
                            // becomes
                            //
                            // a IN (2)
                            (Predicate::In(existing_set), Predicate::In(new_set)) => {
                                *existing_set =
                                    existing_set.intersection(new_set).cloned().collect();
                                // if the result is empty, just remove the predicate
                                if existing_set.is_empty() {
                                    e.take();
                                }
                            }
                            // if we encounter a NOT IN predicate on a column for which we already
                            // have a NOT IN guarantee, we extend the two, i.e.,
                            //
                            // a NOT IN (1, 2) AND a NOT IN (3, 4)
                            //
                            // becomes
                            //
                            // a NOT IN (1, 2, 3, 4)
                            (Predicate::NotIn(existing_set), Predicate::NotIn(new_set)) => {
                                existing_set.append(new_set)
                            }
                            // for non matching predicate types, we just remove by taking the
                            // Option. We will let DataFusion handle the predicate at a higher
                            // filter level in this case...
                            _ => {
                                e.take();
                            }
                        }
                    }
                })
                .or_insert_with(|| Some(predicate));
        }
    }

    Ok(predicate_map
        .into_iter()
        .filter_map(|(column_id, predicate)| predicate.map(|predicate| (column_id, predicate)))
        .collect())
}

/// Implementor of the [`TableFunctionImpl`] trait, to be registered as a user-defined table
/// function in the DataFusion `SessionContext`.
#[derive(Debug)]
pub struct LastCacheFunction {
    db_id: DbId,
    provider: Arc<LastCacheProvider>,
}

impl LastCacheFunction {
    pub fn new(db_id: DbId, provider: Arc<LastCacheProvider>) -> Self {
        Self { db_id, provider }
    }
}

impl TableFunctionImpl for LastCacheFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let Some(Expr::Literal(ScalarValue::Utf8(Some(table_name)))) = args.first() else {
            return plan_err!("first argument must be the table name as a string");
        };

        let cache_name = match args.get(1) {
            Some(Expr::Literal(ScalarValue::Utf8(Some(name)))) => Some(name),
            Some(_) => {
                return plan_err!("second argument, if passed, must be the cache name as a string");
            }
            None => None,
        };
        let db_schema = self
            .provider
            .catalog
            .db_schema_by_id(&self.db_id)
            .expect("db exists");
        let Some(table_def) = db_schema.table_definition(table_name.as_str()) else {
            return plan_err!("provided table name is invalid");
        };
        let Some(cache) = (match cache_name {
            Some(name) => table_def.last_caches.get_by_name(name),
            None => {
                if table_def.last_caches.len() == 1 {
                    table_def.last_caches.resource_iter().next().cloned()
                } else {
                    None
                }
            }
        }) else {
            return plan_err!("could not find cache for the given arguments");
        };

        let Some(schema) =
            self.provider
                .get_cache_schema(&self.db_id, &table_def.table_id, &cache.id)
        else {
            return internal_err!("last cache crate is invalid");
        };

        Ok(Arc::new(LastCacheFunctionProvider {
            db_schema,
            table_def,
            cache_id: cache.id,
            schema,
            provider: Arc::clone(&self.provider),
        }))
    }
}

/// Custom implementor of the [`ExecutionPlan`] trait for use by the last cache
///
/// Wraps a [`MemoryExec`] from DataFusion which it relies on for the actual implementation of the
/// [`ExecutionPlan`] trait. The additional functionality provided by this type is that it tracks
/// the predicates that are pushed down to the underlying cache during query planning/execution.
///
/// # Example
///
/// For a query that does not provide any predicates, or one that does provide predicates, but they
/// do not get pushed down, the `EXPLAIN` for said query will contain a line for the `LastCacheExec`
/// with no predicates, as well as the info emitted for the inner `MemoryExec`, e.g.,
///
/// ```text
/// LastCacheExec: inner=MemoryExec: partitions=1, partition_sizes=[12]
/// ```
///
/// For queries that do have predicates that get pushed down, the output will include them, e.g.,
///
/// ```text
/// LastCacheExec: predicates=[[region@0 IN ('us-east','us-west')]] inner=[...]
/// ```
#[derive(Debug)]
struct LastCacheExec {
    inner: MemoryExec,
    table_def: Arc<TableDefinition>,
    predicates: Option<IndexMap<ColumnId, Predicate>>,
}

impl LastCacheExec {
    fn try_new(
        predicates: Option<IndexMap<ColumnId, Predicate>>,
        table_def: Arc<TableDefinition>,
        partitions: &[Vec<RecordBatch>],
        cache_schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self, DataFusionError> {
        Ok(Self {
            inner: MemoryExec::try_new(partitions, cache_schema, projection)?,
            table_def,
            predicates,
        })
    }

    fn with_show_sizes(self, show_sizes: bool) -> Self {
        Self {
            inner: self.inner.with_show_sizes(show_sizes),
            ..self
        }
    }
}

impl DisplayAs for LastCacheExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "LastCacheExec:")?;
                if let Some(predicates) = self.predicates.as_ref() {
                    write!(f, " predicates=[")?;
                    let mut p_iter = predicates.iter();
                    while let Some((col_id, predicate)) = p_iter.next() {
                        let col_name = self.table_def.column_id_to_name(col_id).unwrap_or_default();
                        write!(f, "[{col_name}@{col_id} {predicate}]")?;
                        if p_iter.size_hint().0 > 0 {
                            write!(f, ", ")?;
                        }
                    }
                    write!(f, "]")?;
                }
                write!(f, " inner=")?;
                self.inner.fmt_as(t, f)
            }
        }
    }
}

impl ExecutionPlan for LastCacheExec {
    fn name(&self) -> &str {
        "LastCacheExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // (copied from MemoryExec):
        // MemoryExec has no children
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Children cannot be replaced in {self:?}")
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
}
