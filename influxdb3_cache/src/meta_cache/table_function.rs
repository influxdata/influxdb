use std::{any::Any, sync::Arc};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{internal_err, plan_err, DFSchema, Result},
    datasource::{function::TableFunctionImpl, TableType},
    execution::context::ExecutionProps,
    logical_expr::TableProviderFilterPushDown,
    physical_expr::{
        create_physical_expr,
        utils::{Guarantee, LiteralGuarantee},
    },
    physical_plan::{memory::MemoryExec, DisplayAs, DisplayFormatType, ExecutionPlan},
    prelude::Expr,
    scalar::ScalarValue,
};
use indexmap::IndexMap;
use influxdb3_catalog::catalog::TableDefinition;
use influxdb3_id::{ColumnId, DbId};

use super::{cache::Predicate, MetaCacheProvider};

/// The name used to call the metadata cache in SQL queries
pub const META_CACHE_UDTF_NAME: &str = "meta_cache";

/// Implementor of the [`TableProvider`] trait that is produced a call to the [`MetaCacheFunction`]
#[derive(Debug)]
struct MetaCacheFunctionProvider {
    /// Reference to the [`MetaCache`][super::cache::MetaCache] being queried's schema
    schema: SchemaRef,
    /// Forwarded ref to the [`MetaCacheProvider`] which is used to get the
    /// [`MetaCache`][super::cache::MetaCache] for the query, along with the `db_id` and
    /// `table_def`. This is done instead of passing forward a reference to the `MetaCache`
    /// directly because doing so is not easy or possible with the Rust borrow checker.
    provider: Arc<MetaCacheProvider>,
    /// The database ID that the called cache is related to
    db_id: DbId,
    /// The table definition that the called cache is related to
    table_def: Arc<TableDefinition>,
    /// The name of the cache, which is determined when calling the `meta_cache` function
    cache_name: Arc<str>,
}

#[async_trait]
impl TableProvider for MetaCacheFunctionProvider {
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
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let read = self.provider.cache_map.read();
        let (batches, predicates) = if let Some(cache) = read
            .get(&self.db_id)
            .and_then(|db| db.get(&self.table_def.table_id))
            .and_then(|tbl| tbl.get(&self.cache_name))
        {
            let predicates = convert_filter_exprs(&self.table_def, self.schema(), filters)?;
            (
                cache
                    .to_record_batch(&predicates, limit)
                    .map(|batch| vec![batch])?,
                (!predicates.is_empty()).then_some(predicates),
            )
        } else {
            (vec![], None)
        };
        let mut exec = MetaCacheExec::try_new(
            predicates,
            Arc::clone(&self.table_def),
            &[batches],
            self.schema(),
            projection.cloned(),
            limit,
        )?;

        let show_sizes = ctx.config_options().explain.show_sizes;
        exec = exec.with_show_sizes(show_sizes);

        Ok(Arc::new(exec))
    }
}

/// Convert the given list of filter expressions to a map of [`ColumnId`] to [`Predicate`]
///
/// The resulting map uses [`IndexMap`] to ensure consistent ordering of the map. This makes testing
/// the filter conversion significantly easier using EXPLAIN queries.
fn convert_filter_exprs(
    table_def: &TableDefinition,
    cache_schema: SchemaRef,
    filters: &[Expr],
) -> Result<IndexMap<ColumnId, Predicate>> {
    let mut predicate_map: IndexMap<ColumnId, Option<Predicate>> = IndexMap::new();

    // for create_physical_expr:
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
    // which we can convert to the `Predicate` type for the metadata cache.
    //
    // The main caveat is that if for some reason there are multiple `Expr`s that apply predicates
    // on a given column, i.e., leading to multiple `LiteralGuarantee`s on a specific column, we
    // discard those predicates and have DataFusion handle the filtering.
    //
    // This is a conservative approach; it may be that we can combine multiple literal guarantees on
    // a single column, but thusfar, from testing in the parent module, this does not seem necessary.

    for expr in filters {
        let physical_expr = create_physical_expr(expr, &schema, &props)?;
        let literal_guarantees = LiteralGuarantee::analyze(&physical_expr);
        for LiteralGuarantee {
            column,
            guarantee,
            literals,
        } in literal_guarantees
        {
            let Some(column_id) = table_def.column_name_to_id(column.name()) else {
                return plan_err!(
                    "invalid column name in filter expression: {}",
                    column.name()
                );
            };
            let value_iter = literals.into_iter().filter_map(|l| match l {
                ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => Some(s),
                _ => None,
            });

            let predicate = match guarantee {
                Guarantee::In => Predicate::new_in(value_iter),
                Guarantee::NotIn => Predicate::new_not_in(value_iter),
            };
            predicate_map
                .entry(column_id)
                .and_modify(|e| {
                    // We do not currently support multiple literal guarantees per column.
                    //
                    // In this case we replace the predicate with None so that it does not filter
                    // any records from the cache downstream. Datafusion will still do filtering at
                    // a higher level, once _all_ records are produced from the cache.
                    e.take();
                })
                .or_insert_with(|| Some(predicate));
        }
    }

    Ok(predicate_map
        .into_iter()
        .filter_map(|(column_id, predicate)| predicate.map(|predicate| (column_id, predicate)))
        .collect())
}

/// Implementor of the [`TableFunctionImpl`] trait, to be registered as a user-defined table function
/// in the Datafusion `SessionContext`.
#[derive(Debug)]
pub struct MetaCacheFunction {
    db_id: DbId,
    provider: Arc<MetaCacheProvider>,
}

impl MetaCacheFunction {
    pub fn new(db_id: DbId, provider: Arc<MetaCacheProvider>) -> Self {
        Self { db_id, provider }
    }
}

impl TableFunctionImpl for MetaCacheFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let Some(Expr::Literal(ScalarValue::Utf8(Some(table_name)))) = args.first() else {
            return plan_err!("first argument must be the table name as a string");
        };
        let cache_name = match args.get(1) {
            Some(Expr::Literal(ScalarValue::Utf8(Some(name)))) => Some(name),
            Some(_) => {
                return plan_err!("second argument, if passed, must be the cache name as a string")
            }
            None => None,
        };

        let Some(table_def) = self
            .provider
            .catalog
            .db_schema_by_id(&self.db_id)
            .and_then(|db| db.table_definition(table_name.as_str()))
        else {
            return plan_err!("provided table name ({}) is invalid", table_name);
        };
        let Some((cache_name, schema)) = self.provider.get_cache_name_and_schema(
            self.db_id,
            table_def.table_id,
            cache_name.map(|n| n.as_str()),
        ) else {
            return plan_err!("could not find meta cache for the given arguments");
        };
        Ok(Arc::new(MetaCacheFunctionProvider {
            schema,
            provider: Arc::clone(&self.provider),
            db_id: self.db_id,
            table_def,
            cache_name,
        }))
    }
}

/// Custom implementor of the [`ExecutionPlan`] trait for use by the metadata cache
///
/// Wraps a [`MemoryExec`] from DataFusion, and mostly re-uses that. The special functionality
/// provided by this type is to track the predicates that are pushed down to the underlying cache
/// during query planning/execution.
///
/// # Example
///
/// For a query that does not provide any predicates, or one that does provide predicates, but they
/// do no get pushed down, the `EXPLAIN` for said query will contain a line for the `MetaCacheExec`
/// with no predicates, including what is emitted by the inner `MemoryExec`:
///
/// ```text
/// MetaCacheExec: inner=MemoryExec: partitions=1, partition_sizes=[1]
/// ```
///
/// For queries that do have predicates that get pushed down, the output will include them, e.g.:
///
/// ```text
/// MetaCacheExec: predicates=[[0 IN (us-east)], [1 IN (a,b)]] inner=MemoryExec: partitions=1, partition_sizes=[1]
/// ```
#[derive(Debug)]
struct MetaCacheExec {
    inner: MemoryExec,
    table_def: Arc<TableDefinition>,
    predicates: Option<IndexMap<ColumnId, Predicate>>,
    limit: Option<usize>,
}

impl MetaCacheExec {
    fn try_new(
        predicates: Option<IndexMap<ColumnId, Predicate>>,
        table_def: Arc<TableDefinition>,
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Self> {
        Ok(Self {
            inner: MemoryExec::try_new(partitions, schema, projection)?,
            predicates,
            table_def,
            limit,
        })
    }

    fn with_show_sizes(self, show_sizes: bool) -> Self {
        Self {
            inner: self.inner.with_show_sizes(show_sizes),
            ..self
        }
    }
}

impl DisplayAs for MetaCacheExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MetaCacheExec:")?;
                if let Some(limit) = self.limit {
                    write!(f, " limit={limit}")?;
                }
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

impl ExecutionPlan for MetaCacheExec {
    fn name(&self) -> &str {
        "MetaCacheExec"
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
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
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
}
