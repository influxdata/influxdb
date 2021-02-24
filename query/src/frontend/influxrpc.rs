use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow_deps::{
    arrow::datatypes::DataType,
    datafusion::{
        error::{DataFusionError, Result as DatafusionResult},
        logical_plan::{
            Expr, ExpressionVisitor, LogicalPlan, LogicalPlanBuilder, Operator, Recursion,
        },
        prelude::col,
    },
};
use data_types::{
    schema::{InfluxColumnType, Schema},
    selection::Selection,
};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use tracing::debug;

use crate::{
    exec::{make_schema_pivot, stringset::StringSet},
    plan::{
        fieldlist::FieldListPlan,
        stringset::{Error as StringSetError, StringSetPlan, StringSetPlanBuilder},
    },
    predicate::{Predicate, PredicateBuilder},
    provider::ProviderBuilder,
    util::schema_has_all_expr_columns,
    Database, PartitionChunk,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("gRPC planner got error making table_name plan for chunk: {}", source))]
    TableNamePlan {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("gRPC planner got error listing partition keys: {}", source))]
    ListingPartitions {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("gRPC planner got error finding column names: {}", source))]
    FindingColumnNames {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("gRPC planner got error finding column values: {}", source))]
    FindingColumnValues {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "gRPC planner got internal error making table_name with default predicate: {}",
        source
    ))]
    InternalTableNamePlanForDefault {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("gRPC planner could not get table_names with default predicate, which should always return values"))]
    InternalTableNameCannotGetPlanForDefault {},

    #[snafu(display("Unsupported predicate in gRPC table_names: {:?}", predicate))]
    UnsupportedPredicateForTableNames { predicate: Predicate },

    #[snafu(display(
        "gRPC planner got error checking if chunk {} could pass predicate: {}",
        chunk_id,
        source
    ))]
    CheckingChunkPredicate {
        chunk_id: u32,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("gRPC planner got error creating string set plan: {}", source))]
    CreatingStringSet { source: StringSetError },

    #[snafu(display(
        "gRPC planner got error adding chunk for table {}: {}",
        table_name,
        source
    ))]
    CreatingProvider {
        table_name: String,
        source: crate::provider::Error,
    },

    #[snafu(display("gRPC planner got error building plan: {}", source))]
    BuildingPlan {
        source: arrow_deps::datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "gRPC planner got error getting table schema for table '{}' in chunk {}: {}",
        table_name,
        chunk_id,
        source
    ))]
    GettingTableSchema {
        table_name: String,
        chunk_id: u32,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("gRPC planner error: unsupported predicate: {}", source))]
    UnsupportedPredicate { source: DataFusionError },

    #[snafu(display(
        "gRPC planner error: column '{}' is not a tag, it is {:?}",
        tag_name,
        influx_column_type
    ))]
    InvalidTagColumn {
        tag_name: String,
        influx_column_type: Option<InfluxColumnType>,
    },

    #[snafu(display(
        "Internal error: tag column '{}' is not Utf8 type, it is {:?} ",
        tag_name,
        data_type
    ))]
    InternalInvalidTagType {
        tag_name: String,
        data_type: DataType,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Plans queries that originate from the InfluxDB Storage gRPC
/// interface, which are in terms of the InfluxDB Data model (e.g.
/// `ParsedLine`). The query methods on this trait such as
/// `tag_keys` are specific to this data model.
///
/// The IOx storage engine implements this trait to provide Timeseries
/// specific queries, but also provides more generic access to the
/// same underlying data via other frontends (e.g. SQL).
///
/// The InfluxDB data model can be thought of as a relational
/// database table where each column has both a type as well as one of
/// the following categories:
///
/// * Tag (always String type)
/// * Field (Float64, Int64, UInt64, String, or Bool)
/// * Time (Int64)
///
/// While the underlying storage is the same for columns in different
/// categories with the same data type, columns of different
/// categories are treated differently in the different query types.
#[derive(Default, Debug)]
pub struct InfluxRPCPlanner {}

impl InfluxRPCPlanner {
    /// Create a new instance of the RPC planner
    pub fn new() -> Self {
        Self {}
    }

    /// Returns a plan that lists the names of tables in this
    /// database that have at least one row that matches the
    /// conditions listed on `predicate`
    pub async fn table_names<D>(&self, database: &D, predicate: Predicate) -> Result<StringSetPlan>
    where
        D: Database + 'static,
    {
        let mut builder = StringSetPlanBuilder::new();

        for chunk in self.filtered_chunks(database, &predicate).await? {
            let new_table_names = chunk
                .table_names(&predicate, builder.known_strings())
                .await
                .map_err(|e| Box::new(e) as _)
                .context(TableNamePlan)?;

            builder = match new_table_names {
                Some(new_table_names) => builder.append(new_table_names.into()),
                None => {
                    // TODO: General purpose plans for
                    // table_names. For now, if we couldn't figure out
                    // the table names from only metadata, generate an
                    // error
                    return UnsupportedPredicateForTableNames { predicate }.fail();
                }
            }
        }

        let plan = builder.build().context(CreatingStringSet)?;
        Ok(plan)
    }

    /// Returns a set of plans that produces the names of "tag"
    /// columns (as defined in the InfluxDB Data model) names in this
    /// database that have more than zero rows which pass the
    /// conditions specified by `predicate`.
    pub async fn tag_keys<D>(&self, database: &D, predicate: Predicate) -> Result<StringSetPlan>
    where
        D: Database + 'static,
    {
        debug!(predicate=?predicate, "planning tag_keys");

        // The basic algorithm is:
        //
        // 1. Find all the potential tables in the chunks
        //
        // 2. For each table/chunk pair, figure out which can be found
        // from only metadata and which need full plans

        // Key is table name, value is set of chunks which had data
        // for that table but that we couldn't evaluate the predicate
        // entirely using the metadata
        let mut need_full_plans = BTreeMap::new();

        let mut known_columns = BTreeSet::new();
        for chunk in self.filtered_chunks(database, &predicate).await? {
            // try and get the table names that have rows that match the predicate
            let table_names = self.chunk_table_names(chunk.as_ref(), &predicate).await?;

            for table_name in table_names {
                debug!(
                    table_name = table_name.as_str(),
                    chunk_id = chunk.id(),
                    "finding columns in table"
                );
                // try and get the list of columns directly from metadata
                let maybe_names = chunk
                    .column_names(&table_name, &predicate)
                    .await
                    .map_err(|e| Box::new(e) as _)
                    .context(FindingColumnNames)?;

                match maybe_names {
                    Some(names) => {
                        debug!(names=?names, chunk_id = chunk.id(), "column names found from metadata");

                        // can restrict the output to only the tag columns
                        let schema = chunk
                            .table_schema(&table_name, Selection::All)
                            .await
                            .expect("to be able to get table schema");
                        let mut names = self.restrict_to_tags(&schema, names);
                        debug!(names=?names, chunk_id = chunk.id(), "column names found from metadata");

                        known_columns.append(&mut names);
                    }
                    None => {
                        debug!(
                            table_name = table_name.as_str(),
                            chunk_id = chunk.id(),
                            "column names need full plan"
                        );
                        // can't get columns only from metadata, need
                        // a general purpose plan
                        need_full_plans
                            .entry(table_name)
                            .or_insert_with(Vec::new)
                            .push(Arc::clone(&chunk));
                    }
                }
            }
        }

        let mut builder = StringSetPlanBuilder::new();

        // At this point, we have a set of column names we know pass
        // in `known_columns`, and potentially some tables in chunks
        // that we need to run a plan to know if they pass the
        // predicate.
        if !need_full_plans.is_empty() {
            // TODO an additional optimization here would be to filter
            // out chunks (and tables) where all columns in that chunk
            // were already known to have data (based on the contents of known_columns)

            for (table_name, chunks) in need_full_plans.into_iter() {
                let plan = self.tag_keys_plan(&table_name, &predicate, chunks).await?;

                if let Some(plan) = plan {
                    builder = builder.append(plan)
                }
            }
        }

        // add the known columns we could find from metadata only
        builder
            .append(known_columns.into())
            .build()
            .context(CreatingStringSet)
    }

    /// Returns a plan which finds the distinct, non-null tag values
    /// in the specified `tag_name` column of this database which pass
    /// the conditions specified by `predicate`.
    pub async fn tag_values<D>(
        &self,
        database: &D,
        tag_name: &str,
        predicate: Predicate,
    ) -> Result<StringSetPlan>
    where
        D: Database + 'static,
    {
        debug!(predicate=?predicate, tag_name, "planning tag_values");

        // The basic algorithm is:
        //
        // 1. Find all the potential tables in the chunks
        //
        // 2. For each table/chunk pair, figure out which have
        // distinct values that can be found from only metadata and
        // which need full plans

        // Key is table name, value is set of chunks which had data
        // for that table but that we couldn't evaluate the predicate
        // entirely using the metadata
        let mut need_full_plans = BTreeMap::new();

        let mut known_values = BTreeSet::new();
        for chunk in self.filtered_chunks(database, &predicate).await? {
            let table_names = self.chunk_table_names(chunk.as_ref(), &predicate).await?;

            for table_name in table_names {
                debug!(
                    table_name = table_name.as_str(),
                    chunk_id = chunk.id(),
                    "finding columns in table"
                );

                // use schema to validate column type
                let schema = chunk
                    .table_schema(&table_name, Selection::All)
                    .await
                    .expect("to be able to get table schema");

                // Skip this table if the tag_name is not a column in this table
                let idx = if let Some(idx) = schema.find_index_of(tag_name) {
                    idx
                } else {
                    continue;
                };

                // Validate that this really is a Tag column
                let (influx_column_type, field) = schema.field(idx);
                ensure!(
                    matches!(influx_column_type, Some(InfluxColumnType::Tag)),
                    InvalidTagColumn {
                        tag_name,
                        influx_column_type,
                    }
                );
                ensure!(
                    field.data_type() == &DataType::Utf8,
                    InternalInvalidTagType {
                        tag_name,
                        data_type: field.data_type().clone(),
                    }
                );

                // try and get the list of values directly from metadata
                let maybe_values = chunk
                    .column_values(&table_name, tag_name, &predicate)
                    .await
                    .map_err(|e| Box::new(e) as _)
                    .context(FindingColumnValues)?;

                match maybe_values {
                    Some(mut names) => {
                        debug!(names=?names, chunk_id = chunk.id(), "column values found from metadata");
                        known_values.append(&mut names);
                    }
                    None => {
                        debug!(
                            table_name = table_name.as_str(),
                            chunk_id = chunk.id(),
                            "need full plan to find column values"
                        );
                        // can't get columns only from metadata, need
                        // a general purpose plan
                        need_full_plans
                            .entry(table_name)
                            .or_insert_with(Vec::new)
                            .push(Arc::clone(&chunk));
                    }
                }
            }
        }

        let mut builder = StringSetPlanBuilder::new();

        let select_exprs = vec![col(tag_name)];

        // At this point, we have a set of tag_values we know at plan
        // time in `known_columns`, and some tables in chunks that we
        // need to run a plan to find what values pass the predicate.
        for (table_name, chunks) in need_full_plans.into_iter() {
            let scan_and_filter = self
                .scan_and_filter(&table_name, &predicate, chunks)
                .await?;

            // if we have any data to scan, make a plan!
            if let Some(TableScanAndFilter {
                plan_builder,
                schema: _,
            }) = scan_and_filter
            {
                // TODO use Expr::is_null() here when this
                // https://issues.apache.org/jira/browse/ARROW-11742
                // is completed.
                let tag_name_is_not_null = Expr::IsNotNull(Box::new(col(tag_name)));

                // TODO: optimize this to use "DISINCT" or do
                // something more intelligent that simply fetching all
                // the values and reducing them in the query Executor
                //
                // Until then, simply use a plan which looks like:
                //
                //    Projection
                //      Filter(is not null)
                //        Filter(predicate)
                //          InMemoryScan
                let plan = plan_builder
                    .project(&select_exprs)
                    .context(BuildingPlan)?
                    .filter(tag_name_is_not_null)
                    .context(BuildingPlan)?
                    .build()
                    .context(BuildingPlan)?;

                builder = builder.append(plan.into());
            }
        }

        // add the known values we could find from metadata only
        builder
            .append(known_values.into())
            .build()
            .context(CreatingStringSet)
    }

    /// Returns a plan that produces a list of columns and their
    /// datatypes (as defined in the data written via `write_lines`),
    /// and which have more than zero rows which pass the conditions
    /// specified by `predicate`.
    pub async fn field_columns<D>(
        &self,
        database: &D,
        predicate: Predicate,
    ) -> Result<FieldListPlan>
    where
        D: Database + 'static,
    {
        debug!(predicate=?predicate, "planning field_columns");

        // Algorithm is to run a "select field_cols from table where
        // <predicate> type plan for each table in the chunks"
        //
        // The executor then figures out which columns have non-null
        // values and stops the plan executing once it has them

        // map table -> Vec<Arc<Chunk>>
        let mut table_chunks = BTreeMap::new();
        let chunks = self.filtered_chunks(database, &predicate).await?;
        for chunk in chunks {
            let table_names = self.chunk_table_names(chunk.as_ref(), &predicate).await?;
            for table_name in table_names {
                table_chunks
                    .entry(table_name)
                    .or_insert_with(Vec::new)
                    .push(Arc::clone(&chunk));
            }
        }

        let mut field_list_plan = FieldListPlan::new();
        for (table_name, chunks) in table_chunks {
            if let Some(plan) = self
                .field_columns_plan(&table_name, &predicate, chunks)
                .await?
            {
                field_list_plan = field_list_plan.append(plan);
            }
        }

        Ok(field_list_plan)
    }

    /// Find all the table names in the specified chunk that pass the predicate
    async fn chunk_table_names<C>(
        &self,
        chunk: &C,
        predicate: &Predicate,
    ) -> Result<BTreeSet<String>>
    where
        C: PartitionChunk + 'static,
    {
        let no_tables = StringSet::new();

        // try and get the table names that have rows that match the predicate
        let table_names = chunk
            .table_names(&predicate, &no_tables)
            .await
            .map_err(|e| Box::new(e) as _)
            .context(TableNamePlan)?;

        debug!(table_names=?table_names, chunk_id = chunk.id(), "chunk tables");

        let table_names = match table_names {
            Some(table_names) => {
                debug!("found table names with original predicate");
                table_names
            }
            None => {
                // couldn't find table names with predicate, get all chunk tables,
                // fall back to filtering ourself
                let table_name_predicate = if let Some(table_names) = &predicate.table_names {
                    PredicateBuilder::new().tables(table_names).build()
                } else {
                    Predicate::default()
                };
                chunk
                    .table_names(&table_name_predicate, &no_tables)
                    .await
                    .map_err(|e| Box::new(e) as _)
                    .context(InternalTableNamePlanForDefault)?
                    // unwrap the Option
                    .context(InternalTableNameCannotGetPlanForDefault)?
            }
        };
        Ok(table_names)
    }

    /// removes any columns from Names that are not "Tag"s in the Influx Data
    /// Model
    fn restrict_to_tags(&self, schema: &Schema, names: BTreeSet<String>) -> BTreeSet<String> {
        names
            .into_iter()
            .filter(|col_name| {
                let idx = schema.find_index_of(col_name).unwrap();
                matches!(schema.field(idx), (Some(InfluxColumnType::Tag), _))
            })
            .collect()
    }

    /// Creates a DataFusion LogicalPlan that returns column *names* as a
    /// single column of Strings for a specific table
    ///
    /// The created plan looks like:
    ///
    /// ```text
    ///  Extension(PivotSchema)
    ///    Filter(predicate)
    ///      TableScan (of chunks)
    /// ```
    async fn tag_keys_plan<C>(
        &self,
        table_name: &str,
        predicate: &Predicate,
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<StringSetPlan>>
    where
        C: PartitionChunk + 'static,
    {
        let scan_and_filter = self.scan_and_filter(table_name, predicate, chunks).await?;

        let TableScanAndFilter {
            plan_builder,
            schema,
        } = match scan_and_filter {
            None => return Ok(None),
            Some(t) => t,
        };

        // now, select only the tag columns
        let select_exprs = schema
            .iter()
            .filter_map(|(influx_column_type, field)| {
                if matches!(influx_column_type, Some(InfluxColumnType::Tag)) {
                    Some(col(field.name()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let plan = plan_builder
            .project(&select_exprs)
            .context(BuildingPlan)?
            .build()
            .context(BuildingPlan)?;

        // And finally pivot the plan
        let plan = make_schema_pivot(plan);
        debug!(table_name=table_name, plan=%plan.display_indent_schema(),
               "created column_name plan for table");

        Ok(Some(plan.into()))
    }

    /// Creates a DataFusion LogicalPlan that returns the timestamp
    /// and all field columns for a specified table:
    ///
    /// The output looks like (field0, field1, ..., time)
    ///
    /// The data is not sorted in any particular order
    ///
    /// returns `None` if the table contains no rows that would pass
    /// the predicate.
    ///
    /// The created plan looks like:
    ///
    /// ```text
    ///  Projection (select the field columns needed)
    ///      Filter(predicate) [optional]
    ///        InMemoryScan
    /// ```
    async fn field_columns_plan<C>(
        &self,
        table_name: &str,
        predicate: &Predicate,
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<LogicalPlan>>
    where
        C: PartitionChunk + 'static,
    {
        let scan_and_filter = self.scan_and_filter(table_name, predicate, chunks).await?;
        let TableScanAndFilter {
            plan_builder,
            schema,
        } = match scan_and_filter {
            None => return Ok(None),
            Some(t) => t,
        };

        // Selection of only fields and time
        let select_exprs = schema
            .iter()
            .filter_map(|(influx_column_type, field)| match influx_column_type {
                Some(InfluxColumnType::Field(_)) => Some(col(field.name())),
                Some(InfluxColumnType::Timestamp) => Some(col(field.name())),
                Some(_) => None,
                None => None,
            })
            .collect::<Vec<_>>();

        let plan = plan_builder
            .project(&select_exprs)
            .context(BuildingPlan)?
            .build()
            .context(BuildingPlan)?;

        Ok(Some(plan))
    }

    /// Create a plan that scans the specified table, and applies any
    /// filtering specified on the predicate, if any.
    ///
    /// If the table can produce no rows based on predicate
    /// evaluation, returns Ok(None)
    ///
    /// The created plan looks like:
    ///
    /// ```text
    ///   Filter(predicate) [optional]
    ///     InMemoryScan
    /// ```
    async fn scan_and_filter<C>(
        &self,
        table_name: &str,
        predicate: &Predicate,
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<TableScanAndFilter>>
    where
        C: PartitionChunk + 'static,
    {
        // Scan all columns to begin with (datafusion projection
        // pushdown optimization will prune out uneeded columns later)
        let projection = None;
        let selection = Selection::All;

        // Prepare the scan of the table
        let mut builder = ProviderBuilder::new(table_name);
        for chunk in chunks {
            let chunk_id = chunk.id();

            // check that it is consitent with this table_name
            assert!(
                chunk.has_table(table_name),
                "Chunk {} did not have table {}, while trying to make a plan for it",
                chunk.id(),
                table_name
            );

            let chunk_table_schema = chunk
                .table_schema(table_name, selection)
                .await
                .map_err(|e| Box::new(e) as _)
                .context(GettingTableSchema {
                    table_name,
                    chunk_id,
                })?;

            builder = builder
                .add_chunk(chunk, chunk_table_schema)
                .context(CreatingProvider { table_name })?;
        }

        let provider = builder.build().context(CreatingProvider { table_name })?;
        let schema = provider.iox_schema();

        let mut plan_builder = LogicalPlanBuilder::scan(table_name, Arc::new(provider), projection)
            .context(BuildingPlan)?;

        // Use a filter node to add general predicates + timestamp
        // range, if any
        if let Some(filter_expr) = predicate.filter_expr() {
            // check to see if this table has all the columns needed
            // to evaluate the predicate (if not, it means no rows can
            // match and thus we should skip this plan)
            if !schema_has_all_expr_columns(&schema, &filter_expr) {
                debug!(table_name=table_name,
                       schema=?schema,
                       filter_expr=?filter_expr,
                       "Skipping table as schema doesn't have all filter_expr columns");
                return Ok(None);
            }
            // Assuming that if a table doesn't have all the columns
            // in an expression it can't be true isn't correct for
            // certain predicates (e.g. IS NOT NULL), so error out
            // here until we have proper support for that case
            check_predicate_support(&filter_expr)?;

            plan_builder = plan_builder.filter(filter_expr).context(BuildingPlan)?;
        }

        Ok(Some(TableScanAndFilter {
            plan_builder,
            schema,
        }))
    }

    /// Returns a list of chunks across all partitions which may
    /// contain data that pass the predicate
    async fn filtered_chunks<D>(
        &self,
        database: &D,
        predicate: &Predicate,
    ) -> Result<Vec<Arc<<D as Database>::Chunk>>>
    where
        D: Database,
    {
        let mut db_chunks = Vec::new();

        // TODO write the following in a functional style (need to get
        // rid of `await` on `Database::chunks`)

        let partition_keys = database
            .partition_keys()
            .map_err(|e| Box::new(e) as _)
            .context(ListingPartitions)?;

        debug!(partition_keys=?partition_keys, "Considering partition keys");

        for key in partition_keys {
            // TODO prune partitions somehow
            let partition_chunks = database.chunks(&key);
            for chunk in partition_chunks {
                let could_pass_predicate = chunk
                    .could_pass_predicate(predicate)
                    .map_err(|e| Box::new(e) as _)
                    .context(CheckingChunkPredicate {
                        chunk_id: chunk.id(),
                    })?;

                debug!(
                    chunk_id = chunk.id(),
                    could_pass_predicate, "Considering chunk"
                );
                if could_pass_predicate {
                    db_chunks.push(chunk)
                }
            }
        }
        Ok(db_chunks)
    }
}

/// Returns `Ok` if we support this predicate, `Err` otherwise.
///
/// Right now, the gRPC planner assumes that if all columns in an
/// expression are not present, the expression can't evaluate to true
/// (aka have rows match).
///
/// This is not true for certain expressions (e.g. IS NULL for
///  example), so error here if we see one of those).

fn check_predicate_support(expr: &Expr) -> Result<()> {
    let visitor = SupportVisitor {};
    expr.accept(visitor).context(UnsupportedPredicate)?;
    Ok(())
}

/// Used to figure out if we know how to deal with this kind of
/// predicate in the grpc buffer
struct SupportVisitor {}

impl ExpressionVisitor for SupportVisitor {
    fn pre_visit(self, expr: &Expr) -> DatafusionResult<Recursion<Self>> {
        match expr {
            Expr::Literal(..) => Ok(Recursion::Continue(self)),
            Expr::Column(..) => Ok(Recursion::Continue(self)),
            Expr::BinaryExpr { op, .. } => {
                match op {
                    Operator::Eq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::Plus
                    | Operator::Minus
                    | Operator::Multiply
                    | Operator::Divide
                    | Operator::And
                    | Operator::Or => Ok(Recursion::Continue(self)),
                    // Unsupported (need to think about ramifications)
                    Operator::NotEq | Operator::Modulus | Operator::Like | Operator::NotLike => {
                        Err(DataFusionError::NotImplemented(format!(
                            "Unsupported operator in gRPC: {:?} in expression {:?}",
                            op, expr
                        )))
                    }
                }
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported expression in gRPC: {:?}",
                expr
            ))),
        }
    }
}

struct TableScanAndFilter {
    /// Represents plan that scans a table and applies optional filtering
    plan_builder: LogicalPlanBuilder,
    /// The IOx schema of the result
    schema: Schema,
}
