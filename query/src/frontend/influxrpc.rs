use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::datatypes::{DataType, Field};
use datafusion::{
    error::{DataFusionError, Result as DatafusionResult},
    logical_plan::{Expr, ExpressionVisitor, LogicalPlan, LogicalPlanBuilder, Operator, Recursion},
    prelude::col,
};
use datafusion_util::AsExpr;

use internal_types::{
    schema::{InfluxColumnType, Schema, TIME_COLUMN_NAME},
    selection::Selection,
};
use observability_deps::tracing::{debug, trace};
use snafu::{ensure, OptionExt, ResultExt, Snafu};

use crate::{
    exec::{field::FieldColumns, make_schema_pivot},
    func::{
        selectors::{selector_first, selector_last, selector_max, selector_min, SelectorOutput},
        window::make_window_bound_expr,
    },
    group_by::{Aggregate, WindowDuration},
    plan::{
        fieldlist::FieldListPlan,
        seriesset::{SeriesSetPlan, SeriesSetPlans},
        stringset::{Error as StringSetError, StringSetPlan, StringSetPlanBuilder},
    },
    predicate::{Predicate, PredicateMatch},
    provider::ProviderBuilder,
    util::schema_has_all_expr_columns,
    QueryChunk, QueryChunkMeta, QueryDatabase,
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
        source: datafusion::error::DataFusionError,
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

    #[snafu(display("Duplicate group column '{}'", column_name))]
    DuplicateGroupColumn { column_name: String },

    #[snafu(display(
        "Group column '{}' not found in tag columns: {}",
        column_name,
        all_tag_column_names
    ))]
    GroupColumnNotFound {
        column_name: String,
        all_tag_column_names: String,
    },

    #[snafu(display("Error creating aggregate expression:  {}", source))]
    CreatingAggregates { source: crate::group_by::Error },

    #[snafu(display("Internal error: unexpected aggregate request for None aggregate",))]
    InternalUnexpectedNoneAggregate {},

    #[snafu(display("Internal error: aggregate {:?} is not a selector", agg))]
    InternalAggregateNotSelector { agg: Aggregate },

    #[snafu(display("Table was removed while planning query: {}", table_name))]
    TableRemoved { table_name: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Plans queries that originate from the InfluxDB Storage gRPC
/// interface, which are in terms of the InfluxDB Data model (e.g.
/// `ParsedLine`). The query methods on this trait such as
/// `tag_keys` are specific to this data model.
///
/// The IOx storage engine implements this trait to provide time-series
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
pub struct InfluxRpcPlanner {}

impl InfluxRpcPlanner {
    /// Create a new instance of the RPC planner
    pub fn new() -> Self {
        Self {}
    }

    /// Returns a plan that lists the names of tables in this
    /// database that have at least one row that matches the
    /// conditions listed on `predicate`
    pub fn table_names<D>(&self, database: &D, predicate: Predicate) -> Result<StringSetPlan>
    where
        D: QueryDatabase + 'static,
    {
        let mut builder = StringSetPlanBuilder::new();

        for chunk in database.chunks(&predicate) {
            // Try and apply the predicate using only metadata
            let pred_result = chunk
                .apply_predicate_to_metadata(&predicate)
                .map_err(|e| Box::new(e) as _)
                .context(CheckingChunkPredicate {
                    chunk_id: chunk.id(),
                })?;

            builder = match pred_result {
                PredicateMatch::AtLeastOne => builder.append_table(chunk.table_name()),
                // no match, ignore table
                PredicateMatch::Zero => builder,
                // can't evaluate predicate, need a new plan
                PredicateMatch::Unknown => {
                    // TODO: General purpose plans for
                    // table_names. For now, return an error
                    debug!(
                        chunk = chunk.id(),
                        ?predicate,
                        table_name = chunk.table_name(),
                        "can not evaluate predicate"
                    );
                    return UnsupportedPredicateForTableNames { predicate }.fail();
                }
            };
        }

        let plan = builder.build().context(CreatingStringSet)?;
        Ok(plan)
    }

    /// Returns a set of plans that produces the names of "tag"
    /// columns (as defined in the InfluxDB Data model) names in this
    /// database that have more than zero rows which pass the
    /// conditions specified by `predicate`.
    pub fn tag_keys<D>(&self, database: &D, predicate: Predicate) -> Result<StringSetPlan>
    where
        D: QueryDatabase + 'static,
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
        for chunk in database.chunks(&predicate) {
            // Try and apply the predicate using only metadata
            let pred_result = chunk
                .apply_predicate_to_metadata(&predicate)
                .map_err(|e| Box::new(e) as _)
                .context(CheckingChunkPredicate {
                    chunk_id: chunk.id(),
                })?;

            if matches!(pred_result, PredicateMatch::Zero) {
                continue;
            }
            let table_name = chunk.table_name();

            // get only tag columns from metadata
            let schema = chunk.schema();

            let column_names: Vec<&str> = schema
                .tags_iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<&str>>();

            let selection = Selection::Some(&column_names);

            // filter the columns further from the predicate
            let maybe_names = chunk
                .column_names(&predicate, selection)
                .map_err(|e| Box::new(e) as _)
                .context(FindingColumnNames)?;

            match maybe_names {
                Some(mut names) => {
                    debug!(table_name, names=?names, chunk_id = chunk.id(), "column names found from metadata");
                    known_columns.append(&mut names);
                }
                None => {
                    debug!(
                        table_name,
                        chunk_id = chunk.id(),
                        "column names need full plan"
                    );
                    // can't get columns only from metadata, need
                    // a general purpose plan
                    need_full_plans
                        .entry(table_name.to_string())
                        .or_insert_with(Vec::new)
                        .push(Arc::clone(&chunk));
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
                let schema = database.table_schema(&table_name).context(TableRemoved {
                    table_name: &table_name,
                })?;
                let plan = self.tag_keys_plan(&table_name, schema, &predicate, chunks)?;

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
    pub fn tag_values<D>(
        &self,
        database: &D,
        tag_name: &str,
        predicate: Predicate,
    ) -> Result<StringSetPlan>
    where
        D: QueryDatabase + 'static,
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
        for chunk in database.chunks(&predicate) {
            // Try and apply the predicate using only metadata
            let pred_result = chunk
                .apply_predicate_to_metadata(&predicate)
                .map_err(|e| Box::new(e) as _)
                .context(CheckingChunkPredicate {
                    chunk_id: chunk.id(),
                })?;

            if matches!(pred_result, PredicateMatch::Zero) {
                continue;
            }
            let table_name = chunk.table_name();

            // use schema to validate column type
            let schema = chunk.schema();

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
                influx_column_type
                    .unwrap()
                    .valid_arrow_type(field.data_type()),
                InternalInvalidTagType {
                    tag_name,
                    data_type: field.data_type().clone(),
                }
            );

            // try and get the list of values directly from metadata
            let maybe_values = chunk
                .column_values(tag_name, &predicate)
                .map_err(|e| Box::new(e) as _)
                .context(FindingColumnValues)?;

            match maybe_values {
                Some(mut names) => {
                    debug!(table_name, names=?names, chunk_id = chunk.id(), "column values found from metadata");
                    known_values.append(&mut names);
                }
                None => {
                    debug!(
                        table_name,
                        chunk_id = chunk.id(),
                        "need full plan to find column values"
                    );
                    // can't get columns only from metadata, need
                    // a general purpose plan
                    need_full_plans
                        .entry(table_name.to_string())
                        .or_insert_with(Vec::new)
                        .push(Arc::clone(&chunk));
                }
            }
        }

        let mut builder = StringSetPlanBuilder::new();

        let select_exprs = vec![col(tag_name)];

        // At this point, we have a set of tag_values we know at plan
        // time in `known_columns`, and some tables in chunks that we
        // need to run a plan to find what values pass the predicate.
        for (table_name, chunks) in need_full_plans.into_iter() {
            let schema = database.table_schema(&table_name).context(TableRemoved {
                table_name: &table_name,
            })?;
            let scan_and_filter = self.scan_and_filter(&table_name, schema, &predicate, chunks)?;

            // if we have any data to scan, make a plan!
            if let Some(TableScanAndFilter {
                plan_builder,
                schema: _,
            }) = scan_and_filter
            {
                let tag_name_is_not_null = Expr::Column(tag_name.into()).is_not_null();

                // TODO: optimize this to use "DISINCT" or do
                // something more intelligent that simply fetching all
                // the values and reducing them in the query Executor
                //
                // Until then, simply use a plan which looks like:
                //
                //    Projection
                //      Filter(is not null)
                //        Filter(predicate)
                //          Scan
                let plan = plan_builder
                    .project(select_exprs.clone())
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
    pub fn field_columns<D>(&self, database: &D, predicate: Predicate) -> Result<FieldListPlan>
    where
        D: QueryDatabase + 'static,
    {
        debug!(predicate=?predicate, "planning field_columns");

        // Algorithm is to run a "select field_cols from table where
        // <predicate> type plan for each table in the chunks"
        //
        // The executor then figures out which columns have non-null
        // values and stops the plan executing once it has them

        // map table -> Vec<Arc<Chunk>>
        let chunks = database.chunks(&predicate);
        let table_chunks = self.group_chunks_by_table(&predicate, chunks)?;

        let mut field_list_plan = FieldListPlan::new();
        for (table_name, chunks) in table_chunks {
            let schema = database.table_schema(&table_name).context(TableRemoved {
                table_name: &table_name,
            })?;
            if let Some(plan) = self.field_columns_plan(&table_name, schema, &predicate, chunks)? {
                field_list_plan = field_list_plan.append(plan);
            }
        }

        Ok(field_list_plan)
    }

    /// Returns a plan that finds all rows which pass the
    /// conditions specified by `predicate` in the form of logical
    /// time series.
    ///
    /// A time series is defined by the unique values in a set of
    /// "tag_columns" for each field in the "field_columns", ordered by
    /// the time column.
    ///
    /// The output looks like:
    /// ```text
    /// (tag_col1, tag_col2, ... field1, field2, ... timestamp)
    /// ```
    ///
    /// The  tag_columns are ordered by name.
    ///
    /// The data is sorted on (tag_col1, tag_col2, ...) so that all
    /// rows for a particular series (groups where all tags are the
    /// same) occur together in the plan
    pub fn read_filter<D>(&self, database: &D, predicate: Predicate) -> Result<SeriesSetPlans>
    where
        D: QueryDatabase + 'static,
    {
        debug!(predicate=?predicate, "planning read_filter");

        // group tables by chunk, pruning if possible
        // key is table name, values are chunks
        let chunks = database.chunks(&predicate);
        let table_chunks = self.group_chunks_by_table(&predicate, chunks)?;

        // now, build up plans for each table
        let mut ss_plans = Vec::with_capacity(table_chunks.len());
        for (table_name, chunks) in table_chunks {
            let prefix_columns: Option<&[&str]> = None;
            let schema = database.table_schema(&table_name).context(TableRemoved {
                table_name: &table_name,
            })?;

            let ss_plan =
                self.read_filter_plan(table_name, schema, prefix_columns, &predicate, chunks)?;
            // If we have to do real work, add it to the list of plans
            if let Some(ss_plan) = ss_plan {
                ss_plans.push(ss_plan);
            }
        }

        Ok(ss_plans.into())
    }

    /// Creates a GroupedSeriesSet plan that produces an output table
    /// with rows grouped by an aggregate function. Note that we still
    /// group by all tags (so group within series) and the
    /// group_columns define the order of the result
    pub fn read_group<D>(
        &self,
        database: &D,
        predicate: Predicate,
        agg: Aggregate,
        group_columns: &[impl AsRef<str>],
    ) -> Result<SeriesSetPlans>
    where
        D: QueryDatabase + 'static,
    {
        debug!(predicate=?predicate, agg=?agg, "planning read_group");

        // group tables by chunk, pruning if possible
        let chunks = database.chunks(&predicate);
        let table_chunks = self.group_chunks_by_table(&predicate, chunks)?;
        let num_prefix_tag_group_columns = group_columns.len();

        // now, build up plans for each table
        let mut ss_plans = Vec::with_capacity(table_chunks.len());
        for (table_name, chunks) in table_chunks {
            let schema = database.table_schema(&table_name).context(TableRemoved {
                table_name: &table_name,
            })?;
            let ss_plan = match agg {
                Aggregate::None => self.read_filter_plan(
                    table_name,
                    Arc::clone(&schema),
                    Some(group_columns),
                    &predicate,
                    chunks,
                )?,
                _ => self.read_group_plan(
                    table_name,
                    schema,
                    &predicate,
                    agg,
                    group_columns,
                    chunks,
                )?,
            };

            // If we have to do real work, add it to the list of plans
            if let Some(ss_plan) = ss_plan {
                let grouped_plan = ss_plan.grouped(num_prefix_tag_group_columns);
                ss_plans.push(grouped_plan);
            }
        }

        Ok(ss_plans.into())
    }

    /// Creates a GroupedSeriesSet plan that produces an output table with rows
    /// that are grouped by window definitions
    pub fn read_window_aggregate<D>(
        &self,
        database: &D,
        predicate: Predicate,
        agg: Aggregate,
        every: WindowDuration,
        offset: WindowDuration,
    ) -> Result<SeriesSetPlans>
    where
        D: QueryDatabase + 'static,
    {
        debug!(
            ?predicate,
            ?agg,
            ?every,
            ?offset,
            "planning read_window_aggregate"
        );

        // group tables by chunk, pruning if possible
        let chunks = database.chunks(&predicate);
        let table_chunks = self.group_chunks_by_table(&predicate, chunks)?;

        // now, build up plans for each table
        let mut ss_plans = Vec::with_capacity(table_chunks.len());
        for (table_name, chunks) in table_chunks {
            let schema = database.table_schema(&table_name).context(TableRemoved {
                table_name: &table_name,
            })?;
            let ss_plan = self.read_window_aggregate_plan(
                table_name, schema, &predicate, agg, &every, &offset, chunks,
            )?;
            // If we have to do real work, add it to the list of plans
            if let Some(ss_plan) = ss_plan {
                ss_plans.push(ss_plan);
            }
        }

        Ok(ss_plans.into())
    }

    /// Creates a map of table_name --> Chunks that have that table that *may* pass the predicate
    fn group_chunks_by_table<C>(
        &self,
        predicate: &Predicate,
        chunks: Vec<Arc<C>>,
    ) -> Result<BTreeMap<String, Vec<Arc<C>>>>
    where
        C: QueryChunk + 'static,
    {
        let mut table_chunks = BTreeMap::new();
        for chunk in chunks {
            // Try and apply the predicate using only metadata
            let pred_result = chunk
                .apply_predicate_to_metadata(predicate)
                .map_err(|e| Box::new(e) as _)
                .context(CheckingChunkPredicate {
                    chunk_id: chunk.id(),
                })?;

            match pred_result {
                PredicateMatch::AtLeastOne |
                // have to include chunk as we can't rule it out
                PredicateMatch::Unknown => {
                    let table_name = chunk.table_name().to_string();
                    table_chunks
                        .entry(table_name)
                        .or_insert_with(Vec::new)
                        .push(Arc::clone(&chunk));
                }
                // Skip chunk here based on metadata
                PredicateMatch::Zero => {
                }
            }
        }
        Ok(table_chunks)
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
    fn tag_keys_plan<C>(
        &self,
        table_name: &str,
        schema: Arc<Schema>,
        predicate: &Predicate,
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<StringSetPlan>>
    where
        C: QueryChunk + 'static,
    {
        let scan_and_filter = self.scan_and_filter(table_name, schema, predicate, chunks)?;

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
            .project(select_exprs)
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
    ///        Scan
    /// ```
    fn field_columns_plan<C>(
        &self,
        table_name: &str,
        schema: Arc<Schema>,
        predicate: &Predicate,
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<LogicalPlan>>
    where
        C: QueryChunk + 'static,
    {
        let scan_and_filter = self.scan_and_filter(table_name, schema, predicate, chunks)?;
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
            .project(select_exprs)
            .context(BuildingPlan)?
            .build()
            .context(BuildingPlan)?;

        Ok(Some(plan))
    }

    /// Creates a plan for computing series sets for a given table,
    /// returning None if the predicate rules out matching any rows in
    /// the table
    ///
    /// prefix_columns, if any, are the prefix of the ordering.
    //
    /// The created plan looks like:
    ///
    ///    Projection (select the columns needed)
    ///      Order by (tag_columns, timestamp_column)
    ///        Filter(predicate)
    ///          Scan
    fn read_filter_plan<C>(
        &self,
        table_name: impl AsRef<str>,
        schema: Arc<Schema>,
        prefix_columns: Option<&[impl AsRef<str>]>,
        predicate: &Predicate,
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<SeriesSetPlan>>
    where
        C: QueryChunk + 'static,
    {
        let table_name = table_name.as_ref();
        let scan_and_filter = self.scan_and_filter(table_name, schema, predicate, chunks)?;

        let TableScanAndFilter {
            plan_builder,
            schema,
        } = match scan_and_filter {
            None => return Ok(None),
            Some(t) => t,
        };

        let tags_and_timestamp: Vec<_> = schema
            .tags_iter()
            .chain(schema.time_iter())
            .map(|f| f.name() as &str)
            .collect();

        // Reorder, if requested
        let tags_and_timestamp: Vec<_> = match prefix_columns {
            Some(prefix_columns) => reorder_prefix(prefix_columns, tags_and_timestamp)?,
            None => tags_and_timestamp,
        };

        // Convert to SortExprs to pass to the plan builder
        let tags_and_timestamp: Vec<_> = tags_and_timestamp
            .into_iter()
            .map(|n| n.as_sort_expr())
            .collect();

        // Order by
        let plan_builder = plan_builder
            .sort(tags_and_timestamp)
            .context(BuildingPlan)?;

        // Select away anything that isn't in the influx data model
        let tags_fields_and_timestamps: Vec<Expr> = schema
            .tags_iter()
            .chain(filtered_fields_iter(&schema, predicate))
            .chain(schema.time_iter())
            .map(|field| field.name().as_expr())
            .collect();

        let plan_builder = plan_builder
            .project(tags_fields_and_timestamps)
            .context(BuildingPlan)?;

        let plan = plan_builder.build().context(BuildingPlan)?;

        let tag_columns = schema
            .tags_iter()
            .map(|field| Arc::from(field.name().as_str()))
            .collect();

        let field_columns = filtered_fields_iter(&schema, predicate)
            .map(|field| Arc::from(field.name().as_str()))
            .collect();

        // TODO: remove the use of tag_columns and field_column names
        // and instead use the schema directly)
        let ss_plan = SeriesSetPlan::new_from_shared_timestamp(
            Arc::from(table_name),
            plan,
            tag_columns,
            field_columns,
        );

        Ok(Some(ss_plan))
    }

    /// Creates a GroupedSeriesSet plan that produces an output table
    /// with rows grouped by an aggregate function. Note that we still
    /// group by all tags (so group within series) and the
    /// group_columns define the order of the result
    ///
    /// Equivalent to this SQL query for 'aggregates': sum, count, mean
    /// SELECT
    ///   tag1...tagN
    ///   agg_function(_val1) as _value1
    ///   ...
    ///   agg_function(_valN) as _valueN
    ///   agg_function(time) as time
    /// GROUP BY
    ///   group_key1, group_key2, remaining tags,
    /// ORDER BY
    ///   group_key1, group_key2, remaining tags
    ///
    /// Note the columns are the same but in a different order
    /// for GROUP BY / ORDER BY
    ///
    /// Equivalent to this SQL query for 'selector' functions: first, last, min,
    /// max as they can have different values of the timestamp column
    ///
    /// SELECT
    ///   tag1...tagN
    ///   agg_function(_val1) as _value1
    ///   agg_function(time) as time1
    ///   ..
    ///   agg_function(_valN) as _valueN
    ///   agg_function(time) as timeN
    /// GROUP BY
    ///   group_key1, group_key2, remaining tags,
    /// ORDER BY
    ///   group_key1, group_key2, remaining tags
    ///
    /// The created plan looks like:
    ///
    ///  OrderBy(gby cols; agg)
    ///     GroupBy(gby cols, aggs, time cols)
    ///       Filter(predicate)
    ///          Scan
    fn read_group_plan<C>(
        &self,
        table_name: impl Into<String>,
        schema: Arc<Schema>,
        predicate: &Predicate,
        agg: Aggregate,
        group_columns: &[impl AsRef<str>],
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<SeriesSetPlan>>
    where
        C: QueryChunk + 'static,
    {
        let table_name = table_name.into();
        let scan_and_filter = self.scan_and_filter(&table_name, schema, predicate, chunks)?;

        let TableScanAndFilter {
            plan_builder,
            schema,
        } = match scan_and_filter {
            None => return Ok(None),
            Some(t) => t,
        };

        // order the tag columns so that the group keys come first (we
        // will group and
        // order in the same order)
        let tag_columns: Vec<_> = schema.tags_iter().map(|f| f.name() as &str).collect();

        let tag_columns: Vec<Arc<str>> = reorder_prefix(group_columns, tag_columns)?
            .into_iter()
            .map(Arc::from)
            .collect();

        // Group by all tag columns
        let group_exprs = tag_columns
            .iter()
            .map(|tag_name| tag_name.as_expr())
            .collect::<Vec<_>>();

        let AggExprs {
            agg_exprs,
            field_columns,
        } = AggExprs::try_new(agg, &schema, predicate)?;

        let sort_exprs = group_exprs
            .iter()
            .map(|expr| expr.as_sort_expr())
            .collect::<Vec<_>>();

        let plan_builder = plan_builder
            .aggregate(group_exprs, agg_exprs)
            .context(BuildingPlan)?
            .sort(sort_exprs)
            .context(BuildingPlan)?;

        // and finally create the plan
        let plan = plan_builder.build().context(BuildingPlan)?;

        let ss_plan = SeriesSetPlan::new(Arc::from(table_name), plan, tag_columns, field_columns);

        Ok(Some(ss_plan))
    }

    /// Creates a GroupedSeriesSet plan that produces an output table with rows
    /// that are grouped by window definitions
    ///
    /// The order of the tag_columns
    ///
    /// The data is sorted on tag_col1, tag_col2, ...) so that all
    /// rows for a particular series (groups where all tags are the
    /// same) occur together in the plan
    ///
    /// Equivalent to this SQL query
    ///
    /// SELECT tag1, ... tagN,
    ///   window_bound(time, every, offset) as time,
    ///   agg_function1(field), as field_name
    /// FROM measurement
    /// GROUP BY
    ///   tag1, ... tagN,
    ///   window_bound(time, every, offset) as time,
    /// ORDER BY
    ///   tag1, ... tagN,
    ///   window_bound(time, every, offset) as time
    ///
    /// The created plan looks like:
    ///
    ///  OrderBy(gby: tag columns, window_function; agg: aggregate(field))
    ///      GroupBy(gby: tag columns, window_function; agg: aggregate(field))
    ///        Filter(predicate)
    ///          Scan
    #[allow(clippy::too_many_arguments)]
    fn read_window_aggregate_plan<C>(
        &self,
        table_name: impl Into<String>,
        schema: Arc<Schema>,
        predicate: &Predicate,
        agg: Aggregate,
        every: &WindowDuration,
        offset: &WindowDuration,
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<SeriesSetPlan>>
    where
        C: QueryChunk + 'static,
    {
        let table_name = table_name.into();
        let scan_and_filter = self.scan_and_filter(&table_name, schema, predicate, chunks)?;

        let TableScanAndFilter {
            plan_builder,
            schema,
        } = match scan_and_filter {
            None => return Ok(None),
            Some(t) => t,
        };

        // Group by all tag columns and the window bounds
        let window_bound = make_window_bound_expr(TIME_COLUMN_NAME.as_expr(), every, offset)
            .alias(TIME_COLUMN_NAME);

        let group_exprs = schema
            .tags_iter()
            .map(|field| field.name().as_expr())
            .chain(std::iter::once(window_bound))
            .collect::<Vec<_>>();

        // aggregate each field
        let agg_exprs = filtered_fields_iter(&schema, predicate)
            .map(|field| make_agg_expr(agg, field.name()))
            .collect::<Result<Vec<_>>>()?;

        // sort by the group by expressions as well
        let sort_exprs = group_exprs
            .iter()
            .map(|expr| expr.as_sort_expr())
            .collect::<Vec<_>>();

        let plan_builder = plan_builder
            .aggregate(group_exprs, agg_exprs)
            .context(BuildingPlan)?
            .sort(sort_exprs)
            .context(BuildingPlan)?;

        // and finally create the plan
        let plan = plan_builder.build().context(BuildingPlan)?;

        let tag_columns = schema
            .tags_iter()
            .map(|field| Arc::from(field.name().as_str()))
            .collect();

        let field_columns = filtered_fields_iter(&schema, predicate)
            .map(|field| Arc::from(field.name().as_str()))
            .collect();

        // TODO: remove the use of tag_columns and field_column names
        // and instead use the schema directly)

        let ss_plan = SeriesSetPlan::new_from_shared_timestamp(
            Arc::from(table_name),
            plan,
            tag_columns,
            field_columns,
        );

        Ok(Some(ss_plan))
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
    ///     Scan
    /// ```
    fn scan_and_filter<C>(
        &self,
        table_name: &str,
        schema: Arc<Schema>,
        predicate: &Predicate,
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<TableScanAndFilter>>
    where
        C: QueryChunk + 'static,
    {
        // Scan all columns to begin with (DataFusion projection
        // push-down optimization will prune out unneeded columns later)
        let projection = None;

        // Prepare the scan of the table
        let mut builder = ProviderBuilder::new(table_name, schema);

        // Since the entire predicate is used in the call to
        // `database.chunks()` there will not be any additional
        // predicates that get pushed down here
        //
        // However, in the future if DataFusion adds extra synthetic
        // predicates that could be pushed down and used for
        // additional pruning we may want to add an extra layer of
        // pruning here.
        builder = builder.add_no_op_pruner();

        for chunk in chunks {
            // check that it is consistent with this table_name
            assert_eq!(
                chunk.table_name(),
                table_name,
                "Chunk {} expected table mismatch",
                chunk.id(),
            );

            builder = builder.add_chunk(chunk);
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
                trace!(table_name=table_name,
                       ?schema,
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
                    | Operator::NotEq
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
                    Operator::Modulus | Operator::Like | Operator::NotLike => {
                        Err(DataFusionError::NotImplemented(format!(
                            "Unsupported operator in gRPC: {:?} in expression {:?}",
                            op, expr
                        )))
                    }
                }
            }
            Expr::ScalarUDF { fun, .. } => {
                if fun.name.as_str() == crate::func::regex::REGEX_MATCH_UDF_NAME
                    || fun.name.as_str() == crate::func::regex::REGEX_NOT_MATCH_UDF_NAME
                {
                    Ok(Recursion::Continue(self))
                } else {
                    Err(DataFusionError::NotImplemented(format!(
                        "Unsupported expression in gRPC: {:?}",
                        expr
                    )))
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
    schema: Arc<Schema>,
}

/// Reorders tag_columns so that its prefix matches exactly
/// prefix_columns. Returns an error if there are duplicates, or other
/// untoward inputs
fn reorder_prefix<'a>(
    prefix_columns: &[impl AsRef<str>],
    tag_columns: Vec<&'a str>,
) -> Result<Vec<&'a str>> {
    // tag_used_set[i] is true if we have used the value in tag_columns[i]
    let mut tag_used_set = vec![false; tag_columns.len()];

    // Note that this is an O(N^2) algorithm. We are assuming the
    // number of tag columns is reasonably small

    let mut new_tag_columns = prefix_columns
        .iter()
        .map(|pc| {
            let found_location = tag_columns
                .iter()
                .enumerate()
                .find(|(_, c)| pc.as_ref() == c as &str);

            if let Some((index, &tag_column)) = found_location {
                if tag_used_set[index] {
                    DuplicateGroupColumn {
                        column_name: pc.as_ref(),
                    }
                    .fail()
                } else {
                    tag_used_set[index] = true;
                    Ok(tag_column)
                }
            } else {
                GroupColumnNotFound {
                    column_name: pc.as_ref(),
                    all_tag_column_names: tag_columns.join(", "),
                }
                .fail()
            }
        })
        .collect::<Result<Vec<_>>>()?;

    new_tag_columns.extend(tag_columns.into_iter().enumerate().filter_map(|(i, c)| {
        // already used in prefix
        if tag_used_set[i] {
            None
        } else {
            Some(c)
        }
    }));

    Ok(new_tag_columns)
}

/// Helper for creating aggregates
pub(crate) struct AggExprs {
    agg_exprs: Vec<Expr>,
    field_columns: FieldColumns,
}

/// Returns an iterator of fields from schema that pass the predicate
fn filtered_fields_iter<'a>(
    schema: &'a Schema,
    predicate: &'a Predicate,
) -> impl Iterator<Item = &'a Field> {
    schema
        .fields_iter()
        .filter(move |f| predicate.should_include_field(f.name()))
}

/// Creates aggregate expressions and tracks field output according to
/// the rules explained on `read_group_plan`
impl AggExprs {
    /// Create the appropriate aggregate expressions, based on the type of the
    /// field
    pub fn try_new(agg: Aggregate, schema: &Schema, predicate: &Predicate) -> Result<Self> {
        match agg {
            Aggregate::Sum | Aggregate::Count | Aggregate::Mean => {
                //  agg_function(_val1) as _value1
                //  ...
                //  agg_function(_valN) as _valueN
                //  agg_function(time) as time

                let agg_exprs = filtered_fields_iter(schema, predicate)
                    .chain(schema.time_iter())
                    .map(|field| make_agg_expr(agg, field.name()))
                    .collect::<Result<Vec<_>>>()?;

                let field_columns = filtered_fields_iter(schema, predicate)
                    .map(|field| Arc::from(field.name().as_str()))
                    .collect::<Vec<_>>()
                    .into();

                Ok(Self {
                    agg_exprs,
                    field_columns,
                })
            }
            Aggregate::First | Aggregate::Last | Aggregate::Min | Aggregate::Max => {
                //   agg_function(_val1) as _value1
                //   agg_function(time) as time1
                //   ..
                //   agg_function(_valN) as _valueN
                //   agg_function(time) as timeN

                // might be nice to use a more functional style here
                let mut agg_exprs = Vec::new();
                let mut field_list = Vec::new();

                for field in filtered_fields_iter(schema, predicate) {
                    agg_exprs.push(make_selector_expr(
                        agg,
                        SelectorOutput::Value,
                        field.name(),
                        field.data_type(),
                        field.name(),
                    )?);

                    let time_column_name = format!("{}_{}", TIME_COLUMN_NAME, field.name());

                    agg_exprs.push(make_selector_expr(
                        agg,
                        SelectorOutput::Time,
                        field.name(),
                        field.data_type(),
                        &time_column_name,
                    )?);

                    field_list.push((
                        Arc::from(field.name().as_str()), // value name
                        Arc::from(time_column_name.as_str()),
                    ));
                }

                let field_columns = field_list.into();
                Ok(Self {
                    agg_exprs,
                    field_columns,
                })
            }
            Aggregate::None => InternalUnexpectedNoneAggregate.fail(),
        }
    }
}

/// Creates a DataFusion expression suitable for calculating an aggregate:
///
/// equivalent to `CAST agg(field) as field`
fn make_agg_expr(agg: Aggregate, field_name: &str) -> Result<Expr> {
    // For timestamps, use `MAX` which corresponds to the last
    // timestamp in the group, unless `MIN` was specifically requested
    // to be consistent with the Go implementation which takes the
    // timestamp at the end of the window
    let agg = if field_name == TIME_COLUMN_NAME && agg != Aggregate::Min {
        Aggregate::Max
    } else {
        agg
    };

    agg.to_datafusion_expr(col(field_name))
        .context(CreatingAggregates)
        .map(|agg| agg.alias(field_name))
}

/// Creates a DataFusion expression suitable for calculating the time
/// part of a selector:
///
/// equivalent to `CAST selector_time(field) as column_name`
fn make_selector_expr(
    agg: Aggregate,
    output: SelectorOutput,
    field_name: &str,
    data_type: &DataType,
    column_name: &str,
) -> Result<Expr> {
    let uda = match agg {
        Aggregate::First => selector_first(data_type, output),
        Aggregate::Last => selector_last(data_type, output),
        Aggregate::Min => selector_min(data_type, output),
        Aggregate::Max => selector_max(data_type, output),
        _ => return InternalAggregateNotSelector { agg }.fail(),
    };
    Ok(uda
        .call(vec![col(field_name), col(TIME_COLUMN_NAME)])
        .alias(column_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reorder_prefix() {
        assert_eq!(reorder_prefix_ok(&[], &[]), &[] as &[&str]);

        assert_eq!(reorder_prefix_ok(&[], &["one"]), &["one"]);
        assert_eq!(reorder_prefix_ok(&["one"], &["one"]), &["one"]);

        assert_eq!(reorder_prefix_ok(&[], &["one", "two"]), &["one", "two"]);
        assert_eq!(
            reorder_prefix_ok(&["one"], &["one", "two"]),
            &["one", "two"]
        );
        assert_eq!(
            reorder_prefix_ok(&["two"], &["one", "two"]),
            &["two", "one"]
        );
        assert_eq!(
            reorder_prefix_ok(&["two", "one"], &["one", "two"]),
            &["two", "one"]
        );

        assert_eq!(
            reorder_prefix_ok(&[], &["one", "two", "three"]),
            &["one", "two", "three"]
        );
        assert_eq!(
            reorder_prefix_ok(&["one"], &["one", "two", "three"]),
            &["one", "two", "three"]
        );
        assert_eq!(
            reorder_prefix_ok(&["two"], &["one", "two", "three"]),
            &["two", "one", "three"]
        );
        assert_eq!(
            reorder_prefix_ok(&["three", "one"], &["one", "two", "three"]),
            &["three", "one", "two"]
        );

        // errors
        assert_eq!(
            reorder_prefix_err(&["one"], &[]),
            "Group column \'one\' not found in tag columns: "
        );
        assert_eq!(
            reorder_prefix_err(&["one"], &["two", "three"]),
            "Group column \'one\' not found in tag columns: two, three"
        );
        assert_eq!(
            reorder_prefix_err(&["two", "one", "two"], &["one", "two"]),
            "Duplicate group column \'two\'"
        );
    }

    fn reorder_prefix_ok(prefix: &[&str], table_columns: &[&str]) -> Vec<String> {
        let table_columns = table_columns.to_vec();

        let res = reorder_prefix(prefix, table_columns);
        let message = format!("Expected OK, got {:?}", res);
        let res = res.expect(&message);

        res.into_iter().map(|s| s.to_string()).collect()
    }

    // returns the error string or panics if `reorder_prefix` doesn't return an
    // error
    fn reorder_prefix_err(prefix: &[&str], table_columns: &[&str]) -> String {
        let table_columns = table_columns.to_vec();

        let res = reorder_prefix(prefix, table_columns);

        match res {
            Ok(r) => {
                panic!(
                    "Expected error result from reorder_prefix_err, but was OK: '{:?}'",
                    r
                );
            }
            Err(e) => format!("{}", e),
        }
    }
}
