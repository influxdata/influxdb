//! Query frontend for InfluxDB Storage gRPC requests
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::datatypes::DataType;
use data_types::chunk_metadata::ChunkId;
use datafusion::{
    error::{DataFusionError, Result as DatafusionResult},
    logical_plan::{
        binary_expr, lit, when, Column, DFSchemaRef, Expr, ExprRewriter, LogicalPlan,
        LogicalPlanBuilder, Operator,
    },
    optimizer::utils::expr_to_columns,
    prelude::col,
    scalar::ScalarValue,
};
use datafusion_util::AsExpr;

use hashbrown::{HashMap, HashSet};
use observability_deps::tracing::{debug, trace};
use predicate::predicate::{BinaryExpr, Predicate, PredicateMatch};
use schema::selection::Selection;
use schema::{InfluxColumnType, Schema, TIME_COLUMN_NAME};
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
        stringset::{
            Error as StringSetError, StringSetPlan, StringSetPlanBuilder, TableNamePlanBuilder,
        },
    },
    provider::ProviderBuilder,
    QueryChunk, QueryChunkMeta, QueryDatabase,
};

/// Any column references to this name are rewritten to be
/// the actual table name by the Influx gRPC planner.
///
/// This is required to support predicates like
/// `_measurement = "foo" OR tag1 = "bar"`
///
/// The plan for each table will have the value of `_measurement`
/// filled in with a literal for the respective name of that field
pub const MEASUREMENT_COLUMN_NAME: &str = "_measurement";

/// Any equality expressions using this column name are removed and replaced
/// with projections on the specified column.
///
/// This is required to support predicates like
/// `_field` = temperature
pub const FIELD_COLUMN_NAME: &str = "_field";

/// Any column references to this name are rewritten to be a disjunctive set of
/// expressions to all field columns for the table schema.
///
/// This is required to support predicates like
/// `_value` = 1.77
///
/// The plan for each table will have expression containing `_value` rewritten
/// into multiple expressions (one for each field column).
pub const VALUE_COLUMN_NAME: &str = "_value";

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
        chunk_id: ChunkId,
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

    #[snafu(display(
        "gRPC planner got error casting aggregate {:?} for {}: {}",
        agg,
        field_name,
        source
    ))]
    CastingAggregates {
        agg: Aggregate,
        field_name: String,
        source: datafusion::error::DataFusionError,
    },

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

    /// Returns a builder that includes
    ///   . A set of table names got from meta data that will participate
    ///      in the requested `predicate`
    ///   . A set of plans of tables of either
    ///       . chunks with deleted data or
    ///       . chunks without deleted data but cannot be decided from meta data
    pub fn table_names<D>(&self, database: &D, predicate: Predicate) -> Result<TableNamePlanBuilder>
    where
        D: QueryDatabase + 'static,
    {
        let mut builder = TableNamePlanBuilder::new();
        let mut normalizer = PredicateNormalizer::new(predicate);

        // Mapping between table and chunks that need full plan
        let mut full_plan_table_chunks = BTreeMap::new();

        // Identify which chunks can answer from its metadata and then record its table,
        // and which chunks needs full plan and group them into their table
        for chunk in database.chunks(normalizer.unnormalized()) {
            let table_name = chunk.table_name();

            // Table is already in the returned table list, no longer needs to discover it from other chunks
            if builder.contains_meta_data_table(table_name.to_string()) {
                continue;
            }

            // If the chunk has delete predicates, we need to scan (do full plan) the data to eliminate
            // deleted data before we can determine if its table participates in the requested predicate.
            if chunk.has_delete_predicates() {
                full_plan_table_chunks
                    .entry(table_name.to_string())
                    .or_insert_with(Vec::new)
                    .push(Arc::clone(&chunk));
            } else {
                // See if we can have enough info from the chunk's meta data to answer
                // that this table participates in the request
                let predicate = normalizer.normalized(table_name);
                //
                // Try and apply the predicate using only metadata
                let pred_result = chunk
                    .apply_predicate_to_metadata(&predicate)
                    .map_err(|e| Box::new(e) as _)
                    .context(CheckingChunkPredicate {
                        chunk_id: chunk.id(),
                    })?;
                //
                match pred_result {
                    PredicateMatch::AtLeastOne => {
                        // Meta data of the table covers predicates of the request
                        builder.append_meta_data_table(table_name.to_string());
                    }
                    PredicateMatch::Unknown => {
                        // We cannot match the predicate to get answer from meta data, let do full plan
                        full_plan_table_chunks
                            .entry(table_name.to_string())
                            .or_insert_with(Vec::new)
                            .push(Arc::clone(&chunk));
                    }
                    PredicateMatch::Zero => {} // this chunk's table does not participate in the request
                }
            }
        }

        // remove items from full_plan_table_chunks whose tables are already in the returned list
        let meta_data_tables = builder.meta_data_table_names();
        for table in meta_data_tables {
            full_plan_table_chunks.remove(&table);
            if full_plan_table_chunks.is_empty() {
                break;
            }
        }

        // No full plans needed
        if full_plan_table_chunks.is_empty() {
            return Ok(builder);
        }

        // Now build plans for full-plan tables
        for (table_name, chunks) in full_plan_table_chunks {
            let schema = database.table_schema(&table_name).context(TableRemoved {
                table_name: &table_name,
            })?;
            if let Some(plan) =
                self.table_name_plan(&table_name, schema, &mut normalizer, chunks)?
            {
                builder.append_plans(table_name, plan);
            }
        }

        Ok(builder)
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
        let mut normalizer = PredicateNormalizer::new(predicate);

        let mut known_columns = BTreeSet::new();
        for chunk in database.chunks(normalizer.unnormalized()) {
            // If there are delete predicates, we need to scan (or do full plan) the data to eliminate
            // deleted data before getting tag keys
            let mut do_full_plan = chunk.has_delete_predicates();

            let table_name = chunk.table_name();
            let predicate = normalizer.normalized(table_name);

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

            // get only tag columns from metadata
            let schema = chunk.schema();

            let column_names: Vec<&str> = schema
                .tags_iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<&str>>();

            let selection = Selection::Some(&column_names);

            if !do_full_plan {
                // filter the columns further from the predicate
                let maybe_names = chunk
                    .column_names(&predicate, selection)
                    .map_err(|e| Box::new(e) as _)
                    .context(FindingColumnNames)?;

                match maybe_names {
                    Some(mut names) => {
                        debug!(
                            table_name,
                            names=?names,
                            chunk_id=%chunk.id().get(),
                            "column names found from metadata",
                        );
                        known_columns.append(&mut names);
                    }
                    None => {
                        do_full_plan = true;
                    }
                }
            }

            // can't get columns only from metadata, need
            // a general purpose plan
            if do_full_plan {
                debug!(
                    table_name,
                    chunk_id=%chunk.id().get(),
                    "column names need full plan"
                );

                need_full_plans
                    .entry(table_name.to_string())
                    .or_insert_with(Vec::new)
                    .push(Arc::clone(&chunk));
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
                let plan = self.tag_keys_plan(&table_name, schema, &mut normalizer, chunks)?;

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

        let mut normalizer = PredicateNormalizer::new(predicate);
        let mut known_values = BTreeSet::new();
        for chunk in database.chunks(normalizer.unnormalized()) {
            // If there are delete predicates, we need to scan (or do full plan) the data to eliminate
            // deleted data before getting tag values
            let mut do_full_plan = chunk.has_delete_predicates();

            let table_name = chunk.table_name();
            let predicate = normalizer.normalized(table_name);

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

            if !do_full_plan {
                // try and get the list of values directly from metadata
                let maybe_values = chunk
                    .column_values(tag_name, &predicate)
                    .map_err(|e| Box::new(e) as _)
                    .context(FindingColumnValues)?;

                match maybe_values {
                    Some(mut names) => {
                        debug!(
                            table_name,
                            names=?names,
                            chunk_id=%chunk.id().get(),
                            "tag values found from metadata",
                        );
                        known_values.append(&mut names);
                    }
                    None => {
                        do_full_plan = true;
                    }
                }
            }

            // can't get columns only from metadata, need
            // a general purpose plan
            if do_full_plan {
                debug!(
                    table_name,
                    chunk_id=%chunk.id().get(),
                    "need full plan to find tag values"
                );

                need_full_plans
                    .entry(table_name.to_string())
                    .or_insert_with(Vec::new)
                    .push(Arc::clone(&chunk));
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
            let scan_and_filter =
                self.scan_and_filter(&table_name, schema, &mut normalizer, chunks)?;

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
        let mut normalizer = PredicateNormalizer::new(predicate);

        // map table -> Vec<Arc<Chunk>>
        let chunks = database.chunks(normalizer.unnormalized());
        let table_chunks = self.group_chunks_by_table(&mut normalizer, chunks)?;

        let mut field_list_plan = FieldListPlan::new();
        for (table_name, chunks) in table_chunks {
            let schema = database.table_schema(&table_name).context(TableRemoved {
                table_name: &table_name,
            })?;
            if let Some(plan) =
                self.field_columns_plan(&table_name, schema, &mut normalizer, chunks)?
            {
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

        let mut normalizer = PredicateNormalizer::new(predicate);

        // group tables by chunk, pruning if possible
        // key is table name, values are chunks
        let chunks = database.chunks(normalizer.unnormalized());
        let table_chunks = self.group_chunks_by_table(&mut normalizer, chunks)?;

        // now, build up plans for each table
        let mut ss_plans = Vec::with_capacity(table_chunks.len());
        for (table_name, chunks) in table_chunks {
            let schema = database.table_schema(&table_name).context(TableRemoved {
                table_name: &table_name,
            })?;

            let ss_plan = self.read_filter_plan(table_name, schema, &mut normalizer, chunks)?;
            // If we have to do real work, add it to the list of plans
            if let Some(ss_plan) = ss_plan {
                ss_plans.push(ss_plan);
            }
        }

        Ok(SeriesSetPlans::new(ss_plans))
    }

    /// Creates one or more GroupedSeriesSet plans that produces an
    /// output table with rows grouped according to group_columns and
    /// an aggregate function which is applied to each *series* (aka
    /// distinct set of tag value). Note the aggregate is not applied
    /// across series within the same group.
    ///
    /// Specifically the data that is output from the plans is
    /// guaranteed to be sorted such that:
    ///
    ///   1. The group_columns are a prefix of the sort key
    ///
    ///   2. All remaining tags appear in the sort key, in order,
    ///   after the prefix (as the tag key may also appear as a group
    ///   key)
    ///
    /// Schematically, the plan looks like:
    ///
    /// (order by {group_coumns, remaining tags})
    ///   (aggregate by group -- agg, gby_exprs=tags)
    ///      (apply filters)
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

        let mut normalizer = PredicateNormalizer::new(predicate);

        // group tables by chunk, pruning if possible
        let chunks = database.chunks(normalizer.unnormalized());
        let table_chunks = self.group_chunks_by_table(&mut normalizer, chunks)?;

        // now, build up plans for each table
        let mut ss_plans = Vec::with_capacity(table_chunks.len());
        for (table_name, chunks) in table_chunks {
            let schema = database.table_schema(&table_name).context(TableRemoved {
                table_name: &table_name,
            })?;
            let ss_plan = match agg {
                Aggregate::None => {
                    self.read_filter_plan(table_name, Arc::clone(&schema), &mut normalizer, chunks)?
                }
                _ => self.read_group_plan(table_name, schema, &mut normalizer, agg, chunks)?,
            };

            // If we have to do real work, add it to the list of plans
            if let Some(ss_plan) = ss_plan {
                ss_plans.push(ss_plan);
            }
        }

        let plan = SeriesSetPlans::new(ss_plans);

        // Note always group (which will resort the frames)
        // by tag, even if there are 0 columns
        let group_columns = group_columns
            .iter()
            .map(|s| Arc::from(s.as_ref()))
            .collect();
        Ok(plan.grouped_by(group_columns))
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

        let mut normalizer = PredicateNormalizer::new(predicate);

        // group tables by chunk, pruning if possible
        let chunks = database.chunks(normalizer.unnormalized());
        let table_chunks = self.group_chunks_by_table(&mut normalizer, chunks)?;

        // now, build up plans for each table
        let mut ss_plans = Vec::with_capacity(table_chunks.len());
        for (table_name, chunks) in table_chunks {
            let schema = database.table_schema(&table_name).context(TableRemoved {
                table_name: &table_name,
            })?;
            let ss_plan = self.read_window_aggregate_plan(
                table_name,
                schema,
                &mut normalizer,
                agg,
                &every,
                &offset,
                chunks,
            )?;
            // If we have to do real work, add it to the list of plans
            if let Some(ss_plan) = ss_plan {
                ss_plans.push(ss_plan);
            }
        }

        Ok(SeriesSetPlans::new(ss_plans))
    }

    /// Creates a map of table_name --> Chunks that have that table that *may* pass the predicate
    fn group_chunks_by_table<C>(
        &self,
        normalizer: &mut PredicateNormalizer,
        chunks: Vec<Arc<C>>,
    ) -> Result<BTreeMap<String, Vec<Arc<C>>>>
    where
        C: QueryChunk + 'static,
    {
        let mut table_chunks = BTreeMap::new();
        for chunk in chunks {
            let predicate = normalizer.normalized(chunk.table_name());
            // Try and apply the predicate using only metadata
            let pred_result = chunk
                .apply_predicate_to_metadata(&predicate)
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
        normalizer: &mut PredicateNormalizer,
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<StringSetPlan>>
    where
        C: QueryChunk + 'static,
    {
        let scan_and_filter = self.scan_and_filter(table_name, schema, normalizer, chunks)?;

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
        normalizer: &mut PredicateNormalizer,
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<LogicalPlan>>
    where
        C: QueryChunk + 'static,
    {
        let scan_and_filter = self.scan_and_filter(table_name, schema, normalizer, chunks)?;
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

    /// Creates a DataFusion LogicalPlan that returns the timestamp
    /// for a specified table:
    ///
    /// The output looks like (time)
    /// The time column is chosen because it must be included in all tables
    ///
    /// The data is not sorted in any particular order
    ///
    /// returns `None` if the table contains no rows that would pass
    /// the predicate.
    ///
    /// The created plan looks like:
    ///
    /// ```text
    ///  Projection (select time)
    ///      Filter(predicate) [optional]
    ///        Scan
    /// ```
    //  TODO: for optimization in the future, build `select count(*)` plan instead,
    //        ,but if we do this, we also need to change the way we handle output
    //        of the function invoking this because it will always return a number
    fn table_name_plan<C>(
        &self,
        table_name: &str,
        schema: Arc<Schema>,
        normalizer: &mut PredicateNormalizer,
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<LogicalPlan>>
    where
        C: QueryChunk + 'static,
    {
        let scan_and_filter = self.scan_and_filter(table_name, schema, normalizer, chunks)?;
        let TableScanAndFilter {
            plan_builder,
            schema,
        } = match scan_and_filter {
            None => return Ok(None),
            Some(t) => t,
        };

        // Selection of only time
        let select_exprs = schema
            .iter()
            .filter_map(|(influx_column_type, field)| match influx_column_type {
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
        normalizer: &mut PredicateNormalizer,
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<SeriesSetPlan>>
    where
        C: QueryChunk + 'static,
    {
        let table_name = table_name.as_ref();
        let scan_and_filter = self.scan_and_filter(table_name, schema, normalizer, chunks)?;
        let predicate = normalizer.normalized(table_name);

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
            // Convert to SortExprs to pass to the plan builder
            .map(|n| n.as_sort_expr())
            .collect();

        // Order by
        let plan_builder = plan_builder
            .sort(tags_and_timestamp)
            .context(BuildingPlan)?;

        // Select away anything that isn't in the influx data model
        let tags_fields_and_timestamps: Vec<Expr> = schema
            .tags_iter()
            .map(|field| field.name().as_expr())
            .chain(filtered_fields_iter(&schema, &predicate).map(|f| f.expr))
            .chain(schema.time_iter().map(|field| field.name().as_expr()))
            .collect();

        let plan_builder = plan_builder
            .project(tags_fields_and_timestamps)
            .context(BuildingPlan)?;

        let plan = plan_builder.build().context(BuildingPlan)?;

        let tag_columns = schema
            .tags_iter()
            .map(|field| Arc::from(field.name().as_str()))
            .collect();

        let field_columns = filtered_fields_iter(&schema, &predicate)
            .map(|field| Arc::from(field.name))
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
    /// with one row per tagset and the values aggregated using a
    /// specific function.
    ///
    /// Equivalent to this SQL query for 'aggregates': sum, count, mean
    /// SELECT
    ///   tag1...tagN
    ///   agg_function(_val1) as _value1
    ///   ...
    ///   agg_function(_valN) as _valueN
    ///   agg_function(time) as time
    /// GROUP BY
    ///   tags,
    /// ORDER BY
    ///   tags
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
    ///   tags
    /// ORDER BY
    ///   tags
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
        normalizer: &mut PredicateNormalizer,
        agg: Aggregate,
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<SeriesSetPlan>>
    where
        C: QueryChunk + 'static,
    {
        let table_name = table_name.into();
        let scan_and_filter = self.scan_and_filter(&table_name, schema, normalizer, chunks)?;
        let predicate = normalizer.normalized(&table_name);

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

        // Group by all tag columns
        let group_exprs = tag_columns
            .iter()
            .map(|tag_name| tag_name.as_expr())
            .collect::<Vec<_>>();

        let AggExprs {
            agg_exprs,
            field_columns,
        } = AggExprs::try_new_for_read_group(agg, &schema, &predicate)?;

        let plan_builder = plan_builder
            .aggregate(group_exprs, agg_exprs)
            .context(BuildingPlan)?;

        // Reorganize the output so it is ordered and sorted on tag columns

        // no columns if there are no tags in the input and no group columns in the query
        let plan_builder = if !tag_columns.is_empty() {
            // reorder columns
            let reorder_exprs = tag_columns
                .iter()
                .map(|tag_name| tag_name.as_expr())
                .collect::<Vec<_>>();

            let sort_exprs = reorder_exprs
                .iter()
                .map(|expr| expr.as_sort_expr())
                .collect::<Vec<_>>();

            let project_exprs = project_exprs_in_schema(&tag_columns, plan_builder.schema());

            plan_builder
                .project(project_exprs)
                .context(BuildingPlan)?
                .sort(sort_exprs)
                .context(BuildingPlan)?
        } else {
            plan_builder
        };

        let plan_builder = cast_aggregates(plan_builder, agg, &field_columns)?;

        let plan = plan_builder.build().context(BuildingPlan)?;

        let tag_columns = tag_columns.iter().map(|s| Arc::from(*s)).collect();
        let ss_plan = SeriesSetPlan::new(Arc::from(table_name), plan, tag_columns, field_columns);

        Ok(Some(ss_plan))
    }

    /// Creates a SeriesSetPlan that produces an output table with rows
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
        normalizer: &mut PredicateNormalizer,
        agg: Aggregate,
        every: &WindowDuration,
        offset: &WindowDuration,
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<SeriesSetPlan>>
    where
        C: QueryChunk + 'static,
    {
        let table_name = table_name.into();
        let scan_and_filter = self.scan_and_filter(&table_name, schema, normalizer, chunks)?;
        let predicate = normalizer.normalized(&table_name);

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

        let AggExprs {
            agg_exprs,
            field_columns,
        } = AggExprs::try_new_for_read_window_aggregate(agg, &schema, &predicate)?;

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

        let plan_builder = cast_aggregates(plan_builder, agg, &field_columns)?;

        // and finally create the plan
        let plan = plan_builder.build().context(BuildingPlan)?;

        let tag_columns = schema
            .tags_iter()
            .map(|field| Arc::from(field.name().as_str()))
            .collect();

        Ok(Some(SeriesSetPlan::new(
            Arc::from(table_name),
            plan,
            tag_columns,
            field_columns,
        )))
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
        normalizer: &mut PredicateNormalizer,
        chunks: Vec<Arc<C>>,
    ) -> Result<Option<TableScanAndFilter>>
    where
        C: QueryChunk + 'static,
    {
        let predicate = normalizer.normalized(table_name);

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

            plan_builder = plan_builder.filter(filter_expr).context(BuildingPlan)?;
        }

        Ok(Some(TableScanAndFilter {
            plan_builder,
            schema,
        }))
    }
}

/// Return a `Vec` of `Exprs` such that it starts with `prefix` cols and
/// then has all columns in `schema` that are not already in the prefix.
fn project_exprs_in_schema(prefix: &[&str], schema: &DFSchemaRef) -> Vec<Expr> {
    let seen: HashSet<_> = prefix.iter().cloned().collect();

    let prefix_exprs = prefix.iter().map(|name| col(name));
    let new_exprs = schema.fields().iter().filter_map(|f| {
        let name = f.name().as_str();
        if !seen.contains(name) {
            Some(col(name))
        } else {
            None
        }
    });

    prefix_exprs.chain(new_exprs).collect()
}

/// casts aggregates (fields named in field_columns) to the types
/// expected by Flux. Currently this means converting count aggregates
/// into Int64
fn cast_aggregates(
    plan_builder: LogicalPlanBuilder,
    agg: Aggregate,
    field_columns: &FieldColumns,
) -> Result<LogicalPlanBuilder> {
    if !matches!(agg, Aggregate::Count) {
        return Ok(plan_builder);
    }

    let schema = plan_builder.schema();

    // in read_group and read_window_aggregate, aggregates are only
    // applied to fields, so all fields are also aggregates.
    let field_names: HashSet<&str> = match field_columns {
        FieldColumns::SharedTimestamp(field_names) => {
            field_names.iter().map(|s| s.as_ref()).collect()
        }
        FieldColumns::DifferentTimestamp(fields_and_timestamps) => fields_and_timestamps
            .iter()
            .map(|(field, _timestamp)| field.as_ref())
            .collect(),
    };

    // Build expressions for each select list
    let cast_exprs = schema
        .fields()
        .iter()
        .map(|df_field| {
            let field_name = df_field.name();
            let expr = if field_names.contains(field_name.as_str()) {
                // CAST(field_name as Int64) as field_name
                col(field_name)
                    .cast_to(&DataType::Int64, schema)
                    .context(CastingAggregates { agg, field_name })?
                    .alias(field_name)
            } else {
                col(field_name)
            };
            Ok(expr)
        })
        .collect::<Result<Vec<_>>>()?;

    plan_builder.project(cast_exprs).context(BuildingPlan)
}

struct TableScanAndFilter {
    /// Represents plan that scans a table and applies optional filtering
    plan_builder: LogicalPlanBuilder,
    /// The IOx schema of the result
    schema: Arc<Schema>,
}

/// Helper for creating aggregates
pub(crate) struct AggExprs {
    agg_exprs: Vec<Expr>,
    field_columns: FieldColumns,
}

// Encapsulates a field column projection as an expression. In the simplest case
// the expression is a `Column` expression. In more complex cases it might be
// a predicate that filters rows for the projection.
#[derive(Clone)]
struct FieldExpr<'a> {
    expr: Expr,
    name: &'a str,
    datatype: &'a DataType,
}

// Returns an iterator of fields from schema that pass the predicate. If there
// are expressions associated with field column projections then these are
// applied to the column via `CASE` statements.
//
// TODO(edd): correctly support multiple `_value` expressions. Right now they
// are OR'd together, which makes sense for equality operators like `_value == xyz`.
fn filtered_fields_iter<'a>(
    schema: &'a Schema,
    predicate: &'a Predicate,
) -> impl Iterator<Item = FieldExpr<'a>> {
    schema.fields_iter().filter_map(move |f| {
        if !predicate.should_include_field(f.name()) {
            return None;
        }

        // For example, assume two fields (`field1` and `field2`) along with
        // a predicate like: `_value = 1.32 OR _value = 2.87`. The projected
        // field columns become:
        //
        // SELECT
        //  CASE WHEN #field1 = Float64(1.32) OR #field1 = Float64(2.87) THEN #field1 END AS field1,
        //  CASE WHEN #field2 = Float64(1.32) OR #field2 = Float64(2.87) THEN #field2 END AS field2
        //
        let expr = predicate
            .value_expr
            .iter()
            .map(|BinaryExpr { left: _, op, right }| {
                binary_expr(col(f.name()), *op, right.as_expr())
            })
            .reduce(|a, b| a.or(b))
            .map(|when_expr| when(when_expr, col(f.name())).end())
            .unwrap_or_else(|| Ok(col(f.name())))
            .unwrap();

        Some(FieldExpr {
            expr: expr.alias(f.name()),
            name: f.name(),
            datatype: f.data_type(),
        })
    })
}

/// Creates aggregate expressions and tracks field output according to
/// the rules explained on `read_group_plan`
impl AggExprs {
    /// Create the appropriate aggregate expressions, based on the type of the
    /// field for a `read_group` plan.
    pub fn try_new_for_read_group(
        agg: Aggregate,
        schema: &Schema,
        predicate: &Predicate,
    ) -> Result<Self> {
        match agg {
            Aggregate::Sum | Aggregate::Count | Aggregate::Mean => {
                Self::agg_for_read_group(agg, schema, predicate)
            }
            Aggregate::First | Aggregate::Last | Aggregate::Min | Aggregate::Max => {
                Self::selector_aggregates(agg, schema, predicate)
            }
            Aggregate::None => InternalUnexpectedNoneAggregate.fail(),
        }
    }

    /// Create the appropriate aggregate expressions, based on the type of the
    /// field for a `read_window_aggregate` plan.
    pub fn try_new_for_read_window_aggregate(
        agg: Aggregate,
        schema: &Schema,
        predicate: &Predicate,
    ) -> Result<Self> {
        match agg {
            Aggregate::Sum | Aggregate::Count | Aggregate::Mean => {
                Self::agg_for_read_window_aggregate(agg, schema, predicate)
            }
            Aggregate::First | Aggregate::Last | Aggregate::Min | Aggregate::Max => {
                Self::selector_aggregates(agg, schema, predicate)
            }
            Aggregate::None => InternalUnexpectedNoneAggregate.fail(),
        }
    }

    // Creates special aggregate "selector" expressions for the fields in the
    // provided schema. Selectors ensure that the relevant aggregate functions
    // are also provided to a distinct time column for each field column.
    //
    // Equivalent SQL would look like:
    //
    //   agg_function(_val1) as _value1
    //   agg_function(time) as time1
    //   ..
    //   agg_function(_valN) as _valueN
    //   agg_function(time) as timeN
    fn selector_aggregates(agg: Aggregate, schema: &Schema, predicate: &Predicate) -> Result<Self> {
        // might be nice to use a more functional style here
        let mut agg_exprs = Vec::new();
        let mut field_list = Vec::new();

        for field in filtered_fields_iter(schema, predicate) {
            let field_name = field.name;
            agg_exprs.push(make_selector_expr(
                agg,
                SelectorOutput::Value,
                field.clone(),
                field_name,
            )?);

            let time_column_name = format!("{}_{}", TIME_COLUMN_NAME, field_name);

            agg_exprs.push(make_selector_expr(
                agg,
                SelectorOutput::Time,
                field,
                &time_column_name,
            )?);

            field_list.push((
                Arc::from(field_name), // value name
                Arc::from(time_column_name.as_str()),
            ));
        }

        let field_columns = field_list.into();
        Ok(Self {
            agg_exprs,
            field_columns,
        })
    }

    // Creates aggregate expressions for use in a read_group plan, which
    // includes the time column.
    //
    // Equivalent SQL would look like this:
    //
    //  agg_function(_val1) as _value1
    //  ...
    //  agg_function(_valN) as _valueN
    //  agg_function(time) as time
    fn agg_for_read_group(agg: Aggregate, schema: &Schema, predicate: &Predicate) -> Result<Self> {
        let agg_exprs = filtered_fields_iter(schema, predicate)
            .map(|field| make_agg_expr(agg, field))
            .chain(schema.time_iter().map(|field| {
                make_agg_expr(
                    agg,
                    FieldExpr {
                        expr: col(field.name()),
                        datatype: field.data_type(),
                        name: field.name(),
                    },
                )
            }))
            .collect::<Result<Vec<_>>>()?;

        let field_columns = filtered_fields_iter(schema, predicate)
            .map(|field| Arc::from(field.name))
            .collect::<Vec<_>>()
            .into();

        Ok(Self {
            agg_exprs,
            field_columns,
        })
    }

    // Creates aggregate expressions for use in a read_window_aggregate plan. No
    // aggregates are created for the time column because the
    // `read_window_aggregate` uses a time column calculated using window
    // bounds.
    //
    // Equivalent SQL would look like this:
    //
    //  agg_function(_val1) as _value1
    //  ...
    //  agg_function(_valN) as _valueN
    fn agg_for_read_window_aggregate(
        agg: Aggregate,
        schema: &Schema,
        predicate: &Predicate,
    ) -> Result<Self> {
        let agg_exprs = filtered_fields_iter(schema, predicate)
            .map(|field| make_agg_expr(agg, field))
            .collect::<Result<Vec<_>>>()?;

        let field_columns = filtered_fields_iter(schema, predicate)
            .map(|field| Arc::from(field.name))
            .collect::<Vec<_>>()
            .into();

        Ok(Self {
            agg_exprs,
            field_columns,
        })
    }
}

/// Creates a DataFusion expression suitable for calculating an aggregate:
///
/// equivalent to `CAST agg(field) as field`
fn make_agg_expr(agg: Aggregate, field_expr: FieldExpr<'_>) -> Result<Expr> {
    // For timestamps, use `MAX` which corresponds to the last
    // timestamp in the group, unless `MIN` was specifically requested
    // to be consistent with the Go implementation which takes the
    // timestamp at the end of the window
    let agg = if field_expr.name == TIME_COLUMN_NAME && agg != Aggregate::Min {
        Aggregate::Max
    } else {
        agg
    };

    let field_name = field_expr.name;
    agg.to_datafusion_expr(field_expr.expr)
        .context(CreatingAggregates)
        .map(|agg| agg.alias(field_name))
}

/// Creates a DataFusion expression suitable for calculating the time part of a
/// selector:
///
/// The output expression is equivalent to `CAST selector_time(field_expression)
/// as col_name`.
///
/// In the simplest scenarios the field expressions are `Column` expressions.
/// In some cases the field expressions are `CASE` statements such as for
/// example:
///
/// CAST selector_time(
///     CASE WHEN field = 1.87 OR field = 1.99 THEN field
///     ELSE NULL
/// END) as col_name
///
fn make_selector_expr<'a>(
    agg: Aggregate,
    output: SelectorOutput,
    field: FieldExpr<'a>,
    col_name: &'a str,
) -> Result<Expr> {
    let uda = match agg {
        Aggregate::First => selector_first(field.datatype, output),
        Aggregate::Last => selector_last(field.datatype, output),
        Aggregate::Min => selector_min(field.datatype, output),
        Aggregate::Max => selector_max(field.datatype, output),
        _ => return InternalAggregateNotSelector { agg }.fail(),
    };

    Ok(uda
        .call(vec![field.expr, col(TIME_COLUMN_NAME)])
        .alias(col_name))
}

/// Creates specialized / normalized predicates that are tailored to a specific
/// table.
#[derive(Debug)]
struct PredicateNormalizer {
    unnormalized: Predicate,
    normalized: HashMap<String, TableNormalizedPredicate>,
}

impl PredicateNormalizer {
    fn new(unnormalized: Predicate) -> Self {
        Self {
            unnormalized,
            normalized: Default::default(),
        }
    }

    /// Return a reference to the unnormalized predicate
    fn unnormalized(&self) -> &Predicate {
        &self.unnormalized
    }

    /// Return a reference to a predicate specialized for `table_name` based on
    /// its `schema`.
    fn normalized(&mut self, table_name: &str) -> Arc<Predicate> {
        if let Some(normalized_predicate) = self.normalized.get(table_name) {
            return normalized_predicate.inner();
        }

        let normalized_predicate =
            TableNormalizedPredicate::new(table_name, self.unnormalized.clone());

        self.normalized
            .entry(table_name.to_string())
            .or_insert(normalized_predicate)
            .inner()
    }
}

/// Predicate that has been "specialized" / normalized for a
/// particular table. Specifically:
///
/// * all references to the [MEASUREMENT_COLUMN_NAME] column in any
/// `Exprs` are rewritten with the actual table name
/// * any expression on the [VALUE_COLUMN_NAME] column is rewritten to be
/// applied across all field columns.
/// * any expression on the [FIELD_COLUMN_NAME] is rewritten to be
/// applied for the particular fields.
///
/// For example if the original predicate was
/// ```text
/// _measurement = "some_table"
/// ```
///
/// When evaluated on table "cpu" then the predicate is rewritten to
/// ```text
/// "cpu" = "some_table"
/// ```
///
/// if the original predicate contained
/// ```text
/// _value > 34.2
/// ```
///
/// When evaluated on table "cpu" then the expression is rewritten as a
/// collection of disjunctive expressions against all field columns
/// ```text
/// ("field1" > 34.2 OR "field2" > 34.2 OR "fieldn" > 34.2)
/// ```
#[derive(Debug)]
struct TableNormalizedPredicate {
    inner: Arc<Predicate>,
}

impl TableNormalizedPredicate {
    fn new(table_name: &str, mut inner: Predicate) -> Self {
        let mut field_projections = BTreeSet::new();
        let mut field_value_exprs = vec![];

        inner.exprs = inner
            .exprs
            .into_iter()
            .map(|e| rewrite_measurement_references(table_name, e))
            // Rewrite any references to `_value = some_value` to literal true values.
            // Keeps track of these expressions, which can then be used to
            // augment field projections with conditions using `CASE` statements.
            .map(|e| rewrite_field_value_references(&mut field_value_exprs, e))
            .map(|e| {
                // Rewrite any references to `_field = a_field_name` with a literal true
                // and keep track of referenced field names to add to the field
                // column projection set.
                rewrite_field_column_references(&mut field_projections, e)
            })
            .collect::<Vec<_>>();
        // Store any field value (`_value`) expressions on the `Predicate`.
        inner.value_expr = field_value_exprs;

        if !field_projections.is_empty() {
            match &mut inner.field_columns {
                Some(field_columns) => field_columns.extend(field_projections.into_iter()),
                None => inner.field_columns = Some(field_projections),
            };
        }

        Self {
            inner: Arc::new(inner),
        }
    }

    fn inner(&self) -> Arc<Predicate> {
        Arc::clone(&self.inner)
    }
}

/// Rewrites all references to the [MEASUREMENT_COLUMN_NAME] column
/// with the actual table name
fn rewrite_measurement_references(table_name: &str, expr: Expr) -> Expr {
    let mut rewriter = MeasurementRewriter { table_name };
    expr.rewrite(&mut rewriter).expect("rewrite is infallible")
}

struct MeasurementRewriter<'a> {
    table_name: &'a str,
}

impl ExprRewriter for MeasurementRewriter<'_> {
    fn mutate(&mut self, expr: Expr) -> DatafusionResult<Expr> {
        Ok(match expr {
            // rewrite col("_measurement") --> "table_name"
            Expr::Column(Column { relation, name }) if name == MEASUREMENT_COLUMN_NAME => {
                // should not have a qualified foo._measurement
                // reference
                assert!(relation.is_none());
                lit(self.table_name)
            }
            // no rewrite needed
            _ => expr,
        })
    }
}

/// Rewrites an expression on `_value` as a boolean true literal, pushing any
/// encountered expressions onto `value_exprs` so they can be moved onto column
/// projections.
fn rewrite_field_value_references(value_exprs: &mut Vec<BinaryExpr>, expr: Expr) -> Expr {
    let mut rewriter = FieldValueRewriter { value_exprs };
    expr.rewrite(&mut rewriter).expect("rewrite is infallible")
}

struct FieldValueRewriter<'a> {
    value_exprs: &'a mut Vec<BinaryExpr>,
}

impl<'a> ExprRewriter for FieldValueRewriter<'a> {
    fn mutate(&mut self, expr: Expr) -> DatafusionResult<Expr> {
        Ok(match expr {
            Expr::BinaryExpr {
                ref left,
                op,
                ref right,
            } => {
                if let Expr::Column(inner) = &**left {
                    if inner.name == VALUE_COLUMN_NAME {
                        self.value_exprs.push(BinaryExpr {
                            left: inner.to_owned(),
                            op,
                            right: right.as_expr(),
                        });
                        return Ok(Expr::Literal(ScalarValue::Boolean(Some(true))));
                    }
                }
                expr
            }
            _ => expr,
        })
    }
}

/// Rewrites a predicate on `_field` as a projection on a specific defined by
/// the literal in the expression.
///
/// For example, the expression `_field = "load4"` is removed from the
/// normalised expression, and a column "load4" added to the predicate
/// projection.
fn rewrite_field_column_references(
    field_projections: &'_ mut BTreeSet<String>,
    expr: Expr,
) -> Expr {
    let mut rewriter = FieldColumnRewriter { field_projections };
    expr.rewrite(&mut rewriter).expect("rewrite is infallible")
}

struct FieldColumnRewriter<'a> {
    field_projections: &'a mut BTreeSet<String>,
}

impl<'a> ExprRewriter for FieldColumnRewriter<'a> {
    fn mutate(&mut self, expr: Expr) -> DatafusionResult<Expr> {
        Ok(match expr {
            Expr::BinaryExpr {
                ref left,
                op,
                ref right,
            } => {
                if let Expr::Column(inner) = &**left {
                    if inner.name != FIELD_COLUMN_NAME || op != Operator::Eq {
                        // TODO(edd): add support for !=
                        return Ok(expr);
                    }

                    if let Expr::Literal(ScalarValue::Utf8(Some(name))) = &**right {
                        self.field_projections.insert(name.to_owned());
                        return Ok(Expr::Literal(ScalarValue::Boolean(Some(true))));
                    }
                }
                expr
            }
            _ => expr,
        })
    }
}

/// Returns true if all columns referred to in `expr` are present in
/// the schema, false otherwise
pub fn schema_has_all_expr_columns(schema: &Schema, expr: &Expr) -> bool {
    let mut predicate_columns = std::collections::HashSet::new();
    expr_to_columns(expr, &mut predicate_columns).unwrap();

    predicate_columns.into_iter().all(|col| {
        let col_name = col.name.as_str();
        col_name == MEASUREMENT_COLUMN_NAME || schema.find_index_of(col_name).is_some()
    })
}

#[cfg(test)]
mod tests {
    use datafusion::logical_plan::{binary_expr, Operator};
    use schema::builder::SchemaBuilder;

    use super::*;

    #[test]
    fn test_schema_has_all_exprs_() {
        let schema = SchemaBuilder::new().tag("t1").timestamp().build().unwrap();

        assert!(schema_has_all_expr_columns(
            &schema,
            &col("t1").eq(lit("foo"))
        ));
        assert!(!schema_has_all_expr_columns(
            &schema,
            &col("t2").eq(lit("foo"))
        ));
        assert!(schema_has_all_expr_columns(
            &schema,
            &col("t1").eq(col("time"))
        ));
        assert!(!schema_has_all_expr_columns(
            &schema,
            &col("t1").eq(col("time2"))
        ));
        assert!(!schema_has_all_expr_columns(
            &schema,
            &col("t1").eq(col("time")).and(col("t3").lt(col("time")))
        ));

        assert!(schema_has_all_expr_columns(
            &schema,
            &col("t1")
                .eq(col("time"))
                .and(col("_measurement").eq(lit("the_table")))
        ));
    }

    #[test]
    fn test_field_value_rewriter() {
        let mut rewriter = FieldValueRewriter {
            value_exprs: &mut vec![],
        };

        let cases = vec![
            (
                binary_expr(col("f1"), Operator::Eq, lit(1.82)),
                binary_expr(col("f1"), Operator::Eq, lit(1.82)),
                vec![],
            ),
            (col("t2"), col("t2"), vec![]),
            (
                binary_expr(col(VALUE_COLUMN_NAME), Operator::Eq, lit(1.82)),
                // _value = 1.82 -> true
                lit(true),
                vec![BinaryExpr {
                    left: Column {
                        relation: None,
                        name: VALUE_COLUMN_NAME.into(),
                    },
                    op: Operator::Eq,
                    right: lit(1.82),
                }],
            ),
        ];

        for (input, exp, mut value_exprs) in cases {
            let rewritten = input.rewrite(&mut rewriter).unwrap();
            assert_eq!(rewritten, exp);
            assert_eq!(rewriter.value_exprs, &mut value_exprs);
        }

        // Test case with single field.
        let mut rewriter = FieldValueRewriter {
            value_exprs: &mut vec![],
        };

        let input = binary_expr(col(VALUE_COLUMN_NAME), Operator::Gt, lit(1.88));
        let rewritten = input.rewrite(&mut rewriter).unwrap();
        assert_eq!(rewritten, lit(true));
        assert_eq!(
            rewriter.value_exprs,
            &mut vec![BinaryExpr {
                left: Column {
                    relation: None,
                    name: VALUE_COLUMN_NAME.into(),
                },
                op: Operator::Gt,
                right: lit(1.88),
            }]
        );
    }

    #[test]
    fn test_field_column_rewriter() {
        let mut field_columns = BTreeSet::new();
        let mut rewriter = FieldColumnRewriter {
            field_projections: &mut field_columns,
        };

        let cases = vec![
            (
                binary_expr(col("f1"), Operator::Eq, lit(1.82)),
                binary_expr(col("f1"), Operator::Eq, lit(1.82)),
                vec![],
            ),
            (
                // TODO - should be rewritten and project onto all field columns
                binary_expr(col(FIELD_COLUMN_NAME), Operator::NotEq, lit("foo")),
                binary_expr(col(FIELD_COLUMN_NAME), Operator::NotEq, lit("foo")),
                vec![],
            ),
            (
                binary_expr(col(FIELD_COLUMN_NAME), Operator::Eq, lit("f1")),
                lit(true),
                vec!["f1"],
            ),
            (
                binary_expr(
                    binary_expr(col(FIELD_COLUMN_NAME), Operator::Eq, lit("f1")),
                    Operator::Or,
                    binary_expr(col(FIELD_COLUMN_NAME), Operator::Eq, lit("f2")),
                ),
                binary_expr(lit(true), Operator::Or, lit(true)),
                vec!["f1", "f2"],
            ),
        ];

        for (input, exp_expr, field_columns) in cases {
            let rewritten = input.rewrite(&mut rewriter).unwrap();

            assert_eq!(rewritten, exp_expr);
            let mut exp_field_columns = field_columns
                .into_iter()
                .map(String::from)
                .collect::<BTreeSet<String>>();
            assert_eq!(rewriter.field_projections, &mut exp_field_columns);
        }
    }
}
