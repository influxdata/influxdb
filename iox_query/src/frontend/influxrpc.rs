//! Query frontend for InfluxDB Storage gRPC requests

use crate::{
    exec::{field::FieldColumns, make_non_null_checker, make_schema_pivot, IOxSessionContext},
    frontend::common::ScanPlanBuilder,
    plan::{
        fieldlist::FieldListPlan,
        seriesset::{SeriesSetPlan, SeriesSetPlans},
        stringset::{Error as StringSetError, StringSetPlan, StringSetPlanBuilder},
    },
    QueryChunk, QueryDatabase,
};
use arrow::datatypes::DataType;
use data_types::ChunkId;
use datafusion::{
    error::DataFusionError,
    logical_plan::{col, when, DFSchemaRef, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder},
};
use datafusion_util::AsExpr;
use hashbrown::HashSet;
use observability_deps::tracing::{debug, trace};
use predicate::{rpc_predicate::InfluxRpcPredicate, Predicate, PredicateMatch};
use query_functions::{
    group_by::{Aggregate, WindowDuration},
    make_window_bound_expr,
    selectors::{selector_first, selector_last, selector_max, selector_min, SelectorOutput},
};
use schema::{selection::Selection, InfluxColumnType, Schema, TIME_COLUMN_NAME};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
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

    #[snafu(display(
        "gRPC planner got error fetching chunks for table '{}': {}",
        table_name,
        source
    ))]
    GettingChunks {
        table_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

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

    #[snafu(display("gRPC planner got error creating predicates: {}", source))]
    CreatingPredicates {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("gRPC planner got error building plan: {}", source))]
    BuildingPlan {
        source: datafusion::error::DataFusionError,
    },

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
    CreatingAggregates {
        source: query_functions::group_by::Error,
    },

    #[snafu(display("Error creating scan:  {}", source))]
    CreatingScan { source: super::common::Error },

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

impl From<super::common::Error> for Error {
    fn from(source: super::common::Error) -> Self {
        Self::CreatingScan { source }
    }
}

impl From<DataFusionError> for Error {
    fn from(source: DataFusionError) -> Self {
        Self::BuildingPlan { source }
    }
}

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
pub struct InfluxRpcPlanner {
    /// Optional executor currently only used to provide span context for tracing.
    ctx: IOxSessionContext,
}

impl InfluxRpcPlanner {
    /// Create a new instance of the RPC planner
    pub fn new() -> Self {
        Self {
            ctx: IOxSessionContext::default(),
        }
    }

    pub fn with_execution_context(self, ctx: IOxSessionContext) -> Self {
        Self { ctx }
    }

    /// Returns a builder that includes
    ///   . A set of table names got from meta data that will participate
    ///      in the requested `predicate`
    ///   . A set of plans of tables of either
    ///       . chunks with deleted data or
    ///       . chunks without deleted data but cannot be decided from meta data
    pub async fn table_names(
        &self,
        database: &dyn QueryDatabase,
        rpc_predicate: InfluxRpcPredicate,
    ) -> Result<StringSetPlan> {
        let _ctx = self.ctx.child_ctx("table_names planning");
        debug!(?rpc_predicate, "planning table_names");

        // Special case predicates that span the entire valid timestamp range
        let rpc_predicate = rpc_predicate.clear_timestamp_if_max_range();

        let mut builder = StringSetPlanBuilder::new();

        // Mapping between table and chunks that need full plan
        let mut full_plan_table_chunks = BTreeMap::new();

        let table_predicates = rpc_predicate
            .table_predicates(database.as_meta())
            .context(CreatingPredicatesSnafu)?;
        for (table_name, predicate) in &table_predicates {
            // Identify which chunks can answer from its metadata and then record its table,
            // and which chunks needs full plan and group them into their table
            let chunks = database
                .chunks(table_name, predicate)
                .await
                .context(GettingChunksSnafu { table_name })?;
            for chunk in chunks {
                trace!(chunk_id=%chunk.id(), %table_name, "Considering table");

                // Table is already in the returned table list, no longer needs to discover it from other chunks
                if builder.contains(table_name) {
                    trace!("already seen");
                    continue;
                }

                // If the chunk has delete predicates, we need to scan (do full plan) the data to eliminate
                // deleted data before we can determine if its table participates in the requested predicate.
                if chunk.has_delete_predicates() {
                    full_plan_table_chunks
                        .entry(table_name)
                        .or_insert_with(Vec::new)
                        .push(Arc::clone(&chunk));
                } else {
                    // Try and apply the predicate using only metadata
                    let pred_result = chunk.apply_predicate_to_metadata(predicate).context(
                        CheckingChunkPredicateSnafu {
                            chunk_id: chunk.id(),
                        },
                    )?;

                    match pred_result {
                        PredicateMatch::AtLeastOneNonNullField => {
                            trace!("Metadata predicate: table matches");
                            // Meta data of the table covers predicates of the request
                            builder.append_string(table_name);
                        }
                        PredicateMatch::Unknown => {
                            trace!("Metadata predicate: unknown match");
                            // We cannot match the predicate to get answer from meta data, let do full plan
                            full_plan_table_chunks
                                .entry(table_name)
                                .or_insert_with(Vec::new)
                                .push(Arc::clone(&chunk));
                        }
                        PredicateMatch::Zero => {
                            trace!("Metadata predicate: zero rows match");
                        } // this chunk's table does not participate in the request
                    }
                }
            }
        }

        // remove items from full_plan_table_chunks whose tables are
        // already in the returned list
        for table in builder.known_strings_iter() {
            trace!(%table, "Table is known to have matches, skipping plan");
            full_plan_table_chunks.remove(table);
            if full_plan_table_chunks.is_empty() {
                break;
            }
        }

        // Now build plans for full-plan tables
        for (table_name, predicate) in &table_predicates {
            if let Some(chunks) = full_plan_table_chunks.remove(table_name) {
                let schema = database
                    .table_schema(table_name)
                    .context(TableRemovedSnafu { table_name })?;

                if let Some(plan) = self.table_name_plan(table_name, schema, predicate, chunks)? {
                    builder = builder.append_other(plan.into());
                }
            }
        }

        builder.build().context(CreatingStringSetSnafu)
    }

    /// Returns a set of plans that produces the names of "tag"
    /// columns (as defined in the InfluxDB Data model) names in this
    /// database that have more than zero rows which pass the
    /// conditions specified by `predicate`.
    pub async fn tag_keys(
        &self,
        database: &dyn QueryDatabase,
        rpc_predicate: InfluxRpcPredicate,
    ) -> Result<StringSetPlan> {
        let ctx = self.ctx.child_ctx("tag_keys planning");
        debug!(?rpc_predicate, "planning tag_keys");

        // Special case predicates that span the entire valid timestamp range
        let rpc_predicate = rpc_predicate.clear_timestamp_if_max_range();

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

        let table_predicates = rpc_predicate
            .table_predicates(database.as_meta())
            .context(CreatingPredicatesSnafu)?;
        for (table_name, predicate) in &table_predicates {
            let chunks = database
                .chunks(table_name, predicate)
                .await
                .context(GettingChunksSnafu { table_name })?;
            for chunk in chunks {
                // If there are delete predicates, we need to scan (or do full plan) the data to eliminate
                // deleted data before getting tag keys
                let mut do_full_plan = chunk.has_delete_predicates();

                // Try and apply the predicate using only metadata
                let pred_result = chunk.apply_predicate_to_metadata(predicate).context(
                    CheckingChunkPredicateSnafu {
                        chunk_id: chunk.id(),
                    },
                )?;

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
                        .column_names(
                            ctx.child_ctx("column_names execution"),
                            predicate,
                            selection,
                        )
                        .context(FindingColumnNamesSnafu)?;

                    match maybe_names {
                        Some(mut names) => {
                            debug!(
                                %table_name,
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
                        %table_name,
                        chunk_id=%chunk.id().get(),
                        "column names need full plan"
                    );

                    need_full_plans
                        .entry(table_name)
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

            for (table_name, predicate) in &table_predicates {
                if let Some(chunks) = need_full_plans.remove(table_name) {
                    let schema = database
                        .table_schema(table_name)
                        .context(TableRemovedSnafu { table_name })?;

                    let plan = self.tag_keys_plan(
                        ctx.child_ctx("tag_keys_plan"),
                        table_name,
                        schema,
                        predicate,
                        chunks,
                    )?;

                    if let Some(plan) = plan {
                        builder = builder.append_other(plan)
                    }
                }
            }
        }

        // add the known columns we could find from metadata only
        builder
            .append_other(known_columns.into())
            .build()
            .context(CreatingStringSetSnafu)
    }

    /// Returns a plan which finds the distinct, non-null tag values
    /// in the specified `tag_name` column of this database which pass
    /// the conditions specified by `predicate`.
    pub async fn tag_values(
        &self,
        database: &dyn QueryDatabase,
        tag_name: &str,
        rpc_predicate: InfluxRpcPredicate,
    ) -> Result<StringSetPlan> {
        let ctx = self.ctx.child_ctx("tag_values planning");
        debug!(?rpc_predicate, tag_name, "planning tag_values");

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

        let table_predicates = rpc_predicate
            .table_predicates(database.as_meta())
            .context(CreatingPredicatesSnafu)?;
        for (table_name, predicate) in &table_predicates {
            let chunks = database
                .chunks(table_name, predicate)
                .await
                .context(GettingChunksSnafu { table_name })?;
            for chunk in chunks {
                // If there are delete predicates, we need to scan (or do full plan) the data to eliminate
                // deleted data before getting tag values
                let mut do_full_plan = chunk.has_delete_predicates();

                // Try and apply the predicate using only metadata
                let pred_result = chunk.apply_predicate_to_metadata(predicate).context(
                    CheckingChunkPredicateSnafu {
                        chunk_id: chunk.id(),
                    },
                )?;

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
                    InvalidTagColumnSnafu {
                        tag_name,
                        influx_column_type,
                    }
                );
                ensure!(
                    influx_column_type
                        .unwrap()
                        .valid_arrow_type(field.data_type()),
                    InternalInvalidTagTypeSnafu {
                        tag_name,
                        data_type: field.data_type().clone(),
                    }
                );

                if !do_full_plan {
                    // try and get the list of values directly from metadata
                    let maybe_values = chunk
                        .column_values(
                            self.ctx.child_ctx("tag_values execution"),
                            tag_name,
                            predicate,
                        )
                        .context(FindingColumnValuesSnafu)?;

                    match maybe_values {
                        Some(mut names) => {
                            debug!(
                                %table_name,
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
                        %table_name,
                        chunk_id=%chunk.id().get(),
                        "need full plan to find tag values"
                    );

                    need_full_plans
                        .entry(table_name)
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
        for (table_name, predicate) in &table_predicates {
            if let Some(chunks) = need_full_plans.remove(table_name) {
                let schema = database
                    .table_schema(table_name)
                    .context(TableRemovedSnafu { table_name })?;

                let scan_and_filter = ScanPlanBuilder::new(schema)
                    .with_session_context(ctx.child_ctx("scan_and_filter planning"))
                    .with_chunks(chunks)
                    .with_predicate(predicate)
                    .build()?;

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
                let plan = scan_and_filter
                    .plan_builder
                    .project(select_exprs.clone())
                    .context(BuildingPlanSnafu)?
                    .filter(tag_name_is_not_null)
                    .context(BuildingPlanSnafu)?
                    .build()
                    .context(BuildingPlanSnafu)?;

                builder = builder.append_other(plan.into());
            }
        }

        // add the known values we could find from metadata only
        builder
            .append_other(known_values.into())
            .build()
            .context(CreatingStringSetSnafu)
    }

    /// Returns a plan that produces a list of columns and their
    /// datatypes (as defined in the data written via `write_lines`),
    /// and which have more than zero rows which pass the conditions
    /// specified by `predicate`.
    pub async fn field_columns(
        &self,
        database: &dyn QueryDatabase,
        rpc_predicate: InfluxRpcPredicate,
    ) -> Result<FieldListPlan> {
        let ctx = self.ctx.child_ctx("field_columns planning");
        debug!(?rpc_predicate, "planning field_columns");

        // Special case predicates that span the entire valid timestamp range
        let rpc_predicate = rpc_predicate.clear_timestamp_if_max_range();

        // Algorithm is to run a "select field_cols from table where
        // <predicate> type plan for each table in the chunks"
        //
        // The executor then figures out which columns have non-null
        // values and stops the plan executing once it has them

        let table_predicates = rpc_predicate
            .table_predicates(database.as_meta())
            .context(CreatingPredicatesSnafu)?;
        let mut field_list_plan = FieldListPlan::with_capacity(table_predicates.len());

        for (table_name, predicate) in &table_predicates {
            let chunks = database
                .chunks(table_name, predicate)
                .await
                .context(GettingChunksSnafu { table_name })?;
            let chunks = prune_chunks_metadata(chunks, predicate)?;

            if chunks.is_empty() {
                continue;
            }

            let schema = database
                .table_schema(table_name)
                .context(TableRemovedSnafu { table_name })?;

            if let Some(plan) = self.field_columns_plan(
                ctx.child_ctx("field_columns plan"),
                schema,
                predicate,
                chunks,
            )? {
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
    pub async fn read_filter(
        &self,
        database: &dyn QueryDatabase,
        rpc_predicate: InfluxRpcPredicate,
    ) -> Result<SeriesSetPlans> {
        let ctx = self.ctx.child_ctx("planning_read_filter");
        debug!(?rpc_predicate, "planning read_filter");

        let table_predicates = rpc_predicate
            .table_predicates(database.as_meta())
            .context(CreatingPredicatesSnafu)?;
        let mut ss_plans = Vec::with_capacity(table_predicates.len());
        for (table_name, predicate) in &table_predicates {
            let chunks = database
                .chunks(table_name, predicate)
                .await
                .context(GettingChunksSnafu { table_name })?;
            let chunks = prune_chunks_metadata(chunks, predicate)?;

            if chunks.is_empty() {
                continue;
            }

            let schema = database
                .table_schema(table_name)
                .context(TableRemovedSnafu { table_name })?;

            let ss_plan = self.read_filter_plan(
                ctx.child_ctx("read_filter plan"),
                table_name,
                schema,
                predicate,
                chunks,
            )?;
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
    pub async fn read_group(
        &self,
        database: &dyn QueryDatabase,
        rpc_predicate: InfluxRpcPredicate,
        agg: Aggregate,
        group_columns: &[impl AsRef<str> + Send + Sync],
    ) -> Result<SeriesSetPlans> {
        let ctx = self.ctx.child_ctx("read_group planning");
        debug!(?rpc_predicate, ?agg, "planning read_group");

        let table_predicates = rpc_predicate
            .table_predicates(database.as_meta())
            .context(CreatingPredicatesSnafu)?;
        let mut ss_plans = Vec::with_capacity(table_predicates.len());

        for (table_name, predicate) in &table_predicates {
            let chunks = database
                .chunks(table_name, predicate)
                .await
                .context(GettingChunksSnafu { table_name })?;
            let chunks = prune_chunks_metadata(chunks, predicate)?;

            if chunks.is_empty() {
                continue;
            }

            let schema = database
                .table_schema(table_name)
                .context(TableRemovedSnafu { table_name })?;

            let ss_plan = match agg {
                Aggregate::None => self.read_filter_plan(
                    ctx.child_ctx("read_filter plan"),
                    table_name,
                    Arc::clone(&schema),
                    predicate,
                    chunks,
                )?,
                _ => self.read_group_plan(
                    ctx.child_ctx("read_group plan"),
                    table_name,
                    schema,
                    predicate,
                    agg,
                    chunks,
                )?,
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
    pub async fn read_window_aggregate(
        &self,
        database: &dyn QueryDatabase,
        rpc_predicate: InfluxRpcPredicate,
        agg: Aggregate,
        every: WindowDuration,
        offset: WindowDuration,
    ) -> Result<SeriesSetPlans> {
        let ctx = self.ctx.child_ctx("read_window_aggregate planning");
        debug!(
            ?rpc_predicate,
            ?agg,
            ?every,
            ?offset,
            "planning read_window_aggregate"
        );

        // group tables by chunk, pruning if possible
        let table_predicates = rpc_predicate
            .table_predicates(database.as_meta())
            .context(CreatingPredicatesSnafu)?;
        let mut ss_plans = Vec::with_capacity(table_predicates.len());
        for (table_name, predicate) in &table_predicates {
            let chunks = database
                .chunks(table_name, predicate)
                .await
                .context(GettingChunksSnafu { table_name })?;
            let chunks = prune_chunks_metadata(chunks, predicate)?;

            if chunks.is_empty() {
                continue;
            }

            let schema = database
                .table_schema(table_name)
                .context(TableRemovedSnafu { table_name })?;

            let ss_plan = self.read_window_aggregate_plan(
                ctx.child_ctx("read_window_aggregate plan"),
                table_name,
                schema,
                predicate,
                agg,
                every,
                offset,
                chunks,
            )?;

            // If we have to do real work, add it to the list of plans
            if let Some(ss_plan) = ss_plan {
                ss_plans.push(ss_plan);
            }
        }

        Ok(SeriesSetPlans::new(ss_plans))
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
    fn tag_keys_plan(
        &self,
        ctx: IOxSessionContext,
        table_name: &str,
        schema: Arc<Schema>,
        predicate: &Predicate,
        chunks: Vec<Arc<dyn QueryChunk>>,
    ) -> Result<Option<StringSetPlan>> {
        let scan_and_filter = ScanPlanBuilder::new(schema)
            .with_session_context(ctx.child_ctx("scan_and_filter planning"))
            .with_predicate(predicate)
            .with_chunks(chunks)
            .build()?;

        // now, select only the tag columns
        let select_exprs = scan_and_filter
            .schema()
            .iter()
            .filter_map(|(influx_column_type, field)| {
                if matches!(influx_column_type, Some(InfluxColumnType::Tag)) {
                    Some(col(field.name()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // If the projection is empty then there is no plan to execute.
        if select_exprs.is_empty() {
            return Ok(None);
        }

        let plan = scan_and_filter
            .plan_builder
            .project(select_exprs)
            .context(BuildingPlanSnafu)?
            .build()
            .context(BuildingPlanSnafu)?;

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
    fn field_columns_plan(
        &self,
        ctx: IOxSessionContext,
        schema: Arc<Schema>,
        predicate: &Predicate,
        chunks: Vec<Arc<dyn QueryChunk>>,
    ) -> Result<Option<LogicalPlan>> {
        let scan_and_filter = ScanPlanBuilder::new(schema)
            .with_session_context(ctx.child_ctx("scan_and_filter planning"))
            .with_predicate(predicate)
            .with_chunks(chunks)
            .build()?;

        // Selection of only fields and time
        let select_exprs = scan_and_filter
            .schema()
            .iter()
            .filter_map(|(influx_column_type, field)| match influx_column_type {
                Some(InfluxColumnType::Field(_)) => Some(col(field.name())),
                Some(InfluxColumnType::Timestamp) => Some(col(field.name())),
                Some(_) => None,
                None => None,
            })
            .collect::<Vec<_>>();

        let plan = scan_and_filter
            .plan_builder
            .project(select_exprs)
            .context(BuildingPlanSnafu)?
            .build()
            .context(BuildingPlanSnafu)?;

        Ok(Some(plan))
    }

    /// Creates a DataFusion LogicalPlan that returns the values in
    /// the fields for a specified table:
    ///
    /// The output produces the table name as a single string if any
    /// non null values are passed in.
    ///
    /// The data is not sorted in any particular order
    ///
    /// returns `None` if the table contains no rows that would pass
    /// the predicate.
    ///
    /// The created plan looks like:
    ///
    /// ```text
    ///  NonNullChecker
    ///    Projection (select fields)
    ///      Filter(predicate) [optional]
    ///        Scan
    /// ```
    fn table_name_plan(
        &self,
        table_name: &str,
        schema: Arc<Schema>,
        predicate: &Predicate,
        chunks: Vec<Arc<dyn QueryChunk>>,
    ) -> Result<Option<LogicalPlan>> {
        debug!(%table_name, "Creating table_name full plan");
        let scan_and_filter = ScanPlanBuilder::new(schema)
            .with_session_context(self.ctx.child_ctx("scan_and_filter planning"))
            .with_predicate(predicate)
            .with_chunks(chunks)
            .build()?;

        // Select only fields requested
        let select_exprs: Vec<_> = filtered_fields_iter(&scan_and_filter.schema(), predicate)
            .map(|field| col(field.name))
            .collect();

        let plan = scan_and_filter
            .plan_builder
            .project(select_exprs)
            .context(BuildingPlanSnafu)?
            .build()
            .context(BuildingPlanSnafu)?;

        // Add the final node that outputs the table name or not, depending
        let plan = make_non_null_checker(table_name, plan);

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
    fn read_filter_plan(
        &self,
        ctx: IOxSessionContext,
        table_name: impl AsRef<str>,
        schema: Arc<Schema>,
        predicate: &Predicate,
        chunks: Vec<Arc<dyn QueryChunk>>,
    ) -> Result<Option<SeriesSetPlan>> {
        let table_name = table_name.as_ref();
        let scan_and_filter = ScanPlanBuilder::new(schema)
            .with_session_context(ctx.child_ctx("scan_and_filter planning"))
            .with_predicate(predicate)
            .with_chunks(chunks)
            .build()?;

        let tags_and_timestamp: Vec<_> = scan_and_filter
            .schema()
            .tags_iter()
            .chain(scan_and_filter.schema().time_iter())
            .map(|f| f.name() as &str)
            // Convert to SortExprs to pass to the plan builder
            .map(|n| n.as_sort_expr())
            .collect();

        // Order by
        let plan_builder = scan_and_filter
            .plan_builder
            .sort(tags_and_timestamp)
            .context(BuildingPlanSnafu)?;

        // Select away anything that isn't in the influx data model
        let schema = scan_and_filter.schema();
        let tags_fields_and_timestamps: Vec<Expr> = schema
            .tags_iter()
            .map(|field| field.name().as_expr())
            .chain(filtered_fields_iter(&schema, predicate).map(|f| f.expr))
            .chain(schema.time_iter().map(|field| field.name().as_expr()))
            .collect();

        let plan_builder = plan_builder
            .project(tags_fields_and_timestamps)
            .context(BuildingPlanSnafu)?;

        let plan = plan_builder.build().context(BuildingPlanSnafu)?;

        let tag_columns = schema
            .tags_iter()
            .map(|field| Arc::from(field.name().as_str()))
            .collect();

        let field_columns = filtered_fields_iter(&schema, predicate)
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
    fn read_group_plan(
        &self,
        ctx: IOxSessionContext,
        table_name: &str,
        schema: Arc<Schema>,
        predicate: &Predicate,
        agg: Aggregate,
        chunks: Vec<Arc<dyn QueryChunk>>,
    ) -> Result<Option<SeriesSetPlan>> {
        let scan_and_filter = ScanPlanBuilder::new(schema)
            .with_session_context(ctx.child_ctx("scan_and_filter planning"))
            .with_predicate(predicate)
            .with_chunks(chunks)
            .build()?;

        // order the tag columns so that the group keys come first (we
        // will group and
        // order in the same order)
        let schema = scan_and_filter.schema();
        let tag_columns: Vec<_> = schema.tags_iter().map(|f| f.name() as &str).collect();

        // Group by all tag columns
        let group_exprs = tag_columns
            .iter()
            .map(|tag_name| tag_name.as_expr())
            .collect::<Vec<_>>();

        let AggExprs {
            agg_exprs,
            field_columns,
        } = AggExprs::try_new_for_read_group(agg, &schema, predicate)?;

        let plan_builder = scan_and_filter
            .plan_builder
            .aggregate(group_exprs, agg_exprs)
            .context(BuildingPlanSnafu)?;

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
                .context(BuildingPlanSnafu)?
                .sort(sort_exprs)
                .context(BuildingPlanSnafu)?
        } else {
            plan_builder
        };

        let plan_builder = cast_aggregates(plan_builder, agg, &field_columns)?;

        let plan = plan_builder.build().context(BuildingPlanSnafu)?;

        let tag_columns = tag_columns.iter().map(|s| Arc::from(*s)).collect();
        let ss_plan = SeriesSetPlan::new(
            Arc::from(table_name.to_string()),
            plan,
            tag_columns,
            field_columns,
        );

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
    fn read_window_aggregate_plan(
        &self,
        ctx: IOxSessionContext,
        table_name: impl Into<String>,
        schema: Arc<Schema>,
        predicate: &Predicate,
        agg: Aggregate,
        every: WindowDuration,
        offset: WindowDuration,
        chunks: Vec<Arc<dyn QueryChunk>>,
    ) -> Result<Option<SeriesSetPlan>> {
        let table_name = table_name.into();
        let scan_and_filter = ScanPlanBuilder::new(schema)
            .with_session_context(ctx.child_ctx("scan_and_filter planning"))
            .with_predicate(predicate)
            .with_chunks(chunks)
            .build()?;

        let schema = scan_and_filter.schema();

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
        } = AggExprs::try_new_for_read_window_aggregate(agg, &schema, predicate)?;

        // sort by the group by expressions as well
        let sort_exprs = group_exprs
            .iter()
            .map(|expr| expr.as_sort_expr())
            .collect::<Vec<_>>();

        let plan_builder = scan_and_filter
            .plan_builder
            .aggregate(group_exprs, agg_exprs)?
            .sort(sort_exprs)?;

        let plan_builder = cast_aggregates(plan_builder, agg, &field_columns)?;

        // and finally create the plan
        let plan = plan_builder.build()?;

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
}

/// Prunes the provided list of chunks using [`QueryChunk::apply_predicate_to_metadata`]
///
/// TODO: Should this logic live with the rest of the chunk pruning logic?
fn prune_chunks_metadata(
    chunks: Vec<Arc<dyn QueryChunk>>,
    predicate: &Predicate,
) -> Result<Vec<Arc<dyn QueryChunk>>> {
    let mut filtered = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        // Try and apply the predicate using only metadata
        let pred_result =
            chunk
                .apply_predicate_to_metadata(predicate)
                .context(CheckingChunkPredicateSnafu {
                    chunk_id: chunk.id(),
                })?;

        trace!(?pred_result, chunk_id=?chunk.id(), "applied predicate to metadata");

        if !matches!(pred_result, PredicateMatch::Zero) {
            filtered.push(chunk)
        }
    }

    Ok(filtered)
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
                    .context(CastingAggregatesSnafu { agg, field_name })?
                    .alias(field_name)
            } else {
                col(field_name)
            };
            Ok(expr)
        })
        .collect::<Result<Vec<_>>>()?;

    plan_builder.project(cast_exprs).context(BuildingPlanSnafu)
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
            .map(|value_expr| value_expr.replace_col(f.name()))
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
            Aggregate::None => InternalUnexpectedNoneAggregateSnafu.fail(),
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
            Aggregate::None => InternalUnexpectedNoneAggregateSnafu.fail(),
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
        .context(CreatingAggregatesSnafu)
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
        _ => return InternalAggregateNotSelectorSnafu { agg }.fail(),
    };

    Ok(uda
        .call(vec![field.expr, col(TIME_COLUMN_NAME)])
        .alias(col_name))
}

#[cfg(test)]
mod tests {
    use datafusion::logical_plan::lit;
    use futures::{future::BoxFuture, FutureExt};
    use predicate::Predicate;

    use crate::{
        exec::Executor,
        test::{TestChunk, TestDatabase},
    };

    use super::*;

    #[tokio::test]
    async fn test_predicate_rewrite_table_names() {
        run_test(|test_db, rpc_predicate| {
            async move {
                InfluxRpcPlanner::new()
                    .table_names(test_db, rpc_predicate)
                    .await
                    .expect("creating plan");
            }
            .boxed()
        })
        .await
    }

    #[tokio::test]
    async fn test_predicate_rewrite_tag_keys() {
        run_test(|test_db, rpc_predicate| {
            async move {
                InfluxRpcPlanner::new()
                    .tag_keys(test_db, rpc_predicate)
                    .await
                    .expect("creating plan");
            }
            .boxed()
        })
        .await
    }

    #[tokio::test]
    async fn test_predicate_rewrite_tag_values() {
        run_test(|test_db, rpc_predicate| {
            async move {
                InfluxRpcPlanner::new()
                    .tag_values(test_db, "foo", rpc_predicate)
                    .await
                    .expect("creating plan");
            }
            .boxed()
        })
        .await
    }

    #[tokio::test]
    async fn test_predicate_rewrite_field_columns() {
        run_test(|test_db, rpc_predicate| {
            async move {
                InfluxRpcPlanner::new()
                    .field_columns(test_db, rpc_predicate)
                    .await
                    .expect("creating plan");
            }
            .boxed()
        })
        .await
    }

    #[tokio::test]
    async fn test_predicate_rewrite_read_filter() {
        run_test(|test_db, rpc_predicate| {
            async move {
                InfluxRpcPlanner::new()
                    .read_filter(test_db, rpc_predicate)
                    .await
                    .expect("creating plan");
            }
            .boxed()
        })
        .await
    }

    #[tokio::test]
    async fn test_predicate_read_group() {
        run_test(|test_db, rpc_predicate| {
            async move {
                let agg = Aggregate::None;
                let group_columns = &["foo"];
                InfluxRpcPlanner::new()
                    .read_group(test_db, rpc_predicate, agg, group_columns)
                    .await
                    .expect("creating plan");
            }
            .boxed()
        })
        .await
    }

    #[tokio::test]
    async fn test_predicate_read_window_aggregate() {
        run_test(|test_db, rpc_predicate| {
            async move {
                let agg = Aggregate::First;
                let every = WindowDuration::from_months(1, false);
                let offset = WindowDuration::from_months(1, false);
                InfluxRpcPlanner::new()
                    .read_window_aggregate(test_db, rpc_predicate, agg, every, offset)
                    .await
                    .expect("creating plan");
            }
            .boxed()
        })
        .await
    }

    /// Runs func() and checks that predicates are simplified prior to
    /// sending them down to the chunks for processing.
    async fn run_test<T>(func: T)
    where
        T: for<'a> Fn(&'a TestDatabase, InfluxRpcPredicate) -> BoxFuture<'a, ()> + Send + Sync,
    {
        // ------------- Test 1 ----------------

        // this is what happens with a grpc predicate on a tag
        //
        // tag(foo) = 'bar' becomes
        //
        // CASE WHEN foo IS NULL then '' ELSE foo END = 'bar'
        //
        // It is critical to be rewritten foo = 'bar' correctly so
        // that it can be evaluated quickly
        let expr = when(col("foo").is_null(), lit(""))
            .otherwise(col("foo"))
            .unwrap();
        let silly_predicate = Predicate::new().with_expr(expr.eq(lit("bar")));

        // verify that the predicate was rewritten to `foo = 'bar'`
        let expr = col("foo").eq(lit("bar"));
        let expected_predicate = Predicate::new().with_expr(expr);

        run_test_with_predicate(&func, silly_predicate, expected_predicate).await;

        // ------------- Test 2 ----------------
        // Validate that _measurement predicates are translated
        //
        // https://github.com/influxdata/influxdb_iox/issues/3601
        // _measurement = 'foo'
        let silly_predicate = Predicate::new().with_expr(col("_measurement").eq(lit("foo")));

        // verify that the predicate was rewritten to `false` as the
        // measurement name is `h20`
        let expr = lit(false);

        let expected_predicate = Predicate::new().with_expr(expr);
        run_test_with_predicate(&func, silly_predicate, expected_predicate).await;

        // ------------- Test 3 ----------------
        // more complicated _measurement predicates are translated
        //
        // https://github.com/influxdata/influxdb_iox/issues/3601
        // (_measurement = 'foo' or measurement = 'h2o') AND time > 5
        let silly_predicate = Predicate::new().with_expr(
            col("_measurement")
                .eq(lit("foo"))
                .or(col("_measurement").eq(lit("h2o")))
                .and(col("time").gt(lit(5))),
        );

        // verify that the predicate was rewritten to time > 5
        let expr = col("time").gt(lit(5));

        let expected_predicate = Predicate::new().with_expr(expr);
        run_test_with_predicate(&func, silly_predicate, expected_predicate).await;
    }

    /// Runs func() with the specified predicate and verifies
    /// `expected_predicate` is received by the chunk
    async fn run_test_with_predicate<T>(
        func: &T,
        predicate: Predicate,
        expected_predicate: Predicate,
    ) where
        T: for<'a> Fn(&'a TestDatabase, InfluxRpcPredicate) -> BoxFuture<'a, ()> + Send + Sync,
    {
        let chunk0 = Arc::new(
            TestChunk::new("h2o")
                .with_id(0)
                .with_tag_column("foo")
                .with_time_column(),
        );

        let executor = Arc::new(Executor::new(1));
        let test_db = TestDatabase::new(Arc::clone(&executor));
        test_db.add_chunk("my_partition_key", Arc::clone(&chunk0));

        let rpc_predicate = InfluxRpcPredicate::new(None, predicate);

        // run the function
        func(&test_db, rpc_predicate).await;

        let actual_predicate = test_db.get_chunks_predicate();

        assert_eq!(
            actual_predicate, expected_predicate,
            "\nActual: {:?}\nExpected: {:?}",
            actual_predicate, expected_predicate
        );

        executor.join().await;
    }
}
