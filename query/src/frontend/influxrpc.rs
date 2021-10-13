//! Query frontend for InfluxDB Storage gRPC requests
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::datatypes::{DataType, Field};
use data_types::chunk_metadata::ChunkId;
use datafusion::{
    error::{DataFusionError, Result as DatafusionResult},
    logical_plan::{lit, Column, DFSchemaRef, Expr, ExprRewriter, LogicalPlan, LogicalPlanBuilder},
    optimizer::utils::expr_to_columns,
    prelude::col,
};
use datafusion_util::AsExpr;

use hashbrown::{HashMap, HashSet};
use observability_deps::tracing::{debug, info, trace};
use predicate::predicate::{Predicate, PredicateMatch};
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
        stringset::{Error as StringSetError, StringSetPlan, StringSetPlanBuilder},
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

    /// Returns a plan that lists the names of tables in this
    /// database that have at least one row that matches the
    /// conditions listed on `predicate`
    pub fn table_names<D>(&self, database: &D, predicate: Predicate) -> Result<StringSetPlan>
    where
        D: QueryDatabase + 'static,
    {
        info!(" = Start building plan for table_name");

        let mut builder = StringSetPlanBuilder::new();
        let mut normalizer = PredicateNormalizer::new(predicate);

        for chunk in database.chunks(normalizer.unnormalized()) {
            // Try and apply the predicate using only metadata
            let table_name = chunk.table_name();
            let predicate = normalizer.normalized(table_name);

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
                    // TODO: General purpose plans for table_names.
                    // https://github.com/influxdata/influxdb_iox/issues/762
                    debug!(
                        chunk=%chunk.id().get(),
                        ?predicate,
                        %table_name,
                        "can not evaluate predicate"
                    );
                    return UnsupportedPredicateForTableNames {
                        predicate: predicate.as_ref().clone(),
                    }
                    .fail();
                }
            };
        }

        let plan = builder.build().context(CreatingStringSet)?;
        info!(" = End building plan for table_name");
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
        info!(" = Start building plan for tag_keys");
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
                    debug!(
                        table_name,
                        chunk_id=%chunk.id().get(),
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
                let plan = self.tag_keys_plan(&table_name, schema, &mut normalizer, chunks)?;

                if let Some(plan) = plan {
                    builder = builder.append(plan)
                }
            }
        }

        // add the known columns we could find from metadata only
        let plan_set = builder
            .append(known_columns.into())
            .build()
            .context(CreatingStringSet);

        info!(" = End building plan for tag_keys");

        plan_set
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
        info!(" = Start building plan for tag_values");
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
                    debug!(
                        table_name,
                        chunk_id=%chunk.id().get(),
                        "need full plan to find tag values"
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
        let plan_set = builder
            .append(known_values.into())
            .build()
            .context(CreatingStringSet);

        info!(" = End building plan for tag_values");

        plan_set
    }

    /// Returns a plan that produces a list of columns and their
    /// datatypes (as defined in the data written via `write_lines`),
    /// and which have more than zero rows which pass the conditions
    /// specified by `predicate`.
    pub fn field_columns<D>(&self, database: &D, predicate: Predicate) -> Result<FieldListPlan>
    where
        D: QueryDatabase + 'static,
    {
        info!(" = Start building plan for field_columns");
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

        info!(" = End building plan for field_columns");
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
        info!(" = Start building plan for read_filter");
        debug!(predicate=?predicate, "planning read_filter");

        let mut normalizer = PredicateNormalizer::new(predicate);

        // group tables by chunk, pruning if possible
        // key is table name, values are chunks
        let chunks = database.chunks(normalizer.unnormalized());
        let table_chunks = self.group_chunks_by_table(&mut normalizer, chunks)?;

        // now, build up plans for each table
        let mut ss_plans = Vec::with_capacity(table_chunks.len());
        for (table_name, chunks) in table_chunks {
            let prefix_columns: Option<&[&str]> = None;
            let schema = database.table_schema(&table_name).context(TableRemoved {
                table_name: &table_name,
            })?;

            let ss_plan =
                self.read_filter_plan(table_name, schema, prefix_columns, &mut normalizer, chunks)?;
            // If we have to do real work, add it to the list of plans
            if let Some(ss_plan) = ss_plan {
                ss_plans.push(ss_plan);
            }
        }

        info!(" = End building plan for read_filter");
        Ok(ss_plans.into())
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
        info!(" = Start building plan for read_group");
        debug!(predicate=?predicate, agg=?agg, "planning read_group");

        let mut normalizer = PredicateNormalizer::new(predicate);

        // group tables by chunk, pruning if possible
        let chunks = database.chunks(normalizer.unnormalized());
        let table_chunks = self.group_chunks_by_table(&mut normalizer, chunks)?;
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
                    &mut normalizer,
                    chunks,
                )?,
                _ => self.read_group_plan(
                    table_name,
                    schema,
                    &mut normalizer,
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

        info!(" = End building plan for read_group");
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
        info!(" = Start building plan for read_window_aggregate");
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

        info!(" = End building plan for read_window_aggregate");
        Ok(ss_plans.into())
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
            .chain(filtered_fields_iter(&schema, &predicate))
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

        let field_columns = filtered_fields_iter(&schema, &predicate)
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
    ///   tags,
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
    ///   tags
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
        normalizer: &mut PredicateNormalizer,
        agg: Aggregate,
        group_columns: &[impl AsRef<str>],
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

        // Reorganize the output so the group columns are first and
        // the output is sorted first on the group columns and then
        // any remaining tags
        //
        // This ensures that the `group_columns` are next to each
        // other in the output in the order expected by flux
        let reordered_tag_columns: Vec<Arc<str>> = reorder_prefix(group_columns, tag_columns)?
            .into_iter()
            .map(Arc::from)
            .collect();

        // no columns if there are no tags in the input and no group columns in the query
        let plan_builder = if !reordered_tag_columns.is_empty() {
            // reorder columns
            let reorder_exprs = reordered_tag_columns
                .iter()
                .map(|tag_name| tag_name.as_expr())
                .collect::<Vec<_>>();

            let sort_exprs = reorder_exprs
                .iter()
                .map(|expr| expr.as_sort_expr())
                .collect::<Vec<_>>();

            let project_exprs =
                project_exprs_in_schema(&reordered_tag_columns, plan_builder.schema());

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

        let ss_plan = SeriesSetPlan::new(
            Arc::from(table_name),
            plan,
            reordered_tag_columns,
            field_columns,
        );

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
fn project_exprs_in_schema(prefix: &[Arc<str>], schema: &DFSchemaRef) -> Vec<Expr> {
    let seen: HashSet<_> = prefix.iter().map(|s| s.as_ref()).collect();

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
    // Equivalent SWL would look like:
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

/// Creates specialized / normalized predicates
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

    /// Return a reference to a predicate specialized for `table_name`
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
///
/// For example if the original predicate was
/// ```text
/// _measurement = "some_table"
/// ```
///
/// When evaluated on table "cpu" then the predicate is rewritten to
/// ```text
/// "cpu" = "some_table"
///
#[derive(Debug)]
struct TableNormalizedPredicate {
    inner: Arc<Predicate>,
}

impl TableNormalizedPredicate {
    fn new(table_name: &str, mut inner: Predicate) -> Self {
        inner.exprs = inner
            .exprs
            .into_iter()
            .map(|e| rewrite_measurement_references(table_name, e))
            .collect::<Vec<_>>();

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
    expr.rewrite(&mut rewriter).expect("rewrite is infallable")
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
    use schema::builder::SchemaBuilder;

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
}
