use arrow::datatypes::SchemaRef;

use datafusion::physical_expr::execution_props::ExecutionProps;
use influxdb_influxql_parser::show_field_keys::ShowFieldKeysStatement;
use influxdb_influxql_parser::show_measurements::ShowMeasurementsStatement;
use influxdb_influxql_parser::show_tag_keys::ShowTagKeysStatement;
use influxdb_influxql_parser::show_tag_values::ShowTagValuesStatement;
use iox_query_params::StatementParams;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use crate::plan::{
    InfluxQLToLogicalPlan, SchemaProvider, parse_regex, strip_influxql_metadata_from_plan,
};
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::ExecutionPlan,
};
use influxdb_influxql_parser::common::MeasurementName;
use influxdb_influxql_parser::parse_statements;
use influxdb_influxql_parser::statement::Statement;
use influxdb_influxql_parser::visit::{Visitable, Visitor};
use iox_query::exec::IOxSessionContext;
use schema::Schema;
use tracing::debug;

struct ContextSchemaProvider<'a> {
    state: &'a SessionState,
    tables: HashMap<String, (Arc<dyn TableSource>, Schema)>,
}

impl SchemaProvider for ContextSchemaProvider<'_> {
    fn get_table_provider(&self, name: &str) -> Result<Arc<dyn TableSource>> {
        self.tables
            .get(name)
            .map(|(t, _)| Arc::clone(t))
            .ok_or_else(|| DataFusionError::Plan(format!("measurement does not exist: {name}")))
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions().get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }

    fn table_names(&self) -> Vec<&'_ str> {
        self.tables.keys().map(|k| k.as_str()).collect::<Vec<_>>()
    }

    fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    fn table_schema(&self, name: &str) -> Option<Schema> {
        self.tables.get(name).map(|(_, s)| s.clone())
    }

    fn execution_props(&self) -> &ExecutionProps {
        self.state.execution_props()
    }
}

/// A physical operator that overrides the `schema` API,
/// to return an amended version owned by `SchemaExec`. The
/// principal use case is to add additional metadata to the schema.
struct SchemaExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,

    /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
    cache: PlanProperties,
}

impl SchemaExec {
    fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        let cache = Self::compute_properties(&input, Arc::clone(&schema));
        Self {
            input,
            schema,
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as equivalence properties, partitioning, ordering, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>, schema: SchemaRef) -> PlanProperties {
        let eq_properties = match input.properties().output_ordering() {
            None => EquivalenceProperties::new(schema),
            Some(output_ordering) => EquivalenceProperties::new_with_orderings(
                schema,
                std::iter::once(output_ordering.iter().cloned()),
            ),
        };

        let output_partitioning = input.output_partitioning().clone();

        PlanProperties::new(
            eq_properties,
            output_partitioning,
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }
}

impl Debug for SchemaExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_as(DisplayFormatType::Default, f)
    }
}

impl ExecutionPlan for SchemaExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    fn statistics(&self) -> Result<datafusion::physical_plan::Statistics, DataFusionError> {
        Ok(datafusion::physical_plan::Statistics::new_unknown(
            &self.schema(),
        ))
    }
}

impl DisplayAs for SchemaExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "SchemaExec")
            }
        }
    }
}

/// Create plans for running InfluxQL queries against databases
#[derive(Debug, Copy, Clone)]
pub struct InfluxQLQueryPlanner;

impl InfluxQLQueryPlanner {
    /// Plan an InfluxQL query against the catalogs registered with `ctx`, and return a
    /// DataFusion physical execution plan that runs on the query executor.
    pub async fn query(
        query: &str,
        params: impl Into<StatementParams> + Send,
        ctx: &IOxSessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ctx = ctx.child_ctx("InfluxQLQueryPlanner::query");
        debug!(text=%query, "planning InfluxQL query");

        let statement = Self::query_to_statement(query)?;
        let logical_plan = Self::statement_to_plan(statement, params, &ctx).await?;

        // Strip "influxql::filled" metadata before physical planning. Removing
        // it avoids schema mismatch errors since parquet files won't have it.
        let logical_plan = strip_influxql_metadata_from_plan(logical_plan)?;

        let input = ctx.create_physical_plan(&logical_plan).await?;

        // Merge schema-level metadata from the logical plan with the
        // schema from the physical plan, as it is not propagated through the
        // physical planning process.
        let input_schema = input.schema();
        let mut md = input_schema.metadata().clone();
        md.extend(logical_plan.schema().metadata().clone());
        let schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
            input_schema.fields().clone(),
            md,
        ));

        Ok(Arc::new(SchemaExec::new(input, schema)))
    }

    /// Process a [`Statement`] and its associated params into a [`LogicalPlan`] fallibly
    pub async fn statement_to_plan(
        statement: Statement,
        params: impl Into<StatementParams> + Send,
        ctx: &IOxSessionContext,
    ) -> Result<LogicalPlan> {
        use std::collections::hash_map::Entry;

        let ctx = ctx.child_ctx("statement_to_plan");
        let session_cfg = ctx.inner().copied_config();
        let cfg = session_cfg.options();
        let schema = ctx
            .inner()
            .catalog(&cfg.catalog.default_catalog)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "failed to resolve catalog: {}",
                    cfg.catalog.default_catalog
                ))
            })?
            .schema(&cfg.catalog.default_schema)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "failed to resolve schema: {}",
                    cfg.catalog.default_schema
                ))
            })?;
        let names = schema.table_names();
        let query_tables = find_all_measurements(&statement, &names)?;

        let mut sp = ContextSchemaProvider {
            state: &ctx.inner().state(),
            tables: HashMap::with_capacity(query_tables.len()),
        };

        for table_name in &query_tables {
            if let Entry::Vacant(v) = sp.tables.entry(table_name.to_string()) {
                let mut ctx = ctx.child_ctx("get table schema");
                ctx.set_metadata("table", table_name.to_owned());

                if let Some(table) = schema.table(table_name).await? {
                    let schema = Schema::try_from(table.schema())
                        .map_err(|err| {
                            DataFusionError::Internal(format!("unable to convert DataFusion schema for measurement {table_name} to IOx schema: {err}"))
                        })?;
                    v.insert((provider_as_source(table), schema));
                }
            }
        }

        let planner = InfluxQLToLogicalPlan::new(&sp, &ctx);
        let logical_plan = planner.statement_to_plan_with_params(statement, params.into())?;
        debug!(plan=%logical_plan.display_graphviz(), "logical plan");
        Ok(logical_plan)
    }

    pub fn query_to_statement(query: &str) -> Result<Statement> {
        let mut statements =
            parse_statements(query).map_err(|e| DataFusionError::Plan(e.to_string()))?;

        if statements.len() != 1 {
            return Err(DataFusionError::NotImplemented(
                "The context currently only supports a single InfluxQL statement".to_string(),
            ));
        }

        Ok(statements.pop().unwrap())
    }
}

fn find_all_measurements(stmt: &Statement, tables: &[String]) -> Result<HashSet<String>> {
    struct Matcher<'a>(&'a mut HashSet<String>, &'a [String]);
    impl Visitor for Matcher<'_> {
        type Error = DataFusionError;

        fn post_visit_measurement_name(
            self,
            mn: &MeasurementName,
        ) -> std::result::Result<Self, Self::Error> {
            match mn {
                MeasurementName::Name(name) => {
                    let name = name.deref();
                    if self.1.contains(name) {
                        self.0.insert(name.to_string());
                    }
                }
                MeasurementName::Regex(re) => {
                    let re = parse_regex(re)?;

                    self.1
                        .iter()
                        .filter(|table| re.is_match(table))
                        .for_each(|table| {
                            self.0.insert(table.into());
                        });
                }
            }

            Ok(self)
        }

        fn post_visit_show_measurements_statement(
            self,
            sm: &ShowMeasurementsStatement,
        ) -> Result<Self, Self::Error> {
            if sm.with_measurement.is_none() {
                self.0.extend(self.1.iter().cloned());
            }

            Ok(self)
        }

        fn post_visit_show_field_keys_statement(
            self,
            sfk: &ShowFieldKeysStatement,
        ) -> Result<Self, Self::Error> {
            if sfk.from.is_none() {
                self.0.extend(self.1.iter().cloned());
            }

            Ok(self)
        }

        fn post_visit_show_tag_values_statement(
            self,
            stv: &ShowTagValuesStatement,
        ) -> Result<Self, Self::Error> {
            if stv.from.is_none() {
                self.0.extend(self.1.iter().cloned());
            }

            Ok(self)
        }

        fn post_visit_show_tag_keys_statement(
            self,
            stk: &ShowTagKeysStatement,
        ) -> std::result::Result<Self, Self::Error> {
            if stk.from.is_none() {
                self.0.extend(self.1.iter().cloned());
            }

            Ok(self)
        }
    }

    let mut m = HashSet::new();
    let vis = Matcher(&mut m, tables);
    stmt.accept(vis)?;

    Ok(m)
}

#[cfg(test)]
mod test {
    use super::*;
    use itertools::Itertools;
    use test_helpers::assert_error;

    #[test]
    fn test_query_to_statement() {
        // succeeds for a single statement
        let _ = InfluxQLQueryPlanner::query_to_statement("SELECT foo FROM bar").unwrap();

        // Fallible
        assert_error!(
            InfluxQLQueryPlanner::query_to_statement("SELECT foo FROM bar; SELECT bar FROM foo"),
            DataFusionError::NotImplemented(ref s) if s == "The context currently only supports a single InfluxQL statement"
        );
    }

    #[test]
    fn test_find_all_measurements() {
        fn find(q: &str) -> Vec<String> {
            let s = InfluxQLQueryPlanner::query_to_statement(q).unwrap();
            let tables = vec!["foo".into(), "bar".into(), "foobar".into()];
            let res = find_all_measurements(&s, &tables).unwrap();
            res.into_iter().sorted().collect()
        }

        assert_eq!(find("SELECT * FROM foo"), vec!["foo"]);
        assert_eq!(find("SELECT * FROM foo, foo"), vec!["foo"]);
        assert_eq!(find("SELECT * FROM foo, bar"), vec!["bar", "foo"]);
        assert_eq!(find("SELECT * FROM foo, none"), vec!["foo"]);
        assert_eq!(find("SELECT * FROM /^foo/"), vec!["foo", "foobar"]);
        assert_eq!(find("SELECT * FROM foo, /^bar/"), vec!["bar", "foo"]);
        assert_eq!(find("SELECT * FROM //"), vec!["bar", "foo", "foobar"]);

        // Find all measurements in subqueries
        assert_eq!(
            find("SELECT * FROM foo, (SELECT * FROM bar)"),
            vec!["bar", "foo"]
        );
        assert_eq!(
            find("SELECT * FROM foo, (SELECT * FROM /bar/)"),
            vec!["bar", "foo", "foobar"]
        );

        // Find all measurements in `SHOW MEASUREMENTS`
        assert_eq!(find("SHOW MEASUREMENTS"), vec!["bar", "foo", "foobar"]);
        assert_eq!(
            find("SHOW MEASUREMENTS WITH MEASUREMENT = foo"),
            vec!["foo"]
        );
        assert_eq!(
            find("SHOW MEASUREMENTS WITH MEASUREMENT =~ /^foo/"),
            vec!["foo", "foobar"]
        );

        // Find all measurements in `SHOW FIELD KEYS`
        assert_eq!(find("SHOW FIELD KEYS"), vec!["bar", "foo", "foobar"]);
        assert_eq!(find("SHOW FIELD KEYS FROM /^foo/"), vec!["foo", "foobar"]);

        // Find all measurements in `SHOW TAG VALUES`
        assert_eq!(
            find("SHOW TAG VALUES WITH KEY = \"k\""),
            vec!["bar", "foo", "foobar"]
        );
        assert_eq!(
            find("SHOW TAG VALUES FROM /^foo/ WITH KEY = \"k\""),
            vec!["foo", "foobar"]
        );

        // Find all measurements in `SHOW TAG KEYS`
        assert_eq!(find("SHOW TAG KEYS"), vec!["bar", "foo", "foobar"]);
        assert_eq!(find("SHOW TAG KEYS FROM /^foo/"), vec!["foo", "foobar"]);

        // Finds no measurements
        assert!(find("SELECT * FROM none").is_empty());
        assert!(find("SELECT * FROM (SELECT * FROM none)").is_empty());
        assert!(find("SELECT * FROM /^l/").is_empty());
        assert!(find("SELECT * FROM (SELECT * FROM /^l/)").is_empty());
    }
}
