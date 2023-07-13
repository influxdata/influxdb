use arrow::datatypes::SchemaRef;
use datafusion::physical_expr::execution_props::ExecutionProps;
use influxdb_influxql_parser::show_field_keys::ShowFieldKeysStatement;
use influxdb_influxql_parser::show_measurements::ShowMeasurementsStatement;
use influxdb_influxql_parser::show_tag_keys::ShowTagKeysStatement;
use influxdb_influxql_parser::show_tag_values::ShowTagValuesStatement;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use crate::plan::{parse_regex, InfluxQLToLogicalPlan, SchemaProvider};
use datafusion::common::Statistics;
use datafusion::datasource::provider_as_source;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Partitioning, SendableRecordBatchStream,
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
use observability_deps::tracing::debug;
use schema::Schema;

struct ContextSchemaProvider<'a> {
    state: &'a SessionState,
    tables: HashMap<String, (Arc<dyn TableSource>, Schema)>,
}

impl<'a> SchemaProvider for ContextSchemaProvider<'a> {
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
}

impl Debug for SchemaExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_as(DisplayFormatType::Default, f)
    }
}

impl ExecutionPlan for SchemaExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::clone(&self.input)]
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

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

impl DisplayAs for SchemaExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "SchemaExec")
            }
        }
    }
}

/// Create plans for running InfluxQL queries against databases
#[derive(Debug, Default)]
pub struct InfluxQLQueryPlanner {}

impl InfluxQLQueryPlanner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Plan an InfluxQL query against the catalogs registered with `ctx`, and return a
    /// DataFusion physical execution plan that runs on the query executor.
    pub async fn query(
        &self,
        query: &str,
        ctx: &IOxSessionContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        debug!(text=%query, "planning InfluxQL query");

        let statement = self.query_to_statement(query)?;
        let logical_plan = self.statement_to_plan(statement, ctx).await?;

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

        Ok(Arc::new(SchemaExec { input, schema }))
    }

    async fn statement_to_plan(
        &self,
        statement: Statement,
        ctx: &IOxSessionContext,
    ) -> Result<LogicalPlan> {
        use std::collections::hash_map::Entry;

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
                if let Some(table) = schema.table(table_name).await {
                    let schema = Schema::try_from(table.schema())
                        .map_err(|err| {
                            DataFusionError::Internal(format!("unable to convert DataFusion schema for measurement {table_name} to IOx schema: {err}"))
                        })?;
                    v.insert((provider_as_source(table), schema));
                }
            }
        }

        let planner = InfluxQLToLogicalPlan::new(&sp, ctx);
        let logical_plan = planner.statement_to_plan(statement)?;
        debug!(plan=%logical_plan.display_graphviz(), "logical plan");
        Ok(logical_plan)
    }

    fn query_to_statement(&self, query: &str) -> Result<Statement> {
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
    impl<'a> Visitor for Matcher<'a> {
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
        let p = InfluxQLQueryPlanner::new();

        // succeeds for a single statement
        let _ = p.query_to_statement("SELECT foo FROM bar").unwrap();

        // Fallible

        assert_error!(
            p.query_to_statement("SELECT foo FROM bar; SELECT bar FROM foo"),
            DataFusionError::NotImplemented(ref s) if s == "The context currently only supports a single InfluxQL statement"
        );
    }

    #[test]
    fn test_find_all_measurements() {
        fn find(q: &str) -> Vec<String> {
            let p = InfluxQLQueryPlanner::new();
            let s = p.query_to_statement(q).unwrap();
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
