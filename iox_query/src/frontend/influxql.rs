use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;

use crate::exec::context::IOxSessionContext;
use crate::plan::influxql;
use crate::plan::influxql::{InfluxQLToLogicalPlan, SchemaProvider};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{LogicalPlan, TableSource};
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::ExecutionPlan,
};
use influxdb_influxql_parser::common::MeasurementName;
use influxdb_influxql_parser::parse_statements;
use influxdb_influxql_parser::statement::Statement;
use influxdb_influxql_parser::visit::{Visitable, Visitor};
use observability_deps::tracing::debug;
use schema::Schema;

struct ContextSchemaProvider {
    tables: HashMap<String, (Arc<dyn TableSource>, Schema)>,
}

impl SchemaProvider for ContextSchemaProvider {
    fn get_table_provider(&self, name: &str) -> Result<Arc<dyn TableSource>> {
        self.tables
            .get(name)
            .map(|(t, _)| Arc::clone(t))
            .ok_or_else(|| DataFusionError::Plan(format!("measurement does not exist: {name}")))
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
}

/// This struct can create plans for running SQL queries against databases
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
        let ctx = ctx.child_ctx("query");
        debug!(text=%query, "planning InfluxQL query");

        let statement = self.query_to_statement(query)?;
        let logical_plan = self.statement_to_plan(statement, &ctx).await?;

        // This would only work for SELECT statements at the moment, as the schema queries do
        // not return ExecutionPlan
        ctx.create_physical_plan(&logical_plan).await
    }

    async fn statement_to_plan(
        &self,
        statement: Statement,
        ctx: &IOxSessionContext,
    ) -> Result<LogicalPlan> {
        use std::collections::hash_map::Entry;

        let session_cfg = ctx.inner().copied_config();
        let cfg = session_cfg.config_options();
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

        let planner = InfluxQLToLogicalPlan::new(&sp);
        let logical_plan = planner.statement_to_plan(statement)?;
        debug!(plan=%logical_plan.display_graphviz(), "logical plan");
        Ok(logical_plan)
    }

    fn query_to_statement(&self, query: &str) -> Result<Statement> {
        let mut statements = parse_statements(query)
            .map_err(|e| DataFusionError::External(format!("{e}").into()))?;

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
                    let re = influxql::parse_regex(re)?;

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

        // Finds no measurements
        assert!(find("SELECT * FROM none").is_empty());
        assert!(find("SELECT * FROM (SELECT * FROM none)").is_empty());
        assert!(find("SELECT * FROM /^l/").is_empty());
        assert!(find("SELECT * FROM (SELECT * FROM /^l/)").is_empty());
    }
}
