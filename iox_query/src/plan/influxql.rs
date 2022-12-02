use crate::{DataFusionError, IOxSessionContext, QueryNamespace};
use datafusion::common::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::LogicalPlan;
use influxdb_influxql_parser::statement::Statement;
use std::sync::Arc;

/// InfluxQL query planner
#[allow(unused)]
#[derive(Debug)]
pub struct InfluxQLToLogicalPlan<'a> {
    ctx: &'a IOxSessionContext,
    state: SessionState,
    database: Arc<dyn QueryNamespace>,
}

impl<'a> InfluxQLToLogicalPlan<'a> {
    pub fn new(ctx: &'a IOxSessionContext, database: Arc<dyn QueryNamespace>) -> Self {
        Self {
            ctx,
            state: ctx.inner().state(),
            database,
        }
    }

    pub fn statement_to_plan(&self, statement: Statement) -> Result<LogicalPlan> {
        match statement {
            Statement::CreateDatabase(_) => {
                Err(DataFusionError::NotImplemented("CREATE DATABASE".into()))
            }
            Statement::Delete(_) => Err(DataFusionError::NotImplemented("DELETE".into())),
            Statement::DropMeasurement(_) => {
                Err(DataFusionError::NotImplemented("DROP MEASUREMENT".into()))
            }
            Statement::Explain(_) => Err(DataFusionError::NotImplemented("EXPLAIN".into())),
            Statement::Select(_) => Err(DataFusionError::NotImplemented("SELECT".into())),
            Statement::ShowDatabases(_) => {
                Err(DataFusionError::NotImplemented("SHOW DATABASES".into()))
            }
            Statement::ShowMeasurements(_) => {
                Err(DataFusionError::NotImplemented("SHOW MEASUREMENTS".into()))
            }
            Statement::ShowRetentionPolicies(_) => Err(DataFusionError::NotImplemented(
                "SHOW RETENTION POLICIES".into(),
            )),
            Statement::ShowTagKeys(_) => {
                Err(DataFusionError::NotImplemented("SHOW TAG KEYS".into()))
            }
            Statement::ShowTagValues(_) => {
                Err(DataFusionError::NotImplemented("SHOW TAG VALUES".into()))
            }
            Statement::ShowFieldKeys(_) => {
                Err(DataFusionError::NotImplemented("SHOW FIELD KEYS".into()))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::exec::{Executor, ExecutorType};
    use crate::test::{TestChunk, TestDatabase};
    use assert_matches::assert_matches;
    use influxdb_influxql_parser::parse_statements;
    use predicate::rpc_predicate::QueryNamespaceMeta;

    fn logical_plan(sql: &str) -> Result<LogicalPlan> {
        let mut statements = parse_statements(sql).unwrap();
        // setup a db
        let chunk0 = Arc::new(
            TestChunk::new("h2o")
                .with_id(0)
                .with_tag_column("foo")
                .with_tag_column("bar")
                .with_i64_field_column("i64_field")
                .with_i64_field_column("i64_field_2")
                .with_time_column()
                .with_one_row_of_data(),
        );
        // index of columns in the above chunk: [bar, foo, i64_field, i64_field_2, time]
        let executor = Arc::new(Executor::new_testing());
        let ctx = executor.new_context(ExecutorType::Query);
        let test_db = Arc::new(TestDatabase::new(Arc::clone(&executor)));
        test_db.add_chunk("my_partition_key", Arc::clone(&chunk0));
        let table = "h2o";

        let _schema = test_db.table_schema(table).unwrap();

        let planner = InfluxQLToLogicalPlan::new(&ctx, test_db);
        planner.statement_to_plan(statements.pop().unwrap())
    }

    /// Verify the list of unsupported statements.
    ///
    /// Some statements will remain unsupported, indefinitely.
    #[test]
    fn test_unsupported_statements() {
        assert_matches!(logical_plan("CREATE DATABASE foo"), Err(DataFusionError::NotImplemented(s)) => assert_eq!(s, "CREATE DATABASE"));
        assert_matches!(logical_plan("DELETE FROM foo"), Err(DataFusionError::NotImplemented(s)) => assert_eq!(s, "DELETE"));
        assert_matches!(logical_plan("DROP MEASUREMENT foo"), Err(DataFusionError::NotImplemented(s)) => assert_eq!(s, "DROP MEASUREMENT"));
        assert_matches!(logical_plan("EXPLAIN SELECT bar FROM foo"), Err(DataFusionError::NotImplemented(s)) => assert_eq!(s, "EXPLAIN"));
        assert_matches!(logical_plan("SELECT bar FROM foo"), Err(DataFusionError::NotImplemented(s)) => assert_eq!(s, "SELECT"));
        assert_matches!(logical_plan("SHOW DATABASES"), Err(DataFusionError::NotImplemented(s)) => assert_eq!(s, "SHOW DATABASES"));
        assert_matches!(logical_plan("SHOW MEASUREMENTS"), Err(DataFusionError::NotImplemented(s)) => assert_eq!(s, "SHOW MEASUREMENTS"));
        assert_matches!(logical_plan("SHOW RETENTION POLICIES"), Err(DataFusionError::NotImplemented(s)) => assert_eq!(s, "SHOW RETENTION POLICIES"));
        assert_matches!(logical_plan("SHOW TAG KEYS"), Err(DataFusionError::NotImplemented(s)) => assert_eq!(s, "SHOW TAG KEYS"));
        assert_matches!(logical_plan("SHOW TAG VALUES WITH KEY = bar"), Err(DataFusionError::NotImplemented(s)) => assert_eq!(s, "SHOW TAG VALUES"));
        assert_matches!(logical_plan("SHOW FIELD KEYS"), Err(DataFusionError::NotImplemented(s)) => assert_eq!(s, "SHOW FIELD KEYS"));
    }
}
