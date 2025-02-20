use std::collections::HashSet;

use influxdb_influxql_parser::{
    common::ParseError,
    explain::ExplainStatement,
    identifier::Identifier,
    parse_statements as parse_internal,
    select::{MeasurementSelection, SelectStatement},
    show_measurements::ExtendedOnClause,
    statement::Statement,
};

#[derive(Debug)]
pub struct Rewritten<S> {
    database: Option<Identifier>,
    retention_policy: Option<Identifier>,
    statement: S,
}

impl<S> Rewritten<S> {
    fn new(statement: S) -> Self {
        Self {
            database: None,
            retention_policy: None,
            statement,
        }
    }

    fn with_database(mut self, db: Option<Identifier>) -> Self {
        self.database = db;
        self
    }

    fn with_retention_policy(mut self, rp: Option<Identifier>) -> Self {
        self.retention_policy = rp;
        self
    }

    pub fn database(&self) -> Option<&Identifier> {
        self.database.as_ref()
    }

    pub fn retention_policy(&self) -> Option<&Identifier> {
        self.retention_policy.as_ref()
    }

    pub fn statement(&self) -> &S {
        &self.statement
    }

    pub fn to_statement(self) -> S {
        self.statement
    }

    pub fn resolve_dbrp(&self) -> Option<String> {
        match (&self.database, &self.retention_policy) {
            (None, None) | (None, Some(_)) => None,
            (Some(db), None) => Some(db.to_string()),
            (Some(db), Some(rp)) => {
                if rp.as_str() != "autogen" && rp.as_str() != "default" {
                    Some(format!("{db}/{rp}"))
                } else {
                    Some(db.to_string())
                }
            }
        }
    }
}

impl From<Rewritten<Statement>> for Statement {
    fn from(r: Rewritten<Statement>) -> Self {
        r.to_statement()
    }
}

impl TryFrom<Statement> for Rewritten<Statement> {
    type Error = Error;

    fn try_from(statement: Statement) -> Result<Self, Self::Error> {
        match statement {
            Statement::ShowMeasurements(mut s) => {
                if let Some(on) = s.on.take() {
                    let (db, rp) = match on {
                        ExtendedOnClause::Database(db) => (Some(db), None),
                        ExtendedOnClause::DatabaseRetentionPolicy(db, rp) => (Some(db), Some(rp)),
                        ExtendedOnClause::AllDatabases
                        | ExtendedOnClause::AllDatabasesAndRetentionPolicies => {
                            return Err(Error::MultiDatabase);
                        }
                    };
                    Ok(Self::new(Statement::ShowMeasurements(s))
                        .with_database(db)
                        .with_retention_policy(rp))
                } else {
                    Ok(Self::new(Statement::ShowMeasurements(s)))
                }
            }
            Statement::ShowRetentionPolicies(mut s) => {
                let identifier = s.database.take().map(Into::into);
                Ok(Self::new(Statement::ShowRetentionPolicies(s)).with_database(identifier))
            }
            Statement::ShowTagKeys(mut s) => {
                let identifier = s.database.take().map(Into::into);
                Ok(Self::new(Statement::ShowTagKeys(s)).with_database(identifier))
            }
            Statement::ShowTagValues(mut s) => {
                let identifier = s.database.take().map(Into::into);
                Ok(Self::new(Statement::ShowTagValues(s)).with_database(identifier))
            }
            Statement::ShowFieldKeys(mut s) => {
                let identifier = s.database.take().map(Into::into);
                Ok(Self::new(Statement::ShowFieldKeys(s)).with_database(identifier))
            }
            Statement::Select(s) => {
                let ss = Rewritten::<SelectStatement>::try_from(*s)?;
                let db = ss.database.to_owned();
                let rp = ss.retention_policy.to_owned();
                Ok(Self::new(Statement::Select(Box::new(ss.to_statement())))
                    .with_database(db)
                    .with_retention_policy(rp))
            }
            Statement::Explain(mut s) => {
                let options = s.options.take();
                let s = Self::try_from(*s.statement)?;
                let db = s.database.to_owned();
                let rp = s.retention_policy.to_owned();
                Ok(Self::new(Statement::Explain(Box::new(ExplainStatement {
                    options,
                    statement: Box::new(s.to_statement()),
                })))
                .with_database(db)
                .with_retention_policy(rp))
            }
            // For all other statements, we just pass them through. Explicitly
            // do not use a catch-all match arm here in the event that new variants
            // are added to the Statement enum, we want the compiler to direct us
            // here to handle, if relevant.
            Statement::CreateDatabase(_)
            | Statement::Delete(_)
            | Statement::DropMeasurement(_)
            | Statement::ShowDatabases(_) => Ok(Self::new(statement)),
        }
    }
}

impl TryFrom<SelectStatement> for Rewritten<SelectStatement> {
    type Error = Error;

    fn try_from(mut select_statement: SelectStatement) -> Result<Self, Self::Error> {
        let mut db_rp_set = HashSet::new();
        let from_clause = select_statement
            .from
            .take()
            .into_iter()
            .map(|ms| {
                let (db, rp, ms) = match ms {
                    MeasurementSelection::Name(mut qn) => {
                        let db = qn.database.take();
                        let rp = qn.retention_policy.take();
                        (db, rp, MeasurementSelection::Name(qn))
                    }
                    // Recursively call try_from on nested sub-queries, and compare their
                    // resulting db/rp to the same at this level. Sub-queries that have
                    // multiple db/rp in them will throw the MultiDatabase error.
                    MeasurementSelection::Subquery(s) => {
                        let ss = Self::try_from(*s)?;
                        (
                            ss.database.to_owned(),
                            ss.retention_policy.to_owned(),
                            MeasurementSelection::Subquery(Box::new(ss.to_statement())),
                        )
                    }
                };
                if db_rp_set.insert((db, rp)) && db_rp_set.len() > 1 {
                    Err(Error::MultiDatabase)
                } else {
                    Ok(ms)
                }
            })
            .collect::<Result<Vec<MeasurementSelection>, Error>>()?;
        select_statement.from.replace(from_clause);
        let mut result = Self::new(select_statement);
        if let Some((db, rp)) = db_rp_set.into_iter().next() {
            result = result.with_database(db).with_retention_policy(rp);
        }
        Ok(result)
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum Error {
    #[error("can only perform queries on a single database")]
    MultiDatabase,
    #[error("parsing error: {0}")]
    Parse(ParseError),
}

pub fn parse_statements(input: &str) -> Result<Vec<Rewritten<Statement>>, Error> {
    parse_internal(input)
        .map_err(Error::Parse)?
        .into_iter()
        .map(Rewritten::<Statement>::try_from)
        .collect::<Result<Vec<Rewritten<Statement>>, Error>>()
}

#[cfg(test)]
mod tests {
    use influxdb_influxql_parser::statement::Statement;

    use crate::{Error, Rewritten, parse_statements};

    fn parse_single(input: &str) -> Rewritten<Statement> {
        parse_statements(input).unwrap().pop().unwrap()
    }

    fn parse_single_failure(input: &str) -> Error {
        parse_statements(input).unwrap_err()
    }

    struct TestCase {
        input: &'static str,
        expected: &'static str,
        db: Option<&'static str>,
        rp: Option<&'static str>,
    }

    impl TestCase {
        fn assert(&self) {
            let s = parse_single(self.input);
            assert_eq!(s.database().map(|db| db.as_str()), self.db);
            assert_eq!(s.retention_policy().map(|rp| rp.as_str()), self.rp);
            assert_eq!(self.expected, s.to_statement().to_string());
        }
    }

    struct TestFailure {
        input: &'static str,
        expected: Error,
    }

    impl TestFailure {
        fn assert(&self) {
            let e = parse_single_failure(self.input);
            assert_eq!(self.expected, e, "input: {}", self.input);
        }
    }

    #[test]
    fn show_measurements() {
        TestCase {
            input: "SHOW MEASUREMENTS",
            expected: "SHOW MEASUREMENTS",
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: "SHOW MEASUREMENTS ON foo",
            expected: "SHOW MEASUREMENTS",
            db: Some("foo"),
            rp: None,
        }
        .assert();
        TestCase {
            input: "SHOW MEASUREMENTS ON foo.bar",
            expected: "SHOW MEASUREMENTS",
            db: Some("foo"),
            rp: Some("bar"),
        }
        .assert();
    }

    #[test]
    fn show_measurements_failure_modes() {
        TestFailure {
            input: "SHOW MEASUREMENTS ON *.*",
            expected: Error::MultiDatabase,
        }
        .assert();
        TestFailure {
            input: r#"SHOW MEASUREMENTS ON *"#,
            expected: Error::MultiDatabase,
        }
        .assert();
    }

    #[test]
    fn show_retention_policies() {
        TestCase {
            input: "SHOW RETENTION POLICIES",
            expected: "SHOW RETENTION POLICIES",
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: "SHOW RETENTION POLICIES ON foo",
            expected: "SHOW RETENTION POLICIES",
            db: Some("foo"),
            rp: None,
        }
        .assert();
    }

    #[test]
    fn show_tag_keys() {
        TestCase {
            input: "SHOW TAG KEYS",
            expected: "SHOW TAG KEYS",
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: "SHOW TAG KEYS FROM cpu",
            expected: "SHOW TAG KEYS FROM cpu",
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: "SHOW TAG KEYS ON foo",
            expected: "SHOW TAG KEYS",
            db: Some("foo"),
            rp: None,
        }
        .assert();
        TestCase {
            input: "SHOW TAG KEYS ON foo FROM cpu",
            expected: "SHOW TAG KEYS FROM cpu",
            db: Some("foo"),
            rp: None,
        }
        .assert();
    }

    #[test]
    fn show_tag_values() {
        TestCase {
            input: "SHOW TAG VALUES WITH KEY = host",
            expected: "SHOW TAG VALUES WITH KEY = host",
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: "SHOW TAG VALUES FROM cpu WITH KEY = host",
            expected: "SHOW TAG VALUES FROM cpu WITH KEY = host",
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: "SHOW TAG VALUES ON foo WITH KEY = host",
            expected: "SHOW TAG VALUES WITH KEY = host",
            db: Some("foo"),
            rp: None,
        }
        .assert();
        TestCase {
            input: "SHOW TAG VALUES ON foo FROM cpu WITH KEY = host",
            expected: "SHOW TAG VALUES FROM cpu WITH KEY = host",
            db: Some("foo"),
            rp: None,
        }
        .assert();
    }

    #[test]
    fn show_field_keys() {
        TestCase {
            input: "SHOW FIELD KEYS",
            expected: "SHOW FIELD KEYS",
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: "SHOW FIELD KEYS FROM cpu",
            expected: "SHOW FIELD KEYS FROM cpu",
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: "SHOW FIELD KEYS ON foo",
            expected: "SHOW FIELD KEYS",
            db: Some("foo"),
            rp: None,
        }
        .assert();
        TestCase {
            input: "SHOW FIELD KEYS ON foo FROM cpu",
            expected: "SHOW FIELD KEYS FROM cpu",
            db: Some("foo"),
            rp: None,
        }
        .assert();
    }

    #[test]
    fn select() {
        TestCase {
            input: "SELECT * FROM cpu",
            expected: "SELECT * FROM cpu",
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: "SELECT * FROM bar.cpu",
            expected: "SELECT * FROM cpu",
            db: None,
            rp: Some("bar"),
        }
        .assert();
        TestCase {
            input: "SELECT * FROM foo.bar.cpu",
            expected: "SELECT * FROM cpu",
            db: Some("foo"),
            rp: Some("bar"),
        }
        .assert();
        TestCase {
            input: r#"SELECT * FROM (SELECT * FROM cpu)"#,
            expected: r#"SELECT * FROM (SELECT * FROM cpu)"#,
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: r#"SELECT * FROM (SELECT * FROM bar.cpu), bar.mem"#,
            expected: r#"SELECT * FROM (SELECT * FROM cpu), mem"#,
            db: None,
            rp: Some("bar"),
        }
        .assert();
        TestCase {
            input: r#"SELECT * FROM (SELECT * FROM foo.bar.cpu), foo.bar.mem"#,
            expected: r#"SELECT * FROM (SELECT * FROM cpu), mem"#,
            db: Some("foo"),
            rp: Some("bar"),
        }
        .assert();
    }

    #[test]
    fn select_failure_modes() {
        TestFailure {
            input: r#"SELECT * FROM foo.bar.cpu, baz.bop.cpu"#,
            expected: Error::MultiDatabase,
        }
        .assert();
        TestFailure {
            input: r#"SELECT * FROM cpu, baz.bop.cpu"#,
            expected: Error::MultiDatabase,
        }
        .assert();
        TestFailure {
            input: r#"SELECT * FROM bar.cpu, baz.bop.cpu"#,
            expected: Error::MultiDatabase,
        }
        .assert();
        TestFailure {
            input: r#"SELECT * FROM foo.bar.cpu, (SELECT * FROM mem)"#,
            expected: Error::MultiDatabase,
        }
        .assert();
    }

    #[test]
    fn explain() {
        TestCase {
            input: "EXPLAIN SELECT * FROM cpu",
            expected: "EXPLAIN SELECT * FROM cpu",
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: "EXPLAIN SELECT * FROM bar.cpu",
            expected: "EXPLAIN SELECT * FROM cpu",
            db: None,
            rp: Some("bar"),
        }
        .assert();
        TestCase {
            input: "EXPLAIN SELECT * FROM foo.bar.cpu",
            expected: "EXPLAIN SELECT * FROM cpu",
            db: Some("foo"),
            rp: Some("bar"),
        }
        .assert();
        TestCase {
            input: r#"EXPLAIN SELECT * FROM (SELECT * FROM cpu)"#,
            expected: r#"EXPLAIN SELECT * FROM (SELECT * FROM cpu)"#,
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: r#"EXPLAIN SELECT * FROM (SELECT * FROM bar.cpu), bar.mem"#,
            expected: r#"EXPLAIN SELECT * FROM (SELECT * FROM cpu), mem"#,
            db: None,
            rp: Some("bar"),
        }
        .assert();
        TestCase {
            input: r#"EXPLAIN SELECT * FROM (SELECT * FROM foo.bar.cpu), foo.bar.mem"#,
            expected: r#"EXPLAIN SELECT * FROM (SELECT * FROM cpu), mem"#,
            db: Some("foo"),
            rp: Some("bar"),
        }
        .assert();
    }

    #[test]
    fn noop_rewrites() {
        TestCase {
            input: "CREATE DATABASE foo",
            expected: "CREATE DATABASE foo",
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: "DELETE FROM cpu",
            expected: "DELETE FROM cpu",
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: "DROP MEASUREMENT cpu",
            expected: "DROP MEASUREMENT cpu",
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: "EXPLAIN SELECT * FROM cpu",
            expected: "EXPLAIN SELECT * FROM cpu",
            db: None,
            rp: None,
        }
        .assert();
        TestCase {
            input: "SHOW DATABASES",
            expected: "SHOW DATABASES",
            db: None,
            rp: None,
        }
        .assert();
    }
}
