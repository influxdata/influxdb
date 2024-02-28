use std::collections::HashSet;

use influxdb_influxql_parser::{
    common::ParseError, identifier::Identifier, parse_statements as parse_internal,
    select::MeasurementSelection, show_measurements::ExtendedOnClause, statement::Statement,
};

#[derive(Debug)]
pub struct RewrittenStatement {
    database: Option<Identifier>,
    retention_policy: Option<Identifier>,
    statement: Statement,
}

impl RewrittenStatement {
    fn new(statement: Statement) -> Self {
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

    pub fn statement(&self) -> &Statement {
        &self.statement
    }

    pub fn to_statement(self) -> Statement {
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

impl From<RewrittenStatement> for Statement {
    fn from(r: RewrittenStatement) -> Self {
        r.to_statement()
    }
}

impl TryFrom<Statement> for RewrittenStatement {
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
                            return Err(Error::MultiDatabase)
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
            Statement::Select(mut s) => {
                let mut db_rp_set = HashSet::new();
                let from_clause = s
                    .from
                    .take()
                    .into_iter()
                    .map(|ms| match ms {
                        MeasurementSelection::Name(mut qn) => {
                            let db = qn.database.take();
                            let rp = qn.retention_policy.take();
                            if db_rp_set.insert((db, rp)) && db_rp_set.len() > 1 {
                                return Err(Error::MultiDatabase);
                            }
                            Ok(MeasurementSelection::Name(qn))
                        }
                        // TODO - handle sub-queries?
                        MeasurementSelection::Subquery(_) => Ok(ms),
                    })
                    .collect::<Result<Vec<MeasurementSelection>, Error>>()?;
                s.from.replace(from_clause);
                let mut result = Self::new(Statement::Select(s));
                if let Some((db, rp)) = db_rp_set.into_iter().next() {
                    result = result.with_database(db).with_retention_policy(rp);
                }
                Ok(result)
            }
            Statement::CreateDatabase(_)
            | Statement::Delete(_)
            | Statement::DropMeasurement(_)
            | Statement::Explain(_)
            | Statement::ShowDatabases(_) => Ok(Self::new(statement)),
        }
    }
}

#[derive(Debug, thiserror::Error, Clone)]
pub enum Error {
    #[error("can only perform queries on a single database")]
    MultiDatabase,
    #[error("parsing error: {0}")]
    Parse(ParseError),
}

pub fn parse_statements(input: &str) -> Result<Vec<RewrittenStatement>, Error> {
    parse_internal(input)
        .map_err(Error::Parse)?
        .into_iter()
        .map(RewrittenStatement::try_from)
        .collect::<Result<Vec<RewrittenStatement>, Error>>()
}
