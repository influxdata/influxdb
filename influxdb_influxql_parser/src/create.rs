//! Types and parsers for the [`CREATE DATABASE`][sql] schema statement.
//!
//! [sql]: https://docs.influxdata.com/influxdb/v1.8/query_language/manage-database/#create-database

use crate::common::ws1;
use crate::identifier::{identifier, Identifier};
use crate::internal::{expect, ParseResult};
use crate::keywords::keyword;
use crate::literal::{duration, unsigned_integer, Duration};
use crate::statement::Statement;
use nom::branch::alt;
use nom::combinator::{map, opt, peek};
use nom::sequence::{pair, preceded, tuple};
use std::fmt::{Display, Formatter};

pub(crate) fn create_statement(i: &str) -> ParseResult<&str, Statement> {
    preceded(
        pair(keyword("CREATE"), ws1),
        expect(
            "Invalid CREATE statement, expected DATABASE following CREATE",
            map(create_database, |s| Statement::CreateDatabase(Box::new(s))),
        ),
    )(i)
}

/// Represents a `CREATE DATABASE` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDatabaseStatement {
    /// Name of database to be created.
    pub name: Identifier,

    /// Duration of retention policy.
    pub duration: Option<Duration>,

    /// Replication factor of retention policy.
    pub replication: Option<u64>,

    /// Shard duration of retention policy.
    pub shard_duration: Option<Duration>,

    /// Retention policy name.
    pub retention_name: Option<Identifier>,
}

impl CreateDatabaseStatement {
    /// Returns true if the "WITH" clause is present.
    pub fn has_with_clause(&self) -> bool {
        self.duration.is_some()
            || self.replication.is_some()
            || self.shard_duration.is_some()
            || self.retention_name.is_some()
    }
}

impl Display for CreateDatabaseStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE DATABASE {}", self.name)?;

        if self.has_with_clause() {
            f.write_str(" WITH")?;

            if let Some(v) = self.duration {
                write!(f, " DURATION {}", v)?;
            }

            if let Some(v) = self.replication {
                write!(f, " REPLICATION {}", v)?;
            }

            if let Some(v) = self.shard_duration {
                write!(f, " SHARD DURATION {}", v)?;
            }

            if let Some(v) = &self.retention_name {
                write!(f, " NAME {}", v)?;
            }
        }
        Ok(())
    }
}

fn create_database(i: &str) -> ParseResult<&str, CreateDatabaseStatement> {
    let (
        remaining,
        (
            _, // "DATABASE"
            name,
            opt_with_clause,
        ),
    ) = tuple((
        keyword("DATABASE"),
        identifier,
        opt(tuple((
            preceded(ws1, keyword("WITH")),
            expect(
                "invalid WITH clause, expected \"DURATION\", \"REPLICATION\", \"SHARD\" or \"NAME\"",
                peek(preceded(
                    ws1,
                    alt((
                        keyword("DURATION"),
                        keyword("REPLICATION"),
                        keyword("SHARD"),
                        keyword("NAME"),
                    )),
                )),
            ),
            opt(preceded(
                preceded(ws1, keyword("DURATION")),
                expect(
                    "invalid DURATION clause, expected duration",
                    preceded(ws1, duration),
                ),
            )),
            opt(preceded(
                preceded(ws1, keyword("REPLICATION")),
                expect(
                    "invalid REPLICATION clause, expected unsigned integer",
                    preceded(ws1, unsigned_integer),
                ),
            )),
            opt(preceded(
                pair(
                    preceded(ws1, keyword("SHARD")),
                    expect(
                        "invalid SHARD DURATION clause, expected \"DURATION\"",
                        preceded(ws1, keyword("DURATION")),
                    ),
                ),
                expect(
                    "invalid SHARD DURATION clause, expected duration",
                    preceded(ws1, duration),
                ),
            )),
            opt(preceded(
                preceded(ws1, keyword("NAME")),
                expect(
                    "invalid NAME clause, expected identifier",
                    identifier,
                ),
            )),
        ))),
    ))(i)?;

    let (_, _, duration, replication, shard_duration, retention_name) =
        opt_with_clause.unwrap_or(("", "", None, None, None, None));

    Ok((
        remaining,
        CreateDatabaseStatement {
            name,
            duration,
            replication,
            shard_duration,
            retention_name,
        },
    ))
}

#[cfg(test)]
mod test {
    use super::create_database;
    use super::create_statement;
    use crate::assert_expect_error;

    #[test]
    fn test_create_statement() {
        create_statement("CREATE DATABASE telegraf").unwrap();
    }

    #[test]
    fn test_create_database() {
        let (rem, got) = create_database("DATABASE telegraf").unwrap();
        assert_eq!(rem, "");
        assert_eq!(got.name, "telegraf".into());

        let (rem, got) = create_database("DATABASE telegraf WITH DURATION 5m").unwrap();
        assert_eq!(rem, "");
        assert_eq!(got.name, "telegraf".into());
        assert_eq!(format!("{}", got.duration.unwrap()), "5m");

        let (rem, got) = create_database("DATABASE telegraf WITH REPLICATION 10").unwrap();
        assert_eq!(rem, "");
        assert_eq!(got.name, "telegraf".into());
        assert_eq!(got.replication.unwrap(), 10);

        let (rem, got) = create_database("DATABASE telegraf WITH SHARD DURATION 6m").unwrap();
        assert_eq!(rem, "");
        assert_eq!(got.name, "telegraf".into());
        assert_eq!(format!("{}", got.shard_duration.unwrap()), "6m");

        let (rem, got) = create_database("DATABASE telegraf WITH NAME \"5 minutes\"").unwrap();
        assert_eq!(rem, "");
        assert_eq!(got.name, "telegraf".into());
        assert_eq!(got.retention_name.unwrap(), "5 minutes".into());

        let (rem, got) = create_database("DATABASE telegraf WITH DURATION 5m REPLICATION 10 SHARD DURATION 6m NAME \"5 minutes\"").unwrap();
        assert_eq!(rem, "");
        assert_eq!(got.name, "telegraf".into());
        assert_eq!(format!("{}", got.duration.unwrap()), "5m");
        assert_eq!(got.replication.unwrap(), 10);
        assert_eq!(format!("{}", got.shard_duration.unwrap()), "6m");
        assert_eq!(got.retention_name.unwrap(), "5 minutes".into());

        // Fallible

        assert_expect_error!(
            create_database("DATABASE telegraf WITH foo"),
            "invalid WITH clause, expected \"DURATION\", \"REPLICATION\", \"SHARD\" or \"NAME\""
        );

        assert_expect_error!(
            create_database("DATABASE telegraf WITH DURATION foo"),
            "invalid DURATION clause, expected duration"
        );

        assert_expect_error!(
            create_database("DATABASE telegraf WITH REPLICATION foo"),
            "invalid REPLICATION clause, expected unsigned integer"
        );

        assert_expect_error!(
            create_database("DATABASE telegraf WITH SHARD foo"),
            "invalid SHARD DURATION clause, expected \"DURATION\""
        );

        assert_expect_error!(
            create_database("DATABASE telegraf WITH SHARD DURATION foo"),
            "invalid SHARD DURATION clause, expected duration"
        );

        assert_expect_error!(
            create_database("DATABASE telegraf WITH NAME 5"),
            "invalid NAME clause, expected identifier"
        );
    }
}
