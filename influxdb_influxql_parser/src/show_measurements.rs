//! Types and parsers for the [`SHOW MEASUREMENTS`][sql] statement.
//!
//! [sql]: https://docs.influxdata.com/influxdb/v1.8/query_language/explore-schema/#show-measurements

use crate::common::{
    limit_clause, offset_clause, qualified_measurement_name, where_clause, ws0, ws1, LimitClause,
    OffsetClause, QualifiedMeasurementName, WhereClause,
};
use crate::identifier::{identifier, Identifier};
use crate::internal::{expect, ParseResult};
use crate::keywords::keyword;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::combinator::{map, opt, value};
use nom::sequence::tuple;
use nom::sequence::{pair, preceded, terminated};
use std::fmt;
use std::fmt::Formatter;

/// Represents an `ON` clause for a `SHOW MEASUREMENTS` statement to specify
/// which database the statement applies to.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ExtendedOnClause {
    /// Represents a specific database and the default retention policy.
    Database(Identifier),
    /// Represents a specific database and retention policy.
    DatabaseRetentionPolicy(Identifier, Identifier),
    /// Represents all databases and their default retention policies.
    AllDatabases,
    /// Represents all databases and all their retention policies.
    AllDatabasesAndRetentionPolicies,
}

impl fmt::Display for ExtendedOnClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("ON ")?;

        match self {
            Self::Database(db) => write!(f, "{}", db),
            Self::DatabaseRetentionPolicy(db, rp) => write!(f, "{}.{}", db, rp),
            Self::AllDatabases => write!(f, "*"),
            Self::AllDatabasesAndRetentionPolicies => write!(f, "*.*"),
        }
    }
}

/// Parse the `ON` clause of the `SHOW MEASUREMENTS` statement.
fn extended_on_clause(i: &str) -> ParseResult<&str, ExtendedOnClause> {
    preceded(
        pair(keyword("ON"), ws1),
        expect(
            "invalid ON clause, expected wildcard or identifier",
            alt((
                value(
                    ExtendedOnClause::AllDatabasesAndRetentionPolicies,
                    tag("*.*"),
                ),
                value(ExtendedOnClause::AllDatabases, tag("*")),
                map(
                    pair(opt(terminated(identifier, tag("."))), identifier),
                    |tup| match tup {
                        (None, db) => ExtendedOnClause::Database(db),
                        (Some(db), rp) => ExtendedOnClause::DatabaseRetentionPolicy(db, rp),
                    },
                ),
            )),
        ),
    )(i)
}

/// Represents a `SHOW MEASUREMENTS` statement.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShowMeasurementsStatement {
    /// Represents the `ON` clause, which limits the search
    /// to databases matching the expression.
    pub on: Option<ExtendedOnClause>,

    /// Represents the `WITH MEASUREMENT` clause, which limits
    /// the search to measurements matching the expression.
    pub with_measurement: Option<WithMeasurementClause>,

    /// Represents the `WHERE` clause, which holds a conditional
    /// expression to filter the measurement list.
    pub condition: Option<WhereClause>,

    /// Represents the `LIMIT` clause, which holds a value to
    /// restrict the number of tag keys returned.
    pub limit: Option<LimitClause>,

    /// Represents the `OFFSET` clause.
    ///
    /// The `OFFSET` clause holds value to specify an offset to start retrieving tag keys.
    pub offset: Option<OffsetClause>,
}

impl fmt::Display for ShowMeasurementsStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SHOW MEASUREMENTS")?;

        if let Some(ref on_clause) = self.on {
            write!(f, " {}", on_clause)?;
        }

        if let Some(ref with_clause) = self.with_measurement {
            write!(f, " {}", with_clause)?;
        }

        if let Some(ref where_clause) = self.condition {
            write!(f, " {}", where_clause)?;
        }

        if let Some(ref limit) = self.limit {
            write!(f, " {}", limit)?;
        }

        if let Some(ref offset) = self.offset {
            write!(f, " {}", offset)?;
        }

        Ok(())
    }
}

/// Represents the expression of a `WITH MEASUREMENT` clause.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WithMeasurementClause {
    /// Limit the measurements identified by the measurement name using an equals operator.
    Equals(QualifiedMeasurementName),
    /// Limit the measurements identified by the measurement name using a
    /// regular expression equals operator.
    Regex(QualifiedMeasurementName),
}

impl fmt::Display for WithMeasurementClause {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("WITH MEASUREMENT ")?;
        match self {
            Self::Equals(ref name) => write!(f, "= {}", name),
            Self::Regex(ref re) => write!(f, "=~ {}", re),
        }
    }
}

fn with_measurement_clause(i: &str) -> ParseResult<&str, WithMeasurementClause> {
    preceded(
        tuple((
            keyword("WITH"),
            ws1,
            expect(
                "invalid WITH clause, expected MEASUREMENT",
                keyword("MEASUREMENT"),
            ),
            ws0,
        )),
        expect(
            "expected = or =~",
            alt((
                map(
                    preceded(pair(tag("=~"), ws0), qualified_measurement_name),
                    WithMeasurementClause::Regex,
                ),
                map(
                    preceded(
                        pair(tag("="), ws0),
                        expect("expected measurement name", qualified_measurement_name),
                    ),
                    WithMeasurementClause::Equals,
                ),
            )),
        ),
    )(i)
}

/// Parse a `SHOW MEASUREMENTS` statement after `SHOW` and any whitespace has been consumed.
pub(crate) fn show_measurements(i: &str) -> ParseResult<&str, ShowMeasurementsStatement> {
    let (
        remaining_input,
        (
            _, // "MEASUREMENTS"
            on_expression,
            measurement_expression,
            condition,
            limit,
            offset,
        ),
    ) = tuple((
        keyword("MEASUREMENTS"),
        opt(preceded(ws1, extended_on_clause)),
        opt(preceded(ws1, with_measurement_clause)),
        opt(preceded(ws1, where_clause)),
        opt(preceded(ws1, limit_clause)),
        opt(preceded(ws1, offset_clause)),
    ))(i)?;

    Ok((
        remaining_input,
        ShowMeasurementsStatement {
            on: on_expression,
            with_measurement: measurement_expression,
            condition,
            limit,
            offset,
        },
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::assert_expect_error;
    use crate::common::MeasurementName;
    use crate::expression::arithmetic::Expr;
    use assert_matches::assert_matches;

    #[test]
    fn test_show_measurements() {
        let (_, got) = show_measurements("measurements").unwrap();
        assert_eq!(
            got,
            ShowMeasurementsStatement {
                on: None,
                ..Default::default()
            },
        );

        let (_, got) = show_measurements("measurements ON foo").unwrap();
        assert_eq!(
            got,
            ShowMeasurementsStatement {
                on: Some(ExtendedOnClause::Database("foo".into())),
                ..Default::default()
            },
        );

        let (_, got) = show_measurements(
            "MEASUREMENTS\tON  foo  WITH  MEASUREMENT\n=  bar WHERE\ntrue LIMIT 10 OFFSET 20",
        )
        .unwrap();
        assert_eq!(
            got,
            ShowMeasurementsStatement {
                on: Some(ExtendedOnClause::Database("foo".into())),
                with_measurement: Some(WithMeasurementClause::Equals(QualifiedMeasurementName {
                    database: None,
                    retention_policy: None,
                    name: "bar".into(),
                })),
                condition: Some(WhereClause::new(Expr::Literal(true.into()).into())),
                limit: Some(10.into()),
                offset: Some(20.into())
            },
        );
        assert_eq!(
            got.to_string(),
            "SHOW MEASUREMENTS ON foo WITH MEASUREMENT = bar WHERE true LIMIT 10 OFFSET 20"
        );

        let (_, got) =
            show_measurements("MEASUREMENTS\tON  foo  WITH  MEASUREMENT\n=~ /bar/ WHERE\ntrue")
                .unwrap();
        assert_eq!(
            got,
            ShowMeasurementsStatement {
                on: Some(ExtendedOnClause::Database("foo".into())),
                with_measurement: Some(WithMeasurementClause::Regex(
                    QualifiedMeasurementName::new(MeasurementName::Regex("bar".into()))
                )),
                condition: Some(WhereClause::new(Expr::Literal(true.into()).into())),
                limit: None,
                offset: None
            },
        );
        assert_eq!(
            got.to_string(),
            "SHOW MEASUREMENTS ON foo WITH MEASUREMENT =~ /bar/ WHERE true"
        );
    }

    #[test]
    fn test_display() {
        let got = format!(
            "{}",
            ShowMeasurementsStatement {
                on: None,
                ..Default::default()
            }
        );
        assert_eq!(got, "SHOW MEASUREMENTS");

        let got = format!(
            "{}",
            ShowMeasurementsStatement {
                on: Some(ExtendedOnClause::Database("foo".into())),
                ..Default::default()
            }
        );
        assert_eq!(got, "SHOW MEASUREMENTS ON foo");

        let got = format!(
            "{}",
            ShowMeasurementsStatement {
                on: Some(ExtendedOnClause::DatabaseRetentionPolicy(
                    "foo".into(),
                    "bar".into()
                )),
                ..Default::default()
            }
        );
        assert_eq!(got, "SHOW MEASUREMENTS ON foo.bar");

        let got = format!(
            "{}",
            ShowMeasurementsStatement {
                on: Some(ExtendedOnClause::AllDatabases),
                ..Default::default()
            }
        );
        assert_eq!(got, "SHOW MEASUREMENTS ON *");

        let got = format!(
            "{}",
            ShowMeasurementsStatement {
                on: Some(ExtendedOnClause::AllDatabasesAndRetentionPolicies),
                ..Default::default()
            }
        );
        assert_eq!(got, "SHOW MEASUREMENTS ON *.*");
    }

    #[test]
    fn test_extended_on_clause() {
        let (_, got) = extended_on_clause("ON cpu").unwrap();
        assert_eq!(got, ExtendedOnClause::Database("cpu".into()));

        let (_, got) = extended_on_clause("ON cpu.autogen").unwrap();
        assert_eq!(
            got,
            ExtendedOnClause::DatabaseRetentionPolicy("cpu".into(), "autogen".into())
        );

        let (_, got) = extended_on_clause("ON *").unwrap();
        assert_matches!(got, ExtendedOnClause::AllDatabases);

        let (_, got) = extended_on_clause("ON *.*").unwrap();
        assert_matches!(got, ExtendedOnClause::AllDatabasesAndRetentionPolicies);

        assert_expect_error!(
            extended_on_clause("ON WHERE cpu = 'test'"),
            "invalid ON clause, expected wildcard or identifier"
        )
    }

    #[test]
    fn test_with_measurement_clause() {
        use crate::common::MeasurementName::*;

        let (_, got) = with_measurement_clause("WITH measurement = foo").unwrap();
        assert_eq!(
            got,
            WithMeasurementClause::Equals(QualifiedMeasurementName::new(Name("foo".into())))
        );

        let (_, got) = with_measurement_clause("WITH measurement =~ /foo/").unwrap();
        assert_eq!(
            got,
            WithMeasurementClause::Regex(QualifiedMeasurementName::new(Regex("foo".into())))
        );

        // Expressions are still valid when whitespace is omitted

        let (_, got) = with_measurement_clause("WITH measurement=foo..bar").unwrap();
        assert_eq!(
            got,
            WithMeasurementClause::Equals(QualifiedMeasurementName::new_db(
                Name("bar".into()),
                "foo".into()
            ))
        );

        let (_, got) = with_measurement_clause("WITH measurement=~/foo/").unwrap();
        assert_eq!(
            got,
            WithMeasurementClause::Regex(QualifiedMeasurementName::new(Regex("foo".into())))
        );

        // Quirks of InfluxQL per https://github.com/influxdata/influxdb_iox/issues/5662

        let (_, got) = with_measurement_clause("WITH measurement =~ foo").unwrap();
        assert_eq!(
            got,
            WithMeasurementClause::Regex(QualifiedMeasurementName::new(Name("foo".into())))
        );

        let (_, got) = with_measurement_clause("WITH measurement = /foo/").unwrap();
        assert_eq!(
            got,
            WithMeasurementClause::Equals(QualifiedMeasurementName::new(Regex("foo".into())))
        );

        // Fallible cases

        // Missing MEASUREMENT token
        assert_expect_error!(
            with_measurement_clause("WITH =~ foo"),
            "invalid WITH clause, expected MEASUREMENT"
        );

        // Unsupported regex not equal operator
        assert_expect_error!(
            with_measurement_clause("WITH measurement !~ foo"),
            "expected = or =~"
        );

        // Must have an identifier
        assert_expect_error!(
            with_measurement_clause("WITH measurement = 1"),
            "expected measurement name"
        );
    }
}
