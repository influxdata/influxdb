//! Parse a [`SHOW MEASUREMENTS`][sql] statement.
//!
//! [sql]: https://docs.influxdata.com/influxdb/v1.8/query_language/explore-schema/#show-measurements

use crate::common::{
    limit_clause, offset_clause, qualified_measurement_name, where_clause, QualifiedMeasurementName,
};
use crate::expression::conditional::ConditionalExpression;
use crate::identifier::{identifier, Identifier};
use crate::internal::{expect, ParseResult};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::{map, opt, value};
use nom::sequence::tuple;
use nom::sequence::{pair, preceded, terminated};
use std::fmt;
use std::fmt::Formatter;

/// OnExpression represents an InfluxQL database or retention policy name
/// or a wildcard.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum OnExpression {
    Database(Identifier),
    DatabaseRetentionPolicy(Identifier, Identifier),
    AllDatabases,
    AllDatabasesAndRetentionPolicies,
}

impl fmt::Display for OnExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(db) => write!(f, "{}", db),
            Self::DatabaseRetentionPolicy(db, rp) => write!(f, "{}.{}", db, rp),
            Self::AllDatabases => write!(f, "*"),
            Self::AllDatabasesAndRetentionPolicies => write!(f, "*.*"),
        }
    }
}

/// Parse the `ON` clause of the `SHOW MEASUREMENTS` statement.
fn on_clause(i: &str) -> ParseResult<&str, OnExpression> {
    preceded(
        pair(tag_no_case("ON"), multispace1),
        expect(
            "invalid ON clause, expected wildcard or identifier",
            alt((
                value(OnExpression::AllDatabasesAndRetentionPolicies, tag("*.*")),
                value(OnExpression::AllDatabases, tag("*")),
                map(
                    pair(opt(terminated(identifier, tag("."))), identifier),
                    |tup| match tup {
                        (None, db) => OnExpression::Database(db),
                        (Some(db), rp) => OnExpression::DatabaseRetentionPolicy(db, rp),
                    },
                ),
            )),
        ),
    )(i)
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ShowMeasurementsStatement {
    /// Limit the search to databases matching the expression.
    pub on_expression: Option<OnExpression>,

    /// Limit the search to measurements matching the expression.
    pub measurement_expression: Option<MeasurementExpression>,

    /// A conditional expression to filter the measurement list.
    pub condition: Option<ConditionalExpression>,

    /// A value to restrict the number of tag keys returned.
    pub limit: Option<u64>,

    /// A value to specify an offset to start retrieving tag keys.
    pub offset: Option<u64>,
}

impl fmt::Display for ShowMeasurementsStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SHOW MEASUREMENTS")?;

        if let Some(ref expr) = self.on_expression {
            write!(f, " ON {}", expr)?;
        }

        if let Some(ref expr) = self.measurement_expression {
            write!(f, " WITH MEASUREMENT {}", expr)?;
        }

        if let Some(ref cond) = self.condition {
            write!(f, " WHERE {}", cond)?;
        }

        if let Some(limit) = self.limit {
            write!(f, " LIMIT {}", limit)?;
        }

        if let Some(offset) = self.offset {
            write!(f, " OFFSET {}", offset)?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MeasurementExpression {
    Equals(QualifiedMeasurementName),
    Regex(QualifiedMeasurementName),
}

impl fmt::Display for MeasurementExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Equals(ref name) => write!(f, "= {}", name),
            Self::Regex(ref re) => write!(f, "=~ {}", re),
        }
    }
}

fn with_measurement_clause(i: &str) -> ParseResult<&str, MeasurementExpression> {
    preceded(
        tuple((
            tag_no_case("WITH"),
            multispace1,
            expect(
                "invalid WITH clause, expected MEASUREMENT",
                tag_no_case("MEASUREMENT"),
            ),
            multispace0,
        )),
        expect(
            "expected = or =~",
            alt((
                map(
                    preceded(pair(tag("=~"), multispace0), qualified_measurement_name),
                    MeasurementExpression::Regex,
                ),
                map(
                    preceded(
                        pair(tag("="), multispace0),
                        expect("expected measurement name", qualified_measurement_name),
                    ),
                    MeasurementExpression::Equals,
                ),
            )),
        ),
    )(i)
}

/// Parse a `SHOW MEASUREMENTS` statement after `SHOW` and any whitespace has been consumed.
pub fn show_measurements(i: &str) -> ParseResult<&str, ShowMeasurementsStatement> {
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
        tag_no_case("MEASUREMENTS"),
        opt(preceded(multispace1, on_clause)),
        opt(preceded(multispace1, with_measurement_clause)),
        opt(preceded(multispace1, where_clause)),
        opt(preceded(multispace1, limit_clause)),
        opt(preceded(multispace1, offset_clause)),
    ))(i)?;

    Ok((
        remaining_input,
        ShowMeasurementsStatement {
            on_expression,
            measurement_expression,
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
                on_expression: None,
                ..Default::default()
            },
        );

        let (_, got) = show_measurements("measurements ON foo").unwrap();
        assert_eq!(
            got,
            ShowMeasurementsStatement {
                on_expression: Some(OnExpression::Database("foo".into())),
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
                on_expression: Some(OnExpression::Database("foo".into())),
                measurement_expression: Some(MeasurementExpression::Equals(
                    QualifiedMeasurementName {
                        database: None,
                        retention_policy: None,
                        name: "bar".into(),
                    }
                )),
                condition: Some(Expr::Literal(true.into()).into()),
                limit: Some(10),
                offset: Some(20)
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
                on_expression: Some(OnExpression::Database("foo".into())),
                measurement_expression: Some(MeasurementExpression::Regex(
                    QualifiedMeasurementName::new(MeasurementName::Regex("bar".into()))
                )),
                condition: Some(Expr::Literal(true.into()).into()),
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
                on_expression: None,
                ..Default::default()
            }
        );
        assert_eq!(got, "SHOW MEASUREMENTS");

        let got = format!(
            "{}",
            ShowMeasurementsStatement {
                on_expression: Some(OnExpression::Database("foo".into())),
                ..Default::default()
            }
        );
        assert_eq!(got, "SHOW MEASUREMENTS ON foo");

        let got = format!(
            "{}",
            ShowMeasurementsStatement {
                on_expression: Some(OnExpression::DatabaseRetentionPolicy(
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
                on_expression: Some(OnExpression::AllDatabases),
                ..Default::default()
            }
        );
        assert_eq!(got, "SHOW MEASUREMENTS ON *");

        let got = format!(
            "{}",
            ShowMeasurementsStatement {
                on_expression: Some(OnExpression::AllDatabasesAndRetentionPolicies),
                ..Default::default()
            }
        );
        assert_eq!(got, "SHOW MEASUREMENTS ON *.*");
    }

    #[test]
    fn test_on_clause() {
        let (_, got) = on_clause("ON cpu").unwrap();
        assert_eq!(got, OnExpression::Database("cpu".into()));

        let (_, got) = on_clause("ON cpu.autogen").unwrap();
        assert_eq!(
            got,
            OnExpression::DatabaseRetentionPolicy("cpu".into(), "autogen".into())
        );

        let (_, got) = on_clause("ON *").unwrap();
        assert_matches!(got, OnExpression::AllDatabases);

        let (_, got) = on_clause("ON *.*").unwrap();
        assert_matches!(got, OnExpression::AllDatabasesAndRetentionPolicies);

        assert_expect_error!(
            on_clause("ON WHERE cpu = 'test'"),
            "invalid ON clause, expected wildcard or identifier"
        )
    }

    #[test]
    fn test_with_measurement_clause() {
        use crate::common::MeasurementName::*;

        let (_, got) = with_measurement_clause("WITH measurement = foo").unwrap();
        assert_eq!(
            got,
            MeasurementExpression::Equals(QualifiedMeasurementName::new(Name("foo".into())))
        );

        let (_, got) = with_measurement_clause("WITH measurement =~ /foo/").unwrap();
        assert_eq!(
            got,
            MeasurementExpression::Regex(QualifiedMeasurementName::new(Regex("foo".into())))
        );

        // Expressions are still valid when whitespace is omitted

        let (_, got) = with_measurement_clause("WITH measurement=foo..bar").unwrap();
        assert_eq!(
            got,
            MeasurementExpression::Equals(QualifiedMeasurementName::new_db(
                Name("bar".into()),
                "foo".into()
            ))
        );

        let (_, got) = with_measurement_clause("WITH measurement=~/foo/").unwrap();
        assert_eq!(
            got,
            MeasurementExpression::Regex(QualifiedMeasurementName::new(Regex("foo".into())))
        );

        // Quirks of InfluxQL per https://github.com/influxdata/influxdb_iox/issues/5662

        let (_, got) = with_measurement_clause("WITH measurement =~ foo").unwrap();
        assert_eq!(
            got,
            MeasurementExpression::Regex(QualifiedMeasurementName::new(Name("foo".into())))
        );

        let (_, got) = with_measurement_clause("WITH measurement = /foo/").unwrap();
        assert_eq!(
            got,
            MeasurementExpression::Equals(QualifiedMeasurementName::new(Regex("foo".into())))
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
