use crate::identifier::{identifier, Identifier};
use crate::internal::{expect, ParseResult};
use crate::Statement;
use crate::Statement::ShowRetentionPolicies;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::multispace1;
use nom::combinator::opt;
use nom::sequence::{pair, preceded, tuple};

fn on_clause(i: &str) -> ParseResult<&str, Identifier> {
    preceded(
        pair(tag_no_case("ON"), multispace1),
        expect("invalid ON clause, expected identifier", identifier),
    )(i)
}

pub fn show_retention_policies(i: &str) -> ParseResult<&str, Statement> {
    let (remaining, (_, _, _, database)) = tuple((
        tag_no_case("RETENTION"),
        multispace1,
        expect(
            "invalid SHOW RETENTION POLICIES statement, expected POLICIES",
            tag_no_case("POLICIES"),
        ),
        opt(preceded(multispace1, on_clause)),
    ))(i)?;

    Ok((remaining, ShowRetentionPolicies { database }))
}

#[cfg(test)]
mod test {
    use crate::assert_expect_error;
    use crate::show_retention_policies::show_retention_policies;

    #[test]
    fn test_show_retention_policies() {
        // no ON clause
        show_retention_policies("RETENTION POLICIES").unwrap();

        // with ON clause
        show_retention_policies("RETENTION POLICIES ON foo").unwrap();

        // Fallible cases

        // missing POLICIES keyword
        assert_expect_error!(
            show_retention_policies("RETENTION ON foo"),
            "invalid SHOW RETENTION POLICIES statement, expected POLICIES"
        );

        // missing database
        assert_expect_error!(
            show_retention_policies("RETENTION POLICIES ON "),
            "invalid ON clause, expected identifier"
        );
    }
}
