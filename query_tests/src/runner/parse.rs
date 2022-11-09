/// A query to run with optional annotations
#[derive(Debug, PartialEq, Eq, Default)]
pub struct Query {
    /// If true, results are sorted first prior to comparison, meaning
    /// that differences in the output order compared with expected
    /// order do not cause a diff
    sorted_compare: bool,

    /// If true, replace UUIDs with static placeholders.
    normalized_uuids: bool,

    /// If true, normalize timings in queries by replacing them with static placeholders.
    normalized_metrics: bool,

    /// The SQL string
    sql: String,
}

impl Query {
    pub fn new(sql: impl Into<String>) -> Self {
        let sql = sql.into();
        Self {
            sorted_compare: false,
            normalized_uuids: false,
            normalized_metrics: false,
            sql,
        }
    }

    pub fn with_sorted_compare(mut self) -> Self {
        self.sorted_compare = true;
        self
    }

    /// Get a reference to the query's sql.
    pub fn sql(&self) -> &str {
        self.sql.as_ref()
    }

    /// Get the query's sorted compare.
    pub fn sorted_compare(&self) -> bool {
        self.sorted_compare
    }

    /// Get queries normalized UUID
    pub fn normalized_uuids(&self) -> bool {
        self.normalized_uuids
    }

    /// Use normalized timing values
    pub fn normalized_metrics(&self) -> bool {
        self.normalized_metrics
    }
}

#[derive(Debug, Default)]
struct QueryBuilder {
    query: Query,
}

impl QueryBuilder {
    fn new() -> Self {
        Default::default()
    }

    fn push_str(&mut self, s: &str) {
        self.query.sql.push_str(s)
    }

    fn push(&mut self, c: char) {
        self.query.sql.push(c)
    }

    fn sorted_compare(&mut self) {
        self.query.sorted_compare = true;
    }

    fn normalized_uuids(&mut self) {
        self.query.normalized_uuids = true;
    }

    fn normalize_metrics(&mut self) {
        self.query.normalized_metrics = true;
    }

    fn is_empty(&self) -> bool {
        self.query.sql.is_empty()
    }

    /// Creates a Query and resets this builder to default
    fn build_and_reset(&mut self) -> Option<Query> {
        (!self.is_empty()).then(|| std::mem::take(&mut self.query))
    }
}

/// Poor man's parser to find all the SQL queries in an input file
#[derive(Debug, PartialEq, Eq)]
pub struct TestQueries {
    queries: Vec<Query>,
}

impl TestQueries {
    pub fn new(queries: Vec<Query>) -> Self {
        Self { queries }
    }

    /// find all queries (more or less a fancy split on `;`
    pub fn from_lines<I, S>(lines: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut queries = vec![];
        let mut builder = QueryBuilder::new();

        lines.into_iter().for_each(|line| {
            let line = line.as_ref().trim();
            const COMPARE_STR: &str = "-- IOX_COMPARE: ";
            if line.starts_with(COMPARE_STR) {
                let (_, options) = line.split_at(COMPARE_STR.len());
                for option in options.split(',') {
                    let option = option.trim();
                    match option {
                        "sorted" => {
                            builder.sorted_compare();
                        }
                        "uuid" => {
                            builder.normalized_uuids();
                        }
                        "metrics" => {
                            builder.normalize_metrics();
                        }
                        _ => {}
                    }
                }
            }

            if line.starts_with("--") {
                return;
            }
            if line.is_empty() {
                return;
            }

            // replace newlines
            if !builder.is_empty() {
                builder.push(' ');
            }
            builder.push_str(line);

            // declare queries when we see a semicolon at the end of the line
            if line.ends_with(';') {
                if let Some(q) = builder.build_and_reset() {
                    queries.push(q);
                }
            }
        });

        if let Some(q) = builder.build_and_reset() {
            queries.push(q);
        }

        Self { queries }
    }

    // Get an iterator over the queries
    pub fn iter(&self) -> impl Iterator<Item = &Query> {
        self.queries.iter()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_queries() {
        let input = r#"
-- This is a test
select * from foo;
-- another comment

select * from bar;
-- This query has been commented out and should not be seen
-- select * from baz;
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(
            queries,
            TestQueries::new(vec![
                Query::new("select * from foo;"),
                Query::new("select * from bar;"),
            ])
        )
    }

    #[test]
    fn test_parse_queries_no_ending_semi() {
        let input = r#"
select * from foo;
-- no ending semi colon
select * from bar
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(
            queries,
            TestQueries::new(vec![
                Query::new("select * from foo;"),
                Query::new("select * from bar")
            ])
        )
    }

    #[test]
    fn test_parse_queries_mulit_line() {
        let input = r#"
select
  *
from
  foo;

select * from bar;

"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(
            queries,
            TestQueries::new(vec![
                Query::new("select * from foo;"),
                Query::new("select * from bar;"),
            ])
        )
    }

    #[test]
    fn test_parse_queries_empty() {
        let input = r#"
-- This is a test
-- another comment
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(queries, TestQueries::new(vec![] as Vec<Query>))
    }

    #[test]
    fn test_parse_queries_sorted_compare() {
        let input = r#"
select * from foo;

-- The second query should be compared to expected after sorting
-- IOX_COMPARE: sorted
select * from bar;

-- Since this query is not annotated, it should not use exected sorted
select * from baz;
select * from baz2;

-- IOX_COMPARE: sorted
select * from waz;
-- (But the compare should work subsequently)
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(
            queries,
            TestQueries::new(vec![
                Query::new("select * from foo;"),
                Query::new("select * from bar;").with_sorted_compare(),
                Query::new("select * from baz;"),
                Query::new("select * from baz2;"),
                Query::new("select * from waz;").with_sorted_compare(),
            ])
        )
    }

    #[test]
    fn test_parse_queries_sorted_compare_after() {
        let input = r#"
select * from foo;
-- IOX_COMPARE: sorted
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(
            queries,
            TestQueries::new(vec![Query::new("select * from foo;"),])
        )
    }

    #[test]
    fn test_parse_queries_sorted_compare_not_match_ignored() {
        let input = r#"
-- IOX_COMPARE: something_else
select * from foo;
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(
            queries,
            TestQueries::new(vec![Query::new("select * from foo;"),])
        )
    }
}
