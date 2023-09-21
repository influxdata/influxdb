use crate::snapshot_comparison::Language;
use arrow::record_batch::RecordBatch;
use arrow_util::test_util::Normalizer;

/// A query to run with optional annotations
#[derive(Debug, PartialEq, Eq, Default)]
pub struct Query {
    /// Describes how query text should be normalized
    normalizer: Normalizer,

    /// Specifies the query language of `text`.
    language: Language,

    /// The query string
    text: String,
}

impl Query {
    #[cfg(test)]
    fn new(text: impl Into<String>) -> Self {
        let text = text.into();
        Self {
            normalizer: Normalizer::new(),
            language: Language::Sql,
            text,
        }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn language(&self) -> Language {
        self.language
    }

    pub fn with_sorted_compare(mut self) -> Self {
        self.normalizer.sorted_compare = true;
        self
    }

    pub fn with_normalized_uuids(mut self) -> Self {
        self.normalizer.normalized_uuids = true;
        self
    }

    pub fn with_normalize_metrics(mut self) -> Self {
        self.normalizer.normalized_metrics = true;
        self
    }

    pub fn with_normalize_filters(mut self) -> Self {
        self.normalizer.normalized_filters = true;
        self
    }

    pub fn with_no_table_borders(mut self) -> Self {
        self.normalizer.no_table_borders = true;
        self
    }

    /// Take the output of running the query and apply the specified normalizations to them
    pub fn normalize_results(&self, results: Vec<RecordBatch>, language: Language) -> Vec<String> {
        language.normalize_results(&self.normalizer, results)
    }

    /// Adds information on what normalizations were applied to the input
    pub fn add_description(&self, output: &mut Vec<String>) {
        self.normalizer.add_description(output)
    }
}

#[derive(Debug, Default)]
struct QueryBuilder {
    pub language: Language,
    pub query: Query,
}

impl QueryBuilder {
    fn new(language: Language) -> Self {
        Self {
            language,
            ..Default::default()
        }
    }

    fn push_str(&mut self, s: &str) {
        self.query.text.push_str(s)
    }

    fn push(&mut self, c: char) {
        self.query.text.push(c)
    }

    fn is_empty(&self) -> bool {
        self.query.text.is_empty()
    }

    /// Creates a Query and resets this builder to default
    fn build_and_reset(&mut self) -> Option<Query> {
        (!self.is_empty()).then(|| {
            let mut q = std::mem::take(&mut self.query);
            q.language = self.language;
            q
        })
    }
}

/// Poor man's parser to find all the SQL queries in an input file
#[derive(Debug, PartialEq, Eq)]
pub struct TestQueries {
    queries: Vec<Query>,
}

impl TestQueries {
    /// find all queries (more or less a fancy split on `;`
    pub fn from_lines<I, S>(lines: I, language: Language) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut queries = vec![];

        let mut builder =
            lines
                .into_iter()
                .fold(QueryBuilder::new(language), |mut builder, line| {
                    let line = line.as_ref().trim();
                    const COMPARE_STR: &str = "-- IOX_COMPARE: ";
                    if line.starts_with(COMPARE_STR) {
                        let (_, options) = line.split_at(COMPARE_STR.len());
                        for option in options.split(',') {
                            let option = option.trim();
                            match option {
                                "sorted" => {
                                    builder.query = builder.query.with_sorted_compare();
                                }
                                "uuid" => {
                                    builder.query = builder.query.with_normalized_uuids();
                                }
                                "metrics" => {
                                    builder.query = builder.query.with_normalize_metrics();
                                }
                                "filters" => {
                                    builder.query = builder.query.with_normalize_filters();
                                }
                                "no_borders" => {
                                    builder.query = builder.query.with_no_table_borders();
                                }
                                _ => {}
                            }
                        }
                    }

                    if line.starts_with("--") {
                        return builder;
                    }
                    if line.is_empty() {
                        return builder;
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
                    builder
                });

        // get last one, if any
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
        let queries = TestQueries::from_lines(input.split('\n'), Language::Sql);
        assert_eq!(
            queries,
            TestQueries {
                queries: vec![
                    Query::new("select * from foo;"),
                    Query::new("select * from bar;"),
                ]
            }
        )
    }

    #[test]
    fn test_parse_queries_no_ending_semi() {
        let input = r#"
select * from foo;
-- no ending semi colon
select * from bar
"#;
        let queries = TestQueries::from_lines(input.split('\n'), Language::Sql);
        assert_eq!(
            queries,
            TestQueries {
                queries: vec![
                    Query::new("select * from foo;"),
                    Query::new("select * from bar")
                ]
            }
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
        let queries = TestQueries::from_lines(input.split('\n'), Language::Sql);
        assert_eq!(
            queries,
            TestQueries {
                queries: vec![
                    Query::new("select * from foo;"),
                    Query::new("select * from bar;"),
                ]
            }
        )
    }

    #[test]
    fn test_parse_queries_empty() {
        let input = r#"
-- This is a test
-- another comment
"#;
        let queries = TestQueries::from_lines(input.split('\n'), Language::Sql);
        assert_eq!(queries, TestQueries { queries: vec![] })
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
        let queries = TestQueries::from_lines(input.split('\n'), Language::Sql);
        assert_eq!(
            queries,
            TestQueries {
                queries: vec![
                    Query::new("select * from foo;"),
                    Query::new("select * from bar;").with_sorted_compare(),
                    Query::new("select * from baz;"),
                    Query::new("select * from baz2;"),
                    Query::new("select * from waz;").with_sorted_compare(),
                ]
            }
        )
    }

    #[test]
    fn test_parse_queries_sorted_compare_after() {
        let input = r#"
select * from foo;
-- IOX_COMPARE: sorted
"#;
        let queries = TestQueries::from_lines(input.split('\n'), Language::Sql);
        assert_eq!(
            queries,
            TestQueries {
                queries: vec![Query::new("select * from foo;")]
            }
        )
    }

    #[test]
    fn test_parse_queries_sorted_compare_not_match_ignored() {
        let input = r#"
-- IOX_COMPARE: something_else
select * from foo;
"#;
        let queries = TestQueries::from_lines(input.split('\n'), Language::Sql);
        assert_eq!(
            queries,
            TestQueries {
                queries: vec![Query::new("select * from foo;")]
            }
        )
    }
}
