/// Poor man's parser to find all the SQL queries in the input
#[derive(Debug, PartialEq)]
pub struct TestQueries {
    queries: Vec<String>,
}

impl TestQueries {
    pub fn new<I, S>(queries: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let queries = queries
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();

        Self { queries }
    }

    /// find all queries (more or less a fancy split on `;`
    pub fn from_lines<I, S>(lines: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut queries = vec![];
        let mut current_line = String::new();

        lines.into_iter().for_each(|line| {
            let line = line.as_ref().trim();
            if line.starts_with("--") {
                return;
            }
            if line.is_empty() {
                return;
            }

            // declare queries when we see a semicolon at the end of the line
            if !current_line.is_empty() {
                current_line.push(' ');
            }
            current_line.push_str(line);

            if line.ends_with(';') {
                // resets current_line to String::new()
                let t = std::mem::take(&mut current_line);
                queries.push(t);
            }
        });

        if !current_line.is_empty() {
            queries.push(current_line);
        }

        Self { queries }
    }

    // Get an iterator over the queries
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.queries.iter().map(|s| s.as_str())
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
            TestQueries::new(vec!["select * from foo;", "select * from bar;"])
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
            TestQueries::new(vec!["select * from foo;", "select * from bar"])
        )
    }

    #[test]
    fn test_parse_queries_empty() {
        let input = r#"
-- This is a test
-- another comment
"#;
        let queries = TestQueries::from_lines(input.split('\n'));
        assert_eq!(queries, TestQueries::new(vec![] as Vec<String>))
    }
}
