use std::sync::Arc;

use arrow::{
    array::{as_string_array, ArrayRef, BooleanArray},
    datatypes::DataType,
};
use datafusion::{
    error::DataFusionError,
    logical_expr::{ScalarFunctionImplementation, ScalarUDF, Volatility},
    physical_plan::ColumnarValue,
    prelude::create_udf,
    scalar::ScalarValue,
};
use once_cell::sync::Lazy;

/// The name of the regex_match UDF given to DataFusion.
pub const REGEX_MATCH_UDF_NAME: &str = "influx_regex_match";

/// The name of the not_regex_match UDF given to DataFusion.
pub const REGEX_NOT_MATCH_UDF_NAME: &str = "influx_regex_not_match";

/// Implementation of regexp_match
pub(crate) static REGEX_MATCH_UDF: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    Arc::new(create_udf(
        REGEX_MATCH_UDF_NAME,
        // takes two arguments: regex, pattern
        vec![DataType::Utf8, DataType::Utf8],
        Arc::new(DataType::Boolean),
        Volatility::Stable,
        regex_match_expr_impl(true),
    ))
});

/// Implementation of regexp_not_match
pub(crate) static REGEX_NOT_MATCH_UDF: Lazy<Arc<ScalarUDF>> = Lazy::new(|| {
    Arc::new(create_udf(
        REGEX_NOT_MATCH_UDF_NAME,
        // takes two arguments: regex, pattern
        vec![DataType::Utf8, DataType::Utf8],
        Arc::new(DataType::Boolean),
        Volatility::Stable,
        regex_match_expr_impl(false),
    ))
});

/// Given a column containing string values and a single regex pattern,
/// `regex_match_expr` determines which values satisfy the pattern and which do
/// not.
///
/// If `matches` is true then this expression will filter values that do not
/// satisfy the regex (equivalent to `col ~= /pattern/`). If `matches` is `false`
/// then the expression will filter values that *do* match the regex, which is
/// equivalent to `col !~ /pattern/`.
///
/// This UDF is designed to support the regex operator that can be pushed down
/// via the InfluxRPC API.
///
fn regex_match_expr_impl(matches: bool) -> ScalarFunctionImplementation {
    // N.B., this function does not utilise the Arrow regexp compute
    // kernel because in order to act as a filter it needs to return a
    // boolean array of comparison results, not an array of strings as
    // the regex compute kernel does and it needs to implement the
    // regexp syntax for influxrpc.

    let func = move |args: &[ColumnarValue]| {
        assert_eq!(args.len(), 2); // only works over a single column and pattern at a time.

        let pattern = match &args[1] {
            // second arg was array (not constant)
            ColumnarValue::Array(_) => {
                return Err(DataFusionError::NotImplemented(format!(
                    "regex_match({}) with non scalar patterns not yet implemented",
                    matches
                )))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(pattern)) => pattern,
            ColumnarValue::Scalar(arg) => {
                return Err(DataFusionError::Internal(format!(
                    "Expected string pattern to regex match({}), got: {:?}",
                    matches, arg
                )))
            }
        };

        let pattern = pattern.as_ref().ok_or_else(|| {
            DataFusionError::NotImplemented(
                "NULL patterns not supported in regex match".to_string(),
            )
        })?;

        // Attempt to make the pattern compatible with what is accepted by
        // the golang regexp library which is different than Rust's regexp
        let pattern = clean_non_meta_escapes(pattern);

        let pattern = regex::Regex::new(&pattern).map_err(|e| {
            DataFusionError::Internal(format!("error compiling regex pattern: {}", e))
        })?;

        match &args[0] {
            ColumnarValue::Array(arr) => {
                let results = as_string_array(arr)
                    .iter()
                    .map(|row| {
                        // in arrow, any value can be null.
                        // Here we decide to make our UDF to return null when either base or exponent is null.
                        row.map(|v| pattern.is_match(v) == matches)
                    })
                    .collect::<BooleanArray>();

                Ok(ColumnarValue::Array(Arc::new(results) as ArrayRef))
            }
            ColumnarValue::Scalar(ScalarValue::Utf8(row)) => {
                let res = row.as_ref().map(|v| pattern.is_match(v) == matches);
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(res)))
            }
            ColumnarValue::Scalar(v) => Err(DataFusionError::Internal(format!(
                "regex_match({}) expected first argument to be utf8, got ('{}')",
                matches, v
            ))),
        }
    };

    Arc::new(func)
}

fn is_valid_character_after_escape(c: char) -> bool {
    // same list as https://docs.rs/regex-syntax/0.6.25/src/regex_syntax/ast/parse.rs.html#1445-1538
    match c {
        '0'..='7' => true,
        '8'..='9' => true,
        'x' | 'u' | 'U' => true,
        'p' | 'P' => true,
        'd' | 's' | 'w' | 'D' | 'S' | 'W' => true,
        _ => regex_syntax::is_meta_character(c),
    }
}

/// Removes all `/` patterns that the rust regex library would reject
/// and rewrites them to their unescaped form.
///
/// For example, `\:` is rewritten to `:` as `\:` is not a valid
/// escape sequence in the `regexp` crate but is valid in golang's
/// regexp implementation.
///
/// This is done for compatibility purposes so that the regular
/// expression matching in Rust more closely follows the matching in
/// golang, used by the influx storage rpc.
///
/// See <https://github.com/rust-lang/regex/issues/501> for more details
pub fn clean_non_meta_escapes(pattern: &str) -> String {
    if pattern.is_empty() {
        return pattern.to_string();
    }

    #[derive(Debug, Copy, Clone)]
    enum SlashState {
        No,
        Single,
        Double,
    }

    let mut next_state = SlashState::No;

    let next_chars = pattern
        .chars()
        .map(Some)
        .skip(1)
        .chain(std::iter::once(None));

    // emit char based on previous
    let new_pattern: String = pattern
        .chars()
        .zip(next_chars)
        .filter_map(|(c, next_char)| {
            let cur_state = next_state;
            next_state = match (c, cur_state) {
                ('\\', SlashState::No) => SlashState::Single,
                ('\\', SlashState::Single) => SlashState::Double,
                ('\\', SlashState::Double) => SlashState::Single,
                _ => SlashState::No,
            };

            // Decide to emit `c` or not
            match (cur_state, c, next_char) {
                (SlashState::No, '\\', Some(next_char))
                | (SlashState::Double, '\\', Some(next_char))
                    if !is_valid_character_after_escape(next_char) =>
                {
                    None
                }
                _ => Some(c),
            }
        })
        .collect();

    new_pattern
}

#[cfg(test)]
mod test {

    use arrow::{
        array::{StringArray, UInt64Array},
        record_batch::RecordBatch,
        util::pretty::pretty_format_batches,
    };
    use datafusion::{
        error::DataFusionError,
        prelude::{col, lit, Expr},
    };
    use datafusion_util::context_with_table;
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn regex_match_expr() {
        let cases = vec![
            (
                ".*", // match everything except NULL values
                true, // keep the values matched
                vec![
                    "+---------------+--------+",
                    "| words         | length |",
                    "+---------------+--------+",
                    "| air           | 3      |",
                    "| aphex twin    | 10     |",
                    "| bruce         | 5      |",
                    "| Blood Orange  | 12     |",
                    "| cocteau twins | 13     |",
                    "+---------------+--------+",
                ],
            ),
            (
                ".*",  // match everything except NULL values
                false, // filter away all the values matched
                vec!["++", "++"],
            ),
            (
                "", // an empty pattern also matches everything except NULL
                true,
                vec![
                    "+---------------+--------+",
                    "| words         | length |",
                    "+---------------+--------+",
                    "| air           | 3      |",
                    "| aphex twin    | 10     |",
                    "| bruce         | 5      |",
                    "| Blood Orange  | 12     |",
                    "| cocteau twins | 13     |",
                    "+---------------+--------+",
                ],
            ),
            (
                ".+O.*", // match just words containing "O".
                true,
                vec![
                    "+--------------+--------+",
                    "| words        | length |",
                    "+--------------+--------+",
                    "| Blood Orange | 12     |",
                    "+--------------+--------+",
                ],
            ),
            (
                "^(a|b).*", // match everything beginning with "a" or "b"
                false,      // negate expression and filter away anything that matches
                vec![
                    "+---------------+--------+",
                    "| words         | length |",
                    "+---------------+--------+",
                    "| Blood Orange  | 12     |",
                    "| cocteau twins | 13     |",
                    "+---------------+--------+",
                ],
            ),
            (
                "twi",
                true, // keep the values matched
                vec![
                    "+---------------+--------+",
                    "| words         | length |",
                    "+---------------+--------+",
                    "| aphex twin    | 10     |",
                    "| cocteau twins | 13     |",
                    "+---------------+--------+",
                ],
            ),
        ];

        for (pattern, matches, expected) in cases.into_iter() {
            let args = vec![col("words"), lit(pattern)];

            let regex_expr = if matches {
                REGEX_MATCH_UDF.call(args)
            } else {
                REGEX_NOT_MATCH_UDF.call(args)
            };

            let actual = run_plan(regex_expr).await.unwrap();

            assert_eq!(
                expected, actual,
                "\n\nEXPECTED:\n{:#?}\nACTUAL:\n{:#?}\n",
                expected, actual
            );
        }
    }

    #[tokio::test]
    async fn regex_match_expr_invalid_regex() {
        // an invalid regex pattern
        let regex_expr = crate::regex_match_expr(col("words"), "[".to_string());

        let actual = run_plan(regex_expr).await.expect_err("expected error");
        assert!(actual.to_string().contains("error compiling regex pattern"))
    }

    // Run a plan against the following input table as "t"
    async fn run_plan(op: Expr) -> Result<Vec<String>, DataFusionError> {
        // define data for table
        let words = vec![
            Some("air"),
            Some("aphex twin"),
            Some("bruce"),
            Some("Blood Orange"),
            None,
            None,
            Some("cocteau twins"),
        ];

        let lengths = words
            .iter()
            .map(|word| word.map(|word| word.len() as u64))
            .collect::<UInt64Array>();

        let words = StringArray::from(words);

        let rb = RecordBatch::try_from_iter(vec![
            ("words", Arc::new(words) as ArrayRef),
            ("length", Arc::new(lengths)),
        ])
        .unwrap();

        let ctx = context_with_table(rb);
        let df = ctx.table("t").await.unwrap();
        let df = df.filter(op).unwrap();

        // execute the query
        let record_batches = df.collect().await?;

        Ok(pretty_format_batches(&record_batches)
            .unwrap()
            .to_string()
            .split('\n')
            .map(|s| s.to_owned())
            .collect())
    }

    #[test]
    fn test_clean_non_meta_escapes() {
        let cases = vec![
            ("", ""),
            (r#"\"#, r#"\"#),
            (r#"\\"#, r#"\\"#),
            // : is not a special meta character
            (r#"\:"#, r#":"#),
            // . is a special meta character
            (r#"\."#, r#"\."#),
            (r#"foo\"#, r#"foo\"#),
            (r#"foo\\"#, r#"foo\\"#),
            (r#"foo\:"#, r#"foo:"#),
            (r#"foo\xff"#, r#"foo\xff"#),
            (r#"fo\\o"#, r#"fo\\o"#),
            (r#"fo\:o"#, r#"fo:o"#),
            (r#"fo\:o\x123"#, r#"fo:o\x123"#),
            (r#"fo\:o\x123\:"#, r#"fo:o\x123:"#),
            (r#"foo\\\:bar"#, r#"foo\\:bar"#),
            (r#"foo\\\:bar\\\:"#, r#"foo\\:bar\\:"#),
            ("foo", "foo"),
        ];

        for (pattern, expected) in cases {
            let cleaned_pattern = clean_non_meta_escapes(pattern);
            assert_eq!(
                cleaned_pattern, expected,
                "Expected '{}' to be cleaned to '{}', got '{}'",
                pattern, expected, cleaned_pattern
            );
        }
    }
}
