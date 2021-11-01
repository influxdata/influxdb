use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BooleanArray, StringArray},
    datatypes::DataType,
};
use datafusion::{
    error::DataFusionError,
    logical_plan::{create_udf, Expr},
    physical_plan::functions::{make_scalar_function, Volatility},
};

/// The name of the regex_match UDF given to DataFusion.
pub const REGEX_MATCH_UDF_NAME: &str = "RegexMatch";
pub const REGEX_NOT_MATCH_UDF_NAME: &str = "RegexNotMatch";

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
pub fn regex_match_expr(input: Expr, pattern: String, matches: bool) -> Expr {
    // N.B., this function does not utilise the Arrow regexp compute kernel because
    // in order to act as a filter it needs to return a boolean array of comparison
    // results, not an array of strings as the regex compute kernel does.
    let func = move |args: &[ArrayRef]| {
        assert_eq!(args.len(), 1); // only works over a single column at a time.

        let input_arr = &args[0].as_any().downcast_ref::<StringArray>().unwrap();

        let pattern = regex::Regex::new(&pattern).map_err(|e| {
            DataFusionError::Internal(format!("error compiling regex pattern: {}", e))
        })?;

        let results = input_arr
            .iter()
            .map(|row| {
                // in arrow, any value can be null.
                // Here we decide to make our UDF to return null when either base or exponent is null.
                row.map(|v| pattern.is_match(v) == matches)
            })
            .collect::<BooleanArray>();

        Ok(Arc::new(results) as ArrayRef)
    };

    // make_scalar_function is a helper to support accepting scalar values as
    // well as arrays.
    let func = make_scalar_function(func);

    let udf_name = if matches {
        REGEX_MATCH_UDF_NAME
    } else {
        REGEX_NOT_MATCH_UDF_NAME
    };

    let udf = create_udf(
        udf_name,
        vec![DataType::Utf8],
        Arc::new(DataType::Boolean),
        Volatility::Stable,
        func,
    );

    udf.call(vec![input])
}

#[cfg(test)]
mod test {
    use arrow::{
        array::{StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
        util::pretty::pretty_format_batches,
    };
    use datafusion::{
        datasource::MemTable,
        error::DataFusionError,
        logical_plan::{col, Expr},
        prelude::ExecutionContext,
    };
    use std::sync::Arc;

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
        ];

        for (pattern, matches, expected) in cases.into_iter() {
            let regex_expr = super::regex_match_expr(col("words"), pattern.to_string(), matches);
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
        let regex_expr = super::regex_match_expr(col("words"), "[".to_string(), true);

        let actual = run_plan(regex_expr).await.expect_err("expected error");
        assert!(actual.to_string().contains("error compiling regex pattern"))
    }

    // Run a plan against the following input table as "t"
    async fn run_plan(op: Expr) -> Result<Vec<String>, DataFusionError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("words", DataType::Utf8, true),
            Field::new("length", DataType::UInt64, false),
        ]));

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
        let rb = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(words.clone())),
                Arc::new(
                    words
                        .iter()
                        .map(|word| word.map(|word| word.len() as u64))
                        .collect::<UInt64Array>(),
                ),
            ],
        )
        .unwrap();

        let provider = MemTable::try_new(Arc::clone(&schema), vec![vec![rb]]).unwrap();
        let mut ctx = ExecutionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();

        let df = ctx.table("t").unwrap();
        let df = df.filter(op).unwrap();

        // execute the query
        let record_batches = df.collect().await?;

        Ok(pretty_format_batches(&record_batches)
            .unwrap()
            .split('\n')
            .map(|s| s.to_owned())
            .collect())
    }
}
