//! Tests that verify output produced by ]GapFillExec].

use std::{
    cmp::Ordering,
    ops::{Bound, Range},
};

use super::*;
use arrow::{
    array::{ArrayRef, DictionaryArray, Int64Array, TimestampNanosecondArray},
    datatypes::{Field, Int32Type, Schema},
    record_batch::RecordBatch,
};
use arrow_util::test_util::batches_to_lines;
use datafusion::{
    error::Result,
    execution::runtime_env::{RuntimeConfig, RuntimeEnv},
    physical_plan::{
        collect, expressions::col as phys_col, expressions::lit as phys_lit, memory::MemoryExec,
    },
    prelude::{SessionConfig, SessionContext},
    scalar::ScalarValue,
};
use futures::executor::block_on;
use observability_deps::tracing::debug;
use schema::{InfluxColumnType, InfluxFieldType};
use test_helpers::assert_error;

#[test]
fn test_gapfill_simple() {
    test_helpers::maybe_start_logging();
    insta::allow_duplicates! { for output_batch_size in [1, 2, 4, 8] {
        for input_batch_size in [1, 2] {
            let batch = TestRecords {
                group_cols: vec![vec![Some("a"), Some("a")]],
                time_col: vec![Some(1_000), Some(1_100)],
                agg_cols: vec![vec![Some(10), Some(11)]],
                input_batch_size,
            };
            let params = get_params_ms(&batch, 25, Some(975), 1_125);
            let tc = TestCase {
                test_records: batch,
                output_batch_size,
                params,
            };
            // For this simple test case, also test that
            // memory is tracked correctly, which is done by
            // TestCase when running with a memory limit.
            let batches = tc.run_with_memory_limit(16384).unwrap();
            let actual = batches_to_lines(&batches);
            insta::assert_yaml_snapshot!(actual, @r###"
            ---
            - +----+--------------------------+----+
            - "| g0 | time                     | a0 |"
            - +----+--------------------------+----+
            - "| a  | 1970-01-01T00:00:00.975Z |    |"
            - "| a  | 1970-01-01T00:00:01Z     | 10 |"
            - "| a  | 1970-01-01T00:00:01.025Z |    |"
            - "| a  | 1970-01-01T00:00:01.050Z |    |"
            - "| a  | 1970-01-01T00:00:01.075Z |    |"
            - "| a  | 1970-01-01T00:00:01.100Z | 11 |"
            - "| a  | 1970-01-01T00:00:01.125Z |    |"
            - +----+--------------------------+----+
            "###);
            assert_batch_count(&batches, output_batch_size);
        }
    }}
}

#[test]
fn test_gapfill_simple_no_group_no_aggr() {
    // There may be no group columns in a gap fill query,
    // and there may be no aggregate columns as well.
    // Such a query is not all that useful but it should work.
    test_helpers::maybe_start_logging();
    insta::allow_duplicates! { for output_batch_size in [1, 2, 4, 8] {
        for input_batch_size in [1, 2, 4] {
            let batch = TestRecords {
                group_cols: vec![],
                time_col: vec![None, Some(1_000), Some(1_100)],
                agg_cols: vec![],
                input_batch_size,
            };
            let params = get_params_ms(&batch, 25, Some(975), 1_125);
            let tc = TestCase {
                test_records: batch,
                output_batch_size,
                params,
            };
            let batches = tc.run().unwrap();
            let actual = batches_to_lines(&batches);
            insta::assert_yaml_snapshot!(actual, @r###"
            ---
            - +--------------------------+
            - "| time                     |"
            - +--------------------------+
            - "|                          |"
            - "| 1970-01-01T00:00:00.975Z |"
            - "| 1970-01-01T00:00:01Z     |"
            - "| 1970-01-01T00:00:01.025Z |"
            - "| 1970-01-01T00:00:01.050Z |"
            - "| 1970-01-01T00:00:01.075Z |"
            - "| 1970-01-01T00:00:01.100Z |"
            - "| 1970-01-01T00:00:01.125Z |"
            - +--------------------------+
            "###);
            assert_batch_count(&batches, output_batch_size);
        }
    }}
}

#[test]
fn test_gapfill_multi_group_simple() {
    test_helpers::maybe_start_logging();
    insta::allow_duplicates! { for output_batch_size in [1, 2, 4, 8, 16] {
        for input_batch_size in [1, 2, 4] {
            let records = TestRecords {
                group_cols: vec![vec![Some("a"), Some("a"), Some("b"), Some("b")]],
                time_col: vec![Some(1_000), Some(1_100), Some(1_025), Some(1_050)],
                agg_cols: vec![vec![Some(10), Some(11), Some(20), Some(21)]],
                input_batch_size,
            };
            let params = get_params_ms(&records, 25, Some(975), 1_125);
            let tc = TestCase {
                test_records: records,
                output_batch_size,
                params,
            };
            let batches = tc.run().unwrap();
            let actual = batches_to_lines(&batches);
            insta::assert_yaml_snapshot!(actual, @r###"
            ---
            - +----+--------------------------+----+
            - "| g0 | time                     | a0 |"
            - +----+--------------------------+----+
            - "| a  | 1970-01-01T00:00:00.975Z |    |"
            - "| a  | 1970-01-01T00:00:01Z     | 10 |"
            - "| a  | 1970-01-01T00:00:01.025Z |    |"
            - "| a  | 1970-01-01T00:00:01.050Z |    |"
            - "| a  | 1970-01-01T00:00:01.075Z |    |"
            - "| a  | 1970-01-01T00:00:01.100Z | 11 |"
            - "| a  | 1970-01-01T00:00:01.125Z |    |"
            - "| b  | 1970-01-01T00:00:00.975Z |    |"
            - "| b  | 1970-01-01T00:00:01Z     |    |"
            - "| b  | 1970-01-01T00:00:01.025Z | 20 |"
            - "| b  | 1970-01-01T00:00:01.050Z | 21 |"
            - "| b  | 1970-01-01T00:00:01.075Z |    |"
            - "| b  | 1970-01-01T00:00:01.100Z |    |"
            - "| b  | 1970-01-01T00:00:01.125Z |    |"
            - +----+--------------------------+----+
            "###);
            assert_batch_count(&batches, output_batch_size);
        }
    }}
}

#[test]
fn test_gapfill_multi_group_simple_origin() {
    test_helpers::maybe_start_logging();
    insta::allow_duplicates! { for output_batch_size in [1, 2, 4, 8, 16] {
        for input_batch_size in [1, 2, 4] {
            let records = TestRecords {
                group_cols: vec![vec![Some("a"), Some("a"), Some("b"), Some("b")]],
                time_col: vec![Some(1_000), Some(1_100), Some(1_025), Some(1_050)],
                agg_cols: vec![vec![Some(10), Some(11), Some(20), Some(21)]],
                input_batch_size,
            };
            let params = get_params_ms_with_origin_fill_strategy(&records, 25, Some(975), 1_125, Some(3), FillStrategy::Null);
            let tc = TestCase {
                test_records: records,
                output_batch_size,
                params,
            };
            let batches = tc.run().unwrap();
            let actual = batches_to_lines(&batches);
            // timestamps are now offset by 3ms
            insta::assert_yaml_snapshot!(actual, @r###"
            ---
            - +----+--------------------------+----+
            - "| g0 | time                     | a0 |"
            - +----+--------------------------+----+
            - "| a  | 1970-01-01T00:00:00.953Z |    |"
            - "| a  | 1970-01-01T00:00:00.978Z |    |"
            - "| a  | 1970-01-01T00:00:01.003Z | 10 |"
            - "| a  | 1970-01-01T00:00:01.028Z |    |"
            - "| a  | 1970-01-01T00:00:01.053Z |    |"
            - "| a  | 1970-01-01T00:00:01.078Z |    |"
            - "| a  | 1970-01-01T00:00:01.103Z | 11 |"
            - "| b  | 1970-01-01T00:00:00.953Z |    |"
            - "| b  | 1970-01-01T00:00:00.978Z |    |"
            - "| b  | 1970-01-01T00:00:01.003Z |    |"
            - "| b  | 1970-01-01T00:00:01.028Z | 20 |"
            - "| b  | 1970-01-01T00:00:01.053Z | 21 |"
            - "| b  | 1970-01-01T00:00:01.078Z |    |"
            - "| b  | 1970-01-01T00:00:01.103Z |    |"
            - +----+--------------------------+----+
            "###);
            assert_batch_count(&batches, output_batch_size);
        }
    }}
}

#[test]
fn test_gapfill_multi_group_with_nulls() {
    test_helpers::maybe_start_logging();
    insta::allow_duplicates! { for output_batch_size in [1, 2, 4, 8, 16, 32] {
        for input_batch_size in [1, 2, 4, 8] {
            let records = TestRecords {
                group_cols: vec![vec![
                    Some("a"),
                    Some("a"),
                    Some("a"),
                    Some("a"),
                    Some("b"),
                    Some("b"),
                    Some("b"),
                ]],
                time_col: vec![
                    None,
                    None,
                    Some(1_000),
                    Some(1_100),
                    None,
                    Some(1_000),
                    Some(1_100),
                ],
                agg_cols: vec![vec![
                    Some(1),
                    None,
                    Some(10),
                    Some(11),
                    Some(2),
                    Some(20),
                    Some(21),
                ]],
                input_batch_size,
            };
            let params = get_params_ms(&records, 25, Some(975), 1_125);
            let tc = TestCase {
                test_records: records,
                output_batch_size,
                params,
            };
            let batches = tc.run().unwrap();
            let actual = batches_to_lines(&batches);
            insta::assert_yaml_snapshot!(actual, @r###"
            ---
            - +----+--------------------------+----+
            - "| g0 | time                     | a0 |"
            - +----+--------------------------+----+
            - "| a  |                          | 1  |"
            - "| a  |                          |    |"
            - "| a  | 1970-01-01T00:00:00.975Z |    |"
            - "| a  | 1970-01-01T00:00:01Z     | 10 |"
            - "| a  | 1970-01-01T00:00:01.025Z |    |"
            - "| a  | 1970-01-01T00:00:01.050Z |    |"
            - "| a  | 1970-01-01T00:00:01.075Z |    |"
            - "| a  | 1970-01-01T00:00:01.100Z | 11 |"
            - "| a  | 1970-01-01T00:00:01.125Z |    |"
            - "| b  |                          | 2  |"
            - "| b  | 1970-01-01T00:00:00.975Z |    |"
            - "| b  | 1970-01-01T00:00:01Z     | 20 |"
            - "| b  | 1970-01-01T00:00:01.025Z |    |"
            - "| b  | 1970-01-01T00:00:01.050Z |    |"
            - "| b  | 1970-01-01T00:00:01.075Z |    |"
            - "| b  | 1970-01-01T00:00:01.100Z | 21 |"
            - "| b  | 1970-01-01T00:00:01.125Z |    |"
            - +----+--------------------------+----+
            "###);
            assert_batch_count(&batches, output_batch_size);
        }
    }}
}

#[test]
fn test_gapfill_multi_group_cols_with_nulls() {
    test_helpers::maybe_start_logging();
    insta::allow_duplicates! { for output_batch_size in [1, 2, 4, 8, 16, 32] {
        for input_batch_size in [1, 2, 4, 8] {
            let records = TestRecords {
                group_cols: vec![
                    vec![
                        Some("a"),
                        Some("a"),
                        Some("a"),
                        Some("a"),
                        Some("a"),
                        Some("a"),
                        Some("a"),
                    ],
                    vec![
                        Some("c"),
                        Some("c"),
                        Some("c"),
                        Some("c"),
                        Some("d"),
                        Some("d"),
                        Some("d"),
                    ],
                ],
                time_col: vec![
                    None,
                    None,
                    Some(1_000),
                    Some(1_100),
                    None,
                    Some(1_000),
                    Some(1_100),
                ],
                agg_cols: vec![vec![
                    Some(1),
                    None,
                    Some(10),
                    Some(11),
                    Some(2),
                    Some(20),
                    Some(21),
                ]],
                input_batch_size,
            };
            let params = get_params_ms(&records, 25, Some(975), 1_125);
            let tc = TestCase {
                test_records: records,
                output_batch_size,
                params,
            };
            let batches = tc.run().unwrap();
            let actual = batches_to_lines(&batches);
            insta::assert_yaml_snapshot!(actual, @r###"
            ---
            - +----+----+--------------------------+----+
            - "| g0 | g1 | time                     | a0 |"
            - +----+----+--------------------------+----+
            - "| a  | c  |                          | 1  |"
            - "| a  | c  |                          |    |"
            - "| a  | c  | 1970-01-01T00:00:00.975Z |    |"
            - "| a  | c  | 1970-01-01T00:00:01Z     | 10 |"
            - "| a  | c  | 1970-01-01T00:00:01.025Z |    |"
            - "| a  | c  | 1970-01-01T00:00:01.050Z |    |"
            - "| a  | c  | 1970-01-01T00:00:01.075Z |    |"
            - "| a  | c  | 1970-01-01T00:00:01.100Z | 11 |"
            - "| a  | c  | 1970-01-01T00:00:01.125Z |    |"
            - "| a  | d  |                          | 2  |"
            - "| a  | d  | 1970-01-01T00:00:00.975Z |    |"
            - "| a  | d  | 1970-01-01T00:00:01Z     | 20 |"
            - "| a  | d  | 1970-01-01T00:00:01.025Z |    |"
            - "| a  | d  | 1970-01-01T00:00:01.050Z |    |"
            - "| a  | d  | 1970-01-01T00:00:01.075Z |    |"
            - "| a  | d  | 1970-01-01T00:00:01.100Z | 21 |"
            - "| a  | d  | 1970-01-01T00:00:01.125Z |    |"
            - +----+----+--------------------------+----+
            "###);
            assert_batch_count(&batches, output_batch_size);
        }
    }}
}

#[test]
fn test_gapfill_multi_group_cols_with_more_nulls() {
    test_helpers::maybe_start_logging();
    insta::allow_duplicates! { for output_batch_size in [1, 2, 4, 8, 16, 32] {
        for input_batch_size in [1, 2, 4, 8] {
            let records = TestRecords {
                group_cols: vec![vec![Some("a"), Some("b"), Some("b"), Some("b"), Some("b")]],
                time_col: vec![
                    Some(1_000),
                    None, // group b
                    None,
                    None,
                    None,
                ],
                agg_cols: vec![vec![
                    Some(10), // group a
                    Some(90), // group b
                    Some(91),
                    Some(92),
                    Some(93),
                ]],
                input_batch_size,
            };
            let params = get_params_ms(&records, 25, Some(975), 1_025);
            let tc = TestCase {
                test_records: records,
                output_batch_size,
                params,
            };
            let batches = tc.run().unwrap();
            let actual = batches_to_lines(&batches);
            insta::assert_yaml_snapshot!(actual, @r###"
            ---
            - +----+--------------------------+----+
            - "| g0 | time                     | a0 |"
            - +----+--------------------------+----+
            - "| a  | 1970-01-01T00:00:00.975Z |    |"
            - "| a  | 1970-01-01T00:00:01Z     | 10 |"
            - "| a  | 1970-01-01T00:00:01.025Z |    |"
            - "| b  |                          | 90 |"
            - "| b  |                          | 91 |"
            - "| b  |                          | 92 |"
            - "| b  |                          | 93 |"
            - "| b  | 1970-01-01T00:00:00.975Z |    |"
            - "| b  | 1970-01-01T00:00:01Z     |    |"
            - "| b  | 1970-01-01T00:00:01.025Z |    |"
            - +----+--------------------------+----+
            "###);
            assert_batch_count(&batches, output_batch_size);
        }
    }}
}

#[test]
fn test_gapfill_multi_aggr_cols_with_nulls() {
    test_helpers::maybe_start_logging();
    insta::allow_duplicates! { for output_batch_size in [1, 2, 4, 8, 16, 32] {
        for input_batch_size in [1, 2, 4, 8] {
            let records = TestRecords {
                group_cols: vec![
                    vec![
                        Some("a"),
                        Some("a"),
                        Some("a"),
                        Some("a"),
                        Some("b"),
                        Some("b"),
                        Some("b"),
                    ],
                    vec![
                        Some("c"),
                        Some("c"),
                        Some("c"),
                        Some("c"),
                        Some("d"),
                        Some("d"),
                        Some("d"),
                    ],
                ],
                time_col: vec![
                    None,
                    None,
                    Some(1_000),
                    Some(1_100),
                    None,
                    Some(1_000),
                    Some(1_100),
                ],
                agg_cols: vec![
                    vec![
                        Some(1),
                        None,
                        Some(10),
                        Some(11),
                        Some(2),
                        Some(20),
                        Some(21),
                    ],
                    vec![
                        Some(3),
                        Some(3),
                        Some(30),
                        None,
                        Some(4),
                        Some(40),
                        Some(41),
                    ],
                ],
                input_batch_size,
            };
            let params = get_params_ms(&records, 25, Some(975), 1_125);
            let tc = TestCase {
                test_records: records,
                output_batch_size,
                params,
            };
            let batches = tc.run().unwrap();
            let actual = batches_to_lines(&batches);
            insta::assert_yaml_snapshot!(actual, @r###"
            ---
            - +----+----+--------------------------+----+----+
            - "| g0 | g1 | time                     | a0 | a1 |"
            - +----+----+--------------------------+----+----+
            - "| a  | c  |                          | 1  | 3  |"
            - "| a  | c  |                          |    | 3  |"
            - "| a  | c  | 1970-01-01T00:00:00.975Z |    |    |"
            - "| a  | c  | 1970-01-01T00:00:01Z     | 10 | 30 |"
            - "| a  | c  | 1970-01-01T00:00:01.025Z |    |    |"
            - "| a  | c  | 1970-01-01T00:00:01.050Z |    |    |"
            - "| a  | c  | 1970-01-01T00:00:01.075Z |    |    |"
            - "| a  | c  | 1970-01-01T00:00:01.100Z | 11 |    |"
            - "| a  | c  | 1970-01-01T00:00:01.125Z |    |    |"
            - "| b  | d  |                          | 2  | 4  |"
            - "| b  | d  | 1970-01-01T00:00:00.975Z |    |    |"
            - "| b  | d  | 1970-01-01T00:00:01Z     | 20 | 40 |"
            - "| b  | d  | 1970-01-01T00:00:01.025Z |    |    |"
            - "| b  | d  | 1970-01-01T00:00:01.050Z |    |    |"
            - "| b  | d  | 1970-01-01T00:00:01.075Z |    |    |"
            - "| b  | d  | 1970-01-01T00:00:01.100Z | 21 | 41 |"
            - "| b  | d  | 1970-01-01T00:00:01.125Z |    |    |"
            - +----+----+--------------------------+----+----+
            "###);
            assert_batch_count(&batches, output_batch_size);
        }
    }}
}

#[test]
fn test_gapfill_simple_no_lower_bound() {
    test_helpers::maybe_start_logging();
    insta::allow_duplicates! { for output_batch_size in [1, 2, 4, 8] {
        for input_batch_size in [1, 2, 4] {
            let batch = TestRecords {
                group_cols: vec![vec![Some("a"), Some("a"), Some("b"), Some("b")]],
                time_col: vec![Some(1_025), Some(1_100), Some(1_050), Some(1_100)],
                agg_cols: vec![vec![Some(10), Some(11), Some(20), Some(21)]],
                input_batch_size,
            };
            let params = get_params_ms(&batch, 25, None, 1_125);
            let tc = TestCase {
                test_records: batch,
                output_batch_size,
                params,
            };
            let batches = tc.run().unwrap();
            let actual = batches_to_lines(&batches);
            insta::assert_yaml_snapshot!(actual, @r###"
            ---
            - +----+--------------------------+----+
            - "| g0 | time                     | a0 |"
            - +----+--------------------------+----+
            - "| a  | 1970-01-01T00:00:01.025Z | 10 |"
            - "| a  | 1970-01-01T00:00:01.050Z |    |"
            - "| a  | 1970-01-01T00:00:01.075Z |    |"
            - "| a  | 1970-01-01T00:00:01.100Z | 11 |"
            - "| a  | 1970-01-01T00:00:01.125Z |    |"
            - "| b  | 1970-01-01T00:00:01.050Z | 20 |"
            - "| b  | 1970-01-01T00:00:01.075Z |    |"
            - "| b  | 1970-01-01T00:00:01.100Z | 21 |"
            - "| b  | 1970-01-01T00:00:01.125Z |    |"
            - +----+--------------------------+----+
            "###);
            assert_batch_count(&batches, output_batch_size);
        }
    }}
}

#[test]
fn test_gapfill_fill_prev() {
    test_helpers::maybe_start_logging();
    insta::allow_duplicates! { for output_batch_size in [1, 2, 4, 8] {
        for input_batch_size in [1, 2, 4] {
            let records = TestRecords {
                group_cols: vec![vec![
                    Some("a"),
                    Some("a"),
                    Some("b"),
                    Some("b"),
                    Some("b"),
                ]],
                time_col: vec![
                    // 975
                    Some(1000),
                    // 1025
                    // 1050
                    Some(1075),
                    // 1100
                    // 1125
                    // --- new series
                    // 975
                    Some(1000),
                    // 1025
                    Some(1050),
                    // 1075
                    Some(1100),
                    // 1125
                ],
                agg_cols: vec![vec![
                    Some(10),
                    Some(11),
                    Some(20),
                    None,
                    Some(21),
                ]],
                input_batch_size,
            };
            let params = get_params_ms_with_fill_strategy(&records, 25, Some(975), 1_125, FillStrategy::PrevNullAsIntentional);
            let tc = TestCase {
                test_records: records,
                output_batch_size,
                params,
            };
            let batches = tc.run().unwrap();
            let actual = batches_to_lines(&batches);
            insta::with_settings!({
                description => format!("input_batch_size: {input_batch_size}, output_batch_size: {output_batch_size}"),
            }, {
                insta::assert_yaml_snapshot!(actual, @r###"
                ---
                - +----+--------------------------+----+
                - "| g0 | time                     | a0 |"
                - +----+--------------------------+----+
                - "| a  | 1970-01-01T00:00:00.975Z |    |"
                - "| a  | 1970-01-01T00:00:01Z     | 10 |"
                - "| a  | 1970-01-01T00:00:01.025Z | 10 |"
                - "| a  | 1970-01-01T00:00:01.050Z | 10 |"
                - "| a  | 1970-01-01T00:00:01.075Z | 11 |"
                - "| a  | 1970-01-01T00:00:01.100Z | 11 |"
                - "| a  | 1970-01-01T00:00:01.125Z | 11 |"
                - "| b  | 1970-01-01T00:00:00.975Z |    |"
                - "| b  | 1970-01-01T00:00:01Z     | 20 |"
                - "| b  | 1970-01-01T00:00:01.025Z | 20 |"
                - "| b  | 1970-01-01T00:00:01.050Z |    |"
                - "| b  | 1970-01-01T00:00:01.075Z |    |"
                - "| b  | 1970-01-01T00:00:01.100Z | 21 |"
                - "| b  | 1970-01-01T00:00:01.125Z | 21 |"
                - +----+--------------------------+----+
                "###)
            });
            assert_batch_count(&batches, output_batch_size);
        }
    }}
}

#[test]
fn test_gapfill_fill_prev_null_as_missing() {
    test_helpers::maybe_start_logging();
    insta::allow_duplicates! {
        for output_batch_size in [16, 1] {
        for input_batch_size in [8, 1] {
            let records = TestRecords {
                group_cols: vec![vec![
                    Some("a"),
                    Some("a"),
                    Some("b"),
                    Some("b"),
                    Some("b"),
                ]],
                time_col: vec![
                    // 975
                    Some(1000),
                    // 1025
                    // 1050
                    Some(1075),
                    // 1100
                    // 1125
                    // --- new series
                    // 975
                    Some(1000),
                    // 1025
                    Some(1050),
                    // 1075
                    Some(1100),
                    // 1125
                ],
                agg_cols: vec![vec![
                    Some(10),  // a: 1000
                    None,      // a: 1075
                    Some(20),  // b: 1000
                    None,      // b: 1050
                    Some(21),  // b: 1100
                ]],
                input_batch_size,
            };
            let params = get_params_ms_with_fill_strategy(&records, 25, Some(975), 1_125, FillStrategy::PrevNullAsMissing);
            let tc = TestCase {
                test_records: records,
                output_batch_size,
                params,
            };
            let batches = tc.run().unwrap();
            let actual = batches_to_lines(&batches);
            insta::with_settings!({
                description => format!("input_batch_size: {input_batch_size}, output_batch_size: {output_batch_size}"),
            }, {
                insta::assert_yaml_snapshot!(actual, @r###"
                ---
                - +----+--------------------------+----+
                - "| g0 | time                     | a0 |"
                - +----+--------------------------+----+
                - "| a  | 1970-01-01T00:00:00.975Z |    |"
                - "| a  | 1970-01-01T00:00:01Z     | 10 |"
                - "| a  | 1970-01-01T00:00:01.025Z | 10 |"
                - "| a  | 1970-01-01T00:00:01.050Z | 10 |"
                - "| a  | 1970-01-01T00:00:01.075Z | 10 |"
                - "| a  | 1970-01-01T00:00:01.100Z | 10 |"
                - "| a  | 1970-01-01T00:00:01.125Z | 10 |"
                - "| b  | 1970-01-01T00:00:00.975Z |    |"
                - "| b  | 1970-01-01T00:00:01Z     | 20 |"
                - "| b  | 1970-01-01T00:00:01.025Z | 20 |"
                - "| b  | 1970-01-01T00:00:01.050Z | 20 |"
                - "| b  | 1970-01-01T00:00:01.075Z | 20 |"
                - "| b  | 1970-01-01T00:00:01.100Z | 21 |"
                - "| b  | 1970-01-01T00:00:01.125Z | 21 |"
                - +----+--------------------------+----+
                "###)
            });
            assert_batch_count(&batches, output_batch_size);
        }
    }}
}

#[test]
fn test_gapfill_fill_prev_null_as_missing_many_nulls() {
    test_helpers::maybe_start_logging();
    insta::allow_duplicates! {
        for output_batch_size in [16, 1] {
        for input_batch_size in [8, 1] {
            let records = TestRecords {
                group_cols: vec![vec![
                    Some("a"),
                    Some("a"),
                    Some("a"),
                    Some("a"),
                    Some("a"),
                    Some("a"),
                    // --- new series
                    Some("b"),
                    Some("b"),
                    Some("b"),
                    Some("b"),
                    Some("b"),
                ]],
                time_col: vec![
                    None,
                    Some(975),
                    Some(1000),
                    Some(1025),
                    Some(1050),
                    // 1075
                    Some(1100),
                    // 1125
                    // --- new series
                    None,
                    Some(975),
                    // 1000
                    Some(1025),
                    Some(1050),
                    // 1075
                    Some(1100),
                    // 1125
                ],
                agg_cols: vec![vec![
                    Some(-1),  // a: null ts
                    Some(10),  // a: 975
                    None,      // a: 1000
                    None,      // a: 1025 (stashed)
                    None,      // a: 1050 (stashed)
                               // a: 1075 (stashed)
                    Some(12),  // a: 1100
                               // a: 1125
                    // --- new series
                    Some(-2),  // b: null ts
                    None,      // b: 975
                               // b: 1000
                    Some(21),  // b: 1025
                    None,      // b: 1050
                               // b: 1075
                    Some(22),  // b: 1100
                               // b: 1125
                ]],
                input_batch_size,
            };
            let params = get_params_ms_with_fill_strategy(&records, 25, Some(975), 1_125, FillStrategy::PrevNullAsMissing);
            let tc = TestCase {
                test_records: records,
                output_batch_size,
                params,
            };
            let batches = tc.run().unwrap();
            let actual = batches_to_lines(&batches);
            insta::with_settings!({
                description => format!("input_batch_size: {input_batch_size}, output_batch_size: {output_batch_size}"),
            }, {
                insta::assert_yaml_snapshot!(actual, @r###"
                ---
                - +----+--------------------------+----+
                - "| g0 | time                     | a0 |"
                - +----+--------------------------+----+
                - "| a  |                          | -1 |"
                - "| a  | 1970-01-01T00:00:00.975Z | 10 |"
                - "| a  | 1970-01-01T00:00:01Z     | 10 |"
                - "| a  | 1970-01-01T00:00:01.025Z | 10 |"
                - "| a  | 1970-01-01T00:00:01.050Z | 10 |"
                - "| a  | 1970-01-01T00:00:01.075Z | 10 |"
                - "| a  | 1970-01-01T00:00:01.100Z | 12 |"
                - "| a  | 1970-01-01T00:00:01.125Z | 12 |"
                - "| b  |                          | -2 |"
                - "| b  | 1970-01-01T00:00:00.975Z |    |"
                - "| b  | 1970-01-01T00:00:01Z     |    |"
                - "| b  | 1970-01-01T00:00:01.025Z | 21 |"
                - "| b  | 1970-01-01T00:00:01.050Z | 21 |"
                - "| b  | 1970-01-01T00:00:01.075Z | 21 |"
                - "| b  | 1970-01-01T00:00:01.100Z | 22 |"
                - "| b  | 1970-01-01T00:00:01.125Z | 22 |"
                - +----+--------------------------+----+
                "###)
            });
            assert_batch_count(&batches, output_batch_size);
        }
    }}
}

/// Show that:
/// - we can have multiple interpolated segments within
///   a series
/// - a null value will break interpolation
/// - times before the first or after the last non-null data point
///   in a series are filled with nulls.
#[test]
fn test_gapfill_fill_interpolate() {
    test_helpers::maybe_start_logging();
    insta::allow_duplicates! {
        for output_batch_size in [16, 1] {
            let input_batch_size = 8;
            let records = TestRecords {
                group_cols: vec![vec![
                    Some("a"),
                    Some("a"),
                    Some("a"),
                    // --- new series
                    Some("b"),
                    Some("b"),
                    Some("b"),
                    Some("b"),
                    Some("b"),
                    Some("b"),
                ]],
                time_col: vec![
                    None,
                    // 975
                    Some(1000),
                    // 1025
                    // 1050
                    Some(1075),
                    // 1100
                    // 1125
                    // --- new series
                    None,
                    Some(975),
                    Some(1000),
                    Some(1025),
                    // 1050
                    Some(1075),
                    // 1100
                    Some(1125),
                ],
                agg_cols: vec![vec![
                    Some(-1),
                    // null,       975
                    Some(100), // 1000
                    // 200        1025
                    // 300        1050
                    Some(400), // 1075
                    //            1100
                    //            1125
                    // --- new series
                    Some(-10),
                    Some(1100), //  975
                    None, // 1200  1000 (this null value will be filled)
                    Some(1300), // 1025
                    // 1325        1050
                    Some(1350), // 1075
                    Some(1550), // 1100
                    //             1125
                ]],
                input_batch_size,
            };
            let params = get_params_ms_with_fill_strategy(
                &records,
                25,
                Some(975),
                1_125,
                FillStrategy::LinearInterpolate
            );
            let tc = TestCase {
                test_records: records,
                output_batch_size,
                params,
            };
            let batches = tc.run().unwrap();
            let actual = batches_to_lines(&batches);
            insta::with_settings!({
                description => format!("input_batch_size: {input_batch_size}, output_batch_size: {output_batch_size}"),
            }, {
                insta::assert_yaml_snapshot!(actual, @r###"
                ---
                - +----+--------------------------+------+
                - "| g0 | time                     | a0   |"
                - +----+--------------------------+------+
                - "| a  |                          | -1   |"
                - "| a  | 1970-01-01T00:00:00.975Z |      |"
                - "| a  | 1970-01-01T00:00:01Z     | 100  |"
                - "| a  | 1970-01-01T00:00:01.025Z | 200  |"
                - "| a  | 1970-01-01T00:00:01.050Z | 300  |"
                - "| a  | 1970-01-01T00:00:01.075Z | 400  |"
                - "| a  | 1970-01-01T00:00:01.100Z |      |"
                - "| a  | 1970-01-01T00:00:01.125Z |      |"
                - "| b  |                          | -10  |"
                - "| b  | 1970-01-01T00:00:00.975Z | 1100 |"
                - "| b  | 1970-01-01T00:00:01Z     | 1200 |"
                - "| b  | 1970-01-01T00:00:01.025Z | 1300 |"
                - "| b  | 1970-01-01T00:00:01.050Z | 1325 |"
                - "| b  | 1970-01-01T00:00:01.075Z | 1350 |"
                - "| b  | 1970-01-01T00:00:01.100Z | 1450 |"
                - "| b  | 1970-01-01T00:00:01.125Z | 1550 |"
                - +----+--------------------------+------+
                "###)
            });
            assert_batch_count(&batches, output_batch_size);
        }
    }
}

#[test]
fn test_gapfill_simple_no_lower_bound_with_nulls() {
    test_helpers::maybe_start_logging();
    insta::allow_duplicates! { for output_batch_size in [1, 2, 4, 8] {
        for input_batch_size in [1, 2, 4] {
            let batch = TestRecords {
                group_cols: vec![vec![
                    Some("a"),
                    Some("a"),
                    Some("a"),
                    Some("b"),
                    Some("b"),
                    Some("b"),
                    Some("b"),
                    Some("c"),
                    Some("c"),
                    Some("c"),
                    Some("c"),
                    Some("c"),
                ]],
                time_col: vec![
                    None, // group a
                    Some(1_025),
                    Some(1_100),
                    None, // group b
                    None,
                    None,
                    None, // group c
                    None,
                    None,
                    None,
                    Some(1_050),
                    Some(1_100),
                ],
                agg_cols: vec![vec![
                    Some(1), // group a
                    Some(10),
                    Some(11),
                    Some(90), // group b
                    Some(91),
                    Some(92),
                    Some(93),
                    None, // group c
                    None,
                    Some(2),
                    Some(20),
                    Some(21),
                ]],
                input_batch_size,
            };
            let params = get_params_ms(&batch, 25, None, 1_125);
            let tc = TestCase {
                test_records: batch,
                output_batch_size,
                params,
            };
            let batches = tc.run().unwrap();
            let actual = batches_to_lines(&batches);
            insta::assert_yaml_snapshot!(actual, @r###"
            ---
            - +----+--------------------------+----+
            - "| g0 | time                     | a0 |"
            - +----+--------------------------+----+
            - "| a  |                          | 1  |"
            - "| a  | 1970-01-01T00:00:01.025Z | 10 |"
            - "| a  | 1970-01-01T00:00:01.050Z |    |"
            - "| a  | 1970-01-01T00:00:01.075Z |    |"
            - "| a  | 1970-01-01T00:00:01.100Z | 11 |"
            - "| a  | 1970-01-01T00:00:01.125Z |    |"
            - "| b  |                          | 90 |"
            - "| b  |                          | 91 |"
            - "| b  |                          | 92 |"
            - "| b  |                          | 93 |"
            - "| c  |                          |    |"
            - "| c  |                          |    |"
            - "| c  |                          | 2  |"
            - "| c  | 1970-01-01T00:00:01.050Z | 20 |"
            - "| c  | 1970-01-01T00:00:01.075Z |    |"
            - "| c  | 1970-01-01T00:00:01.100Z | 21 |"
            - "| c  | 1970-01-01T00:00:01.125Z |    |"
            - +----+--------------------------+----+
            "###);
            assert_batch_count(&batches, output_batch_size);
        }
    }}
}

#[test]
fn test_gapfill_oom() {
    // Show that a graceful error is produced if memory limit is exceeded
    test_helpers::maybe_start_logging();
    let input_batch_size = 128;
    let output_batch_size = 128;
    let batch = TestRecords {
        group_cols: vec![vec![Some("a"), Some("a")]],
        time_col: vec![Some(1_000), Some(1_100)],
        agg_cols: vec![vec![Some(10), Some(11)]],
        input_batch_size,
    };
    let params = get_params_ms(&batch, 25, Some(975), 1_125);
    let tc = TestCase {
        test_records: batch,
        output_batch_size,
        params,
    };
    let result = tc.run_with_memory_limit(1);
    assert_error!(result, DataFusionError::ResourcesExhausted(_));
}

fn assert_batch_count(actual_batches: &[RecordBatch], batch_size: usize) {
    let num_rows = actual_batches.iter().map(|b| b.num_rows()).sum::<usize>();
    let expected_batch_count = f64::ceil(num_rows as f64 / batch_size as f64) as usize;
    assert_eq!(expected_batch_count, actual_batches.len());
}

type ExprVec = Vec<Arc<dyn PhysicalExpr>>;

pub(super) struct TestRecords {
    pub group_cols: Vec<Vec<Option<&'static str>>>,
    // Stored as millisecods since intervals use millis,
    // to let test cases be consistent and easier to read.
    pub time_col: Vec<Option<i64>>,
    pub agg_cols: Vec<Vec<Option<i64>>>,
    pub input_batch_size: usize,
}

impl TestRecords {
    fn schema(&self) -> SchemaRef {
        // In order to test input with null timestamps, we need the
        // timestamp column to be nullable. Unforunately this means
        // we can't use the IOx schema builder here.
        let mut fields = vec![];
        for i in 0..self.group_cols.len() {
            fields.push(Field::new(
                format!("g{i}"),
                (&InfluxColumnType::Tag).into(),
                true,
            ));
        }
        fields.push(Field::new(
            "time",
            (&InfluxColumnType::Timestamp).into(),
            true,
        ));
        for i in 0..self.agg_cols.len() {
            fields.push(Field::new(
                format!("a{i}"),
                (&InfluxColumnType::Field(InfluxFieldType::Integer)).into(),
                true,
            ));
        }
        Schema::new(fields).into()
    }

    fn len(&self) -> usize {
        self.time_col.len()
    }

    fn exprs(&self) -> Result<(ExprVec, ExprVec)> {
        let mut group_expr: ExprVec = vec![];
        let mut aggr_expr: ExprVec = vec![];
        let ngroup_cols = self.group_cols.len();
        for i in 0..self.schema().fields().len() {
            match i.cmp(&ngroup_cols) {
                Ordering::Less => group_expr.push(Arc::new(Column::new(&format!("g{i}"), i))),
                Ordering::Equal => group_expr.push(Arc::new(Column::new("t", i))),
                Ordering::Greater => {
                    let idx = i - ngroup_cols + 1;
                    aggr_expr.push(Arc::new(Column::new(&format!("a{idx}"), i)));
                }
            }
        }
        Ok((group_expr, aggr_expr))
    }
}

impl TryFrom<TestRecords> for Vec<RecordBatch> {
    type Error = DataFusionError;

    fn try_from(value: TestRecords) -> Result<Self> {
        let mut arrs: Vec<ArrayRef> =
            Vec::with_capacity(value.group_cols.len() + value.agg_cols.len() + 1);
        for gc in &value.group_cols {
            let arr = Arc::new(DictionaryArray::<Int32Type>::from_iter(gc.iter().cloned()));
            arrs.push(arr);
        }
        // Scale from milliseconds to the nanoseconds that are actually stored.
        let scaled_times: TimestampNanosecondArray = value
            .time_col
            .iter()
            .map(|o| o.map(|v| v * 1_000_000))
            .collect();
        arrs.push(Arc::new(scaled_times));
        for ac in &value.agg_cols {
            let arr = Arc::new(Int64Array::from_iter(ac));
            arrs.push(arr);
        }

        let one_batch =
            RecordBatch::try_new(value.schema(), arrs).map_err(DataFusionError::ArrowError)?;
        let mut batches = vec![];
        let mut offset = 0;
        while offset < one_batch.num_rows() {
            let len = std::cmp::min(value.input_batch_size, one_batch.num_rows() - offset);
            let batch = one_batch.slice(offset, len);
            batches.push(batch);
            offset += value.input_batch_size;
        }
        Ok(batches)
    }
}

struct TestCase {
    test_records: TestRecords,
    output_batch_size: usize,
    params: GapFillExecParams,
}

impl TestCase {
    fn run(self) -> Result<Vec<RecordBatch>> {
        block_on(async {
            let session_ctx = SessionContext::with_config(
                SessionConfig::default().with_batch_size(self.output_batch_size),
            )
            .into();
            Self::execute_with_config(&session_ctx, self.plan()?).await
        })
    }

    fn run_with_memory_limit(self, limit: usize) -> Result<Vec<RecordBatch>> {
        block_on(async {
            let session_ctx = SessionContext::with_config_rt(
                SessionConfig::default().with_batch_size(self.output_batch_size),
                RuntimeEnv::new(RuntimeConfig::default().with_memory_limit(limit, 1.0))?.into(),
            )
            .into();
            let result = Self::execute_with_config(&session_ctx, self.plan()?).await;

            if result.is_ok() {
                // Verify that the operator reports usage in a
                // symmetrical way.
                let pool = &session_ctx.runtime_env().memory_pool;
                assert_eq!(0, pool.reserved());
            }

            result
        })
    }

    fn plan(self) -> Result<Arc<GapFillExec>> {
        let schema = self.test_records.schema();
        let (group_expr, aggr_expr) = self.test_records.exprs()?;

        let input_batch_size = self.test_records.input_batch_size;

        let num_records = self.test_records.len();
        let batches: Vec<RecordBatch> = self.test_records.try_into()?;
        assert_batch_count(&batches, input_batch_size);
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            num_records
        );

        debug!(
            "input_batch_size is {input_batch_size}, output_batch_size is {}",
            self.output_batch_size
        );
        let input = Arc::new(MemoryExec::try_new(&[batches], schema, None)?);
        let plan = Arc::new(GapFillExec::try_new(
            input,
            group_expr,
            aggr_expr,
            self.params.clone(),
        )?);
        Ok(plan)
    }

    async fn execute_with_config(
        session_ctx: &Arc<SessionContext>,
        plan: Arc<GapFillExec>,
    ) -> Result<Vec<RecordBatch>> {
        let task_ctx = Arc::new(TaskContext::from(session_ctx.as_ref()));
        collect(plan, task_ctx).await
    }
}

fn bound_included_from_option<T>(o: Option<T>) -> Bound<T> {
    if let Some(v) = o {
        Bound::Included(v)
    } else {
        Bound::Unbounded
    }
}

fn phys_fill_strategies(
    records: &TestRecords,
    fill_strategy: FillStrategy,
) -> Result<Vec<(Arc<dyn PhysicalExpr>, FillStrategy)>> {
    let start = records.group_cols.len() + 1; // 1 is for time col
    let end = start + records.agg_cols.len();
    let mut v = Vec::with_capacity(records.agg_cols.len());
    for f in &records.schema().fields()[start..end] {
        v.push((
            phys_col(f.name(), &records.schema())?,
            fill_strategy.clone(),
        ));
    }
    Ok(v)
}

fn get_params_ms_with_fill_strategy(
    batch: &TestRecords,
    stride_ms: i64,
    start: Option<i64>,
    end: i64,
    fill_strategy: FillStrategy,
) -> GapFillExecParams {
    get_params_ms_with_origin_fill_strategy(batch, stride_ms, start, end, None, fill_strategy)
}

fn get_params_ms_with_origin_fill_strategy(
    batch: &TestRecords,
    stride_ms: i64,
    start: Option<i64>,
    end: i64,
    origin_ms: Option<i64>,
    fill_strategy: FillStrategy,
) -> GapFillExecParams {
    // stride is in ms
    let stride = ScalarValue::new_interval_mdn(0, 0, stride_ms * 1_000_000);
    let origin =
        origin_ms.map(|o| phys_lit(ScalarValue::TimestampNanosecond(Some(o * 1_000_000), None)));

    GapFillExecParams {
        stride: phys_lit(stride),
        time_column: Column::new("t", batch.group_cols.len()),
        origin,
        // timestamps are nanos, so scale them accordingly
        time_range: Range {
            start: bound_included_from_option(start.map(|start| {
                phys_lit(ScalarValue::TimestampNanosecond(
                    Some(start * 1_000_000),
                    None,
                ))
            })),
            end: Bound::Included(phys_lit(ScalarValue::TimestampNanosecond(
                Some(end * 1_000_000),
                None,
            ))),
        },
        fill_strategy: phys_fill_strategies(batch, fill_strategy).unwrap(),
    }
}

fn get_params_ms(
    batch: &TestRecords,
    stride: i64,
    start: Option<i64>,
    end: i64,
) -> GapFillExecParams {
    get_params_ms_with_fill_strategy(batch, stride, start, end, FillStrategy::Null)
}
