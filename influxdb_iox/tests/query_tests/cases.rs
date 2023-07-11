//! This module contains data driven tests that run sql queries from input files on disk.
//!
//! # Cookbook: Adding a new Test
//!
//! How to make a new test:
//!
//! 1. Add a new file .sql to the `cases/in` directory
//! 2. Run the tests (e.g. `TEST_INTEGRATION=1 TEST_INFLUXDB_IOX_CATALOG_DSN=postgresql://postgres@localhost:5432/postgres cargo test --test end_to_end`)
//! 3. You will get a failure message that contains commands of how to update the expected output file
//!
//! ## Example output
//!
//! ```
//! Possibly helpful commands:
//!   # See diff
//!   diff -du "cases/in/pushdown.sql.expected" "cases/out/pushdown.sql.out"
//!   # Update expected
//!   cp -f "cases/in/pushdown.sql.out" "cases/out/pushdown.sql.expected"
//! ```
//!
//! # Cookbook: Adding a new test scenario
//!
//! Each test can be defined in terms of a "setup" (a set of actions
//! taken to prepare the state of database) which are defined in setups.rs.
//!
//! In the future, we envision more fine grained control of these
//! setups (by implementing some of the database commands as IOX_TEST
//! commands), but for now they are hard coded.
//!
//! The SQL files refer to the setups with a specially formatted comment:
//!
//! ```sql
//! -- IOX_SETUP: OneMeasurementFourChunksWithDuplicates
//! ```
//!
//! To add a new setup, follow the pattern in `influxdb_iox/tests/query_tests/setups.rs`.

use super::framework::{ChunkStage, TestCase};

#[tokio::test]
async fn basic() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/basic.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn date_bin() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/date_bin.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn dedup_and_predicates_parquet() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/dedup_and_predicates_parquet.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn dedup_and_predicates_parquet_ingester() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/dedup_and_predicates_parquet_ingester.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn duplicates_ingester() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/duplicates_ingester.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn duplicates_parquet() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/duplicates_parquet.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn duplicates_parquet_20() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/duplicates_parquet_20.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn duplicates_parquet_20_and_ingester() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/duplicates_parquet_20_and_ingester.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn duplicates_parquet_50() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/duplicates_parquet_50.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn duplicates_different_domains() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/duplicates_different_domains.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn gapfill() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/gapfill.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn new_sql_system_tables() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/new_sql_system_tables.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn periods() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/periods.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn equals() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/equals.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn pushdown() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/pushdown.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn retention() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/retention.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn selectors() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/selectors.sql",
        chunk_stage: ChunkStage::All,
    }
    .run()
    .await;
}

#[tokio::test]
async fn several_chunks() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/several_chunks.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn sql_information_schema() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/sql_information_schema.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn timestamps() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/timestamps.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn two_chunks() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/two_chunks.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn two_chunks_missing_columns() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/two_chunks_missing_columns.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn schema_merge() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/schema_merge.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn restaurant() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/restaurant.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn union_all() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/union_all.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn aggregates() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/aggregates.sql",
        chunk_stage: ChunkStage::All,
    }
    .run()
    .await;
}

#[tokio::test]
async fn aggregates_with_nulls() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/aggregates_with_nulls.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn different_tag_sets() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/different_tag_sets.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn bugs() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/bugs.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

#[tokio::test]
async fn custom_partitioning() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/custom_partitioning.sql",
        chunk_stage: ChunkStage::Ingester,
    }
    .run()
    .await;
}

mod influxql {
    use super::*;

    #[tokio::test]
    async fn issue_6112() {
        test_helpers::maybe_start_logging();

        TestCase {
            input: "cases/in/issue_6112.influxql",
            chunk_stage: ChunkStage::Ingester,
        }
        .run()
        .await;
    }

    /// Test window-like functions, which utilise user-defined aggregate and
    /// window functions.
    #[tokio::test]
    async fn window_like() {
        test_helpers::maybe_start_logging();

        TestCase {
            input: "cases/in/window_like.influxql",
            chunk_stage: ChunkStage::Ingester,
        }
        .run()
        .await;
    }

    /// Test TOP/BOTTOM functions, which use window functions to project
    /// the top or bottom rows in groups.
    #[tokio::test]
    async fn top_bottom() {
        test_helpers::maybe_start_logging();

        TestCase {
            input: "cases/in/top_bottom.influxql",
            chunk_stage: ChunkStage::Ingester,
        }
        .run()
        .await;
    }

    /// Test PERCENTILE functions.
    #[tokio::test]
    async fn percentile() {
        test_helpers::maybe_start_logging();

        TestCase {
            input: "cases/in/percentile.influxql",
            chunk_stage: ChunkStage::Ingester,
        }
        .run()
        .await;
    }

    #[tokio::test]
    async fn influxql_metadata() {
        test_helpers::maybe_start_logging();

        TestCase {
            input: "cases/in/influxql_metadata.influxql",
            chunk_stage: ChunkStage::Ingester,
        }
        .run()
        .await;
    }
}
