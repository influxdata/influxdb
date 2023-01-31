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
//!   diff -du "cases/in/pushdown.expected" "cases/out/pushdown.out"
//!   # Update expected
//!   cp -f "cases/in/pushdown.out" "cases/out/pushdown.expected"
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
//! To add a new setup, follow the pattern in `influxdb_iox/tests/query_tests2/setups.rs`.

use super::framework::{ChunkStage, TestCase};

#[tokio::test]
async fn basic() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/basic.sql",
        chunk_stage: ChunkStage::All,
    }
    .run()
    .await;
}

#[tokio::test]
async fn dedup_and_predicates_parquet() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/dedup_and_predicates_parquet.sql",
        chunk_stage: ChunkStage::Parquet,
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
        chunk_stage: ChunkStage::Parquet,
    }
    .run()
    .await;
}

#[tokio::test]
async fn duplicates_parquet_many() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/duplicates_parquet_many.sql",
        chunk_stage: ChunkStage::Parquet,
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
async fn pushdown() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/pushdown.sql",
        chunk_stage: ChunkStage::Parquet,
    }
    .run()
    .await;
}

#[tokio::test]
async fn retention() {
    test_helpers::maybe_start_logging();

    TestCase {
        input: "cases/in/retention.sql",
        chunk_stage: ChunkStage::Parquet,
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
