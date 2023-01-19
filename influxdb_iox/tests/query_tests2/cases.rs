use super::framework::{ChunkStage, TestCase};

// TODO: Generate these tests from the files on disk.
// See <https://github.com/influxdata/influxdb_iox/issues/6610>.

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
#[ignore]
async fn new_sql_system_tables() {
    unimplemented!("Test snapshot might need updating?");
    // test_helpers::maybe_start_logging();
    //
    // TestCase {
    //     input: "cases/in/new_sql_system_tables.sql",
    //     chunk_stage: ChunkStage::Ingester,
    // }
    // .run()
    // .await;
}

#[tokio::test]
#[ignore]
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
#[ignore]
async fn retention() {
    unimplemented!("See <https://github.com/influxdata/influxdb_iox/issues/6592>");
    // test_helpers::maybe_start_logging();
    //
    // TestCase {
    //     input: "cases/in/retention.sql",
    //     chunk_stage: ChunkStage::Parquet,
    // }
    // .run()
    // .await;
}

#[tokio::test]
#[ignore]
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
