use test_helpers_end_to_end::{
    maybe_skip_integration, DataGenerator, MiniCluster, Step, StepTest, TestConfig,
};

#[tokio::test]
pub async fn test_metrics() {
    let database_url = maybe_skip_integration!();
    let test_config = TestConfig::new_all_in_one(Some(database_url));
    let mut cluster = MiniCluster::create_all_in_one(test_config).await;

    let lp = DataGenerator::new().line_protocol().to_owned();

    StepTest::new(
        &mut cluster,
        vec![
            Step::WriteLineProtocol(lp),
            Step::WaitForReadable,
            Step::VerifiedMetrics(Box::new(|_state, metrics| {
                let metrics_lines: Vec<_> = metrics.trim().split('\n').collect();

                let catalog_op_metrics_count = metrics_lines
                    .iter()
                    .filter(|x| x.starts_with("catalog_op_duration_seconds_bucket"))
                    .count();
                assert!(
                    catalog_op_metrics_count >= 180,
                    "Expected at least 180 catalog op metrics, got: {}\n\n{}",
                    catalog_op_metrics_count,
                    metrics
                );

                let process_metrics_count = metrics_lines
                    .iter()
                    .filter(|x| x.starts_with("process_start_time_seconds"))
                    .count();
                assert!(
                    process_metrics_count >= 1,
                    "Expected `process_start_time_seconds` metric but found none: \n\n{}",
                    metrics
                );
            })),
        ],
    )
    .run()
    .await;
}

#[cfg(feature = "jemalloc_replacing_malloc")]
#[tokio::test]
pub async fn test_jemalloc_metrics() {
    use test_helpers::assert_contains;

    let database_url = maybe_skip_integration!();
    let test_config = TestConfig::new_all_in_one(Some(database_url));
    let mut cluster = MiniCluster::create_all_in_one(test_config).await;

    StepTest::new(
        &mut cluster,
        vec![Step::VerifiedMetrics(Box::new(|_state, metrics| {
            let lines: Vec<_> = metrics
                .trim()
                .split('\n')
                .filter(|x| x.starts_with("jemalloc_memstats_bytes"))
                .collect();

            assert_eq!(lines.len(), 6);
            assert_contains!(lines[0], r#"jemalloc_memstats_bytes{stat="active"}"#);
            assert_contains!(lines[1], r#"jemalloc_memstats_bytes{stat="alloc"}"#);
            assert_contains!(lines[2], r#"jemalloc_memstats_bytes{stat="metadata"}"#);
            assert_contains!(lines[3], r#"jemalloc_memstats_bytes{stat="mapped"}"#);
            assert_contains!(lines[4], r#"jemalloc_memstats_bytes{stat="resident"}"#);
            assert_contains!(lines[5], r#"jemalloc_memstats_bytes{stat="retained"}"#);
        }))],
    )
    .run()
    .await;
}
