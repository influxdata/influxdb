//! IOx Compactor Layout tests
//!
//! These tests do almost everything the compactor would do in a
//! production system *except* for reading/writing parquet data.
//!
//! The input to each test is the parquet file layout of a partition.
//!
//! The output is a representation of the steps the compactor chose to
//! take and the final layout of the parquet files in the partition.
//!
//! # Interpreting test lines
//!
//! This test uses `insta` to compare inlined string represetention of
//! what the compactor did.
//!
//! Each line in the representation represents either some metadata or
//! a parquet file, with a visual depiction of its `min_time` and
//! `max_time` (the minimum timestamp and maximum timestamp for data
//! in the file).
//!
//! For example:
//!
//! ```text
//!     - L0.3[300,350] 5kb                                           |-L0.3-|
//! ```
//!
//! Represents the following [`ParquetFile`]:
//!
//! ```text
//! ParquetFile {
//!  id: 3,
//!  compaction_level: L0
//!  min_time: 300,
//!  max_time: 350
//!  file_size_bytes: 5*1024
//! }
//! ```
//!
//! The `|-L0.3-|` shows the relative location of `min_time` (`|-`)
//! and `max_time (`-|`) on a time line to help visualize the output
//!
//! A file with `?` represents a `ParquetFileParam` (aka a file that
//! will be added to the catalog but is not yet and thus has no id
//! assigned). So the following represents the same file as above, but
//! without an entry in the catalog:
//!
//!
//! ```text
//!     - L0.?[300,350] 5kb                                           |-L0.3-|
//! ```
mod accumulated_size;
mod backfill;
mod common_use_cases;
mod core;
mod created_at;
mod knobs;
mod large_files;
mod large_overlaps;
mod many_files;
mod single_timestamp;
mod stuck;

use std::{sync::atomic::Ordering, time::Duration};

use compactor_test_utils::{display_size, format_files, TestSetup, TestSetupBuilder};
use data_types::{CompactionLevel, ParquetFile};
use iox_tests::TestParquetFileBuilder;
use iox_time::Time;

pub(crate) const ONE_MB: u64 = 1024 * 1024;

/// creates a TestParquetFileBuilder setup for layout tests
pub(crate) fn parquet_builder() -> TestParquetFileBuilder {
    TestParquetFileBuilder::default()
        .with_compaction_level(CompactionLevel::Initial)
        // need some LP to generate the schema
        .with_line_protocol("table,tag1=A,tag2=B,tag3=C field_int=1i 100")
}

/// Creates the default TestSetupBuilder for layout tests
///
/// NOTE: The builder is configured with parameters that are intended
/// to be as close as possible to what is configured on production
/// systems so that we can predict and reason about what the compactor
/// will do in production.
pub(crate) async fn layout_setup_builder() -> TestSetupBuilder<false> {
    TestSetup::builder()
        .await
        .with_percentage_max_file_size(20)
        .with_split_percentage(80)
        .with_max_num_files_per_plan(200)
        .with_min_num_l1_files_to_compact(10)
        .with_max_desired_file_size_bytes(100 * ONE_MB)
        .simulate_without_object_store()
}

/// Creates a scenario with ten 9 * 1MB overlapping L0 files
pub(crate) async fn all_overlapping_l0_files(setup: TestSetup) -> TestSetup {
    for i in 0..10 {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(100)
                    .with_max_time(200000)
                    .with_file_size_bytes(9 * ONE_MB)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i + 1)),
            )
            .await;
    }
    setup
}

/// runs the scenario and returns a string based output for comparison
pub(crate) async fn run_layout_scenario(setup: &TestSetup) -> Vec<String> {
    // verify the files are ok to begin with
    setup.verify_invariants().await;

    setup.catalog.time_provider.inc(Duration::from_nanos(200));

    let input_files = setup.list_by_table_not_to_delete().await;
    let mut output = format_files("**** Input Files ", &sort_files(input_files));

    // check if input files trip warnings (situations may be deliberate)
    output.extend(setup.generate_warnings().await);

    // run the actual compaction
    let compact_result = setup.run_compact().await;

    // record what the compactor actually did
    if !setup.suppress_run_output {
        output.extend(compact_result.run_log);
    }

    // Record any skipped compactions (is after what the compactor actually did)
    output.extend(get_skipped_compactions(setup).await);

    // record the final state of the catalog
    let output_files = setup.list_by_table_not_to_delete().await;

    let bytes_written = setup.bytes_written.load(Ordering::Relaxed) as i64;

    output.extend(format_files(
        format!(
            "**** Final Output Files ({} written)",
            display_size(bytes_written)
        ),
        &sort_files(output_files),
    ));

    if !setup.suppress_writes_breakdown {
        output.extend(vec![
            "**** Breakdown of where bytes were written".to_string()
        ]);

        let mut breakdown = Vec::new();
        for (op, written) in &*setup.bytes_written_per_plan.lock().unwrap() {
            let written = *written as i64;
            breakdown.push(format!("{} written by {}", display_size(written), op));
        }
        breakdown.sort();
        output.extend(breakdown);
    }

    // verify that the output of the compactor was valid as well
    setup.verify_invariants().await;

    // verify required splits times were all observed.  They're removed as they occur, so the vec should be empty.
    let required_split_times = setup.required_split_times.lock().unwrap().clone();
    assert!(
        required_split_times.is_empty(),
        "required split times not observed: {:?}",
        required_split_times
    );

    // check if output files trip warnings (warnings here deserve scrutiny, but may be justifiable)
    output.extend(setup.generate_warnings().await);

    output
}

fn sort_files(mut files: Vec<ParquetFile>) -> Vec<ParquetFile> {
    // sort by ascending parquet file id for more consistent display
    files.sort_by(|f1, f2| f1.id.cmp(&f2.id));
    files
}

async fn get_skipped_compactions(setup: &TestSetup) -> Vec<String> {
    let skipped = setup
        .catalog
        .catalog
        .repositories()
        .await
        .partitions()
        .list_skipped_compactions()
        .await
        .unwrap();

    skipped
        .iter()
        .map(|skipped| {
            format!(
                "SKIPPED COMPACTION for {:?}: {}",
                skipped.partition_id, skipped.reason
            )
        })
        .collect()
}
