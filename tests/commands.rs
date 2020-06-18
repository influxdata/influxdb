use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use assert_cmd::assert::Assert;
use assert_cmd::Command;
use libflate::gzip;
use predicates::prelude::*;

/// Validates that p is a valid parquet file
fn validate_parquet_file(p: &Path) {
    // Verify file extension is parquet
    let file_extension = p
        .extension()
        .map_or(String::from(""), |ext| ext.to_string_lossy().to_string());
    assert_eq!(file_extension, "parquet");

    // TODO: verify we can decode the contents of the parquet file and
    // it is as expected. For now, just use a check against the magic
    // `PAR1` bytes all parquet files start with.
    let mut reader = BufReader::new(File::open(p).expect("Error reading file"));

    let mut first_four = [0; 4];
    reader
        .read_exact(&mut first_four)
        .expect("Error reading first four bytes");
    assert_eq!(&first_four, b"PAR1");
}

#[test]
fn convert_bad_input_filename() {
    let mut cmd = Command::cargo_bin("delorean").unwrap();
    let assert = cmd
        .arg("convert")
        .arg("non_existent_input.lp")
        .arg("non_existent_output")
        .assert();

    assert
        .failure()
        .code(1)
        .stderr(predicate::str::contains(
            "Error reading non_existent_input.lp",
        ))
        .stderr(predicate::str::contains("No such file or directory"));
}

#[test]
fn convert_line_protocol_good_input_filename() {
    let mut cmd = Command::cargo_bin("delorean").unwrap();

    let parquet_path = delorean_test_helpers::tempfile::Builder::new()
        .prefix("dstool_e2e")
        .suffix(".parquet")
        .tempfile()
        .expect("error creating temp file")
        .into_temp_path();
    let parquet_filename_string = parquet_path.to_string_lossy().to_string();

    let assert = cmd
        .arg("convert")
        .arg("tests/fixtures/lineproto/temperature.lp")
        .arg(&parquet_filename_string)
        .assert();

    let expected_success_string = format!(
        "Completing writing to {} successfully",
        parquet_filename_string
    );

    assert
        .success()
        .stderr(predicate::str::contains("dstool convert starting"))
        .stderr(predicate::str::contains(
            "Writing output for measurement h2o_temperature",
        ))
        .stderr(predicate::str::contains(expected_success_string));

    validate_parquet_file(&parquet_path);
}

#[test]
fn convert_tsm_good_input_filename() {
    let mut cmd = Command::cargo_bin("delorean").unwrap();

    let parquet_path = delorean_test_helpers::tempfile::Builder::new()
        .prefix("dstool_e2e_tsm")
        .suffix(".parquet")
        .tempfile()
        .expect("error creating temp file")
        .into_temp_path();
    let parquet_filename_string = parquet_path.to_string_lossy().to_string();

    let assert = cmd
        .arg("convert")
        .arg("tests/fixtures/000000000000005-000000002.tsm.gz")
        .arg(&parquet_filename_string)
        .assert();

    // TODO this should succeed when TSM -> parquet conversion is implemented.
    assert
        .failure()
        .code(1)
        .stderr(predicate::str::contains("Conversion failed"))
        .stderr(predicate::str::contains(
            "Not implemented: TSM Conversion not supported yet",
        ));

    // TODO add better success expectations

    // let expected_success_string = format!(
    //     "Completing writing to {} successfully",
    //     parquet_filename_string
    // );

    // assert
    //     .success()
    //     .stderr(predicate::str::contains("dstool convert starting"))
    //     .stderr(predicate::str::contains(
    //         "Writing output for measurement h2o_temperature",
    //     ))
    //     .stderr(predicate::str::contains(expected_success_string));

    // validate_parquet_file(&parquet_path);
}

#[test]
fn convert_multiple_measurements() {
    let mut cmd = Command::cargo_bin("delorean").unwrap();

    // Create a directory
    let parquet_output_path = delorean_test_helpers::tempfile::Builder::new()
        .prefix("dstool_e2e")
        .tempdir()
        .expect("error creating temp directory");

    let parquet_output_dir_path = parquet_output_path.path().to_string_lossy().to_string();

    let assert = cmd
        .arg("convert")
        .arg("tests/fixtures/lineproto/air_and_water.lp")
        .arg(&parquet_output_dir_path)
        .assert();

    let expected_success_string = format!(
        "Completing writing to {} successfully",
        parquet_output_dir_path
    );

    assert
        .success()
        .stderr(predicate::str::contains("dstool convert starting"))
        .stderr(predicate::str::contains("Writing to output directory"))
        .stderr(predicate::str::contains(
            "Writing output for measurement h2o_temperature",
        ))
        .stderr(predicate::str::contains(
            "Writing output for measurement air_temperature",
        ))
        .stderr(predicate::str::contains(expected_success_string));

    // check that the two files have been written successfully
    let mut output_files: Vec<_> = fs::read_dir(parquet_output_path.path())
        .expect("reading directory")
        .map(|dir_ent| {
            let dir_ent = dir_ent.expect("error reading dir entry");
            validate_parquet_file(&dir_ent.path());
            dir_ent.file_name().to_string_lossy().to_string()
        })
        .collect();

    // Ensure the order is consistent before comparing them
    output_files.sort();
    assert_eq!(
        output_files,
        vec!["air_temperature.parquet", "h2o_temperature.parquet"]
    );
}

#[test]
fn meta_bad_input_filename() {
    let mut cmd = Command::cargo_bin("delorean").unwrap();
    let assert = cmd.arg("meta").arg("non_existent_input").assert();

    assert
        .failure()
        .code(2)
        .stderr(predicate::str::contains("Metadata dump failed"))
        .stderr(predicate::str::contains(
            "Metadata dump failed: Unknown input type: No extension for non_existent_input",
        ));
}

#[test]
fn meta_non_existent_input_filename() {
    let mut cmd = Command::cargo_bin("delorean").unwrap();
    let assert = cmd.arg("meta").arg("non_existent_input.tsm").assert();

    assert
        .failure()
        .code(2)
        .stderr(predicate::str::contains("Metadata dump failed"))
        .stderr(predicate::str::contains(
            "Metadata dump failed: Error reading non_existent_input.tsm",
        ));
}

#[test]
fn meta_bad_input_filename_gz() {
    let mut cmd = Command::cargo_bin("delorean").unwrap();
    let assert = cmd.arg("meta").arg("non_existent_input.gz").assert();

    assert
        .failure()
        .code(2)
        .stderr(predicate::str::contains("Metadata dump failed"))
        .stderr(predicate::str::contains(
            "Metadata dump failed: Unknown input type: No extension before .gz for non_existent_input.gz",
        ));
}

// gunzip's the contents of the file at input_path into a temporary path
fn uncompress_gz(
    input_path: &str,
    output_extension: &str,
) -> delorean_test_helpers::tempfile::TempPath {
    let gz_file = File::open(input_path).expect("Error opening input");

    let output_path = delorean_test_helpers::tempfile::Builder::new()
        .prefix("dstool_decompressed_e2e")
        .suffix(output_extension)
        .tempfile()
        .expect("error creating temp file")
        .into_temp_path();

    let mut output_file = File::create(&output_path).expect("error opening output");
    let mut decoder = gzip::Decoder::new(gz_file).expect("error creating gzip decoder");
    std::io::copy(&mut decoder, &mut output_file).expect("error copying stream");
    output_path
}

/// Validates some of the metadata output content for this tsm file
fn assert_meta_000000000000005_000000002_tsm(assert: Assert) {
    assert
        .success()
        .stdout(predicate::str::contains("TSM Metadata Report"))
        .stdout(predicate::str::contains(
            "(05c19117091a1000, 05c19117091a1001) 2159 index entries, 2159 total records",
        ))
        .stdout(predicate::str::contains(
            "task_scheduler_total_schedule_fails",
        ));
}

#[test]
fn meta_000000000000005_000000002_tsm() {
    let input_tsm = uncompress_gz("tests/fixtures/000000000000005-000000002.tsm.gz", ".tsm");
    let mut cmd = Command::cargo_bin("delorean").unwrap();
    let assert = cmd
        .arg("meta")
        .arg(input_tsm.to_string_lossy().to_string())
        .assert();
    assert_meta_000000000000005_000000002_tsm(assert);
}

#[test]
fn meta_000000000000005_000000002_tsm_gz() {
    let mut cmd = Command::cargo_bin("delorean").unwrap();
    let assert = cmd
        .arg("meta")
        .arg("tests/fixtures/000000000000005-000000002.tsm.gz")
        .assert();

    assert_meta_000000000000005_000000002_tsm(assert);
}

/// Validates some of the metadata output content for this tsm file
fn assert_meta_cpu_usage_tsm(assert: Assert) {
    assert
        .success()
        .stdout(predicate::str::contains("TSM Metadata Report"))
        .stdout(predicate::str::contains(
            "(05b4927b3fe38000, 05b4927b3fe38001) 2735 index entries, 2735 total records",
        ))
        .stdout(predicate::str::contains(
            "task_scheduler_total_schedule_fails",
        ));
}

#[test]
fn meta_cpu_usage_tsm() {
    let input_tsm = uncompress_gz("tests/fixtures/cpu_usage.tsm.gz", ".tsm");
    let mut cmd = Command::cargo_bin("delorean").unwrap();
    let assert = cmd
        .arg("meta")
        .arg(input_tsm.to_string_lossy().to_string())
        .assert();

    assert_meta_cpu_usage_tsm(assert);
}

#[test]
fn meta_cpu_usage_tsm_gz() {
    let mut cmd = Command::cargo_bin("delorean").unwrap();
    let assert = cmd
        .arg("meta")
        .arg("tests/fixtures/cpu_usage.tsm.gz")
        .assert();

    assert_meta_cpu_usage_tsm(assert);
}
