mod dstool_tests {
    use assert_cmd::Command;
    use predicates::prelude::*;

    #[test]
    fn convert_bad_input_filename() {
        let mut cmd = Command::cargo_bin("dstool").unwrap();
        let assert = cmd
            .arg("convert")
            .arg("non_existent_input")
            .arg("non_existent_output")
            .assert();

        assert
            .failure()
            .code(2)
            .stderr(predicate::str::contains("Error reading non_existent_input"))
            .stderr(predicate::str::contains("No such file or directory"));
    }

    #[test]
    fn convert_good_input_filename() {
        let mut cmd = Command::cargo_bin("dstool").unwrap();

        let parquet_path = delorean_test_helpers::tempfile::Builder::new()
            .prefix("dstool_e2e")
            .suffix(".parquet")
            .tempfile()
            .expect("error creating temp file")
            .into_temp_path();
        let parquet_filename_string = parquet_path.to_string_lossy().to_string();

        let assert = cmd
            .arg("convert")
            .arg("../tests/fixtures/lineproto/temperature.lp")
            .arg(&parquet_filename_string)
            .assert();

        let expected_success_string = format!(
            "Completing writing {} successfully",
            parquet_filename_string
        );

        assert
            .success()
            .stderr(predicate::str::contains("dstool starting"))
            .stderr(predicate::str::contains("Schema deduced"))
            .stderr(predicate::str::contains(expected_success_string));

        // TODO: add a dump command to dstool and verify that the dump
        // of the written parquet file is as expected.

        parquet_path.close().expect("deleting temporary file");
    }
}
