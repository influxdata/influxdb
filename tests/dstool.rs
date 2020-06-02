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
        let assert = cmd
            .arg("convert")
            .arg("tests/fixtures/lineproto/temperature.txt")
            .arg("/tmp/out.parquet")
            .assert();

        assert
            .failure()
            .code(101)
            .stderr(predicate::str::contains("dstool starting"))
            .stderr(predicate::str::contains(
                "not implemented: The actual conversion",
            ));
    }
}
