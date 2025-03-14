use test_helpers::assert_contains;

use super::run_and_err;

#[test_log::test(tokio::test)]
async fn test_cluster_id_node_id_cant_match() {
    let serve_args = &[
        "serve",
        "--cluster-id",
        "foo",
        "--node-id",
        "foo",
        "--object-store",
        "memory",
    ];

    let expected = "Must provide different values for the cluster-id and node-id";

    // let output = assert_cmd::Command::cargo_bin("influxdb3")
    //     .unwrap()
    //     .args(serve_args)
    //     .assert()
    //     .failure()
    //     .get_output()
    //     .stdout
    //     .clone();
    // let actual = String::from_utf8(output).unwrap();
    let actual = run_and_err(serve_args);
    assert_contains!(actual, expected);
}
