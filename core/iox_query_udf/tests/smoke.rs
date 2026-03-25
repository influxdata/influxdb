#![expect(unused_crate_dependencies)]

use tokio::runtime::Handle;

// If this panics, this likely means that the version of `datafusion_udf_wasm_host` (pinned on the workspace-level
// `Cargo.toml`) and the prebuilt binaries (pinned in `iox_query_udf/build.rs`) are out of sync. They cannot work
// with each other because the host-guest interface is not compatible.
//
// To fix this, adjust the pinned prebuilt binaries in `iox_query_udf/build.rs`, ideally to the same GIT SHA that
// you use for `datafusion_udf_wasm`. If there is no matching release available under
// https://github.com/influxdata/datafusion-udf-wasm/releases , follow these instructions to create one:
//
//   https://github.com/influxdata/datafusion-udf-wasm/blob/main/CONTRIBUTING.md#triggering-a-build
#[tokio::test]
async fn test_load() {
    let parser = iox_query_udf::udf_parser();

    let query = r#"
CREATE FUNCTION add_one()
LANGUAGE python
AS '
def add_one(x: int) -> int:
    return x + 1
';

SELECT add_one(1);
"#;

    parser
        .parse(
            query,
            &Default::default(),
            Handle::current(),
            &Default::default(),
        )
        .await
        .unwrap();
}
