mod all_in_one;
// CLI errors when run with heappy (only works via `cargo run`):
// loading shared libraries: libjemalloc.so.2: cannot open shared object file: No such file or directory"
mod catalog;
#[cfg(not(feature = "heappy"))]
mod cli;
mod command_rename_support;
mod compactor;
mod debug;
mod error;
mod flightsql;
mod influxql;
mod ingester;
mod logging;
mod metrics;
mod namespace;
mod querier;
mod remote;
mod router;
mod schema;
mod table;
mod tracing;

/// extracts the parquet filename from JSON that looks like
/// ```text
/// {
///    "id": "1",
///    ...
//     "objectStoreId": "fa6cdcd1-cbc2-4fb7-8b51-4773079124dd",
///    ...
/// }
/// ```
fn get_object_store_id(output: &[u8]) -> String {
    let v: serde_json::Value = serde_json::from_slice(output).unwrap();
    // We only process the first value, so panic if it isn't there
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    let id = arr[0]
        .as_object()
        .unwrap()
        .get("objectStoreId")
        .unwrap()
        .as_str()
        .unwrap();

    id.to_string()
}
