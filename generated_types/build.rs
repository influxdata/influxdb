//! Compiles Protocol Buffers into native Rust types.

use std::env;
use std::path::{Path, PathBuf};

type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("protos");

    generate_grpc_types(&root)?;

    Ok(())
}

/// Schema used with IOx specific gRPC requests
///
/// Creates:
///
/// - `com.github.influxdata.idpe.storage.read.rs`
/// - `influxdata.iox.catalog.v1.rs`
/// - `influxdata.iox.management.v1.rs`
/// - `influxdata.iox.write.v1.rs`
/// - `influxdata.platform.storage.rs`
fn generate_grpc_types(root: &Path) -> Result<()> {
    let storage_path = root.join("influxdata/platform/storage");
    let idpe_path = root.join("com/github/influxdata/idpe/storage/read");
    let catalog_path = root.join("influxdata/iox/catalog/v1");
    let management_path = root.join("influxdata/iox/management/v1");
    let write_path = root.join("influxdata/iox/write/v1");

    let proto_files = vec![
        storage_path.join("test.proto"),
        storage_path.join("predicate.proto"),
        storage_path.join("storage_common.proto"),
        storage_path.join("service.proto"),
        storage_path.join("storage_common_idpe.proto"),
        idpe_path.join("source.proto"),
        catalog_path.join("catalog.proto"),
        catalog_path.join("parquet_metadata.proto"),
        catalog_path.join("predicate.proto"),
        management_path.join("database_rules.proto"),
        management_path.join("chunk.proto"),
        management_path.join("partition.proto"),
        management_path.join("service.proto"),
        management_path.join("shard.proto"),
        management_path.join("jobs.proto"),
        write_path.join("service.proto"),
        root.join("influxdata/pbdata/v1/influxdb_pb_data_protocol.proto"),
        root.join("grpc/health/v1/service.proto"),
        root.join("google/longrunning/operations.proto"),
        root.join("google/rpc/error_details.proto"),
        root.join("google/rpc/status.proto"),
    ];

    // Tell cargo to recompile if any of these proto files are changed
    for proto_file in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_file.display());
    }

    let mut config = prost_build::Config::new();

    config
        .compile_well_known_types()
        .disable_comments(&[".google"])
        // approximates jsonpb. This is still not enough to deal with the special cases like Any
        // Tracking issue for proper jsonpb support in prost: https://github.com/danburkert/prost/issues/277
        .type_attribute(
            ".",
            "#[derive(serde::Serialize,serde::Deserialize)] #[serde(rename_all = \"camelCase\")]",
        )
        .extern_path(".google.protobuf", "::google_types::protobuf")
        .bytes(&[".influxdata.iox.catalog.v1.AddParquet.metadata"]);

    let descriptor_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("proto_descriptor.bin");
    tonic_build::configure()
        .file_descriptor_set_path(&descriptor_path)
        .format(true)
        .compile_with_config(config, &proto_files, &[root.into()])?;

    Ok(())
}
