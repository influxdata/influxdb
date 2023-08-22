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

/// Schema used with IOx specific Ingester-Querier gRPC requests
///
/// Creates:
///
/// - `influxdata.iox.ingester.v1.rs`
fn generate_grpc_types(root: &Path) -> Result<()> {
    let ingester_path_v1 = root.join("influxdata/iox/ingester/v1");
    let ingester_path_v2 = root.join("influxdata/iox/ingester/v2");

    let proto_files = vec![
        ingester_path_v1.join("query.proto"),
        ingester_path_v2.join("query.proto"),
    ];

    // Tell cargo to recompile if any of these proto files are changed
    for proto_file in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_file.display());
    }

    let mut config = prost_build::Config::new();

    config
        .compile_well_known_types()
        .disable_comments([".google"])
        .extern_path(".google.protobuf", "::pbjson_types")
        .btree_map([
            ".influxdata.iox.ingester.v1.IngesterQueryResponseMetadata.unpersisted_partitions",
        ])
        .bytes([".influxdata.iox.ingester.v2"]);

    let descriptor_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("proto_descriptor.bin");
    tonic_build::configure()
        .file_descriptor_set_path(&descriptor_path)
        // protoc in ubuntu builder needs this option
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_with_config(config, &proto_files, &[root])?;

    let descriptor_set = std::fs::read(descriptor_path)?;

    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_set)?
        .build(&[".influxdata.iox"])?;

    Ok(())
}
