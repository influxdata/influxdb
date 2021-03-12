//! Compiles Protocol Buffers and FlatBuffers schema definitions into
//! native Rust types.

use std::{
    path::{Path, PathBuf},
    process::Command,
};

type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("protos");

    generate_grpc_types(&root)?;
    generate_wal_types(&root)?;

    Ok(())
}

/// Schema used with IOx specific gRPC requests
///
/// Creates
/// - `influxdata.platform.storage.rs`
/// - `com.github.influxdata.idpe.storage.read.rs`
/// - `influxdata.iox.management.v1.rs`
fn generate_grpc_types(root: &Path) -> Result<()> {
    let storage_path = root.join("influxdata/platform/storage");
    let idpe_path = root.join("com/github/influxdata/idpe/storage/read");
    let management_path = root.join("influxdata/iox/management/v1");
    let write_path = root.join("influxdata/iox/write/v1");

    let proto_files = vec![
        storage_path.join("test.proto"),
        storage_path.join("predicate.proto"),
        storage_path.join("storage_common.proto"),
        storage_path.join("service.proto"),
        storage_path.join("storage_common_idpe.proto"),
        idpe_path.join("source.proto"),
        management_path.join("base_types.proto"),
        management_path.join("database_rules.proto"),
        management_path.join("chunk.proto"),
        management_path.join("service.proto"),
        management_path.join("jobs.proto"),
        write_path.join("service.proto"),
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
        .extern_path(".google.protobuf", "::google_types::protobuf");

    tonic_build::configure().compile_with_config(config, &proto_files, &[root.into()])?;

    Ok(())
}

/// Schema used in the WAL
///
/// Creates `wal_generated.rs`
fn generate_wal_types(root: &Path) -> Result<()> {
    let wal_file = root.join("wal.fbs");

    println!("cargo:rerun-if-changed={}", wal_file.display());
    let out_dir: PathBuf = std::env::var_os("OUT_DIR")
        .expect("Could not determine `OUT_DIR`")
        .into();

    let status = Command::new("flatc")
        .arg("--rust")
        .arg("-o")
        .arg(&out_dir)
        .arg(wal_file)
        .status();

    match status {
        Ok(status) if !status.success() => panic!("`flatc` failed to compile the .fbs to Rust"),
        Ok(_status) => {} // Successfully compiled
        Err(err) => panic!("Could not execute `flatc`: {}", err),
    }

    Ok(())
}
