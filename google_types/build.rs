//! Compiles Protocol Buffers and FlatBuffers schema definitions into
//! native Rust types.

use std::env;
use std::path::PathBuf;

type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("protos");

    let proto_files = vec![root.join("google/protobuf/types.proto")];

    // Tell cargo to recompile if any of these proto files are changed
    for proto_file in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_file.display());
    }

    let descriptor_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("proto_descriptor.bin");
    prost_build::Config::new()
        .file_descriptor_set_path(&descriptor_path)
        .compile_well_known_types()
        .disable_comments(&["."])
        .bytes(&[".google"])
        .compile_protos(&proto_files, &[root])?;

    let descriptor_set = std::fs::read(descriptor_path)?;
    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_set)?
        .exclude([".google.protobuf.Duration", ".google.protobuf.Timestamp"])
        .build(&[".google"])?;

    Ok(())
}
