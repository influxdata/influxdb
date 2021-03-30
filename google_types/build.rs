//! Compiles Protocol Buffers and FlatBuffers schema definitions into
//! native Rust types.

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

    prost_build::Config::new()
        .compile_well_known_types()
        .disable_comments(&["."])
        // approximates jsonpb. This is still not enough to deal with the special cases like Any.
        .type_attribute(
            ".google",
            "#[derive(serde::Serialize,serde::Deserialize)] #[serde(rename_all = \"camelCase\")]",
        )
        .bytes(&[".google"])
        .compile_protos(&proto_files, &[root])?;

    Ok(())
}
