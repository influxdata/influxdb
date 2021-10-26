use std::env;
use std::path::{Path, PathBuf};

type Result<T> = std::io::Result<T>;

fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("protos");

    generate_grpc_types(&root)
}

fn generate_grpc_types(root: &Path) -> Result<()> {
    let proto_files = vec![root.join("test.proto")];

    // Tell cargo to recompile if any of these proto files are changed
    for proto_file in &proto_files {
        println!("cargo:rerun-if-changed={}", proto_file.display());
    }
    let mut config = prost_build::Config::new();

    config.disable_comments(&[".google"]);

    tonic_build::configure()
        .format(true)
        .compile_with_config(config, &proto_files, &[root.into()])
}
