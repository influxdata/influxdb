use std::{path::PathBuf, process::Command};

type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

fn main() -> Result<()> {
    // Protobuf schema used with gRPC requests
    tonic_build::compile_protos("proto/delorean/delorean.proto")?;

    // Flatbuffers schema used in the WAL
    println!("cargo:rerun-if-changed=proto/delorean/wal.fbs");
    let out_dir: PathBuf = std::env::var_os("OUT_DIR")
        .expect("Could not determine `OUT_DIR`")
        .into();

    let status = Command::new("flatc")
        .arg("--rust")
        .arg("-o")
        .arg(&out_dir)
        .arg("proto/delorean/wal.fbs")
        .status();

    match status {
        Ok(status) if !status.success() => panic!("`flatc` failed to compile the .fbs to Rust"),
        Ok(_status) => {} // Successfully compiled
        Err(err) => panic!("Could not execute `flatc`: {}", err),
    }

    Ok(())
}
