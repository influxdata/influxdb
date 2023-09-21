use std::{error::Error, path::PathBuf};

use prost_build::Config;

fn main() -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto");
    let proto = root.join("gossip.proto");

    println!("cargo:rerun-if-changed={}", proto.display());

    Config::new()
        .bytes(["."])
        .compile_protos(&[proto], &[root])?;

    Ok(())
}
