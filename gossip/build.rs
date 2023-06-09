use std::{error::Error, path::PathBuf};

use prost_build::Config;

fn main() -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto");

    Config::new()
        .bytes(["."])
        .compile_protos(&[root.join("gossip.proto")], &[root])?;

    Ok(())
}
