//! Compiles Protocol Buffers into native Rust types.

use std::io::Result;

fn main() -> Result<()> {
    tonic_build::compile_protos("proto/test.proto")
}
