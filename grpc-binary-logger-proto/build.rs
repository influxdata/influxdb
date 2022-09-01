//! Compiles Protocol Buffers into native Rust types.
//! <https://github.com/grpc/grpc/blob/master/doc/binary-logging.md>

use std::io::Result;

fn main() -> Result<()> {
    tonic_build::compile_protos("proto/grpc/binlog/v1/binarylog.proto")
}
