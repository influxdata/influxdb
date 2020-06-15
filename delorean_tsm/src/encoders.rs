pub mod float;
pub mod integer;
mod simple8b;
pub mod string;
pub mod timestamp;

/// Max number of bytes needed to store a varint-encoded 32-bit integer.
const MAX_VAR_INT_32: usize = 5;

/// Max number of bytes needed to store a varint-encoded 64-bit integer.
const MAX_VAR_INT_64: usize = 10;
