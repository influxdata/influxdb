//! Crate that implements [`LinearBuffer`].
mod allocation;
mod extend;
mod linear_buffer;

pub use extend::LinearBufferExtend;
pub use linear_buffer::{LinearBuffer, Slice};
