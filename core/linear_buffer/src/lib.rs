//! Crate that implements [`LinearBuffer`].
mod allocation;
mod extend;
mod linear_buffer;

// This is only done if we do NOT run under MIRI to avoid costlly compliation of a lot of unused dependencies.
#[cfg(not(miri))]
pub use extend::LinearBufferExtend;
pub use linear_buffer::{LinearBuffer, Slice};
