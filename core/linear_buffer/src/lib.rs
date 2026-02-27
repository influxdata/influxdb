//! Crate that implements [`LinearBuffer`].
mod allocation;
mod extend;
mod linear_buffer;

// Workaround for "unused crate" lint false positives.
// This is only done if we do NOT run under MIRI to avoid costlly compliation of a lot of unused dependencies.
#[cfg(not(miri))]
use workspace_hack as _;

pub use extend::LinearBufferExtend;
pub use linear_buffer::{LinearBuffer, Slice};
