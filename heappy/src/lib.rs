#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

mod profiler;
pub use profiler::*;

mod collector;
#[cfg(feature = "jemalloc_shim")]
mod jemalloc_shim;

// On linux you need to reference at least one symbol in a module if we want it be be actually linked.
// Otherwise the hooks like `pub unsafe extern "C" fn malloc(size: size_t) -> *mut c_void` defined in the shim
// module won't override the respective weak symbols from libc, since they don't ever get linked in the final executable.
// On macos this is not necessary, but it doesn't hurt.
// (e.g. the functions that override weak symbols exported by libc)
#[cfg(feature = "jemalloc_shim")]
pub fn dummy_force_link() {
    jemalloc_shim::dummy_force_link();
}
