//! The standard API includes: the [`malloc`], [`calloc`], [`realloc`], and
//! [`free`], which conform to to ISO/IEC 9899:1990 (“ISO C90”),
//! [`posix_memalign`] which conforms to conforms to POSIX.1-2016, and
//! [`aligned_alloc`].

use crate::profiler::Profiler;
use libc::{c_int, c_void, size_t};

#[link(name = "jemalloc")]
extern "C" {
    #[link_name = "_rjem_malloc"]
    pub fn sys_malloc(size: size_t) -> *mut c_void;

    #[link_name = "_rjem_calloc"]
    pub fn sys_calloc(number: size_t, size: size_t) -> *mut c_void;

    #[link_name = "_rjem_free"]
    pub fn sys_free(ptr: *mut c_void);

    #[link_name = "_rjem_realloc"]
    pub fn sys_realloc(ptr: *mut c_void, size: size_t) -> *mut c_void;

    #[link_name = "_rjem_malloc_usable_size"]
    pub fn sys_malloc_usable_size(ptr: *const c_void) -> size_t;

    #[link_name = "_rjem_posix_memalign"]
    pub fn sys_posix_memalign(ptr: *mut *mut c_void, alignment: size_t, size: size_t) -> c_int;

    #[cfg(target_os = "macos")]
    #[link_name = "_rjem_posix_aligned_alloc"]
    pub fn sys_aligned_alloc(alignment: size_t, size: size_t) -> *mut c_void;

    #[cfg(not(target_os = "macos"))]
    #[link_name = "_rjem_aligned_alloc"]
    pub fn sys_aligned_alloc(alignment: size_t, size: size_t) -> *mut c_void;
}

// On linux we need to reference at least one symbol in a module for it to not be pruned at link time.
pub(crate) fn dummy_force_link() {}

#[no_mangle]
pub unsafe extern "C" fn malloc(size: size_t) -> *mut c_void {
    Profiler::track_allocated(size as isize);
    sys_malloc(size)
}

#[no_mangle]
pub unsafe extern "C" fn calloc(number: size_t, size: size_t) -> *mut c_void {
    Profiler::track_allocated((size * number) as isize);
    sys_calloc(number, size)
}

#[no_mangle]
pub unsafe extern "C" fn free(ptr: *mut c_void) {
    #[cfg(feature = "measure_free")]
    {
        let size = sys_malloc_usable_size(ptr) as isize;
        Profiler::track_allocated(-size);
    }
    sys_free(ptr)
}

#[no_mangle]
pub unsafe extern "C" fn realloc(ptr: *mut c_void, size: size_t) -> *mut c_void {
    let old_size = sys_malloc_usable_size(ptr) as isize;
    Profiler::track_allocated(size as isize - old_size);
    sys_realloc(ptr, size)
}

#[no_mangle]
pub unsafe extern "C" fn malloc_usable_size(ptr: *const c_void) -> size_t {
    sys_malloc_usable_size(ptr)
}

#[no_mangle]
pub unsafe extern "C" fn posix_memalign(
    ptr: *mut *mut c_void,
    alignment: size_t,
    size: size_t,
) -> c_int {
    sys_posix_memalign(ptr, alignment, size)
}

#[no_mangle]
pub unsafe extern "C" fn aligned_alloc(alignment: size_t, size: size_t) -> *mut c_void {
    Profiler::track_allocated(size as isize); // TODO take alignment in account
    sys_aligned_alloc(alignment, size)
}
