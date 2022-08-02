use super::{parse_lines, Result};
use std::ffi::{CStr, CString};
use std::panic::catch_unwind;

/// Validates a line protocol batch.
/// Returns NULL on success, otherwise it returns an error message.
///
/// # Safety
///
/// The caller must free the error message with `free_error`.
#[no_mangle]
pub unsafe extern "C" fn validate_lines(lp: *const libc::c_char) -> *const libc::c_char {
    catch_unwind(|| {
        let bytes = CStr::from_ptr(lp).to_bytes();
        let str = String::from_utf8(bytes.to_vec()).unwrap();
        let lines = parse_lines(&str).into_iter().collect::<Result<Vec<_>>>();
        match lines {
            Ok(_) => std::ptr::null(),
            Err(e) => CString::new(e.to_string()).unwrap().into_raw() as *const _,
        }
    })
    .unwrap_or_else(|_| CString::new("panic").unwrap().into_raw() as *const _)
}

/// Free an error message returned by `validate_lines`
///
/// # Safety
///
/// The string must be returned by [`validate_lines`].
#[no_mangle]
pub unsafe extern "C" fn free_error(error: *mut libc::c_char) {
    let _ = CString::from_raw(error);
}
