//! Common
//!
//! Collection of helper functions

/// Serialize to application/x-www-form-urlencoded syntax
pub fn urlencode<T: AsRef<str>>(s: T) -> String {
    ::url::form_urlencoded::byte_serialize(s.as_ref().as_bytes()).collect()
}
