use crate::CatalogError;
use std::fmt;
use std::fmt::Formatter;

/// Key types which may be validated by the [is_valid] function.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Type {
    Tag,
    Field,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Type::Tag => f.write_str("tag"),
            Type::Field => f.write_str("field"),
        }
    }
}

/// Validate that a key (tag or field) is valid.
/// Keys cannot be empty and cannot contain control characters (ASCII 0x00-0x1F and 0x7F).
/// This ensures compatibility with the line protocol parser.
pub(crate) fn is_valid(key_type: Type, key: impl AsRef<str>) -> crate::Result<()> {
    if key.as_ref().is_empty() {
        return Err(CatalogError::InvalidConfiguration {
            message: format!("{key_type} key cannot be empty").into(),
        });
    }

    // Check for control characters
    if key.as_ref().chars().any(char::is_control) {
        return Err(CatalogError::InvalidConfiguration {
            message: format!("{key_type} key cannot contain control characters").into(),
        });
    }

    Ok(())
}

/// Returns a function to validate keys for the specified `key_type` using the [is_valid] function.
pub(crate) fn is_valid_for<T: AsRef<str>>(key_type: Type) -> impl Fn(T) -> crate::Result<()> {
    move |key| is_valid(key_type, key)
}
