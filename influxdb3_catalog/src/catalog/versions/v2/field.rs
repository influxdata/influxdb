//! Field name parsing and representation.
//!
//! Use either [parse_field_name_auto] or [parse_field_name_aware] to parse
use std::fmt;
use std::fmt::Display;

pub(crate) const FIELD_FAMILY_DELIMITER: &str = "::";

#[derive(Debug, PartialOrd, PartialEq)]
pub(super) enum FieldName<'a> {
    /// An unqualified field only contains a field name.
    Unqualified(&'a str),
    /// A qualified field includes the field family and field name.
    Qualified(&'a str, &'a str),
}

impl Display for FieldName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldName::Unqualified(name) => f.write_str(name),
            FieldName::Qualified(qualifier, name) => {
                f.write_str(qualifier)?;
                f.write_str(FIELD_FAMILY_DELIMITER)?;
                f.write_str(name)
            }
        }
    }
}

/// Parses a field name without any awareness of field families.
///
/// This function will always return [FieldName::Unqualified].
pub(super) fn parse_field_name_auto(s: &str) -> FieldName<'_> {
    FieldName::Unqualified(s)
}

/// Parses a field name with awareness of field families.
pub(super) fn parse_field_name_aware(s: &str) -> FieldName<'_> {
    match s.find(FIELD_FAMILY_DELIMITER) {
        Some(pos) => {
            let (qualifier, name) = s.split_at(pos);
            let name = &name[FIELD_FAMILY_DELIMITER.len()..];
            // If there's no field name after the delimiter, treat as unqualified
            if name.is_empty() || qualifier.is_empty() {
                FieldName::Unqualified(s)
            } else {
                FieldName::Qualified(qualifier, name)
            }
        }
        None => FieldName::Unqualified(s),
    }
}

#[cfg(test)]
mod tests;
