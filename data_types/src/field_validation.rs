//! A collection of extension traits for types that
//! implement TryInto<U, Error=FieldViolation>
//!
//! Allows associating field context with the generated errors
//! as they propagate up the struct topology

use generated_types::google::FieldViolation;
use std::convert::TryInto;

/// An extension trait that adds the method `scope` to any type
/// implementing `TryInto<U, Error = FieldViolation>`
pub(crate) trait FromField<T> {
    fn scope(self, field: impl Into<String>) -> Result<T, FieldViolation>;
}

impl<T, U> FromField<U> for T
where
    T: TryInto<U, Error = FieldViolation>,
{
    /// Try to convert type using TryInto calling `FieldViolation::scope`
    /// on any returned error
    fn scope(self, field: impl Into<String>) -> Result<U, FieldViolation> {
        self.try_into().map_err(|e| e.scope(field))
    }
}

/// An extension trait that adds the methods `optional` and `required` to any
/// Option containing a type implementing `TryInto<U, Error = FieldViolation>`
pub(crate) trait FromFieldOpt<T> {
    /// Try to convert inner type, if any, using TryInto calling
    /// `FieldViolation::scope` on any error encountered
    ///
    /// Returns None if empty
    fn optional(self, field: impl Into<String>) -> Result<Option<T>, FieldViolation>;

    /// Try to convert inner type, using TryInto calling `FieldViolation::scope`
    /// on any error encountered
    ///
    /// Returns an error if empty
    fn required(self, field: impl Into<String>) -> Result<T, FieldViolation>;
}

impl<T, U> FromFieldOpt<U> for Option<T>
where
    T: TryInto<U, Error = FieldViolation>,
{
    fn optional(self, field: impl Into<String>) -> Result<Option<U>, FieldViolation> {
        self.map(|t| t.scope(field)).transpose()
    }

    fn required(self, field: impl Into<String>) -> Result<U, FieldViolation> {
        match self {
            None => Err(FieldViolation::required(field)),
            Some(t) => t.scope(field),
        }
    }
}

/// An extension trait that adds the methods `optional` and `required` to any
/// String
///
/// Prost will default string fields to empty, whereas IOx sometimes
/// uses Option<String>, this helper aids mapping between them
///
/// TODO: Review mixed use of Option<String> and String in IOX
pub(crate) trait FromFieldString {
    /// Returns a Ok if the String is not empty
    fn required(self, field: impl Into<String>) -> Result<String, FieldViolation>;

    /// Wraps non-empty strings in Some(_), returns None for empty strings
    fn optional(self) -> Option<String>;
}

impl FromFieldString for String {
    fn required(self, field: impl Into<String>) -> Result<String, FieldViolation> {
        if self.is_empty() {
            return Err(FieldViolation::required(field));
        }
        Ok(self)
    }

    fn optional(self) -> Option<String> {
        if self.is_empty() {
            return None;
        }
        Some(self)
    }
}

/// An extension trait that adds the method `vec_field` to any Vec of a type
/// implementing `TryInto<U, Error = FieldViolation>`
pub(crate) trait FromFieldVec<T> {
    /// Converts to a `Vec<U>`, short-circuiting on the first error and
    /// returning a correctly scoped `FieldViolation` for where the error
    /// was encountered
    fn vec_field(self, field: impl Into<String>) -> Result<T, FieldViolation>;
}

impl<T, U> FromFieldVec<Vec<U>> for Vec<T>
where
    T: TryInto<U, Error = FieldViolation>,
{
    fn vec_field(self, field: impl Into<String>) -> Result<Vec<U>, FieldViolation> {
        let res: Result<_, _> = self
            .into_iter()
            .enumerate()
            .map(|(i, t)| t.scope(i.to_string()))
            .collect();

        res.map_err(|e| e.scope(field))
    }
}
