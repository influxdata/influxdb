//! Partition templating with per-namespace & table override types.
//!
//! The override types utilise per-entity wrappers for type safety, ensuring a
//! namespace override is not used in a table (and vice versa), as well as to
//! ensure the correct chain of inheritance is adhered to at compile time.
//!
//! A partitioning template is resolved by evaluating the following (in order of
//! precedence):
//!
//!    1. Table name override, if specified.
//!    2. Namespace name override, if specified.
//!    3. Default partitioning scheme (YYYY-MM-DD)
//!
//! Each of the [`NamespacePartitionTemplateOverride`] and
//! [`TablePartitionTemplateOverride`] stores only the override, if provided,
//! and implicitly resolves to the default partitioning scheme if no override is
//! specified (indicated by the presence of [`Option::None`] in the wrapper).
//!
//! ## Default Partition Key
//!
//! The default partition key format is specified by [`PARTITION_BY_DAY_PROTO`],
//! with a template consisting of a single part: a YYYY-MM-DD representation of
//! the time row timestamp.
//!
//! ## Partition Key Format
//!
//! Should a partition template be used that generates a partition key
//! containing more than one part, those parts are delimited by the `|`
//! character ([`PARTITION_KEY_DELIMITER`]), chosen to be an unusual character
//! that is unlikely to occur in user-provided column values in order to
//! minimise the need to encode the value in the common case, while still
//! providing legible / printable keys. Should the user-provided column value
//! contain the `|` key, it is [percent encoded] (in addition to `!` below, and
//! the `%` character itself) to prevent ambiguity.
//!
//! It is an invariant that the resulting partition key derived from a given
//! template has the same number and ordering of parts.
//!
//! If the partition key template references a [`TemplatePart::TagValue`] column
//! that is not present in the row, a single `!` is inserted, indicating a NULL
//! template key part. If the value of the part is an empty string (""), a `^`
//! is inserted to ensure a non-empty partition key is always generated. Like
//! the `|` key above, any occurrence of these characters in a user-provided
//! column value is percent encoded.
//!
//! Because this serialisation format can be unambiguously reversed, the
//! [`build_column_values()`] function can be used to obtain the set of
//! [`TemplatePart::TagValue`] the key was constructed from.
//!
//! ### Value Truncation
//!
//! Partition key parts are limited to, at most, 200 bytes in length
//! ([`PARTITION_KEY_MAX_PART_LEN`]). If any single partition key part exceeds
//! this length limit, it is truncated and the truncation marker `#`
//! ([`PARTITION_KEY_PART_TRUNCATED`]) is appended.
//!
//! When rebuilding column values using [`build_column_values()`], a truncated
//! key part yields [`ColumnValue::Prefix`], which can only be used for prefix
//! matching - equality matching against a string always returns false.
//!
//! Two considerations must be made when truncating the generated key:
//!
//!  * The string may contain encoded sequences in the form %XX, and the string
//!    should not be split within an encoded sequence, or decoding the string
//!    will fail.
//!
//!  * This may be a unicode string - what the user might consider a "character"
//!    may in fact be multiple unicode code-points, each of which may span
//!    multiple bytes.
//!
//! Slicing a unicode code-point in half may lead to an invalid UTF-8 string,
//! which will prevent it from being used in Rust (and likely many other
//! languages/systems). Because partition keys are represented as strings and
//! not bytes, splitting a code-point in half MUST be avoided.
//!
//! Further to this, a sequence of multiple code-points can represent a single
//! "character" - this is called a grapheme. For example, the representation of
//! the Tamil "ni" character "நி" is composed of two multi-byte code-points; the
//! Tamil letter "na" which renders as "ந" and the vowel sign "ி", each 3 bytes
//! long. If split after the first 3 bytes, the compound "ni" character will be
//! incorrectly rendered as the single "na"/"ந" character.
//!
//! Depending on what the consumer of the split string considers a character,
//! prefix/equality matching may produce differing results if a grapheme is
//! split. If the caller performs a byte-wise comparison, everything is fine -
//! if they perform a "character" comparison, then the equality may be lost
//! depending on what they consider a character.
//!
//! Therefore this implementation takes the conservative approach of never
//! splitting code-points (for UTF-8 correctness) nor graphemes for simplicity
//! and compatibility for the consumer. This may be relaxed in the future to
//! allow splitting graphemes, but by being conservative we give ourselves this
//! option - we can't easily do the reverse!
//!
//! ## Part Limit & Maximum Key Size
//!
//! The number of parts in a partition template is limited to 8
//! ([`MAXIMUM_NUMBER_OF_TEMPLATE_PARTS`]), validated at creation time.
//!
//! Together with the above value truncation, this bounds the maximum length of
//! a partition key to 1,607 bytes (1.57 KiB).
//!
//! ### Reserved Characters
//!
//! Reserved characters that are percent encoded (in addition to non-ASCII
//! characters), and their meaning:
//!
//!   * `|` - partition key part delimiter ([`PARTITION_KEY_DELIMITER`])
//!   * `!` - NULL/missing partition key part ([`PARTITION_KEY_VALUE_NULL`])
//!   * `^` - empty string partition key part ([`PARTITION_KEY_VALUE_EMPTY`])
//!   * `#` - key part truncation marker ([`PARTITION_KEY_PART_TRUNCATED`])
//!   * `%` - required for unambiguous reversal of percent encoding
//!
//! These characters are defined in [`ENCODED_PARTITION_KEY_CHARS`] and chosen
//! due to their low likelihood of occurrence in user-provided column values.
//!
//! ### Reserved Tag Values
//!
//! Reserved tag values that cannot be used:
//!
//!   * `time` - The time column has special meaning and is covered by strftime
//!              formatters ([`TAG_VALUE_KEY_TIME`])
//!
//! ### Examples
//!
//! When using the partition template below:
//!
//! ```text
//!      [
//!          TemplatePart::TimeFormat("%Y"),
//!          TemplatePart::TagValue("a"),
//!          TemplatePart::TagValue("b")
//!      ]
//! ```
//!
//! The following partition keys are derived:
//!
//!   * `time=2023-01-01, a=bananas, b=plátanos`   -> `2023|bananas|plátanos`
//!   * `time=2023-01-01, b=plátanos`              -> `2023|!|plátanos`
//!   * `time=2023-01-01, another=cat, b=plátanos` -> `2023|!|plátanos`
//!   * `time=2023-01-01`                          -> `2023|!|!`
//!   * `time=2023-01-01, a=cat|dog, b=!`          -> `2023|cat%7Cdog|%21`
//!   * `time=2023-01-01, a=%50`                   -> `2023|%2550|!`
//!   * `time=2023-01-01, a=`                      -> `2023|^|!`
//!   * `time=2023-01-01, a=<long string>`         -> `2023|<long string>#|!`
//!
//! When using the default partitioning template (YYYY-MM-DD) there is no
//! encoding necessary, as the derived partition key contains a single part, and
//! no reserved characters.
//!
//! [percent encoded]: https://url.spec.whatwg.org/#percent-encoded-bytes

use generated_types::influxdata::iox::partition_template::v1 as proto;
use once_cell::sync::Lazy;
use percent_encoding::{percent_decode_str, AsciiSet, CONTROLS};
use std::{borrow::Cow, sync::Arc};
use thiserror::Error;

/// Reasons a user-specified partition template isn't valid.
#[derive(Debug, Error)]
#[allow(missing_copy_implementations)]
pub enum ValidationError {
    /// The partition template didn't define any parts.
    #[error("Custom partition template must have at least one part")]
    NoParts,

    /// The partition template exceeded the maximum allowed number of parts.
    #[error(
        "Custom partition template specified {specified} parts. \
        Partition templates may have a maximum of {MAXIMUM_NUMBER_OF_TEMPLATE_PARTS} parts."
    )]
    TooManyParts {
        /// The number of template parts that were present in the user-provided custom partition
        /// template.
        specified: usize,
    },

    /// The partition template defines a [`TimeFormat`] part, but the
    /// provided strftime formatter is invalid.
    ///
    /// [`TimeFormat`]: [`proto::template_part::Part::TimeFormat`]
    #[error("invalid strftime format in partition template: {0}")]
    InvalidStrftime(String),

    /// The partition template defines a [`TagValue`] part, but the provided
    /// value is invalid.
    ///
    /// [`TagValue`]: [`proto::template_part::Part::TagValue`]
    #[error("invalid tag value in partition template: {0}")]
    InvalidTagValue(String),
}

/// The maximum number of template parts a custom partition template may specify, to limit the
/// amount of space in the catalog used by the custom partition template and the partition keys
/// created with it.
pub const MAXIMUM_NUMBER_OF_TEMPLATE_PARTS: usize = 8;

/// The sentinel character used to delimit partition key parts in the partition
/// key string.
pub const PARTITION_KEY_DELIMITER: char = '|';

/// The sentinel character used to indicate an empty string partition key part
/// in the partition key string.
pub const PARTITION_KEY_VALUE_EMPTY: char = '^';

/// The `str` form of the [`PARTITION_KEY_VALUE_EMPTY`] character.
pub const PARTITION_KEY_VALUE_EMPTY_STR: &str = "^";

/// The sentinel character used to indicate a missing partition key part in the
/// partition key string.
pub const PARTITION_KEY_VALUE_NULL: char = '!';

/// The `str` form of the [`PARTITION_KEY_VALUE_NULL`] character.
pub const PARTITION_KEY_VALUE_NULL_STR: &str = "!";

/// The maximum permissible length of a partition key part, after encoding
/// reserved & non-ASCII characters.
pub const PARTITION_KEY_MAX_PART_LEN: usize = 200;

/// The truncation sentinel character, used to explicitly identify a partition
/// key as having been truncated.
///
/// Truncated partition key parts can only be used for prefix matching, and
/// yield a [`ColumnValue::Prefix`] from [`build_column_values()`].
pub const PARTITION_KEY_PART_TRUNCATED: char = '#';

/// The reserved tag value key for the `time` column, which is reserved as
/// a specifically formatted column for the time associated with any given
/// data point.
pub const TAG_VALUE_KEY_TIME: &str = "time";

/// The minimal set of characters that must be encoded during partition key
/// generation when they form part of a partition key part, in order to be
/// unambiguously reversible.
///
/// See module-level documentation & [`build_column_values()`].
pub const ENCODED_PARTITION_KEY_CHARS: AsciiSet = CONTROLS
    .add(PARTITION_KEY_DELIMITER as u8)
    .add(PARTITION_KEY_VALUE_NULL as u8)
    .add(PARTITION_KEY_VALUE_EMPTY as u8)
    .add(PARTITION_KEY_PART_TRUNCATED as u8)
    .add(b'%'); // Required for reversible unambiguous encoding

/// Allocationless and protobufless access to the parts of a template needed to
/// actually do partitioning.
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum TemplatePart<'a> {
    TagValue(&'a str),
    TimeFormat(&'a str),
}

/// The default partitioning scheme is by each day according to the "time" column.
pub static PARTITION_BY_DAY_PROTO: Lazy<Arc<proto::PartitionTemplate>> = Lazy::new(|| {
    Arc::new(proto::PartitionTemplate {
        parts: vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TimeFormat(
                "%Y-%m-%d".to_owned(),
            )),
        }],
    })
});

/// A partition template specified by a namespace record.
///
/// Internally this type is [`None`] when no namespace-level override is
/// specified, resulting in the default being used.
#[derive(Debug, PartialEq, Clone, Default, sqlx::Type, Hash)]
#[sqlx(transparent, no_pg_array)]
pub struct NamespacePartitionTemplateOverride(Option<serialization::Wrapper>);

impl NamespacePartitionTemplateOverride {
    /// A const "default" impl for testing.
    pub const fn const_default() -> Self {
        Self(None)
    }

    /// Return the protobuf representation of this template.
    pub fn as_proto(&self) -> Option<&proto::PartitionTemplate> {
        self.0.as_ref().map(|v| v.inner())
    }
}

impl TryFrom<proto::PartitionTemplate> for NamespacePartitionTemplateOverride {
    type Error = ValidationError;

    fn try_from(partition_template: proto::PartitionTemplate) -> Result<Self, Self::Error> {
        Ok(Self(Some(serialization::Wrapper::try_from(
            partition_template,
        )?)))
    }
}

/// A partition template specified by a table record.
#[derive(Debug, PartialEq, Eq, Clone, Default, sqlx::Type, Hash)]
#[sqlx(transparent, no_pg_array)]
pub struct TablePartitionTemplateOverride(Option<serialization::Wrapper>);

impl TablePartitionTemplateOverride {
    /// When a table is being explicitly created, the creation request might have contained a
    /// custom partition template for that table. If the custom partition template is present, use
    /// it. Otherwise, use the namespace's partition template.
    ///
    /// # Errors
    ///
    /// This function will return an error if the custom partition template specified is invalid.
    pub fn try_new(
        custom_table_template: Option<proto::PartitionTemplate>,
        namespace_template: &NamespacePartitionTemplateOverride,
    ) -> Result<Self, ValidationError> {
        match (custom_table_template, namespace_template.0.as_ref()) {
            (Some(table_proto), _) => {
                Ok(Self(Some(serialization::Wrapper::try_from(table_proto)?)))
            }
            (None, Some(namespace_serialization_wrapper)) => {
                Ok(Self(Some(namespace_serialization_wrapper.clone())))
            }
            (None, None) => Ok(Self(None)),
        }
    }

    /// Returns the number of parts in this template.
    #[allow(clippy::len_without_is_empty)] // Senseless - there must always be >0 parts.
    pub fn len(&self) -> usize {
        self.parts().count()
    }

    /// Iterate through the protobuf parts and lend out what the `mutable_batch` crate needs to
    /// build `PartitionKey`s. If this table doesn't have a custom template, use the application
    /// default of partitioning by day.
    pub fn parts(&self) -> impl Iterator<Item = TemplatePart<'_>> {
        self.0
            .as_ref()
            .map(|serialization_wrapper| serialization_wrapper.inner())
            .unwrap_or_else(|| &PARTITION_BY_DAY_PROTO)
            .parts
            .iter()
            .flat_map(|part| part.part.as_ref())
            .map(|part| match part {
                proto::template_part::Part::TagValue(value) => TemplatePart::TagValue(value),
                proto::template_part::Part::TimeFormat(fmt) => TemplatePart::TimeFormat(fmt),
            })
    }

    /// Size in bytes, including `self`.
    ///
    /// This accounts for the entire allocation of this object, even when it shared (via an internal [`Arc`]).
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .0
                .as_ref()
                .map(|wrapper| {
                    let inner = wrapper.inner();

                    // inner is wrapped into an Arc, so we need to account for that allocation
                    std::mem::size_of::<proto::PartitionTemplate>()
                        + (inner.parts.capacity() * std::mem::size_of::<proto::TemplatePart>())
                        + inner
                            .parts
                            .iter()
                            .map(|part| {
                                part.part
                                    .as_ref()
                                    .map(|part| match part {
                                        proto::template_part::Part::TagValue(s) => s.capacity(),
                                        proto::template_part::Part::TimeFormat(s) => s.capacity(),
                                    })
                                    .unwrap_or_default()
                            })
                            .sum::<usize>()
                })
                .unwrap_or_default()
    }

    /// Return the protobuf representation of this template.
    pub fn as_proto(&self) -> Option<&proto::PartitionTemplate> {
        self.0.as_ref().map(|v| v.inner())
    }
}

/// This manages the serialization/deserialization of the `proto::PartitionTemplate` type to and
/// from the database through `sqlx` for the `NamespacePartitionTemplateOverride` and
/// `TablePartitionTemplateOverride` types. It's an internal implementation detail to minimize code
/// duplication.
mod serialization {
    use super::{ValidationError, MAXIMUM_NUMBER_OF_TEMPLATE_PARTS, TAG_VALUE_KEY_TIME};
    use chrono::{format::StrftimeItems, Utc};
    use generated_types::influxdata::iox::partition_template::v1 as proto;
    use std::{fmt::Write, sync::Arc};

    #[derive(Debug, Clone, PartialEq, Hash)]
    pub struct Wrapper(Arc<proto::PartitionTemplate>);

    impl Wrapper {
        /// Read access to the inner proto
        pub fn inner(&self) -> &proto::PartitionTemplate {
            &self.0
        }

        /// THIS IS FOR TESTING PURPOSES ONLY AND SHOULD NOT BE USED IN PRODUCTION CODE.
        ///
        /// The application shouldn't be putting invalid templates into the database because all
        /// creation of `Wrapper`s should be going through the
        /// `TryFrom::try_from<proto::PartitionTemplate>` constructor that rejects invalid
        /// templates. However, that leaves the possibility of the database getting an invalid
        /// template through some other means, and we want to be able to construct those easily in
        /// tests to make sure code using partition templates can handle the unlikely possibility
        /// of an invalid template in the database.
        pub(super) fn for_testing_possibility_of_invalid_value_in_database(
            proto: proto::PartitionTemplate,
        ) -> Self {
            Self(Arc::new(proto))
        }
    }

    // protobuf types normally don't implement `Eq`, but for this concrete type this is OK
    impl Eq for Wrapper {}

    impl TryFrom<proto::PartitionTemplate> for Wrapper {
        type Error = ValidationError;

        fn try_from(partition_template: proto::PartitionTemplate) -> Result<Self, Self::Error> {
            // There must be at least one part.
            if partition_template.parts.is_empty() {
                return Err(ValidationError::NoParts);
            }

            // There may not be more than `MAXIMUM_NUMBER_OF_TEMPLATE_PARTS` parts.
            let specified = partition_template.parts.len();
            if specified > MAXIMUM_NUMBER_OF_TEMPLATE_PARTS {
                return Err(ValidationError::TooManyParts { specified });
            }

            // All time formats must be valid and tag values may not specify any
            // restricted values.
            for part in &partition_template.parts {
                match &part.part {
                    Some(proto::template_part::Part::TimeFormat(fmt)) => {
                        // Empty is not a valid time format
                        if fmt.is_empty() {
                            return Err(ValidationError::InvalidStrftime(fmt.into()));
                        }

                        // Chrono will panic during timestamp formatting if this
                        // formatter directive is used!
                        //
                        // An upper-case Z does not trigger the panic code path so
                        // is not checked for.
                        if fmt.contains("%#z") {
                            return Err(ValidationError::InvalidStrftime(
                                "%#z cannot be used".to_string(),
                            ));
                        }

                        // Currently we can only tell whether a nonempty format is valid by trying
                        // to use it. See <https://github.com/chronotope/chrono/issues/47>
                        let mut dev_null = String::new();
                        write!(
                            dev_null,
                            "{}",
                            Utc::now().format_with_items(StrftimeItems::new(fmt))
                        )
                        .map_err(|_| ValidationError::InvalidStrftime(fmt.into()))?
                    }
                    Some(proto::template_part::Part::TagValue(value)) => {
                        // Empty is not a valid tag value
                        if value.is_empty() {
                            return Err(ValidationError::InvalidTagValue(value.into()));
                        }

                        if value.contains(TAG_VALUE_KEY_TIME) {
                            return Err(ValidationError::InvalidTagValue(format!(
                                "{TAG_VALUE_KEY_TIME} cannot be used"
                            )));
                        }
                    }
                    None => {}
                }
            }

            Ok(Self(Arc::new(partition_template)))
        }
    }

    impl<DB> sqlx::Type<DB> for Wrapper
    where
        sqlx::types::Json<Self>: sqlx::Type<DB>,
        DB: sqlx::Database,
    {
        fn type_info() -> DB::TypeInfo {
            <sqlx::types::Json<Self> as sqlx::Type<DB>>::type_info()
        }
    }

    impl<'q, DB> sqlx::Encode<'q, DB> for Wrapper
    where
        DB: sqlx::Database,
        for<'b> sqlx::types::Json<&'b proto::PartitionTemplate>: sqlx::Encode<'q, DB>,
    {
        fn encode_by_ref(
            &self,
            buf: &mut <DB as sqlx::database::HasArguments<'q>>::ArgumentBuffer,
        ) -> sqlx::encode::IsNull {
            <sqlx::types::Json<&proto::PartitionTemplate> as sqlx::Encode<'_, DB>>::encode_by_ref(
                &sqlx::types::Json(&self.0),
                buf,
            )
        }
    }

    impl<'q, DB> sqlx::Decode<'q, DB> for Wrapper
    where
        DB: sqlx::Database,
        sqlx::types::Json<proto::PartitionTemplate>: sqlx::Decode<'q, DB>,
    {
        fn decode(
            value: <DB as sqlx::database::HasValueRef<'q>>::ValueRef,
        ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
            Ok(Self(
                <sqlx::types::Json<proto::PartitionTemplate> as sqlx::Decode<'_, DB>>::decode(
                    value,
                )?
                .0
                .into(),
            ))
        }
    }
}

/// The value of a column, reversed from a partition key.
///
/// See [`build_column_values()`].
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue<'a> {
    /// The inner value is the exact, unmodified input column value.
    Identity(Cow<'a, str>),

    /// The inner value is a variable length prefix of the input column value.
    ///
    /// The string value is always guaranteed to be valid UTF-8.
    ///
    /// Attempting to equality match this variant against a string will always
    /// be false - use [`ColumnValue::is_prefix_match_of()`] to prefix match
    /// instead.
    Prefix(Cow<'a, str>),
}

impl<'a> ColumnValue<'a> {
    /// Returns true if `other` is a byte-wise prefix match of `self`.
    ///
    /// This method can be called for both [`ColumnValue::Identity`] and
    /// [`ColumnValue::Prefix`].
    pub fn is_prefix_match_of<T>(&self, other: T) -> bool
    where
        T: AsRef<[u8]>,
    {
        let this = match self {
            ColumnValue::Identity(v) => v.as_bytes(),
            ColumnValue::Prefix(v) => v.as_bytes(),
        };

        other.as_ref().starts_with(this)
    }
}

impl<'a, T> PartialEq<T> for ColumnValue<'a>
where
    T: AsRef<str>,
{
    fn eq(&self, other: &T) -> bool {
        match self {
            ColumnValue::Identity(v) => other.as_ref().eq(v.as_ref()),
            ColumnValue::Prefix(_) => false,
        }
    }
}

/// Reverse a `partition_key` generated from the given partition key `template`,
/// reconstructing the set of tag values in the form of `(column name, column
/// value)` tuples that the `partition_key` was generated from.
///
/// The `partition_key` MUST have been generated by `template`.
///
/// Values are returned as a [`Cow`], avoiding the need for value copying if
/// they do not need decoding. See module docs for encoding/decoding.
///
/// # Panics
///
/// This method panics if a column value is not valid UTF8 after decoding.
pub fn build_column_values<'a>(
    template: &'a TablePartitionTemplateOverride,
    partition_key: &'a str,
) -> impl Iterator<Item = (&'a str, ColumnValue<'a>)> {
    // Exploded parts of the generated key on the "/" character.
    //
    // Any uses of the "/" character within the partition key's user-provided
    // values are url encoded, so this is an unambiguous field separator.
    let key_parts = partition_key.split(PARTITION_KEY_DELIMITER);

    // Obtain an iterator of template parts, from which the meaning of the key
    // parts can be inferred.
    let template_parts = template.parts();

    // Invariant: the number of key parts generated from a given template always
    // matches the number of template parts.
    //
    // The key_parts iterator is not an ExactSizeIterator, so an assert can't be
    // placed here to validate this property.

    // Produce an iterator of (template_part, template_value)
    template_parts
        .zip(key_parts)
        .filter_map(|(template, mut value)| {
            // Perform re-mapping of sentinel values.
            match value {
                PARTITION_KEY_VALUE_NULL_STR => {
                    // Skip null or empty partition key parts, indicated by the
                    // presence of a single "!" character as the part value.
                    return None;
                }
                PARTITION_KEY_VALUE_EMPTY_STR => {
                    // Re-map the empty string sentinel "^"" to an empty string
                    // value.
                    value = "";
                }
                _ => {}
            }

            match template {
                TemplatePart::TagValue(col_name) => Some((col_name, value)),
                TemplatePart::TimeFormat(_) => None,
            }
        })
        // Reverse the urlencoding of all value parts
        .map(|(name, value)| {
            let decoded = percent_decode_str(value)
                .decode_utf8()
                .expect("invalid partition key part encoding");

            // Inspect the final character in the string, pre-decoding, to
            // determine if it has been truncated.
            if value
                .as_bytes()
                .last()
                .map(|v| *v == PARTITION_KEY_PART_TRUNCATED as u8)
                .unwrap_or_default()
            {
                // Remove the truncation marker.
                let len = decoded.len() - 1;

                // Only allocate if needed; re-borrow a subslice of `Cow::Borrowed` if not.
                let column_cow = match decoded {
                    Cow::Borrowed(s) => Cow::Borrowed(&s[..len]),
                    Cow::Owned(s) => Cow::Owned(s[..len].to_string()),
                };
                return (name, ColumnValue::Prefix(column_cow));
            }

            (name, ColumnValue::Identity(decoded))
        })
}

/// In production code, the template should come from protobuf that is either from the database or
/// from a gRPC request. In tests, building protobuf is painful, so here's an easier way to create
/// a `TablePartitionTemplateOverride`.
///
/// This deliberately goes around the validation of the templates so that tests can verify code
/// handles potentially invalid templates!
pub fn test_table_partition_override(
    parts: Vec<TemplatePart<'_>>,
) -> TablePartitionTemplateOverride {
    let parts = parts
        .into_iter()
        .map(|part| {
            let part = match part {
                TemplatePart::TagValue(value) => proto::template_part::Part::TagValue(value.into()),
                TemplatePart::TimeFormat(fmt) => proto::template_part::Part::TimeFormat(fmt.into()),
            };

            proto::TemplatePart { part: Some(part) }
        })
        .collect();

    let proto = proto::PartitionTemplate { parts };
    TablePartitionTemplateOverride(Some(
        serialization::Wrapper::for_testing_possibility_of_invalid_value_in_database(proto),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use sqlx::Encode;
    use test_helpers::assert_error;

    #[test]
    fn test_max_partition_key_len() {
        let max_len: usize =
            // 8 parts, at most 200 bytes long.
            (MAXIMUM_NUMBER_OF_TEMPLATE_PARTS * PARTITION_KEY_MAX_PART_LEN)
            // 7 delimiting characters between parts.
            + (MAXIMUM_NUMBER_OF_TEMPLATE_PARTS - 1);

        // If this changes, the module documentation should be changed too.
        //
        // This shouldn't change without consideration of primary key overlap as
        // a result.
        assert_eq!(max_len, 1_607, "update module docs please");
    }

    #[test]
    fn empty_parts_is_invalid() {
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate { parts: vec![] });

        assert_error!(err, ValidationError::NoParts);
    }

    #[test]
    fn more_than_8_parts_is_invalid() {
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("region".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("region".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("region".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("region".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("region".into())),
                },
            ],
        });

        assert_error!(err, ValidationError::TooManyParts { specified } if specified == 9);
    }

    /// Chrono will panic when formatting a timestamp if the "%#z" formatting
    /// directive is used...
    #[test]
    fn test_secret_formatter_advice_panic() {
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TimeFormat("%#z".into())),
            }],
        });

        assert_error!(err, ValidationError::InvalidStrftime(_));

        // This doesn't trigger the panic, but is included for completeness.
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TimeFormat("%#Z".into())),
            }],
        });

        assert_error!(err, ValidationError::InvalidStrftime(_));
    }

    #[test]
    fn invalid_strftime_format_is_invalid() {
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TimeFormat("%3F".into())),
            }],
        });

        assert_error!(err, ValidationError::InvalidStrftime(ref format) if format == "%3F");
    }

    #[test]
    fn empty_strftime_format_is_invalid() {
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TimeFormat("".into())),
            }],
        });

        assert_error!(err, ValidationError::InvalidStrftime(ref format) if format.is_empty());
    }

    /// "time" is a special column already covered by strftime, being a time
    /// series database and all.
    #[test]
    fn time_tag_value_is_invalid() {
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TagValue("time".into())),
            }],
        });

        assert_error!(err, ValidationError::InvalidTagValue(_));
    }

    #[test]
    fn empty_tag_value_is_invalid() {
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TagValue("".into())),
            }],
        });

        assert_error!(err, ValidationError::InvalidTagValue(ref value) if value.is_empty());
    }

    fn identity(s: &str) -> ColumnValue<'_> {
        ColumnValue::Identity(s.into())
    }

    fn prefix<'a, T>(s: T) -> ColumnValue<'a>
    where
        T: Into<Cow<'a, str>>,
    {
        ColumnValue::Prefix(s.into())
    }

    /// Generate a test that asserts "partition_key" is reversible, yielding
    /// "want" assuming the partition "template" was used.
    macro_rules! test_build_column_values {
        (
            $name:ident,
            template = $template:expr,              // Array/vec of TemplatePart
            partition_key = $partition_key:expr,    // String derived partition key
            want = $want:expr                       // Expected build_column_values() output
        ) => {
            paste::paste! {
                #[test]
                fn [<test_build_column_values_ $name>]() {
                    let template = $template.into_iter().collect::<Vec<_>>();
                    let template = test_table_partition_override(template);

                    // normalise the values into a (str, ColumnValue) for the comparison
                    let want = $want
                        .into_iter()
                        .collect::<Vec<_>>();

                    let input = String::from($partition_key);
                    let got = build_column_values(&template, input.as_str())
                        .collect::<Vec<_>>();

                    assert_eq!(got, want);
                }
            }
        };
    }

    test_build_column_values!(
        module_doc_example_1,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        partition_key = "2023|bananas|plátanos",
        want = [("a", identity("bananas")), ("b", identity("plátanos"))]
    );

    test_build_column_values!(
        module_doc_example_2,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        partition_key = "2023|!|plátanos",
        want = [("b", identity("plátanos"))]
    );

    test_build_column_values!(
        module_doc_example_4,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        partition_key = "2023|!|!",
        want = []
    );

    test_build_column_values!(
        module_doc_example_5,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        partition_key = "2023|cat%7Cdog|%21",
        want = [("a", identity("cat|dog")), ("b", identity("!"))]
    );

    test_build_column_values!(
        module_doc_example_6,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        partition_key = "2023|%2550|!",
        want = [("a", identity("%50"))]
    );

    test_build_column_values!(
        module_doc_example_7,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        partition_key = "2023|BANANAS#|!",
        want = [("a", prefix("BANANAS"))]
    );

    test_build_column_values!(
        unicode_code_point_prefix,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        partition_key = "2023|%28%E3%83%8E%E0%B2%A0%E7%9B%8A%E0%B2%A0%29%E3%83%8E%E5%BD%A1%E2%94%BB%E2%94%81%E2%94%BB#|!",
        want = [("a", prefix("(ノಠ益ಠ)ノ彡┻━┻"))]
    );

    test_build_column_values!(
        unicode_grapheme,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        partition_key = "2023|%E0%AE%A8%E0%AE%BF#|!",
        want = [("a", prefix("நி"))]
    );

    test_build_column_values!(
        unambiguous,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        partition_key = "2023|is%7Cnot%21ambiguous%2510%23|!",
        want = [("a", identity("is|not!ambiguous%10#"))]
    );

    test_build_column_values!(
        empty_tag_only,
        template = [TemplatePart::TagValue("a")],
        partition_key = "!",
        want = []
    );

    #[test]
    fn test_null_partition_key_char_str_equality() {
        assert_eq!(
            PARTITION_KEY_VALUE_NULL.to_string(),
            PARTITION_KEY_VALUE_NULL_STR
        );
    }

    #[test]
    fn test_column_value_partial_eq() {
        assert_eq!(identity("bananas"), "bananas");

        assert_ne!(identity("bananas"), "bananas2");
        assert_ne!(identity("bananas2"), "bananas");

        assert_ne!(prefix("bananas"), "bananas");
        assert_ne!(prefix("bananas"), "bananas2");
        assert_ne!(prefix("bananas2"), "bananas");
    }

    #[test]
    fn test_column_value_is_prefix_match() {
        let b = "bananas".to_string();
        assert!(identity("bananas").is_prefix_match_of(b));

        assert!(identity("bananas").is_prefix_match_of("bananas"));
        assert!(identity("bananas").is_prefix_match_of("bananas2"));

        assert!(prefix("bananas").is_prefix_match_of("bananas"));
        assert!(prefix("bananas").is_prefix_match_of("bananas2"));

        assert!(!identity("bananas2").is_prefix_match_of("bananas"));
        assert!(!prefix("bananas2").is_prefix_match_of("bananas"));
    }

    /// This test asserts the default derived partitioning scheme with no
    /// overrides.
    ///
    /// Changing this default during the lifetime of a cluster will cause the
    /// implicit (not overridden) partition schemes to change, potentially
    /// breaking the system invariant that a given primary keys maps to a
    /// single partition.
    ///
    /// You shouldn't be changing this!
    #[test]
    fn test_default_template_fixture() {
        let ns = NamespacePartitionTemplateOverride::default();
        let table = TablePartitionTemplateOverride::try_new(None, &ns).unwrap();
        let got = table.parts().collect::<Vec<_>>();
        assert_matches!(got.as_slice(), [TemplatePart::TimeFormat("%Y-%m-%d")]);
    }

    #[test]
    fn len_of_default_template_is_1() {
        let ns = NamespacePartitionTemplateOverride::default();
        let t = TablePartitionTemplateOverride::try_new(None, &ns).unwrap();

        assert_eq!(t.len(), 1);
    }

    #[test]
    fn no_custom_table_template_specified_gets_namespace_template() {
        let namespace_template =
            NamespacePartitionTemplateOverride::try_from(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                }],
            })
            .unwrap();
        let table_template =
            TablePartitionTemplateOverride::try_new(None, &namespace_template).unwrap();

        assert_eq!(table_template.len(), 1);
        assert_eq!(table_template.0, namespace_template.0);
    }

    #[test]
    fn custom_table_template_specified_ignores_namespace_template() {
        let custom_table_template = proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TagValue("region".into())),
            }],
        };
        let namespace_template =
            NamespacePartitionTemplateOverride::try_from(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                }],
            })
            .unwrap();
        let table_template = TablePartitionTemplateOverride::try_new(
            Some(custom_table_template.clone()),
            &namespace_template,
        )
        .unwrap();

        assert_eq!(table_template.len(), 1);
        assert_eq!(table_template.0.unwrap().inner(), &custom_table_template);
    }

    // The JSON representation of the partition template protobuf is stored in the database, so
    // the encode/decode implementations need to be stable if we want to avoid having to
    // migrate the values stored in the database.

    #[test]
    fn proto_encode_json_stability() {
        let custom_template = proto::PartitionTemplate {
            parts: vec![
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("region".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                },
            ],
        };
        let expected_json_str = "{\"parts\":[\
            {\"tagValue\":\"region\"},\
            {\"timeFormat\":\"year-%Y\"}\
        ]}";

        let namespace = NamespacePartitionTemplateOverride::try_from(custom_template).unwrap();
        let mut buf = Default::default();
        let _ = <NamespacePartitionTemplateOverride as Encode<'_, sqlx::Sqlite>>::encode_by_ref(
            &namespace, &mut buf,
        );

        fn extract_sqlite_argument_text(
            argument_value: &sqlx::sqlite::SqliteArgumentValue,
        ) -> String {
            match argument_value {
                sqlx::sqlite::SqliteArgumentValue::Text(cow) => cow.to_string(),
                other => panic!("Expected Text values, got: {other:?}"),
            }
        }

        let namespace_json_str: String = buf.iter().map(extract_sqlite_argument_text).collect();
        assert_eq!(namespace_json_str, expected_json_str);

        let table = TablePartitionTemplateOverride::try_new(None, &namespace).unwrap();
        let mut buf = Default::default();
        let _ = <TablePartitionTemplateOverride as Encode<'_, sqlx::Sqlite>>::encode_by_ref(
            &table, &mut buf,
        );
        let table_json_str: String = buf.iter().map(extract_sqlite_argument_text).collect();
        assert_eq!(table_json_str, expected_json_str);
        assert_eq!(table.len(), 2);
    }
}
