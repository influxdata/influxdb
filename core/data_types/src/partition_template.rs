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
//! If the partition key template references a [`TemplatePart::TagValue`] or
//! [`TemplatePart::Bucket`] column that is not present in the row, a single
//! `!` is inserted, indicating a NULL template key part. If the value of a
//! [`TemplatePart::TagValue`] part is an empty string (""), a `^` is inserted
//! to ensure a non-empty partition key is always generated. An empty string
//! value ("") is assigned a bucket ID in the same way as for any other string
//! value for a [`TemplatePart::Bucket`] part. Like the `|` key above, any
//! occurrence of reserved characters in a user-provided column value is percent
//! encoded.
//!
//! Because this serialisation format can be unambiguously reversed, the
//! [`TablePartitionTemplateOverride::column_values()`] function can be used to obtain the set of
//! [`TemplatePart::TagValue`] and/or [`TemplatePart::Bucket`] the key
//! was constructed from.
//!
//! ### Value Truncation
//!
//! Partition key parts are limited to, at most, 200 bytes in length
//! ([`PARTITION_KEY_MAX_PART_LEN`]). If any single partition key part exceeds
//! this length limit, it is truncated and the truncation marker `#`
//! ([`PARTITION_KEY_PART_TRUNCATED`]) is appended.
//!
//! When rebuilding column values using [`TablePartitionTemplateOverride::column_values`], a truncated
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
//! ## Part Limits & Maximum Key Size
//!
//! The number of parts in a partition template is limited to 8
//! ([`MAXIMUM_NUMBER_OF_TEMPLATE_PARTS`]), validated at creation time.
//!
//! Together with the above value truncation, this bounds the maximum length of
//! a partition key to 1,607 bytes (1.57 KiB).
//!
//! ### Required Parts
//!
//! Any partition template for a _newly_ created record (namespace or table)
//! MUST contain a [`TemplatePart::TimeFormat`] part. Partition templates for
//! pre-existing records are allowed to forgo this requirement as no reasonable
//! migration path existed at the time this extra validation was introduced.
//!
//! Partitioning without time has bad long-term performance implications, but
//! existing tables set up in this way must continue to operate in some manner.
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
//! ### Tag Name Restrictions
//!
//! Any given tag name may be used at most once across partition key parts that
//! operate on tags, like [`TemplatePart::TagValue`] & [`TemplatePart::Bucket`].
//! If a partition template partitions on the unique values of the tag with name
//! "bananas", then the tag name of "bananas" CANNOT be used to bucket-partition
//! on the values of the "bananas" tag.
//!
//! There are some reserved tag names that cannot be used for
//! [`TemplatePart::TagValue`] or [`TemplatePart::Bucket`] partition template
//! parts:
//!
//!   * `time` - The time column has special meaning and is covered by strftime
//!     formatters ([`TAG_VALUE_KEY_TIME`])
//!
//! ### Examples
//!
//! When using the partition template below:
//!
//! ```text
//!      [
//!          TemplatePart::TimeFormat("%Y"),
//!          TemplatePart::TagValue("a"),
//!          TemplatePart::TagValue("b"),
//!          TemplatePart::Bucket("c", 10)
//!      ]
//! ```
//!
//! The following partition keys are derived:
//!
//!   * `time=2023-01-01, a=bananas, b=plátanos, c=ananas`   -> `2023|bananas|plátanos|5`
//!   * `time=2023-01-01, b=plátanos`                        -> `2023|!|plátanos|!`
//!   * `time=2023-01-01, another=cat, b=plátanos`           -> `2023|!|plátanos|!`
//!   * `time=2023-01-01`                                    -> `2023|!|!|!`
//!   * `time=2023-01-01, a=cat|dog, b=!, c=!`               -> `2023|cat%7Cdog|%21|8`
//!   * `time=2023-01-01, a=%50, c=%50`                      -> `2023|%2550|!|9`
//!   * `time=2023-01-01, a=, c=`                            -> `2023|^|!|0`
//!   * `time=2023-01-01, a=<long string>`                   -> `2023|<long string>#|!|!`
//!   * `time=2023-01-01, c=<long string>`                   -> `2023|!|!|<bucket ID for un-truncated long string>`
//!
//! When using the default partitioning template (YYYY-MM-DD) there is no
//! encoding necessary, as the derived partition key contains a single part, and
//! no reserved characters. [`TemplatePart::Bucket`] parts by definition will
//! always be within the part length limit and contain no restricted characters
//! so are also not percent-encoded and/or truncated.
//!
//! [percent encoded]: https://url.spec.whatwg.org/#percent-encoded-bytes
use std::{
    borrow::Cow,
    cmp::min,
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter, Write},
    ops::Range,
    sync::{Arc, LazyLock},
};

use chrono::{
    DateTime, Days, Months, Utc,
    format::{self, Numeric, StrftimeItems},
};
use generated_types::influxdata::iox::partition_template::v1 as proto;
use murmur3::murmur3_32;
use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str};
use schema::TIME_COLUMN_NAME;
use thiserror::Error;

use crate::{MismatchedNumPartsError, PartitionKey, PartitionKeyBuilder};

/// Reasons a user-specified partition template isn't valid.
#[derive(Debug, Error)]
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

    /// The partition template defines a [`TagValue`] part or [`Bucket`] part,
    /// but the provided tag name value is invalid.
    ///
    /// [`TagValue`]: [`proto::template_part::Part::TagValue`]
    /// [`Bucket`]: [`proto::template_part::Part::Bucket`]
    #[error("invalid tag name value in partition template: {0}")]
    InvalidTagValue(String),

    /// The partition template defines a [`Bucket`] part, but the provided
    /// number of buckets is invalid.
    ///
    /// [`Bucket`]: [`proto::template_part::Part::Bucket`]
    #[error(
        "number of buckets in partition template must be in range \
        [{ALLOWED_BUCKET_QUANTITIES:?}), number specified: {0}"
    )]
    InvalidNumberOfBuckets(u32),

    /// The partition template defines a [`TagValue`] or [`Bucket`] part
    /// which repeats a tag name used in another [`TagValue`] or [`Bucket`] part.
    /// This is not allowed
    ///
    /// [`TagValue`]: [`proto::template_part::Part::TagValue`]
    /// [`Bucket`]: [`proto::template_part::Part::Bucket`]
    #[error("tag name value cannot be repeated in partition template: {0}")]
    RepeatedTagValue(String),

    /// An attempt to make a new partition template without a [`TimeFormat`]
    /// part has been made. This is not allowed.
    ///
    /// [`TimeFormat`]: [`proto::template_part::Part::TimeFormat`]
    #[error("partition template must include a time format part")]
    TimeFormatPartRequired,

    /// The partition template defines a [`TimeFormat`] part, but the
    /// provided strftime formatter contains unsupported specifiers.
    ///
    /// [`TimeFormat`]: [`proto::template_part::Part::TimeFormat`]
    #[error("unsupported specifier found in time format \"{0}\" - supported specifiers: {1}")]
    UnsupportedStrfTimeSpecifier(String, String),

    /// The partition template defines a [`TimeFormat`] part, but the
    /// provided strftime format that is missing required specifiers.
    ///
    /// For example: using %m without %Y.
    ///
    /// [`TimeFormat`]: [`proto::template_part::Part::TimeFormat`]
    #[error("missing required specifier(s) in time format of partition template: {0}")]
    MissingRequiredStrfTimeSpecifier(String),
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
/// yield a [`ColumnValue::Prefix`] from [`TablePartitionTemplateOverride::column_values`].
pub const PARTITION_KEY_PART_TRUNCATED: char = '#';

/// The reserved tag value key for the `time` column, which is reserved as
/// a specifically formatted column for the time associated with any given
/// data point.
pub const TAG_VALUE_KEY_TIME: &str = "time";

/// The range of bucket quantities allowed for [`Bucket`] template parts.
///
/// [`Bucket`]: [`proto::template_part::Part::Bucket`]
pub const ALLOWED_BUCKET_QUANTITIES: Range<u32> = Range {
    start: 1,
    end: 100_000,
};

/// The minimal set of characters that must be encoded during partition key
/// generation when they form part of a partition key part, in order to be
/// unambiguously reversible.
///
/// See module-level documentation & [`TablePartitionTemplateOverride::column_values`].
pub const ENCODED_PARTITION_KEY_CHARS: AsciiSet = CONTROLS
    .add(PARTITION_KEY_DELIMITER as u8)
    .add(PARTITION_KEY_VALUE_NULL as u8)
    .add(PARTITION_KEY_VALUE_EMPTY as u8)
    .add(PARTITION_KEY_PART_TRUNCATED as u8)
    .add(b'%'); // Required for reversible unambiguous encoding

/// Allocationless and protobufless access to the parts of a template needed to
/// actually do partitioning.
#[derive(Debug, Clone, Copy)]
pub enum TemplatePart<'a> {
    /// A tag-value partition part.
    ///
    /// Specifies the name of the tag column.
    TagValue(&'a str),

    /// A strftime formatter.
    ///
    /// Specifies the formatter spec applied to the [`TIME_COLUMN_NAME`] column.
    TimeFormat(&'a str),

    /// A bucketing partition part.
    ///
    /// Specifies the name of the tag column used to derive which of the `n`
    /// buckets the data belongs in, through the mechanism implemented by the
    /// [`bucket_for_tag_value`] function.
    Bucket(&'a str, u32),
}

impl<'a> TemplatePart<'a> {
    /// Column name.
    pub fn column_name(&self) -> &'a str {
        match self {
            TemplatePart::TagValue(c) => c,
            TemplatePart::TimeFormat(_) => TIME_COLUMN_NAME,
            TemplatePart::Bucket(c, _) => c,
        }
    }
}

/// The default partitioning scheme is by each day according to the "time" column.
pub static PARTITION_BY_DAY_PROTO: LazyLock<Arc<proto::PartitionTemplate>> = LazyLock::new(|| {
    Arc::new(proto::PartitionTemplate {
        parts: vec![proto::TemplatePart {
            part: Some(proto::template_part::Part::TimeFormat(
                "%Y-%m-%d".to_owned(),
            )),
        }],
    })
});

// This applies murmur3 32 bit hashing to the tag value string, as Iceberg would.
//
// * <https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements>
fn iceberg_hash(tag_value: &str) -> u32 {
    murmur3_32(&mut tag_value.as_bytes(), 0).expect("read of tag value string must never error")
}

/// Hash bucket the provided tag value to a bucket ID in the range `[0,num_buckets)`.
///
/// This applies murmur3 32 bit hashing to the tag value string, zero-ing the sign bit
/// then modulo assigning it to a bucket as Iceberg would.
///
/// * <https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements>
/// * <https://iceberg.apache.org/spec/#bucket-transform-details>
///
///
/// # Panics
///
/// If `num_buckets` is zero, this will panic. Validation MUST prevent
/// [`TemplatePart::Bucket`] from being constructed with a zero bucket count. It just
/// makes no sense and shouldn't need to be checked here.
#[inline(always)]
pub fn bucket_for_tag_value(tag_value: &str, num_buckets: u32) -> u32 {
    // Hash the tag value as iceberg would.
    let hash = iceberg_hash(tag_value);
    // Then bucket it as iceberg would, by removing the sign bit from the
    // 32 bit murmur hash and modulo by the number of buckets to assign
    // across.
    (hash & i32::MAX as u32) % num_buckets
}

/// Returns exploded parts of the given partition key on the "|" character ([`PARTITION_KEY_DELIMITER`]).
///
/// Any uses of the "|" character within the partition key's user-provided
/// values are url encoded, so this is an unambiguous field separator.
fn split_partition_key(key: &str) -> impl Iterator<Item = &str> {
    key.split(PARTITION_KEY_DELIMITER)
}

/// A partition template specified by a namespace record.
///
/// Internally this type is [`None`] when no namespace-level override is
/// specified, resulting in the default being used.
///
/// When creating a partition template for a namespace that does not yet have a
/// record you MUST use [`try_new()`]. This performs additional validation
/// checks which are not done by [`try_from()`].
///
/// This discrepancy exists because the additional validation checks were
/// added at a later date to prevent creation of typically problematic
/// partitioning schemes. Some clusters may already contain namespaces with
/// these schemes in their catalog and must continue to function, as they
/// were valid at time of creation and are still 'operable'.
///
/// [`try_new()`]: NamespacePartitionTemplateOverride::try_new()
/// [`try_from()`]: NamespacePartitionTemplateOverride::try_from()
#[derive(Debug, PartialEq, Clone, Default, sqlx::Type, Hash)]
#[sqlx(transparent, no_pg_array)]
pub struct NamespacePartitionTemplateOverride(Option<serialization::Wrapper>);

/// Function validates a strftime format string for use in an existing partition template.
/// This is more permissive, since we don't break existing (though probably sub-optimal)
/// partition templates when we tighten restriction on new format strings.
///
/// This function basically weeds out the format strings that would cause stability
/// issues.
///
/// # Errors
///
/// This function will return ValidationError if the strftime format string is invalid.
fn validate_existing_time_format(fmt: &str) -> Result<(), ValidationError> {
    // Empty is not a valid time format
    if fmt.is_empty() {
        return Err(ValidationError::InvalidStrftime(fmt.into()));
    }

    // Chrono will panic during timestamp formatting if this
    // formatter directive is used!
    //
    // An upper-case Z does not trigger the panic code path so
    // is not checked for.  Technically this excludes the legal
    // case of "%%#z" - we can fix this later if anyone cares.
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
    .map_err(|_| ValidationError::InvalidStrftime(fmt.into()))
}

/// Function validates a strftime format string for use in a NEW partition template.
/// This is where newer restrictions are applied to reject the creation of ill-advised
/// partition templates. This will not break existing partition templates.
///
/// Restrictions:
/// - | cannot be used
/// - at least one specifier is required
/// - no unsupported specifiers
/// - no dangling specifiers (i.e. %m without %Y)
/// - no repeated specifiers
///
/// # Errors
///
/// This function will return a ValidationError with an actionable message for the user.
fn validate_new_time_format(fmt: &str) -> Result<(), ValidationError> {
    // Don't allow the field delimiter inside the time format.
    if fmt.contains("|") {
        return Err(ValidationError::InvalidStrftime(
            "'|' cannot be used in the time format".to_string(),
        ));
    }

    // Error messages need to use the character representation of the specifier
    static SUPPORTED_SPEC_CHAR_MAP: LazyLock<HashMap<Numeric, char>> = LazyLock::new(|| {
        HashMap::from([
            (Numeric::Year, 'Y'),
            (Numeric::Month, 'm'),
            (Numeric::Day, 'd'),
        ])
    });

    // String of supported specifiers
    static SUPPORTED_SPECIFIERS_STR: LazyLock<String> = LazyLock::new(|| {
        SUPPORTED_SPEC_CHAR_MAP
            .values()
            .map(|v| format!("%{v}"))
            .collect::<Vec<_>>()
            .join(", ")
    });

    // Some specifiers make sense only in the context of another.  For example, day of the month (%d) requires
    // the month (%m) to be present. In this map, the key requires the value.
    static SPEC_DEPS: LazyLock<HashMap<Numeric, Numeric>> = LazyLock::new(|| {
        HashMap::from([
            (Numeric::Day, Numeric::Month),
            (Numeric::Month, Numeric::Year),
        ])
    });

    let mut specs_found_set: HashSet<Numeric> = HashSet::new();

    for item in StrftimeItems::new(fmt) {
        match item {
            // these don't have specifiers
            format::Item::Literal(_)
            | format::Item::OwnedLiteral(_)
            | format::Item::Space(_)
            | format::Item::OwnedSpace(_) => continue,

            // fmt doesn't parse
            format::Item::Error => {
                return Err(ValidationError::InvalidStrftime(fmt.into()));
            }

            // no Fixed specifiers currently supported
            format::Item::Fixed(_) => {
                return Err(ValidationError::UnsupportedStrfTimeSpecifier(
                    fmt.into(),
                    SUPPORTED_SPECIFIERS_STR.as_str().to_owned(),
                ));
            }

            // All supported specifiers are Numeric
            format::Item::Numeric(n, _pad) => {
                if SUPPORTED_SPEC_CHAR_MAP.contains_key(&n) {
                    let spec = SUPPORTED_SPEC_CHAR_MAP[&n];

                    if !specs_found_set.insert(n) {
                        return Err(ValidationError::InvalidStrftime(format!(
                            "Duplicate specifier (%{spec}) found in time format: \"{fmt}\""
                        )));
                    }
                } else {
                    return Err(ValidationError::UnsupportedStrfTimeSpecifier(
                        fmt.into(),
                        SUPPORTED_SPECIFIERS_STR.as_str().to_owned(),
                    ));
                }
            }
        }
    }

    if specs_found_set.is_empty() {
        return Err(ValidationError::InvalidStrftime(format!(
            "No specifiers found in time format: \"{fmt}\""
        )));
    }

    for spec in &specs_found_set {
        if let Some(required) = SPEC_DEPS.get(spec)
            && !specs_found_set.contains(required)
        {
            return Err(ValidationError::MissingRequiredStrfTimeSpecifier(format!(
                "Specifier %{} in \"{}\" requires %{}",
                SUPPORTED_SPEC_CHAR_MAP[spec], fmt, SUPPORTED_SPEC_CHAR_MAP[required]
            )));
        }
    }

    // While redundant, ensures rejection of anything that would fail in serialization later.
    validate_existing_time_format(fmt)?;

    Ok(())
}

fn validate_time_format_in_parts(parts: &[proto::TemplatePart]) -> Result<(), ValidationError> {
    let mut has_time_format = false;

    for part in parts {
        if let Some(proto::template_part::Part::TimeFormat(fmt)) = part.part.as_ref() {
            has_time_format = true;
            validate_new_time_format(fmt)?; // Call the existing validation function
        }
    }

    if !has_time_format {
        return Err(ValidationError::TimeFormatPartRequired);
    }

    Ok(())
}

impl NamespacePartitionTemplateOverride {
    /// A const "default" impl for testing.
    pub const fn const_default() -> Self {
        Self(None)
    }

    /// When a new namespace is created, the request to do so may include a
    /// custom partition template. This must be validated before use, with
    /// additional constraints applied to new namespaces.
    ///
    /// # Errors
    ///
    /// This function will return an error if the custom partition template
    /// specified is invalid. If `partition_template` does not contain a
    /// valid [`TimeFormat`] part it will also be rejected.
    ///
    /// [`TimeFormat`]: [`proto::template_part::Part::TimeFormat`]
    pub fn try_new(partition_template: proto::PartitionTemplate) -> Result<Self, ValidationError> {
        let namespace_template = Self::try_from(partition_template.clone())?;

        validate_time_format_in_parts(partition_template.parts.as_slice())?;

        Ok(namespace_template)
    }

    /// Return the protobuf representation of this template.
    pub fn as_proto(&self) -> Option<&proto::PartitionTemplate> {
        self.0.as_ref().map(|v| v.inner())
    }

    /// Loan out the application-type template parts of the contained partition
    /// template for this namespace, as per the behaviour of
    /// [`serialization::Wrapper::parts()`].
    pub fn parts(&self) -> impl Iterator<Item = TemplatePart<'_>> {
        serialization::Wrapper::parts(self.0.as_ref())
    }
}

impl NamespacePartitionTemplateOverride {
    /// Create a [`PartitionKey`] intended to be used with this
    /// [`NamespacePartitionTemplateOverride`], where the caller provides a slice of items to
    /// construct the key with. This just calls methods on [`PartitionKeyBuilder`] internally, and
    /// is provided only as a convenience method.
    pub fn part_key<T: Display>(
        &self,
        parts: &[T],
    ) -> Result<PartitionKey, MismatchedNumPartsError> {
        let mut builder = PartitionKeyBuilder::new(self);

        for part in parts {
            builder = builder.push(part);
        }

        builder.build()
    }
}

impl TryFrom<proto::PartitionTemplate> for NamespacePartitionTemplateOverride {
    type Error = ValidationError;

    /// An existing namepsace pulled from the catalog may contain a valid, but since
    /// disallowed partition template that would fail the validation checks
    /// performed by [`NamespacePartitionTemplateOverride::try_new()`].
    ///
    /// This function
    ///
    /// # Errors
    ///
    /// This function will return an error if the custom partition template specified is invalid.
    fn try_from(partition_template: proto::PartitionTemplate) -> Result<Self, Self::Error> {
        Ok(Self(Some(serialization::Wrapper::try_from(
            partition_template,
        )?)))
    }
}

/// A partition template specified by a table record.
///
/// When creating a partition template for a table that does not yet have a
/// record you MUST use [`try_new()`]. This performs additional validation
/// checks which are not done by [`try_from_existing()`].
///
/// This discrepancy exists because the additional validation checks were
/// added at a later date to prevent creation of typically problematic
/// partitioning schemes. Some clusters may already contain tables with these
/// schemes in their catalog and must continue to function, as they were valid
/// at time of creation and are still 'operable'.
///
///
/// [`try_new()`]: TablePartitionTemplateOverride::try_new()
/// [`try_from_existing()`]: TablePartitionTemplateOverride::try_from_existing()
#[derive(Debug, PartialEq, Eq, Clone, Default, sqlx::Type, Hash)]
#[sqlx(transparent, no_pg_array)]
pub struct TablePartitionTemplateOverride(Option<serialization::Wrapper>);

impl TablePartitionTemplateOverride {
    /// When a new table is being explicitly created, the request to do so may
    /// include a custom partition template which overrides the namespace level
    /// partitioning scheme. If the custom partition template is present, use
    /// it. Otherwise, use the namespace's partition template.
    ///
    /// # Errors
    ///
    /// This function will return an error if the custom partition template
    /// specified is invalid. If `custom_table_template` does not contain a
    /// [`TimeFormat`] part it will also be rejected.
    ///
    /// [`TimeFormat`]: [`proto::template_part::Part::TimeFormat`]
    pub fn try_new(
        custom_table_template: Option<proto::PartitionTemplate>,
        namespace_template: &NamespacePartitionTemplateOverride,
    ) -> Result<Self, ValidationError> {
        // If a custom table template was specified for the new table and no
        // part in it is a [`TimeFormat`] part then it should not be created.
        if let Some(table_template) = &custom_table_template {
            validate_time_format_in_parts(table_template.parts.as_slice())?;
        }

        match (custom_table_template, namespace_template.0.as_ref()) {
            (Some(table_proto), _) => Self::try_from_existing(table_proto),
            (None, Some(namespace_serialization_wrapper)) => {
                Ok(Self(Some(namespace_serialization_wrapper.clone())))
            }
            (None, None) => Ok(Self(None)),
        }
    }

    /// An existing table pulled from the catalog may contain a valid, but since
    /// disallowed partition template that would fail the validation checks
    /// performed by [`TablePartitionTemplateOverride::try_new()`].
    ///
    /// # Errors
    ///
    /// This function will return an error if the custom partition template specified is invalid.
    pub fn try_from_existing(
        table_proto: proto::PartitionTemplate,
    ) -> Result<Self, ValidationError> {
        Ok(Self(Some(serialization::Wrapper::try_from(table_proto)?)))
    }

    /// Returns the number of parts in this template.
    #[expect(clippy::len_without_is_empty)] // Senseless - there must always be >0 parts.
    pub fn len(&self) -> usize {
        self.parts().count()
    }

    /// Loan out the application-type template parts of the contained partition
    /// template for this table, as per the behaviour of
    /// [`serialization::Wrapper::parts()`].
    pub fn parts(&self) -> impl Iterator<Item = TemplatePart<'_>> {
        serialization::Wrapper::parts(self.0.as_ref())
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
                                        proto::template_part::Part::Bucket(proto::Bucket {
                                            tag_name,
                                            num_buckets: _,
                                        }) => tag_name.capacity() + std::mem::size_of::<u32>(),
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

    /// Reverse a `partition_key` generated from the given `template`, reconstructing the set of
    /// tag values and bucket IDs the `partition_key` was generated from, in the form of `(column
    /// name, column value)` tuples.
    ///
    /// The `partition_key` MUST have been generated by the same `template`.
    ///
    /// If you need to process many partition keys against the same template, consider using the
    /// more efficient [`Self::column_values_builder`] API instead.
    ///
    /// # Panics
    ///
    /// This method panics if a column value is not valid UTF8 after decoding, or
    /// when a bucket ID is not valid (not a u32 or within the expected number of
    /// buckets).
    pub fn column_values<'a>(
        &'a self,
        key: &'a str,
    ) -> impl Iterator<Item = (&'a str, Option<ColumnValue<'a>>)> {
        self.column_values_builder()
            .into_column_values_from_key(key)
    }

    /// Creates a [`ColumnValuesBuilder`] that can be used to reverse partition keys generated from
    /// this template by calling the method [`ColumnValuesBuilder::build_from_key`].
    ///
    /// Some prepared state is stored in this builder to avoid redundant work when processing
    /// many partition keys against the same template. If you only need to process a single partition
    /// key for a single template, consider using the simpler [`Self::column_values`] API
    /// instead.
    pub fn column_values_builder(&self) -> ColumnValuesBuilder<'_> {
        ColumnValuesBuilder::new(self.parts())
    }
}

/// Display the serde_json representation so that the output
/// can be copy/pasted into CLI tools, etc as the partition
/// template is specified as JSON
impl Display for TablePartitionTemplateOverride {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.as_proto()
                .map(|proto| serde_json::to_string(proto)
                    .expect("serialization should be infallible"))
                .unwrap_or_default()
        )
    }
}

impl TryFrom<Option<proto::PartitionTemplate>> for TablePartitionTemplateOverride {
    type Error = ValidationError;

    fn try_from(p: Option<proto::PartitionTemplate>) -> Result<Self, Self::Error> {
        Ok(Self(p.map(serialization::Wrapper::try_from).transpose()?))
    }
}

impl TryFrom<proto::PartitionTemplate> for TablePartitionTemplateOverride {
    type Error = ValidationError;

    /// An existing namepsace pulled from the catalog may contain a valid, but since
    /// disallowed partition template.
    ///
    /// # Errors
    ///
    /// This function will return an error if the custom partition template specified is invalid.
    fn try_from(partition_template: proto::PartitionTemplate) -> Result<Self, Self::Error> {
        Ok(Self(Some(serialization::Wrapper::try_from(
            partition_template,
        )?)))
    }
}

/// This manages the serialization/deserialization of the `proto::PartitionTemplate` type to and
/// from the database through `sqlx` for the `NamespacePartitionTemplateOverride` and
/// `TablePartitionTemplateOverride` types. It's an internal implementation detail to minimize code
/// duplication.
mod serialization {
    use super::{
        ALLOWED_BUCKET_QUANTITIES, MAXIMUM_NUMBER_OF_TEMPLATE_PARTS, PARTITION_BY_DAY_PROTO,
        TAG_VALUE_KEY_TIME, TemplatePart, ValidationError, validate_existing_time_format,
    };
    use generated_types::influxdata::iox::partition_template::v1 as proto;
    use std::{collections::HashSet, sync::Arc};

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

        /// Iterate through the protobuf parts and lend out what information
        /// necessary to build partition keys and restrict new templates.
        ///
        /// If `wrapper` doesn't have a custom template, this uses the
        /// application default of [`PARTITION_BY_DAY_PROTO`].
        pub(super) fn parts(wrapper: Option<&Self>) -> impl Iterator<Item = TemplatePart<'_>> {
            wrapper
                .map(|serialization_wrapper| serialization_wrapper.inner())
                .unwrap_or_else(|| &PARTITION_BY_DAY_PROTO)
                .parts
                .iter()
                .flat_map(|part| part.part.as_ref())
                .map(|part| match part {
                    proto::template_part::Part::TagValue(value) => TemplatePart::TagValue(value),
                    proto::template_part::Part::TimeFormat(fmt) => TemplatePart::TimeFormat(fmt),
                    proto::template_part::Part::Bucket(proto::Bucket {
                        tag_name,
                        num_buckets,
                    }) => TemplatePart::Bucket(tag_name, *num_buckets),
                })
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

            let mut seen_tags: HashSet<&str> = HashSet::with_capacity(specified);

            // All time formats must be valid and tag values may not specify any
            // restricted values.
            for part in &partition_template.parts {
                match &part.part {
                    Some(proto::template_part::Part::TimeFormat(fmt)) => {
                        validate_existing_time_format(fmt)?;
                    }
                    Some(proto::template_part::Part::TagValue(value)) => {
                        // Empty is not a valid tag value
                        if value.is_empty() {
                            return Err(ValidationError::InvalidTagValue(value.into()));
                        }

                        // "time" cannot be used due to the insane number of
                        // partitions this would create.
                        if value == TAG_VALUE_KEY_TIME {
                            return Err(ValidationError::InvalidTagValue(format!(
                                "{TAG_VALUE_KEY_TIME} cannot be used"
                            )));
                        }

                        if !seen_tags.insert(value.as_str()) {
                            return Err(ValidationError::RepeatedTagValue(value.into()));
                        }
                    }
                    Some(proto::template_part::Part::Bucket(proto::Bucket {
                        tag_name,
                        num_buckets,
                    })) => {
                        if tag_name.is_empty() {
                            return Err(ValidationError::InvalidTagValue(tag_name.into()));
                        }

                        if tag_name == TAG_VALUE_KEY_TIME {
                            return Err(ValidationError::InvalidTagValue(format!(
                                "{TAG_VALUE_KEY_TIME} cannot be used"
                            )));
                        }

                        if !seen_tags.insert(tag_name.as_str()) {
                            return Err(ValidationError::RepeatedTagValue(tag_name.into()));
                        }

                        if !ALLOWED_BUCKET_QUANTITIES.contains(num_buckets) {
                            return Err(ValidationError::InvalidNumberOfBuckets(*num_buckets));
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
            buf: &mut DB::ArgumentBuffer<'q>,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
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
            value: DB::ValueRef<'q>,
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
/// See [`TablePartitionTemplateOverride::column_values`].
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

    /// Datetime.
    Datetime {
        /// Inclusive begin of the datatime partition range.
        begin: DateTime<Utc>,

        /// Exclusive end of the datatime partition range.
        end: DateTime<Utc>,
    },

    /// Bucket information.
    Bucket {
        /// ID of the bucket selected through a modulo hash
        /// of the input column value.
        id: u32,

        /// The divisor of the modulo hash specified in the partition template used to derive this `ColumnValue`.
        num_buckets: u32,
    },
}

impl ColumnValue<'_> {
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
            ColumnValue::Datetime { .. } | ColumnValue::Bucket { .. } => {
                return false;
            }
        };

        other.as_ref().starts_with(this)
    }
}

impl<T> PartialEq<T> for ColumnValue<'_>
where
    T: AsRef<str>,
{
    fn eq(&self, other: &T) -> bool {
        match self {
            ColumnValue::Identity(v) => other.as_ref().eq(v.as_ref()),
            ColumnValue::Prefix(_) => false,
            ColumnValue::Datetime { .. } => false,
            ColumnValue::Bucket { .. } => false,
        }
    }
}

#[derive(Debug)]
/// Reverses partition keys into [`ColumnValue`]s using the `template` passed to
/// [`ColumnValuesBuilder::new`].
///
/// This builder stores some "prepared" state from the template string parts, so that redundant work
/// can be avoided when the builder is used to process multiple partition keys. For example, time
/// format strings are parsed and processed in advance.
pub struct ColumnValuesBuilder<'t> {
    parts: Vec<ColumnValueBuilder<'t>>,
}

impl<'t> ColumnValuesBuilder<'t> {
    /// Constructs a [`ColumnValuesBuilder`] from an iterator of [`TemplatePart`]s.
    ///
    /// The returned builder contains prepared state that can be used to reverse multiple
    /// partition keys generated from the same source template by calling the
    /// [`ColumnValuesBuilder::build_from_key`] method on each partition key.
    pub fn new(template: impl IntoIterator<Item = TemplatePart<'t>>) -> Self {
        Self {
            parts: template.into_iter().map(ColumnValueBuilder::new).collect(),
        }
    }

    /// Reverse a `partition_key` generated from the `template` used to create this builder,
    /// reconstructing the set of tag values and bucket IDs the `partition_key` was generated from,
    /// in the form of `(column name, column value)` tuples.
    ///
    /// The `partition_key` MUST have been generated by the same `template`.
    ///
    /// # Panics
    ///
    /// This method panics if a column value is not valid UTF8 after decoding, or
    /// when a bucket ID is not valid (not a u32 or within the expected number of
    /// buckets).
    pub fn build_from_key<'a>(
        &'a self,
        partition_key: &'t str,
    ) -> impl Iterator<Item = (&'t str, Option<ColumnValue<'t>>)> + 'a {
        // Invariant: the number of key parts generated from a given template
        // always matches the number of template parts, and the partition key
        // must be produced from the same template used to construct this builder
        //
        // The split_partition_key iterator is not an ExactSizeIterator, so an assert can't be
        // placed here to validate this property.
        self.parts
            .iter()
            .zip(split_partition_key(partition_key))
            .map(move |(builder, value)| {
                let name = builder.column_name();
                let value = builder.build_from_key_part(value);
                (name, value)
            })
    }

    /// A private variant of [`Self::build_from_key`] that consumes the builder. This is used by
    /// the public [`TablePartitionTemplateOverride::column_values`] method as an easier API for
    /// when only one partition key is needed.
    fn into_column_values_from_key(
        self,
        partition_key: &'t str,
    ) -> impl Iterator<Item = (&'t str, Option<ColumnValue<'t>>)> {
        self.parts
            .into_iter()
            .zip(split_partition_key(partition_key))
            .map(move |(builder, value)| {
                let name = builder.column_name();
                let value = builder.build_from_key_part(value);
                (name, value)
            })
    }

    /// Get the number of [`TemplatePart`]s used to create this builder.
    #[expect(clippy::len_without_is_empty)] // there must always be >0 parts.
    pub fn len(&self) -> usize {
        self.parts.len()
    }
}

#[derive(Debug)]
/// Prepared state for decoding a single partition key part into a [`ColumnValue`]
///
/// The enum variants are analogous to the [`TemplatePart`] variants of the same name.
enum ColumnValueBuilder<'t> {
    /// Stores prepared state needed to convert a[`TemplatePart::TagValue`] into a [ColumnValue]
    ///
    /// No extra state is needed here so we just store the name of the tag column.
    TagValue(&'t str),

    /// Stores prepared state needed to convert a [`TemplatePart::TimeFormat`] into a [`ColumnValue`]
    ///
    /// See [`TimeFormatColumnValueState`] for detailed explanation of the state values.
    /// A value of [`None`] indicates the time format of this template is not supported or not implemented.
    TimeFormat(Option<TimeFormatColumnValueState<'t>>),

    /// Stores prepared state needed to convert a [`TemplatePart::Bucket`] into a [ColumnValue]
    ///
    /// No extra state is needed here so we just store the name of the tag column and the bucket ID
    Bucket(&'t str, u32),
}

impl<'t> ColumnValueBuilder<'t> {
    /// Create prepared state from a given [`TemplatePart`]
    fn new(part: TemplatePart<'t>) -> Self {
        match part {
            TemplatePart::TagValue(tag) => Self::TagValue(tag),
            TemplatePart::TimeFormat(format) => {
                Self::TimeFormat(TimeFormatColumnValueState::try_new(format))
            }
            TemplatePart::Bucket(tag, bucket) => Self::Bucket(tag, bucket),
        }
    }

    /// Convert part of a partition key into a [`ColumnValue`].
    ///
    /// The partition key part MUST have been generated from the same [`TemplatePart`] used to
    /// construct this builder.
    fn build_from_key_part(&self, value: &'t str) -> Option<ColumnValue<'t>> {
        (value != PARTITION_KEY_VALUE_NULL_STR)
            .then(|| match self {
                Self::TagValue(_) => parse_part_tag_value(value),
                Self::TimeFormat(None) => None,
                Self::TimeFormat(Some(time_builder)) => time_builder.parse_part_time_format(value),
                Self::Bucket(_, num_buckets) => parse_part_bucket(value, *num_buckets),
            })
            .flatten()
    }

    /// Get the column name. Same as [`TemplatePart::column_name`].
    fn column_name(&self) -> &'t str {
        match self {
            Self::TagValue(c) => c,
            Self::TimeFormat(_) => TIME_COLUMN_NAME,
            Self::Bucket(c, _) => c,
        }
    }
}

fn parse_part_tag_value(value: &str) -> Option<ColumnValue<'_>> {
    // Perform re-mapping of sentinel values.
    let value = match value {
        PARTITION_KEY_VALUE_EMPTY_STR => {
            // Re-map the empty string sentinel "^"" to an empty string
            // value.
            ""
        }
        _ => value,
    };

    // Reverse the urlencoding of all value parts
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
        Some(ColumnValue::Prefix(column_cow))
    } else {
        Some(ColumnValue::Identity(decoded))
    }
}

fn parse_part_bucket(value: &str, num_buckets: u32) -> Option<ColumnValue<'_>> {
    // Parse the bucket ID from the given value string.
    let id = value
        .parse::<u32>()
        .expect("invalid partition key bucket encoding");
    // Invariant: If the bucket ID (0 indexed) is greater than the number of
    // buckets to spread data across the partition key is invalid.
    assert!(id < num_buckets);

    Some(ColumnValue::Bucket { id, num_buckets })
}

fn parsed_implicit_defaults(mut parsed: format::Parsed) -> Option<format::Parsed> {
    parsed.year?;

    if parsed.month.is_none() {
        if parsed.day.is_some() {
            return None;
        }

        parsed.set_month(1).ok()?;
    }

    if parsed.day.is_none() {
        if parsed.hour_div_12.is_some() || parsed.hour_mod_12.is_some() {
            return None;
        }

        parsed.set_day(1).ok()?;
    }

    if parsed.hour_div_12.is_none() || parsed.hour_mod_12.is_none() {
        // consistency check
        if parsed.hour_div_12.is_some() {
            return None;
        }
        if parsed.hour_mod_12.is_some() {
            return None;
        }

        if parsed.minute.is_some() {
            return None;
        }

        parsed.set_hour(0).ok()?;
    }

    if parsed.minute.is_none() {
        if parsed.second.is_some() {
            return None;
        }
        if parsed.nanosecond.is_some() {
            return None;
        }

        parsed.set_minute(0).ok()?;
    }

    Some(parsed)
}

/// Prepared state for decoding a partition key timestamp into the time range of its column values
#[derive(Debug)]
pub struct TimeFormatColumnValueState<'t> {
    /// The parsed time format string. This is used to construct a DateTime from the partition key
    /// timestamp
    parsed_format: Vec<format::Item<'t>>,
    /// Computed time duration to add to a partition key timestamp (representing the lower bound of
    /// the partition time interval) to get the upper bound of the partition time interval
    partition_duration: PartitionDuration,
}

impl<'t> TimeFormatColumnValueState<'t> {
    /// Construct a new [`TimeFormatColumnValueState`] from a formatted time string slice.
    ///
    /// A formatted time string slice may look like:
    /// - '%Y-%m-%d'
    /// - '%Y-%m'
    pub fn try_new(format: &'t str) -> Option<Self> {
        let parsed_format = StrftimeItems::new(format).collect::<Vec<_>>();
        let partition_duration = PartitionDuration::from_format_items(parsed_format.iter())?;
        Some(Self {
            parsed_format,
            partition_duration,
        })
    }

    /// Return a reference to the [`PartitionDuration`].
    pub fn partition_duration(&self) -> &PartitionDuration {
        &self.partition_duration
    }

    fn parse_part_time_format(&self, value: &'t str) -> Option<ColumnValue<'t>> {
        use chrono::format::{Parsed, parse};
        let Self {
            parsed_format,
            partition_duration,
        } = self;

        let mut parsed = Parsed::new();
        parse(&mut parsed, value, parsed_format.iter()).ok()?;

        // fill in defaults
        let parsed = parsed_implicit_defaults(parsed)?;

        let begin = parsed.to_datetime_with_timezone(&Utc).ok()?;
        let end: DateTime<Utc> = begin + *partition_duration;
        Some(ColumnValue::Datetime { begin, end })
    }
}

/// Logically represents the duration of time that a [`ColumnValue::Datetime`] created from a
/// [`TemplatePart::TimeFormat`] represents.
///
/// Currently this is constrained to the set of durations that we support for partition pruning. As
/// such, not all possible time formats can be represented with this enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PartitionDuration {
    /// A daily partition time duration. Example time format: %Y-%m-%d
    Daily,
    /// A monthly partition time duration. Example time format: %Y-%m
    Monthly,
    /// A yearly partition time duration. Example time format: %Y
    Yearly,
}

impl PartitionDuration {
    /// Convert from a numeric time specifier. For example [`chrono::format::Numeric::Year`] is converted to
    /// [`PartitionDuration::Yearly`]. A value of [`None`] indicates that the time format is not supported by
    /// partition pruning.
    fn from_numeric_format_part(value: format::Numeric) -> Option<Self> {
        use chrono::format::Numeric;
        Some(match value {
            Numeric::Year => PartitionDuration::Yearly,
            Numeric::Month => PartitionDuration::Monthly,
            Numeric::Day => PartitionDuration::Daily,
            _ => return None,
        })
    }

    /// Convert from an iterator of chrono time format items, obtained by parsing a
    /// [`TemplatePart::TimeFormat`] string.
    fn from_format_items<'a>(
        items: impl IntoIterator<Item = &'a format::Item<'a>>,
    ) -> Option<Self> {
        use chrono::format::Item;
        let mut min_duration = None;
        for item in items {
            match item {
                // ignore these
                Item::Literal(_) | Item::OwnedLiteral(_) | Item::Space(_) | Item::OwnedSpace(_) => {
                    continue;
                }
                // break out on error
                Item::Error => return None,
                // not implemented. break out
                Item::Fixed(_) => return None,
                Item::Numeric(numeric, _pad) => {
                    let duration = PartitionDuration::from_numeric_format_part(numeric.clone())?;
                    // find smaller of current minimum and new duration
                    min_duration = Some(match min_duration {
                        Some(current_min) => min(current_min, duration),
                        None => duration,
                    })
                }
            }
        }
        min_duration
    }
}

impl<Tz: chrono::TimeZone> std::ops::Add<PartitionDuration> for chrono::DateTime<Tz> {
    type Output = Self;
    /// Add a [`PartitionDuration`] to a [`chrono::DateTime`].
    fn add(self, rhs: PartitionDuration) -> Self::Output {
        match rhs {
            PartitionDuration::Yearly => self + Months::new(12),
            PartitionDuration::Monthly => self + Months::new(1),
            PartitionDuration::Daily => self + Days::new(1),
        }
    }
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
                TemplatePart::Bucket(value, num_buckets) => {
                    proto::template_part::Part::Bucket(proto::Bucket {
                        tag_name: value.into(),
                        num_buckets,
                    })
                }
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
    use assert_matches::assert_matches;
    use chrono::TimeZone;
    use proptest::prelude::*;
    use sqlx::Encode;
    use test_helpers::assert_error;

    use super::*;

    #[test]
    fn test_partition_template_to_string() {
        let template_empty: TablePartitionTemplateOverride =
            TablePartitionTemplateOverride::default();

        let template: Vec<TemplatePart<'_>> =
            [TemplatePart::TimeFormat("%Y"), TemplatePart::TagValue("a")]
                .into_iter()
                .collect::<Vec<_>>();
        let template: TablePartitionTemplateOverride = test_table_partition_override(template);

        assert_eq!(template_empty.to_string(), "");
        assert_eq!(
            template.to_string(),
            "{\"parts\":[{\"timeFormat\":\"%Y\"},{\"tagValue\":\"a\"}]}"
        );
    }

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
    fn new_namespace_template_requires_time_format() {
        // Without tag value
        assert_matches!(
            NamespacePartitionTemplateOverride::try_new(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("region".into())),
                }]
            }),
            Err(ValidationError::TimeFormatPartRequired)
        );
        // Adding tag value to same template results in acceptance.
        assert_matches!(
            NamespacePartitionTemplateOverride::try_new(proto::PartitionTemplate {
                parts: vec![
                    proto::TemplatePart {
                        part: Some(proto::template_part::Part::TagValue("region".into())),
                    },
                    proto::TemplatePart {
                        part: Some(proto::template_part::Part::TimeFormat("year-%Y".into())),
                    },
                ]
            }),
            Ok(v) => {
                assert_eq!(v.parts().count(), 2);
            }
        );
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

    /// Assert that attempting to use tag partitioning against the "time" column
    /// returns an error, but succeeds when "time" is a substring.
    #[test]
    fn test_tag_partitioning_time() {
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TagValue("time".into())),
            }],
        });

        assert_error!(
            err.as_ref(),
            ValidationError::InvalidTagValue(s) if s == "time cannot be used"
        );

        // Allow substring occurrences of "time".
        assert_matches!(
            serialization::Wrapper::try_from(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("time_prefix".into())),
                }],
            }),
            Ok(_)
        );

        assert_matches!(
            serialization::Wrapper::try_from(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("suffix_time".into())),
                }],
            }),
            Ok(_)
        );

        assert_matches!(
            serialization::Wrapper::try_from(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue(
                        "banana_time_yeah".into()
                    )),
                }],
            }),
            Ok(_)
        );
    }

    /// Assert that attempting to use bucket partitioning against the "time"
    /// column returns an error, but succeeds when "time" is a substring.
    #[test]
    fn test_bucket_partitioning_time() {
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::Bucket(proto::Bucket {
                    tag_name: "time".into(),
                    num_buckets: 42,
                })),
            }],
        });

        assert_error!(
            err.as_ref(),
            ValidationError::InvalidTagValue(s) if s == "time cannot be used"
        );

        // Allow substring occurrences of "time".
        assert_matches!(
            serialization::Wrapper::try_from(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::Bucket(proto::Bucket {
                        tag_name: "time_prefix".into(),
                        num_buckets: 42
                    })),
                }],
            }),
            Ok(_)
        );

        assert_matches!(
            serialization::Wrapper::try_from(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::Bucket(proto::Bucket {
                        tag_name: "suffix_time".into(),
                        num_buckets: 42
                    })),
                }],
            }),
            Ok(_)
        );

        assert_matches!(
            serialization::Wrapper::try_from(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::Bucket(proto::Bucket {
                        tag_name: "banana_time_yeah".into(),
                        num_buckets: 42,
                    })),
                }],
            }),
            Ok(_)
        );
    }

    #[test]
    fn repeated_tag_name_value_is_invalid() {
        // Test [`TagValue`]
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("bananas".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("bananas".into())),
                },
            ],
        });

        assert_error!(err, ValidationError::RepeatedTagValue ( ref specified ) if specified == "bananas");

        // Test [`Bucket`]
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::Bucket(proto::Bucket {
                        tag_name: "bananas".into(),
                        num_buckets: 42,
                    })),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::Bucket(proto::Bucket {
                        tag_name: "bananas".into(),
                        num_buckets: 42,
                    })),
                },
            ],
        });

        assert_error!(err, ValidationError::RepeatedTagValue ( ref specified ) if specified == "bananas");

        // Test a combination of [`TagValue`] and [`Bucket`]
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("bananas".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::Bucket(proto::Bucket {
                        tag_name: "bananas".into(),
                        num_buckets: 42,
                    })),
                },
            ],
        });

        assert_error!(err, ValidationError::RepeatedTagValue ( ref specified ) if specified == "bananas");
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

    /// "time" is a special column already covered by strftime, being a time
    /// series database and all.
    #[test]
    fn bucket_time_tag_name_is_invalid() {
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::Bucket(proto::Bucket {
                    tag_name: "time".into(),
                    num_buckets: 42,
                })),
            }],
        });

        assert_error!(err, ValidationError::InvalidTagValue(_));
    }

    #[test]
    fn bucket_empty_tag_name_is_invalid() {
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::Bucket(proto::Bucket {
                    tag_name: "".into(),
                    num_buckets: 42,
                })),
            }],
        });

        assert_error!(err, ValidationError::InvalidTagValue(ref value) if value.is_empty());
    }

    #[test]
    fn bucket_zero_num_buckets_is_invalid() {
        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::Bucket(proto::Bucket {
                    tag_name: "arán".into(),
                    num_buckets: 0,
                })),
            }],
        });

        assert_error!(err, ValidationError::InvalidNumberOfBuckets(0));
    }

    #[test]
    fn bucket_too_high_num_buckets_is_invalid() {
        const TOO_HIGH: u32 = 100_000;

        let err = serialization::Wrapper::try_from(proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::Bucket(proto::Bucket {
                    tag_name: "arán".into(),
                    num_buckets: TOO_HIGH,
                })),
            }],
        });

        assert_error!(err, ValidationError::InvalidNumberOfBuckets(TOO_HIGH));
    }

    fn identity(s: &str) -> ColumnValue<'_> {
        ColumnValue::Identity(s.into())
    }

    fn bucket(id: u32, num_buckets: u32) -> ColumnValue<'static> {
        ColumnValue::Bucket { id, num_buckets }
    }

    fn prefix<'a, T>(s: T) -> ColumnValue<'a>
    where
        T: Into<Cow<'a, str>>,
    {
        ColumnValue::Prefix(s.into())
    }

    fn year(y: i32) -> ColumnValue<'static> {
        ColumnValue::Datetime {
            begin: Utc.with_ymd_and_hms(y, 1, 1, 0, 0, 0).unwrap(),
            end: Utc.with_ymd_and_hms(y + 1, 1, 1, 0, 0, 0).unwrap(),
        }
    }

    #[test]
    fn test_iceberg_string_hash() {
        assert_eq!(iceberg_hash("iceberg"), 1210000089);
    }

    // This is a test fixture designed to catch accidental changes to the
    // Iceberg-like hash-bucket partitioning behaviour.
    //
    // You shouldn't be changing this!
    #[test]
    fn test_hash_bucket_fixture() {
        // These are values lifted from the iceberg spark test suite for
        // `BucketString`, sadly not provided in the reference/spec:
        //
        // https://github.com/apache/iceberg/blob/31e31fd819c846f49d2bd459b8bfadfdc3c2bc3a/spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/sql/TestSparkBucketFunction.java#L151-L169
        //
        assert_eq!(bucket_for_tag_value("abcdefg", 5), 4);
        assert_eq!(bucket_for_tag_value("abc", 128), 122);
        assert_eq!(bucket_for_tag_value("abcde", 64), 54);
        assert_eq!(bucket_for_tag_value("测试", 12), 8);
        assert_eq!(bucket_for_tag_value("测试raul试测", 16), 1);
        assert_eq!(bucket_for_tag_value("", 16), 0);

        // These are pre-existing arbitrary fixture values
        assert_eq!(bucket_for_tag_value("bananas", 10), 1);
        assert_eq!(bucket_for_tag_value("plátanos", 100), 98);
        assert_eq!(bucket_for_tag_value("crobhaing bananaí", 1000), 166);
        assert_eq!(bucket_for_tag_value("bread", 42), 9);
        assert_eq!(bucket_for_tag_value("arán", 76), 72);
        assert_eq!(bucket_for_tag_value("banana arán", 1337), 1284);
        assert_eq!(
            bucket_for_tag_value("uasmhéid bananaí", u32::MAX),
            1109892861
        );
    }

    /// Test to approximate and show how the tag value maps to the partition key
    /// for the example cases in the mod-doc. The behaviour that renders the key
    /// itself is a combination of this bucket assignment and the render logic.
    #[test]
    fn test_bucket_for_mod_doc() {
        assert_eq!(bucket_for_tag_value("ananas", 10), 5);
        assert_eq!(bucket_for_tag_value("!", 10), 8);
        assert_eq!(bucket_for_tag_value("%50", 10), 9);
        assert_eq!(bucket_for_tag_value("", 10), 0);
    }

    proptest! {
        #[test]
        fn prop_consistent_bucketing_within_limits(tag_values in proptest::collection::vec(any::<String>(), (1, 10)), num_buckets in any::<u32>()) {
            for value in tag_values {
                // First pass assign
                let want_bucket = bucket_for_tag_value(&value, num_buckets);
                // The assigned bucket must fit within the domain given to the bucketer.
                assert!(want_bucket < num_buckets);
                // Feed in the same tag value, expect the same result.
                let got_bucket = bucket_for_tag_value(&value, num_buckets);
                assert_eq!(want_bucket, got_bucket);
            }
        }
    }

    /// Generate a test that asserts "partition_key" is reversible, yielding
    /// "want" assuming the partition "template" was used.
    macro_rules! test_build_column_values {
        (
            $name:ident,
            template = $template:expr,                 // Array/vec of TemplatePart
            partition_key = $partition_key:expr,       // String derived partition key
            want = $want:expr                          // Expected build_column_values() output
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
                    let got = template.column_values(input.as_str())
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
            TemplatePart::Bucket("c", 10),
        ],
        partition_key = "2023|bananas|plátanos|5",
        want = [
            (TIME_COLUMN_NAME, Some(year(2023))),
            ("a", Some(identity("bananas"))),
            ("b", Some(identity("plátanos"))),
            ("c", Some(bucket(5, 10))),
        ]
    );

    test_build_column_values!(
        module_doc_example_2, // Examples 2 and 3 are the same partition key
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
            TemplatePart::Bucket("c", 10),
        ],
        partition_key = "2023|!|plátanos|!",
        want = [
            (TIME_COLUMN_NAME, Some(year(2023))),
            ("a", None),
            ("b", Some(identity("plátanos"))),
            ("c", None),
        ]
    );

    test_build_column_values!(
        module_doc_example_4,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
            TemplatePart::Bucket("c", 10),
        ],
        partition_key = "2023|!|!|!",
        want = [
            (TIME_COLUMN_NAME, Some(year(2023))),
            ("a", None),
            ("b", None),
            ("c", None)
        ]
    );

    test_build_column_values!(
        module_doc_example_5,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
            TemplatePart::Bucket("c", 10),
        ],
        partition_key = "2023|cat%7Cdog|%21|8",
        want = [
            (TIME_COLUMN_NAME, Some(year(2023))),
            ("a", Some(identity("cat|dog"))),
            ("b", Some(identity("!"))),
            ("c", Some(bucket(8, 10))),
        ]
    );

    test_build_column_values!(
        module_doc_example_6,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
            TemplatePart::Bucket("c", 10),
        ],
        partition_key = "2023|%2550|!|9",
        want = [
            (TIME_COLUMN_NAME, Some(year(2023))),
            ("a", Some(identity("%50"))),
            ("b", None),
            ("c", Some(bucket(9, 10))),
        ]
    );

    test_build_column_values!(
        module_doc_example_7,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
            TemplatePart::Bucket("c", 10),
        ],
        partition_key = "2023|^|!|0",
        want = [
            (TIME_COLUMN_NAME, Some(year(2023))),
            ("a", Some(identity(""))),
            ("b", None),
            ("c", Some(bucket(0, 10))),
        ]
    );

    test_build_column_values!(
        module_doc_example_8,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
            TemplatePart::Bucket("c", 10),
        ],
        partition_key = "2023|BANANAS#|!|!|!",
        want = [
            (TIME_COLUMN_NAME, Some(year(2023))),
            ("a", Some(prefix("BANANAS"))),
            ("b", None),
            ("c", None)
        ]
    );

    test_build_column_values!(
        unicode_code_point_prefix,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
            TemplatePart::Bucket("c", 10),
        ],
        partition_key = "2023|%28%E3%83%8E%E0%B2%A0%E7%9B%8A%E0%B2%A0%29%E3%83%8E%E5%BD%A1%E2%94%BB%E2%94%81%E2%94%BB#|!|!",
        want = [
            (TIME_COLUMN_NAME, Some(year(2023))),
            ("a", Some(prefix("(ノಠ益ಠ)ノ彡┻━┻"))),
            ("b", None),
            ("c", None),
        ]
    );

    test_build_column_values!(
        unicode_grapheme,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        partition_key = "2023|%E0%AE%A8%E0%AE%BF#|!",
        want = [
            (TIME_COLUMN_NAME, Some(year(2023))),
            ("a", Some(prefix("நி"))),
            ("b", None),
        ]
    );

    test_build_column_values!(
        unambiguous,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        partition_key = "2023|is%7Cnot%21ambiguous%2510%23|!",
        want = [
            (TIME_COLUMN_NAME, Some(year(2023))),
            ("a", Some(identity("is|not!ambiguous%10#"))),
            ("b", None),
        ]
    );

    test_build_column_values!(
        datetime_fixed,
        template = [TemplatePart::TimeFormat("foo"),],
        partition_key = "foo",
        want = [(TIME_COLUMN_NAME, None)]
    );

    test_build_column_values!(
        datetime_null,
        template = [TemplatePart::TimeFormat("%Y"),],
        partition_key = "!",
        want = [(TIME_COLUMN_NAME, None)]
    );

    test_build_column_values!(
        datetime_range_y,
        template = [TemplatePart::TimeFormat("%Y"),],
        partition_key = "2023",
        want = [(
            TIME_COLUMN_NAME,
            Some(ColumnValue::Datetime {
                begin: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap(),
                end: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            }),
        )]
    );

    test_build_column_values!(
        datetime_range_y_m,
        template = [TemplatePart::TimeFormat("%Y-%m"),],
        partition_key = "2023-09",
        want = [(
            TIME_COLUMN_NAME,
            Some(ColumnValue::Datetime {
                begin: Utc.with_ymd_and_hms(2023, 9, 1, 0, 0, 0).unwrap(),
                end: Utc.with_ymd_and_hms(2023, 10, 1, 0, 0, 0).unwrap(),
            }),
        )]
    );

    test_build_column_values!(
        datetime_range_y_m_overflow_year,
        template = [TemplatePart::TimeFormat("%Y-%m"),],
        partition_key = "2023-12",
        want = [(
            TIME_COLUMN_NAME,
            Some(ColumnValue::Datetime {
                begin: Utc.with_ymd_and_hms(2023, 12, 1, 0, 0, 0).unwrap(),
                end: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            }),
        )]
    );

    test_build_column_values!(
        datetime_range_y_m_d,
        template = [TemplatePart::TimeFormat("%Y-%m-%d"),],
        partition_key = "2023-09-01",
        want = [(
            TIME_COLUMN_NAME,
            Some(ColumnValue::Datetime {
                begin: Utc.with_ymd_and_hms(2023, 9, 1, 0, 0, 0).unwrap(),
                end: Utc.with_ymd_and_hms(2023, 9, 2, 0, 0, 0).unwrap(),
            }),
        )]
    );

    test_build_column_values!(
        datetime_range_y_m_d_overflow_month,
        template = [TemplatePart::TimeFormat("%Y-%m-%d"),],
        partition_key = "2023-09-30",
        want = [(
            TIME_COLUMN_NAME,
            Some(ColumnValue::Datetime {
                begin: Utc.with_ymd_and_hms(2023, 9, 30, 0, 0, 0).unwrap(),
                end: Utc.with_ymd_and_hms(2023, 10, 1, 0, 0, 0).unwrap(),
            }),
        )]
    );

    test_build_column_values!(
        datetime_range_y_m_d_overflow_year,
        template = [TemplatePart::TimeFormat("%Y-%m-%d"),],
        partition_key = "2023-12-31",
        want = [(
            TIME_COLUMN_NAME,
            Some(ColumnValue::Datetime {
                begin: Utc.with_ymd_and_hms(2023, 12, 31, 0, 0, 0).unwrap(),
                end: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            }),
        )]
    );

    test_build_column_values!(
        datetime_range_d_m_y,
        template = [TemplatePart::TimeFormat("%d-%m-%Y"),],
        partition_key = "01-09-2023",
        want = [(
            TIME_COLUMN_NAME,
            Some(ColumnValue::Datetime {
                begin: Utc.with_ymd_and_hms(2023, 9, 1, 0, 0, 0).unwrap(),
                end: Utc.with_ymd_and_hms(2023, 9, 2, 0, 0, 0).unwrap(),
            }),
        )]
    );

    test_build_column_values!(
        datetime_unsupported_numeric,
        template = [TemplatePart::TimeFormat("%d-%m-%Y-%H"),],
        partition_key = "01-09-2023",
        want = [(TIME_COLUMN_NAME, None,)]
    );

    test_build_column_values!(
        datetime_unsupported_fixed,
        template = [TemplatePart::TimeFormat("%d-%m-%Y-%b"),],
        partition_key = "01-09-2023",
        want = [(TIME_COLUMN_NAME, None,)]
    );

    test_build_column_values!(
        bucket_part_fixture,
        template = [
            TemplatePart::Bucket("a", 41),
            TemplatePart::Bucket("b", 91),
            TemplatePart::Bucket("c", 144)
        ],
        partition_key = "1|2|3",
        want = [
            ("a", Some(bucket(1, 41))),
            ("b", Some(bucket(2, 91))),
            ("c", Some(bucket(3, 144))),
        ]
    );

    #[test]
    #[should_panic]
    fn test_build_column_values_bucket_part_out_of_range_panics() {
        let template = [
            TemplatePart::Bucket("a", 42),
            TemplatePart::Bucket("b", 42),
            TemplatePart::Bucket("c", 42),
        ]
        .into_iter()
        .collect::<Vec<_>>();
        let template = test_table_partition_override(template);

        // normalise the values into a (str, ColumnValue) for the comparison
        let input = String::from("1|1|43");
        let _ = template.column_values(input.as_str()).collect::<Vec<_>>();
    }

    #[test]
    #[should_panic]
    fn test_build_column_values_bucket_part_not_u32_panics() {
        let template = [
            TemplatePart::Bucket("a", 42),
            TemplatePart::Bucket("b", 42),
            TemplatePart::Bucket("c", 42),
        ]
        .into_iter()
        .collect::<Vec<_>>();
        let template = test_table_partition_override(template);

        // normalise the values into a (str, ColumnValue) for the comparison
        let input = String::from("1|1|bananas");
        let _ = template.column_values(input.as_str()).collect::<Vec<_>>();
    }

    test_build_column_values!(
        datetime_not_compact_y_d,
        template = [TemplatePart::TimeFormat("%Y-%d"),],
        partition_key = "2023-01",
        want = [(TIME_COLUMN_NAME, None)]
    );

    test_build_column_values!(
        datetime_not_compact_m,
        template = [TemplatePart::TimeFormat("%m"),],
        partition_key = "01",
        want = [(TIME_COLUMN_NAME, None)]
    );

    test_build_column_values!(
        datetime_not_compact_d,
        template = [TemplatePart::TimeFormat("%d"),],
        partition_key = "01",
        want = [(TIME_COLUMN_NAME, None)]
    );

    test_build_column_values!(
        datetime_range_unimplemented_y_m_d_h,
        template = [TemplatePart::TimeFormat("%Y-%m-%dT%H"),],
        partition_key = "2023-12-31T00",
        want = [(TIME_COLUMN_NAME, None)]
    );

    test_build_column_values!(
        datetime_range_unimplemented_y_m_d_h_m,
        template = [TemplatePart::TimeFormat("%Y-%m-%dT%H:%M"),],
        partition_key = "2023-12-31T00:00",
        want = [(TIME_COLUMN_NAME, None)]
    );

    test_build_column_values!(
        datetime_range_unimplemented_y_m_d_h_m_s,
        template = [TemplatePart::TimeFormat("%Y-%m-%dT%H:%M:%S"),],
        partition_key = "2023-12-31T00:00:00",
        want = [(TIME_COLUMN_NAME, None)]
    );

    test_build_column_values!(
        empty_tag_only,
        template = [TemplatePart::TagValue("a")],
        partition_key = "!",
        want = [("a", None)]
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
    /// You shouldn't be changing the values!
    #[test]
    fn test_default_template_fixture() {
        let ns = NamespacePartitionTemplateOverride::default();
        let got_ns = ns.parts().collect::<Vec<_>>();
        assert_matches!(got_ns.as_slice(), [TemplatePart::TimeFormat("%Y-%m-%d")]);
        let table = TablePartitionTemplateOverride::try_new(None, &ns).unwrap();
        let got_table = table.parts().collect::<Vec<_>>();
        assert_matches!(got_table.as_slice(), [TemplatePart::TimeFormat("%Y-%m-%d")]);
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
    fn custom_table_template_with_time_specified_overrides_namespace_template() {
        let custom_table_template = proto::PartitionTemplate {
            parts: vec![
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue("region".into())),
                },
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::TimeFormat("%Y".into())),
                },
            ],
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

        assert_eq!(table_template.len(), 2);
        assert_eq!(table_template.0.unwrap().inner(), &custom_table_template);
    }

    #[test]
    fn new_time_format_validation_fn_allows_good_input() {
        assert!(validate_new_time_format("%Y-%m-%d is 100%% ok").is_ok());
        assert!(validate_new_time_format("%Y-%m").is_ok());
        assert!(validate_new_time_format("%Y").is_ok());
        assert!(validate_new_time_format("%Y %m %d").is_ok());
        assert!(validate_new_time_format("%d %m %Y").is_ok());
        assert!(validate_new_time_format("in the year %Y").is_ok());
    }

    #[test]
    fn existing_time_format_validation_fn_allows_questionable_serialized_formats() {
        assert!(validate_existing_time_format("%Y wk:%M").is_ok()); // nonsense in use by a customer
        assert!(validate_existing_time_format("%d").is_ok());
    }

    #[test]
    fn existing_time_format_validation_fn_allows_good_formats() {
        assert!(validate_existing_time_format("%Y-%m-%d").is_ok());
        assert!(validate_new_time_format("%Y %m").is_ok());
        assert!(validate_new_time_format("%Y").is_ok());
    }

    #[test]
    fn new_time_format_restricts_pipe_delimiter() {
        assert_matches!(
            validate_new_time_format("%Y|%m|%d"),
            Err(ValidationError::InvalidStrftime(_))
        );
    }

    #[test]
    fn new_time_format_restricts_unsupported_spec() {
        assert_matches!(
            validate_new_time_format("%Y-%m-%d %H:%M:%S"),
            Err(ValidationError::UnsupportedStrfTimeSpecifier(_, _))
        );
    }

    #[test]
    fn new_time_format_restricts_unsupported_spec_customer_example() {
        // Real world example that can't be used in new tables or namespaces because %M is unsupported.
        // Sadly, we can't ensure literals make sense...
        assert_matches!(
            validate_new_time_format("%Y wk:%M"),
            Err(ValidationError::UnsupportedStrfTimeSpecifier(_, _))
        );
    }

    #[test]
    fn new_time_format_restricts_unsupported_spec_in_fixed_category() {
        assert_matches!(
            validate_new_time_format("%Y %B"),
            Err(ValidationError::UnsupportedStrfTimeSpecifier(_, _))
        );
        assert_matches!(
            validate_new_time_format("%Y %#z"),
            Err(ValidationError::UnsupportedStrfTimeSpecifier(_, _))
        );
    }

    #[test]
    fn new_time_format_rejects_missing_specifiers() {
        assert_matches!(
            validate_new_time_format("%m/%d"), // needs %Y
            Err(ValidationError::MissingRequiredStrfTimeSpecifier(_))
        );
        assert_matches!(
            validate_new_time_format("%Y %d"), // needs %m
            Err(ValidationError::MissingRequiredStrfTimeSpecifier(_))
        );
    }

    #[test]
    fn new_time_formats_restrict_invalid() {
        assert_matches!(
            validate_new_time_format(""),
            Err(ValidationError::InvalidStrftime(_))
        );
        assert_matches!(
            validate_new_time_format("no spec"),
            Err(ValidationError::InvalidStrftime(_))
        );
        assert_matches!(
            validate_new_time_format("%Y-%m %Y/%m/%d"),
            Err(ValidationError::InvalidStrftime(_))
        );
    }

    #[test]
    fn new_custom_table_template_requires_time_specified() {
        let custom_table_template = proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TagValue("region".into())),
            }],
        };
        assert_matches!(
            TablePartitionTemplateOverride::try_new(
                Some(custom_table_template.clone()),
                &NamespacePartitionTemplateOverride::default(),
            ),
            Err(ValidationError::TimeFormatPartRequired)
        );
    }

    #[test]
    fn existing_custom_table_template_does_not_require_time_specified() {
        let custom_table_template = proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TagValue("region".into())),
            }],
        };

        let table_template =
            TablePartitionTemplateOverride::try_from_existing(custom_table_template.clone())
                .unwrap();

        assert_eq!(table_template.len(), 1);
        assert_eq!(table_template.0.unwrap().inner(), &custom_table_template);
    }

    #[test]
    fn existing_bad_time_format_deserializes_without_error() {
        let custom_table_template = proto::PartitionTemplate {
            parts: vec![proto::TemplatePart {
                part: Some(proto::template_part::Part::TimeFormat("%Y wk:%M".into())), // nonsense existing format
            }],
        };

        let table_template =
            TablePartitionTemplateOverride::try_from_existing(custom_table_template.clone())
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
                proto::TemplatePart {
                    part: Some(proto::template_part::Part::Bucket(proto::Bucket {
                        tag_name: "bananas".into(),
                        num_buckets: 42,
                    })),
                },
            ],
        };
        let expected_json_str = "{\"parts\":[\
            {\"tagValue\":\"region\"},\
            {\"timeFormat\":\"year-%Y\"},\
            {\"bucket\":{\"tagName\":\"bananas\",\"numBuckets\":42}}\
        ]}";

        let namespace = NamespacePartitionTemplateOverride::try_from(custom_template).unwrap();
        let mut buf = Default::default();
        let _ = <NamespacePartitionTemplateOverride as Encode<'_, sqlx::Sqlite>>::encode_by_ref(
            &namespace, &mut buf,
        );

        fn extract_sqlite_argument_text(
            argument_value: &sqlx::sqlite::SqliteArgumentValue<'_>,
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
        assert_eq!(table.len(), 3);
    }

    #[test]
    fn test_template_size_reporting() {
        const BASE_SIZE: usize = std::mem::size_of::<TablePartitionTemplateOverride>()
            + std::mem::size_of::<proto::PartitionTemplate>();

        let first_string = "^";
        let template =
            TablePartitionTemplateOverride::try_from_existing(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue(first_string.into())),
                }],
            })
            .expect("failed to create table partition template ");

        assert_eq!(
            template.size(),
            BASE_SIZE + std::mem::size_of::<proto::TemplatePart>() + first_string.len()
        );

        let second_string = "region";
        let template =
            TablePartitionTemplateOverride::try_from_existing(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::TagValue(second_string.into())),
                }],
            })
            .expect("failed to create table partition template ");

        assert_eq!(
            template.size(),
            BASE_SIZE + std::mem::size_of::<proto::TemplatePart>() + second_string.len()
        );

        let time_string = "year-%Y";
        let template =
            TablePartitionTemplateOverride::try_from_existing(proto::PartitionTemplate {
                parts: vec![
                    proto::TemplatePart {
                        part: Some(proto::template_part::Part::TagValue(second_string.into())),
                    },
                    proto::TemplatePart {
                        part: Some(proto::template_part::Part::TimeFormat(time_string.into())),
                    },
                ],
            })
            .expect("failed to create table partition template ");
        assert_eq!(
            template.size(),
            BASE_SIZE
                + std::mem::size_of::<proto::TemplatePart>()
                + second_string.len()
                + std::mem::size_of::<proto::TemplatePart>()
                + time_string.len()
        );

        let template =
            TablePartitionTemplateOverride::try_from_existing(proto::PartitionTemplate {
                parts: vec![proto::TemplatePart {
                    part: Some(proto::template_part::Part::Bucket(proto::Bucket {
                        tag_name: second_string.into(),
                        num_buckets: 42,
                    })),
                }],
            })
            .expect("failed to create table partition template");
        assert_eq!(
            template.size(),
            BASE_SIZE
                + std::mem::size_of::<proto::TemplatePart>()
                + second_string.len()
                + std::mem::size_of::<u32>()
        );
    }
}
