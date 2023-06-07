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
//! ### Reserved Characters
//!
//! Reserved characters that are percent encoded (in addition to non-printable
//! characters), and their meaning:
//!
//!   * `|` - partition key part delimiter ([`PARTITION_KEY_DELIMITER`])
//!   * `!` - NULL/missing partition key part ([`PARTITION_KEY_VALUE_NULL`])
//!   * `^` - empty string partition key part ([`PARTITION_KEY_VALUE_EMPTY`])
//!   * `%` - required for unambiguous reversal of percent encoding
//!
//! These characters are defined in [`ENCODED_PARTITION_KEY_CHARS`] and chosen
//! due to their low likelihood of occurrence in user-provided column values.
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
//!   * `time=2023-01-01, a=bananas, b=plátanos`   -> `2023|bananas|plátanos
//!   * `time=2023-01-01, b=plátanos`              -> `2023|!|plátanos`
//!   * `time=2023-01-01, another=cat, b=plátanos` -> `2023|!|plátanos`
//!   * `time=2023-01-01`                          -> `2023|!|!`
//!   * `time=2023-01-01, a=cat|dog, b=!`          -> `2023|cat%7Cdog|%21`
//!   * `time=2023-01-01, a=%50`                   -> `2023|%2550|!`
//!   * `time=2023-01-01, a=`                      -> `2023|^|!`
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

/// TODO: Actually validate
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

/// The minimal set of characters that must be encoded during partition key
/// generation when they form part of a partition key part, in order to be
/// unambiguously reversible.
///
/// See module-level documentation & [`build_column_values()`].
pub const ENCODED_PARTITION_KEY_CHARS: AsciiSet = CONTROLS
    .add(PARTITION_KEY_DELIMITER as u8)
    .add(PARTITION_KEY_VALUE_NULL as u8)
    .add(PARTITION_KEY_VALUE_EMPTY as u8)
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
#[derive(Debug, PartialEq, Clone, Default, sqlx::Type)]
#[sqlx(transparent)]
pub struct NamespacePartitionTemplateOverride(Option<serialization::Wrapper>);

impl TryFrom<proto::PartitionTemplate> for NamespacePartitionTemplateOverride {
    type Error = ValidationError;

    fn try_from(partition_template: proto::PartitionTemplate) -> Result<Self, Self::Error> {
        Ok(Self(Some(serialization::Wrapper::try_from(
            partition_template,
        )?)))
    }
}

/// A partition template specified by a table record.
#[derive(Debug, PartialEq, Clone, Default, sqlx::Type)]
#[sqlx(transparent)]
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
}

/// This manages the serialization/deserialization of the `proto::PartitionTemplate` type to and
/// from the database through `sqlx` for the `NamespacePartitionTemplateOverride` and
/// `TablePartitionTemplateOverride` types. It's an internal implementation detail to minimize code
/// duplication.
mod serialization {
    use super::{ValidationError, MAXIMUM_NUMBER_OF_TEMPLATE_PARTS};
    use chrono::{format::StrftimeItems, Utc};
    use generated_types::influxdata::iox::partition_template::v1 as proto;
    use std::{fmt::Write, sync::Arc};

    #[derive(Debug, Clone, PartialEq)]
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

            // All time formats must be valid
            for part in &partition_template.parts {
                if let Some(proto::template_part::Part::TimeFormat(fmt)) = &part.part {
                    // Empty is not a valid time format
                    if fmt.is_empty() {
                        return Err(ValidationError::InvalidStrftime(fmt.into()));
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
) -> impl Iterator<Item = (&'a str, Cow<'a, str>)> {
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
            (
                name,
                percent_decode_str(value)
                    .decode_utf8()
                    .expect("invalid partition key part encoding"),
            )
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

                    // normalise the values into a (str, string) for the comparison
                    let want = $want
                        .into_iter()
                        .map(|(k, v)| {
                            let v: &str = v;
                            (k, v.to_string())
                        })
                        .collect::<Vec<_>>();

                    let got = build_column_values(&template, $partition_key)
                        .map(|(k, v)| (k, v.to_string()))
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
        want = [("a", "bananas"), ("b", "plátanos")]
    );

    test_build_column_values!(
        module_doc_example_2,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        partition_key = "2023|!|plátanos",
        want = [("b", "plátanos")]
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
        want = [("a", "cat|dog"), ("b", "!")]
    );

    test_build_column_values!(
        module_doc_example_6,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        partition_key = "2023|%2550|!",
        want = [("a", "%50")]
    );

    test_build_column_values!(
        unambiguous,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        partition_key = "2023|is%7Cnot%21ambiguous%2510|!",
        want = [("a", "is|not!ambiguous%10")]
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
    fn test_empty_partition_key_char_str_equality() {
        assert_eq!(
            PARTITION_KEY_VALUE_EMPTY.to_string(),
            PARTITION_KEY_VALUE_EMPTY_STR
        );
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
    }
}
