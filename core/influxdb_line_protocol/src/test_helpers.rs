use std::{collections::HashSet, ops::Range};

use proptest::{option, prelude::*};
use schema::InfluxFieldType;

use crate::{EscapedStr, FieldSet, FieldValue, Measurement, ParsedLine, Series, TagSet};

/// Define the behaviour of [`arbitrary_field_set`].
#[derive(Debug, Clone, Copy)]
pub enum FieldNames {
    /// Generate field names that are consistent with the field type,
    /// ensuring type errors rarely occur (string can generate `T` or `F`
    /// which are interpreted as booleans).
    ConsistentWithTypes,

    /// Generate completely random field names.
    Random,
}

/// Generate an arbitrary tag column name and value tuple.
pub fn arbitrary_tag() -> impl Strategy<Value = (EscapedStr<'static>, EscapedStr<'static>)> {
    prop_oneof![
        any::<(String, String)>()
            .prop_map(|(tag, val)| (EscapedStr::CopiedValue(tag), EscapedStr::CopiedValue(val))),
        // Generate from a small set of tags to increase collisions.
        (0..10, any::<String>()).prop_map(|(tag, val)| (
            EscapedStr::CopiedValue(format!("tag_{tag}")),
            EscapedStr::CopiedValue(val)
        )),
    ]
}

/// Generate an arbitrary timestamp.
pub fn arbitrary_timestamp() -> impl Strategy<Value = i64> {
    // March 22, 2022, at 09:43:59 AM UTC, written in nanoseconds since unix epoch. Not quite sure
    // why this time was chosen as the base.
    const BASE: i64 = 1647942239000000000;

    prop_oneof![
        // Small sample set of 20.
        2 => BASE..(BASE + 20),
        // 1 day of timestamps.
        2 => BASE..(BASE + (24 * 60 * 60)),
        // Pure random.
        1 => any::<i64>(),
    ]
}

/// Generate an arbitrary table name.
pub fn arbitrary_table_name() -> impl Strategy<Value = String> {
    prop_oneof![any::<String>(), "bananas", "platanos"].prop_map(String::from)
}

/// Generate a [`FieldValue`] of arbitrary type.
pub fn arbitrary_field() -> impl Strategy<Value = FieldValue<'static>> {
    prop_oneof![
        (0_u64..1030).prop_map(FieldValue::U64),
        (0_i64..1030).prop_map(FieldValue::I64),
        (0.0_f64..1030.0).prop_map(FieldValue::F64),
        arbitrary_table_name().prop_map(|v| FieldValue::String(EscapedStr::CopiedValue(v))),
        any::<bool>().prop_map(FieldValue::Boolean),
    ]
}

/// Generate an arbitrary set of named fields.
///
/// The generation of field names with respect to their values is
/// dictated by the [`FieldNames`] argument provided.
pub fn arbitrary_field_set(type_errors: FieldNames) -> impl Strategy<Value = FieldSet<'static>> {
    let generated = arbitrary_field().prop_flat_map(move |v| {
        let type_name = Just(match &v {
            FieldValue::U64(_) => "col_u64",
            FieldValue::I64(_) => "col_i64",
            FieldValue::F64(_) => "col_f64",
            FieldValue::String(_) => "col_string",
            FieldValue::Boolean(_) => "col_boolean",
        })
        .prop_map(|v| EscapedStr::CopiedValue(v.to_string()));

        if matches!(type_errors, FieldNames::ConsistentWithTypes) {
            return (type_name.boxed(), Just(v));
        }

        let name = prop_oneof![
            5 => type_name,
            5 => "col_[0-9]".prop_map(|v| EscapedStr::CopiedValue(v.to_string())),
            3 => any::<String>().prop_map(|v| EscapedStr::CopiedValue(v.to_string())),
            1 => Just(EscapedStr::CopiedValue("time".to_string())),
        ];

        (name.boxed(), Just(v))
    });

    prop::collection::vec(generated, 1..10).prop_map(FieldSet::from)
}

prop_compose! {
    /// Generate a [`TagSet`] of arbitrary tag columns and values.
    pub fn arbitrary_tag_set()(
        tuples in prop::collection::vec(arbitrary_tag(), 0..10),
    ) -> TagSet<'static> {
        tuples.into_iter().collect()
    }
}

prop_compose! {
    /// Generate an arbitrary [`Series`] for an arbitrary table name, with a
    /// collection of arbitrary tags.
    ///
    /// The `raw_input` is a static dummy value.
    pub fn arbitrary_series()(
        table_name in arbitrary_table_name(),
        tag_set in option::of(arbitrary_tag_set()),
    ) -> Series<'static> {
        Series {
            raw_input: "bananas", // Ignored
            measurement: Measurement::CopiedValue(table_name),
            tag_set
        }
    }
}

/// Generate a [`ParsedLine`] with randomly generated timestamps (or lack
/// of).
pub fn arbitrary_parsed_line(
    series: impl Strategy<Value = Series<'static>>,
    field_set: impl Strategy<Value = FieldSet<'static>>,
) -> impl Strategy<Value = ParsedLine<'static>> {
    let ts = option::of(arbitrary_timestamp());

    (ts, field_set, series).prop_map(|(timestamp, field_set, series)| ParsedLine {
        series,
        field_set,
        timestamp,
    })
}

prop_compose! {
    pub fn arbitrary_parsed_lines_same_series()(
        series in arbitrary_series(),
        field_and_times in proptest::collection::vec(
            (arbitrary_field_set(FieldNames::ConsistentWithTypes), arbitrary_timestamp()),
            0..20,
        )
    ) -> Vec<ParsedLine<'static>> {
        field_and_times.into_iter().map(|(field_set, timestamp)| ParsedLine {
            series: series.clone(),
            field_set,
            timestamp: Some(timestamp)
        }).collect()
    }
}

fn arbitrary_field_type() -> impl Strategy<Value = InfluxFieldType> {
    prop_oneof![
        Just(InfluxFieldType::Float),
        Just(InfluxFieldType::Integer),
        Just(InfluxFieldType::UInteger),
        Just(InfluxFieldType::Boolean),
        Just(InfluxFieldType::String),
    ]
}

pub fn arbitrary_nonempty_range_within(high_bound: usize) -> impl Strategy<Value = Range<usize>> {
    (0..high_bound).prop_flat_map(move |bottom| {
        (bottom + 1..high_bound + 1).prop_map(move |high| bottom..high)
    })
}

pub fn arbitrary_friendly_string() -> impl Strategy<Value = String> {
    prop_oneof![
        "attribute",
        "value",
        "quality",
        "price",
        "rating",
        "range",
        "size",
        "length",
        "width",
        "speed",
        "GHz",
        "temperature",
        "duration",
        "longitude",
        "latitude",
        "sturdiness",
        "weakness",
        "element",
        "variant",
        "brightness",
        "lifetime",
        "battery",
        "round",
        "big",
        "scary",
        "tiny",
        "foolish",
        "spicy",
        "adjective",
        "exuberant",
        "happy",
        "tired",
        "lazy",
        "whatever",
        "morose",
        "evil",
        "scheming",
        "delicious",
        "bad",
        "meh",
        "mid"
    ]
}

fn arbitrary_escaped_str() -> impl Strategy<Value = EscapedStr<'static>> {
    arbitrary_friendly_string().prop_map(EscapedStr::CopiedValue)
}

/// Create a strategy for parsed lines, using the given input parameters. Used from inside
/// `arbitrary_parsed_lines_same_table`.
fn parsed_lines_with_input(
    // Each line must have at least one field, so this is used to determine exactly which index
    // must be non-null for each line
    guaranteed_valid_field_idxs: Vec<usize>,
    // the names of the columns - both the tags and the fields
    col_names: HashSet<EscapedStr<'static>>,
    // how many of `col_names` should be taken to be tags. The rest will be used for fields
    tag_count: usize,
    // The types of the fields. The length of this vec should be `col_names.len() - tag_count`
    field_types: Vec<InfluxFieldType>,
) -> impl Strategy<Value = Vec<ParsedLine<'static>>> {
    let mut strats = Vec::with_capacity(guaranteed_valid_field_idxs.len());
    for guaranteed_field in guaranteed_valid_field_idxs {
        let mut column_names = col_names.clone().into_iter();

        let tags = column_names
            .by_ref()
            .take(tag_count)
            .map(|name| option::of(arbitrary_escaped_str().prop_map(move |v| (name.clone(), v))))
            .collect::<Vec<_>>();

        let fields = column_names
            .zip(field_types.iter())
            .enumerate()
            .map(move |(idx, (name, ty))| {
                let strat = match ty {
                    InfluxFieldType::Float => any::<f64>()
                        .prop_map(move |f| (name.clone(), FieldValue::F64(f)))
                        .boxed(),
                    InfluxFieldType::Integer => any::<i64>()
                        .prop_map(move |i| (name.clone(), FieldValue::I64(i)))
                        .boxed(),
                    InfluxFieldType::UInteger => any::<u64>()
                        .prop_map(move |u| (name.clone(), FieldValue::U64(u)))
                        .boxed(),
                    InfluxFieldType::Boolean => any::<bool>()
                        .prop_map(move |b| (name.clone(), FieldValue::Boolean(b)))
                        .boxed(),
                    InfluxFieldType::String => arbitrary_escaped_str()
                        .prop_map(move |s| (name.clone(), FieldValue::String(s)))
                        .boxed(),
                };

                // no we can't just put 1.0 for the probably of option::weighted, it
                // panics if you do that.
                if idx == guaranteed_field {
                    strat.prop_map(Some).boxed()
                } else {
                    option::weighted(0.75, strat).boxed()
                }
            })
            .collect::<Vec<_>>();

        let to_push = (tags, fields, any::<i64>()).prop_map(move |(tags, fields, time)| {
            let tags = tags.into_iter().flatten().collect::<TagSet<'_>>();
            let fields = fields.into_iter().flatten().collect::<FieldSet<'_>>();

            ParsedLine {
                series: Series {
                    raw_input: "",
                    measurement: Measurement::SingleSlice("test_table"),
                    tag_set: Some(tags),
                },
                field_set: fields,
                timestamp: Some(time),
            }
        });

        strats.push(to_push);
    }

    strats
}

/// Generate parsed lines that all belong to the same table/measurement and follow the general
/// rules of types and 'no duplicated fields of different types' and 'each line must have at
/// least one value' and such
pub fn arbitrary_parsed_lines_same_table() -> impl Strategy<Value = Vec<ParsedLine<'static>>> {
    (
        // Need to do at least 24 for the top bound so that we have a chance to get the full 3
        // bytes of the null mask
        2usize..24,
        0usize..3,
        1usize..8,
    )
        .prop_flat_map(|(row_count, tag_count, field_count)| {
            (
                proptest::collection::hash_set(arbitrary_escaped_str(), tag_count + field_count),
                proptest::collection::vec(arbitrary_field_type(), field_count),
                proptest::collection::vec(0..field_count, row_count),
            )
                .prop_flat_map(
                    move |(col_names, field_types, guaranteed_valid_field_idxs)| {
                        parsed_lines_with_input(
                            guaranteed_valid_field_idxs,
                            col_names,
                            tag_count,
                            field_types,
                        )
                    },
                )
        })
}
