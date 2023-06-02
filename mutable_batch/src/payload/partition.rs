//! Functions for partitioning rows from a [`MutableBatch`]
//!
//! The returned ranges can then be used with
//! [`MutableBatch::extend_from_range`].
//!
//! The partitioning template, derived partition key format, and encodings are
//! described in detail in the [`data_types::partition_template`] module.
use std::{borrow::Cow, fmt::Write, ops::Range};

use chrono::{format::StrftimeItems, TimeZone, Utc};
use data_types::partition_template::{
    TablePartitionTemplateOverride, TemplatePart, ENCODED_PARTITION_KEY_CHARS,
    PARTITION_KEY_DELIMITER, PARTITION_KEY_VALUE_EMPTY_STR, PARTITION_KEY_VALUE_NULL_STR,
};
use percent_encoding::utf8_percent_encode;
use schema::{InfluxColumnType, TIME_COLUMN_NAME};
use thiserror::Error;

use crate::{
    column::{Column, ColumnData},
    MutableBatch,
};

/// An error generating a partition key for a row.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PartitionKeyError {
    /// The partition template defines a [`Template::TimeFormat`] part, but the
    /// provided strftime formatter is invalid.
    #[error("invalid strftime format in partition template: {0}")]
    InvalidStrftime(String),

    /// The partition template defines a [`Template::TagValue`] part, but the
    /// column type is not "tag".
    #[error("tag value partitioner does not accept input columns of type {0:?}")]
    TagValueNotTag(InfluxColumnType),

    /// A "catch all" error for when a formatter returns [`std::fmt::Error`],
    /// which contains no context.
    #[error("partition key generation error")]
    FmtError(#[from] std::fmt::Error),
}

/// Returns an iterator identifying consecutive ranges for a given partition key
pub fn partition_batch<'a>(
    batch: &'a MutableBatch,
    template: &'a TablePartitionTemplateOverride,
) -> impl Iterator<Item = (Result<String, PartitionKeyError>, Range<usize>)> + 'a {
    range_encode(partition_keys(batch, template.parts()))
}

/// A [`TablePartitionTemplateOverride`] is made up of one of more
/// [`TemplatePart`]s that are rendered and joined together by
/// [`PARTITION_KEY_DELIMITER`] to form a single partition key.
///
/// To avoid allocating intermediate strings, and performing column lookups for
/// every row, each [`TemplatePart`] is converted to a [`Template`].
///
/// [`Template::fmt_row`] can then be used to render the template for that
/// particular row to the provided string, without performing any additional
/// column lookups
#[derive(Debug)]
enum Template<'a> {
    TagValue(&'a Column),
    TimeFormat(&'a [i64], StrftimeItems<'a>),

    /// This batch is missing a partitioning tag column.
    MissingTag,
}

impl<'a> Template<'a> {
    /// Renders this template to `out` for the row `idx`.
    fn fmt_row<W: std::fmt::Write>(
        &self,
        out: &mut W,
        idx: usize,
    ) -> Result<(), PartitionKeyError> {
        match self {
            Template::TagValue(col) if col.valid.get(idx) => match &col.data {
                ColumnData::Tag(col_data, dictionary, _) => out.write_str(never_empty(
                    Cow::from(utf8_percent_encode(
                        dictionary.lookup_id(col_data[idx]).unwrap(),
                        &ENCODED_PARTITION_KEY_CHARS,
                    ))
                    .as_ref(),
                ))?,
                _ => return Err(PartitionKeyError::TagValueNotTag(col.influx_type())),
            },
            Template::TimeFormat(t, format) => {
                let mut s = String::new();
                write!(
                    s,
                    "{}",
                    Utc.timestamp_nanos(t[idx])
                        .format_with_items(format.clone()) // Cheap clone of refs
                )
                .map_err(|_| PartitionKeyError::InvalidStrftime(format!("{format:?}")))?;

                out.write_str(
                    Cow::from(utf8_percent_encode(
                        s.as_str(),
                        &ENCODED_PARTITION_KEY_CHARS,
                    ))
                    .as_ref(),
                )?
            }
            // Either a tag that has no value for this given row index, or the
            // batch does not contain this tag at all.
            Template::TagValue(_) | Template::MissingTag => {
                out.write_str(PARTITION_KEY_VALUE_NULL_STR)?
            }
        }

        Ok(())
    }
}

/// Return `s` if it is non-empty, else [`PARTITION_KEY_VALUE_EMPTY_STR`].
#[inline(always)]
fn never_empty(s: &str) -> &str {
    if s.is_empty() {
        return PARTITION_KEY_VALUE_EMPTY_STR;
    }
    s
}

/// Returns an iterator of partition keys for the given table batch
fn partition_keys<'a>(
    batch: &'a MutableBatch,
    template_parts: impl Iterator<Item = TemplatePart<'a>>,
) -> impl Iterator<Item = Result<String, PartitionKeyError>> + 'a {
    // Extract the timestamp data.
    let time = match batch.column(TIME_COLUMN_NAME).map(|v| &v.data) {
        Ok(ColumnData::I64(data, _)) => data.as_slice(),
        Ok(v) => unreachable!("incorrect type for time column: {v:?}"),
        Err(e) => panic!("error reading time column: {e:?}"),
    };

    // Convert TemplatePart into an ordered array of Template
    let template = template_parts
        .map(|v| match v {
            TemplatePart::TagValue(col_name) => batch
                .column(col_name)
                .map_or_else(|_| Template::MissingTag, Template::TagValue),
            TemplatePart::TimeFormat(fmt) => Template::TimeFormat(time, StrftimeItems::new(fmt)),
        })
        .collect::<Vec<_>>();

    // Yield a partition key string for each row in `batch`
    (0..batch.row_count).map(move |idx| {
        let mut string = String::new();

        // Evaluate each template part for this row
        for (col_idx, col) in template.iter().enumerate() {
            col.fmt_row(&mut string, idx)?;

            // If this isn't the last element in the template, insert a field
            // delimiter.
            if col_idx + 1 != template.len() {
                string.push(PARTITION_KEY_DELIMITER);
            }
        }

        Ok(string)
    })
}

/// Takes an iterator and merges consecutive elements together
fn range_encode<I>(mut iterator: I) -> impl Iterator<Item = (I::Item, Range<usize>)>
where
    I: Iterator,
    I::Item: Eq,
{
    let mut last: Option<I::Item> = None;
    let mut range: Range<usize> = 0..0;
    std::iter::from_fn(move || loop {
        match (iterator.next(), last.take()) {
            (Some(cur), Some(next)) => match cur == next {
                true => {
                    range.end += 1;
                    last = Some(next);
                }
                false => {
                    let t = range.clone();
                    range.start = range.end;
                    range.end += 1;
                    last = Some(cur);
                    return Some((next, t));
                }
            },
            (Some(cur), None) => {
                range.end += 1;
                last = Some(cur);
            }
            (None, Some(next)) => return Some((next, range.clone())),
            (None, None) => return None,
        }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    use crate::writer::Writer;

    use assert_matches::assert_matches;
    use data_types::partition_template::{build_column_values, test_table_partition_override};
    use proptest::{prelude::*, prop_compose, proptest, strategy::Strategy};
    use rand::prelude::*;

    fn make_rng() -> StdRng {
        let seed = rand::rngs::OsRng::default().next_u64();
        println!("Seed: {seed}");
        StdRng::seed_from_u64(seed)
    }

    /// A fixture test asserting the default partition key format, derived from
    /// the default partition key template.
    #[test]
    fn test_default_fixture() {
        let mut batch = MutableBatch::new();
        let mut writer = Writer::new(&mut batch, 1);

        writer.write_time("time", vec![1].into_iter()).unwrap();
        writer
            .write_tag("region", Some(&[0b00000001]), vec!["bananas"].into_iter())
            .unwrap();
        writer.commit();

        let template_parts = TablePartitionTemplateOverride::new(None, &Default::default());
        let keys: Vec<_> = partition_keys(&batch, template_parts.parts())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(keys, vec!["1970-01-01".to_string()])
    }

    #[test]
    fn test_range_encode() {
        let collected: Vec<_> = range_encode(vec![5, 5, 5, 7, 2, 2, 3].into_iter()).collect();
        assert_eq!(collected, vec![(5, 0..3), (7, 3..4), (2, 4..6), (3, 6..7)])
    }

    #[test]
    fn test_range_encode_fuzz() {
        let mut rng = make_rng();
        let original: Vec<_> = std::iter::from_fn(|| Some(rng.next_u32() % 20))
            .take(1000)
            .collect();

        let rle: Vec<_> = range_encode(original.iter().cloned()).collect();

        let mut last_range = rle[0].1.clone();
        for (_, range) in &rle[1..] {
            assert_eq!(range.start, last_range.end);
            assert_ne!(range.start, range.end);
            last_range = range.clone();
        }

        let hydrated: Vec<_> = rle
            .iter()
            .flat_map(|(v, r)| std::iter::repeat(*v).take(r.end - r.start))
            .collect();

        assert_eq!(original, hydrated)
    }

    #[test]
    fn test_partition() {
        let mut batch = MutableBatch::new();
        let mut writer = Writer::new(&mut batch, 5);

        writer
            .write_time("time", vec![1, 2, 3, 4, 5].into_iter())
            .unwrap();

        writer
            .write_tag(
                "region",
                Some(&[0b00001010]),
                vec!["west", "east"].into_iter(),
            )
            .unwrap();

        let template_parts = [
            TemplatePart::TimeFormat("%Y-%m-%d %H:%M:%S"),
            TemplatePart::TagValue("region"),
            TemplatePart::TagValue("bananas"), // column not present
        ];

        writer.commit();

        let keys: Vec<_> = partition_keys(&batch, template_parts.into_iter())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(
            keys,
            vec![
                "1970-01-01 00:00:00|!|!".to_string(),
                "1970-01-01 00:00:00|west|!".to_string(),
                "1970-01-01 00:00:00|!|!".to_string(),
                "1970-01-01 00:00:00|east|!".to_string(),
                "1970-01-01 00:00:00|!|!".to_string()
            ]
        )
    }

    #[test]
    fn partitioning_on_fields_panics() {
        let mut batch = MutableBatch::new();
        let mut writer = Writer::new(&mut batch, 5);

        writer
            .write_time("time", vec![1, 2, 3, 4, 5].into_iter())
            .unwrap();

        writer
            .write_string(
                "region",
                Some(&[0b00001010]),
                vec!["west", "east"].into_iter(),
            )
            .unwrap();

        let template_parts = [TemplatePart::TagValue("region")];

        writer.commit();

        let got: Result<Vec<_>, _> = partition_keys(&batch, template_parts.into_iter()).collect();
        assert_matches::assert_matches!(got, Err(PartitionKeyError::TagValueNotTag(_)));
    }

    // Generate a test that asserts the derived partition key matches
    // "want_key", when using the provided "template" parts and set of "tags".
    //
    // Additionally validates that the derived key is reversible into the
    // expected set of "want_reversed_tags" from the original inputs.
    macro_rules! test_partition_key {
        (
            $name:ident,
            template = $template:expr,              // Array/vec of TemplatePart
            tags = $tags:expr,                      // Array/vec of (tag_name, value) tuples
            want_key = $want_key:expr,              // Expected partition key string
            want_reversed_tags = $want_reversed_tags:expr // Array/vec of (tag_name, value) reversed from $tags
        ) => {
            paste::paste! {
                #[test]
                fn [<test_partition_key_ $name>]() {
                    let mut batch = MutableBatch::new();
                    let mut writer = Writer::new(&mut batch, 1);

                    let template = $template.into_iter().collect::<Vec<_>>();
                    let template = test_table_partition_override(template);

                    // Timestamp: 2023-05-29T13:03:16Z
                    writer
                        .write_time("time", vec![1685365396931384064].into_iter())
                        .unwrap();

                    for (col, value) in $tags {
                        writer
                            .write_tag(col, Some(&[0b00000001]), vec![value].into_iter())
                            .unwrap();
                    }

                    writer.commit();

                    let keys: Vec<_> = partition_keys(&batch, template.parts()).collect::<Result<Vec<_>, _>>().unwrap();
                    assert_eq!(keys, vec![$want_key.to_string()]);

                    // Reverse the encoding.
                    let reversed = build_column_values(&template, &keys[0]);

                    // normalise the tags into a (str, string) for the comparison
                    let want = $want_reversed_tags
                        .into_iter()
                        .map(|(k, v)| {
                            let v: &str = v;
                            (k, v.to_string())
                        })
                        .collect::<Vec<_>>();

                    let got = reversed
                        .map(|(k, v)| (k, v.to_string()))
                        .collect::<Vec<_>>();
                    assert_eq!(got, want);
                }
            }
        };
    }

    test_partition_key!(
        simple,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        tags = [("a", "bananas"), ("b", "are_good")],
        want_key = "2023|bananas|are_good",
        want_reversed_tags = [("a", "bananas"), ("b", "are_good")]
    );

    test_partition_key!(
        non_ascii,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
        ],
        tags = [("a", "bananas"), ("b", "plátanos")],
        want_key = "2023|bananas|pl%C3%A1tanos",
        want_reversed_tags = [("a", "bananas"), ("b", "plátanos")]
    );

    test_partition_key!(
        single_tag_template_tag_not_present,
        template = [TemplatePart::TagValue("a")],
        tags = [("b", "bananas")],
        want_key = "!",
        want_reversed_tags = []
    );

    test_partition_key!(
        single_tag_template_tag_empty,
        template = [TemplatePart::TagValue("a")],
        tags = [("a", "")],
        want_key = "^",
        want_reversed_tags = [("a", "")]
    );

    test_partition_key!(
        missing_tag,
        template = [TemplatePart::TagValue("a"), TemplatePart::TagValue("b")],
        tags = [("a", "bananas")],
        want_key = "bananas|!",
        want_reversed_tags = [("a", "bananas")]
    );

    test_partition_key!(
        unambiguous,
        template = [
            TemplatePart::TimeFormat("%Y"),
            TemplatePart::TagValue("a"),
            TemplatePart::TagValue("b"),
            TemplatePart::TagValue("c"),
            TemplatePart::TagValue("d"),
            TemplatePart::TagValue("e"),
        ],
        tags = [("a", "|"), ("b", "!"), ("d", "%7C%21%257C"), ("e", "^")],
        want_key = "2023|%7C|%21|!|%257C%2521%25257C|%5E",
        want_reversed_tags = [("a", "|"), ("b", "!"), ("d", "%7C%21%257C"), ("e", "^")]
    );

    /// A test using an invalid strftime format string.
    #[test]
    fn test_invalid_strftime() {
        let mut batch = MutableBatch::new();
        let mut writer = Writer::new(&mut batch, 1);

        writer.write_time("time", vec![1].into_iter()).unwrap();
        writer
            .write_tag("region", Some(&[0b00000001]), vec!["bananas"].into_iter())
            .unwrap();
        writer.commit();

        let template = [TemplatePart::TimeFormat("%3F")]
            .into_iter()
            .collect::<Vec<_>>();
        let template = test_table_partition_override(template);

        let ret = partition_keys(&batch, template.parts()).collect::<Result<Vec<_>, _>>();

        assert_matches!(ret, Err(PartitionKeyError::InvalidStrftime(_)));
    }

    // These values are arbitrarily chosen when building an input to the
    // partitioner.

    // Arbitrary tag names are selected from this set of candidates (to ensure
    // there's always some overlap, rather than truly random strings).
    const TEST_TAG_NAME_SET: &[&str] = &["A", "B", "C", "D", "E", "F"];

    // Arbitrary template parts are selected from this set.
    const TEST_TEMPLATE_PARTS: &[TemplatePart<'static>] = &[
        TemplatePart::TimeFormat("%Y|%m|%d!-string"),
        TemplatePart::TimeFormat("%Y|%m|%d!-%%bananas"),
        TemplatePart::TimeFormat("%Y/%m/%d"),
        TemplatePart::TimeFormat("%Y-%m-%d"),
        TemplatePart::TagValue(""),
        TemplatePart::TagValue("A"),
        TemplatePart::TagValue("B"),
        TemplatePart::TagValue("C"),
        TemplatePart::TagValue("tags!"),
        TemplatePart::TagValue("%tags!"),
        TemplatePart::TagValue("my_tag"),
        TemplatePart::TagValue("my|tag"),
        TemplatePart::TagValue("%%%%|!!!!|"),
    ];

    prop_compose! {
        /// Yields a vector of up to 12 unique template parts, chosen from
        /// [`TEST_TEMPLATE_PARTS`].
        fn arbitrary_template_parts()(set in proptest::collection::vec(
                proptest::sample::select(TEST_TEMPLATE_PARTS),
                (0, 12) // Set size range
            )) -> Vec<TemplatePart<'static>> {
            let mut set = set;
            set.dedup_by(|a, b| format!("{a:?}") == format!("{b:?}"));
            set
        }
    }

    prop_compose! {
        /// Yield a HashMap of between 1 and 10 (column_name, random string
        /// value) with tag names chosen from [`TEST_TAG_NAME_SET`].
        fn arbitrary_tag_value_map()(v in proptest::collection::hash_map(
                proptest::sample::select(TEST_TAG_NAME_SET).prop_map(ToString::to_string),
                any::<String>(),
                (1, 10) // Set size range
            )) -> HashMap<String, String> {
            v
        }
    }

    proptest! {
        /// A property test that asserts a write comprised of an arbitrary
        /// subset of [`TEST_TAG_NAME_SET`] with randomised values, that is
        /// partitioned using a partitioning template arbitrarily selected from
        /// [`TEST_TEMPLATE_PARTS`], can be reversed to the full set of tags via
        /// [`build_column_values()`].
        #[test]
        fn prop_reversible_mapping(template in arbitrary_template_parts(), tag_values in arbitrary_tag_value_map()) {
            let mut batch = MutableBatch::new();
            let mut writer = Writer::new(&mut batch, 1);

            let template = template.clone().into_iter().collect::<Vec<_>>();
            let template = test_table_partition_override(template);

            // Timestamp: 2023-05-29T13:03:16Z
            writer
                .write_time("time", vec![1685365396931384064].into_iter())
                .unwrap();

            for (col, value) in &tag_values {
                writer
                    .write_tag(col.as_str(), Some(&[0b00000001]), vec![value.as_str()].into_iter())
                    .unwrap();
            }

            writer.commit();
            let keys: Vec<_> = partition_keys(&batch, template.parts())
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(keys.len(), 1);

            // Reverse the encoding.
            let reversed = build_column_values(&template, &keys[0]).map(|(k, v)| (k, v.to_string())).collect::<Vec<_>>();

            // Build the expected set of reversed tags by filtering out any
            // NULL tags (preserving empty string values).
            let want_reversed = template.parts().filter_map(|v| match v {
                TemplatePart::TagValue(col_name) if tag_values.contains_key(col_name) => {
                    // This tag had a (potentially empty) value wrote and should
                    // appear in the reversed output.
                    Some((col_name, tag_values.get(col_name).unwrap().to_string()))
                }
                _ => None,
            }).collect::<Vec<_>>();

            assert_eq!(reversed, want_reversed);
        }

        /// A property test that asserts the partitioner tolerates (does not
        /// panic) randomised, potentially invalid strfitme formatter strings.
        #[test]
        fn prop_arbitrary_strftime_format(fmt in any::<String>()) {
            let mut batch = MutableBatch::new();
            let mut writer = Writer::new(&mut batch, 1);

            // Generate a single time-based partitioning template with a
            // randomised format string.
            let template = vec![
                TemplatePart::TimeFormat(&fmt),
            ];
            let template = test_table_partition_override(template);

            // Timestamp: 2023-05-29T13:03:16Z
            writer
                .write_time("time", vec![1685365396931384064].into_iter())
                .unwrap();

            writer
                .write_tag("bananas", Some(&[0b00000001]), vec!["great"].into_iter())
                .unwrap();

            writer.commit();
            let ret = partition_keys(&batch, template.parts()).collect::<Result<Vec<_>, _>>();

            // The is allowed to succeed or fail under this test (but not
            // panic), and the returned error/value must match certain
            // properties:
            match ret {
                Ok(v) => { assert_eq!(v.len(), 1); },
                Err(e) => { assert_matches!(e, PartitionKeyError::InvalidStrftime(_)); },
            }
        }
    }
}
