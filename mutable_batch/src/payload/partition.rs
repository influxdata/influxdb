//! Functions for partitioning rows from a [`MutableBatch`]
//!
//! The returned ranges can then be used with [`MutableBatch::extend_from_range`]

use crate::{
    column::{Column, ColumnData},
    MutableBatch,
};
use chrono::{format::StrftimeItems, TimeZone, Utc};
use data_types::{TablePartitionTemplateOverride, TemplatePart};
use schema::{InfluxColumnType, TIME_COLUMN_NAME};
use std::ops::Range;

/// Returns an iterator identifying consecutive ranges for a given partition key
pub fn partition_batch<'a>(
    batch: &'a MutableBatch,
    template: &'a TablePartitionTemplateOverride,
) -> impl Iterator<Item = (String, Range<usize>)> + 'a {
    range_encode(partition_keys(batch, template.parts()))
}

/// A [`TablePartitionTemplateOverride`] is made up of one of more [`TemplatePart`]s that are
/// rendered and joined together by hyphens to form a single partition key.
///
/// To avoid allocating intermediate strings, and performing column lookups for every row,
/// each [`TemplatePart`] is converted to a [`Template`].
///
/// [`Template::fmt_row`] can then be used to render the template for that particular row
/// to the provided string, without performing any additional column lookups
enum Template<'a> {
    TagValue(&'a Column, &'a str),
    MissingTag(&'a str),
    TimeFormat(&'a [i64], StrftimeItems<'a>),
}

impl<'a> Template<'a> {
    /// Renders this template to `out` for the row `idx`
    fn fmt_row<W: std::fmt::Write>(&self, out: &mut W, idx: usize) -> std::fmt::Result {
        match self {
            Template::TagValue(col, col_name) if col.valid.get(idx) => {
                out.write_str(col_name)?;
                out.write_char('_')?;
                match &col.data {
                    ColumnData::F64(col_data, _) => write!(out, "{}", col_data[idx]),
                    ColumnData::I64(col_data, _) => write!(out, "{}", col_data[idx]),
                    ColumnData::U64(col_data, _) => write!(out, "{}", col_data[idx]),
                    ColumnData::String(col_data, _) => {
                        write!(out, "{}", col_data.get(idx).unwrap())
                    }
                    ColumnData::Bool(col_data, _) => match col_data.get(idx) {
                        true => out.write_str("true"),
                        false => out.write_str("false"),
                    },
                    ColumnData::Tag(col_data, dictionary, _) => {
                        out.write_str(dictionary.lookup_id(col_data[idx]).unwrap())
                    }
                }
            }
            Template::TagValue(_, col_name) | Template::MissingTag(col_name) => {
                out.write_str(col_name)
            }
            Template::TimeFormat(t, format) => {
                let formatted = Utc
                    .timestamp_nanos(t[idx])
                    .format_with_items(format.clone());
                write!(out, "{formatted}")
            }
        }
    }
}

/// Returns an iterator of partition keys for the given table batch
fn partition_keys<'a>(
    batch: &'a MutableBatch,
    template_parts: impl Iterator<Item = TemplatePart<'a>>,
) -> impl Iterator<Item = String> + 'a {
    let time = batch.column(TIME_COLUMN_NAME).expect("time column");
    let time = match &time.data {
        ColumnData::I64(col_data, _) => col_data.as_slice(),
        x => unreachable!("expected i32 for time got {}", x),
    };

    let cols: Vec<_> = template_parts
        .map(|part| match part {
            TemplatePart::TagValue(name) => batch.column(name).map_or_else(
                |_| Template::MissingTag(name),
                |col| match col.influx_type {
                    InfluxColumnType::Tag => Template::TagValue(col, name),
                    other => panic!(
                        "Partitioning only works on tag columns, \
                            but column `{name}` was type `{other:?}`"
                    ),
                },
            ),
            TemplatePart::TimeFormat(fmt) => Template::TimeFormat(time, StrftimeItems::new(fmt)),
        })
        .collect();

    (0..batch.row_count).map(move |idx| {
        let mut string = String::new();
        for (col_idx, col) in cols.iter().enumerate() {
            col.fmt_row(&mut string, idx)
                .expect("string writing is infallible");

            if col_idx + 1 != cols.len() {
                string.push('-');
            }
        }
        string
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
    use super::*;
    use crate::writer::Writer;
    use rand::prelude::*;

    fn make_rng() -> StdRng {
        let seed = rand::rngs::OsRng::default().next_u64();
        println!("Seed: {seed}");
        StdRng::seed_from_u64(seed)
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
            TemplatePart::TagValue("bananas"),
        ];

        writer.commit();

        let keys: Vec<_> = partition_keys(&batch, template_parts.into_iter()).collect();

        assert_eq!(
            keys,
            vec![
                "1970-01-01 00:00:00-region-bananas".to_string(),
                "1970-01-01 00:00:00-region_west-bananas".to_string(),
                "1970-01-01 00:00:00-region-bananas".to_string(),
                "1970-01-01 00:00:00-region_east-bananas".to_string(),
                "1970-01-01 00:00:00-region-bananas".to_string()
            ]
        )
    }

    #[test]
    #[should_panic(
        expected = "Partitioning only works on tag columns, but column `region` was type \
        `Field(String)`"
    )]
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

        let _keys: Vec<_> = partition_keys(&batch, template_parts.into_iter()).collect();
    }
}
