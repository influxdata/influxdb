//! Functions for partitioning rows from a [`MutableBatch`]
//!
//! The returned ranges can then be used with [`MutableBatch::extend_from_range`]

use crate::{
    column::{Column, ColumnData},
    MutableBatch,
};
use chrono::{format::StrftimeItems, TimeZone, Utc};
use data_types::{PartitionTemplate, TemplatePart};
use schema::TIME_COLUMN_NAME;
use std::ops::Range;

/// Returns an iterator identifying consecutive ranges for a given partition key
pub fn partition_batch<'a>(
    batch: &'a MutableBatch,
    table_name: &'a str,
    template: &'a PartitionTemplate,
) -> impl Iterator<Item = (String, Range<usize>)> + 'a {
    range_encode(partition_keys(batch, table_name, template))
}

/// A [`PartitionTemplate`] is made up of one of more [`TemplatePart`] that are rendered and
/// joined together by hyphens to form a single partition key
///
/// To avoid allocating intermediate strings, and performing column lookups for every row,
/// each [`TemplatePart`] is converted to a [`Template`].
///
/// [`Template::fmt_row`] can then be used to render the template for that particular row
/// to the provided string, without performing any additional column lookups
enum Template<'a> {
    Table(&'a str),
    Column(&'a Column, &'a str),
    MissingColumn(&'a str),
    TimeFormat(&'a [i64], StrftimeItems<'a>),
}

impl<'a> Template<'a> {
    /// Renders this template to `out` for the row `idx`
    fn fmt_row<W: std::fmt::Write>(&self, out: &mut W, idx: usize) -> std::fmt::Result {
        match self {
            Template::Table(table_name) => write!(out, "{table_name}"),
            Template::Column(col, col_name) if col.valid.get(idx) => {
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
            Template::Column(_, col_name) | Template::MissingColumn(col_name) => {
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
    table_name: &'a str,
    template: &'a PartitionTemplate,
) -> impl Iterator<Item = String> + 'a {
    let time = batch.column(TIME_COLUMN_NAME).expect("time column");
    let time = match &time.data {
        ColumnData::I64(col_data, _) => col_data.as_slice(),
        x => unreachable!("expected i32 for time got {}", x),
    };

    let cols: Vec<_> = template
        .parts
        .iter()
        .map(|part| match part {
            TemplatePart::Table => Template::Table(table_name),
            TemplatePart::Column(name) => batch.column(name).map_or_else(
                |_| Template::MissingColumn(name),
                |col| Template::Column(col, name),
            ),
            TemplatePart::TimeFormat(fmt) => Template::TimeFormat(time, StrftimeItems::new(fmt)),
            TemplatePart::RegexCapture(_) => unimplemented!(),
            TemplatePart::StrftimeColumn(_) => unimplemented!(),
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
            .write_f64("f64", None, vec![2., 4.5, 6., 3., 6.].into_iter())
            .unwrap();

        writer
            .write_tag(
                "region",
                Some(&[0b00001010]),
                vec!["west", "east"].into_iter(),
            )
            .unwrap();

        let template = PartitionTemplate {
            parts: vec![
                TemplatePart::Table,
                TemplatePart::TimeFormat("%Y-%m-%d %H:%M:%S".to_string()),
                TemplatePart::Column("f64".to_string()),
                TemplatePart::Column("region".to_string()),
                TemplatePart::Column("bananas".to_string()),
            ],
        };

        writer.commit();

        let keys: Vec<_> = partition_keys(&batch, "foo", &template).collect();

        assert_eq!(
            keys,
            vec![
                "foo-1970-01-01 00:00:00-f64_2-region-bananas".to_string(),
                "foo-1970-01-01 00:00:00-f64_4.5-region_west-bananas".to_string(),
                "foo-1970-01-01 00:00:00-f64_6-region-bananas".to_string(),
                "foo-1970-01-01 00:00:00-f64_3-region_east-bananas".to_string(),
                "foo-1970-01-01 00:00:00-f64_6-region-bananas".to_string()
            ]
        )
    }
}
