//! Functions for filtering rows from a [`MutableBatch`]
//!
//! The returned ranges can then be used with `MutableBatch::extend_from_range`

use crate::column::ColumnData;
use crate::MutableBatch;
use schema::TIME_COLUMN_NAME;
use std::ops::Range;

/// Given a [`MutableBatch`] a time predicate and a set of row ranges, returns the row
/// indexes that pass the predicate
///
/// # Panic
///
/// Panics if `batch` does not contain a time column of the correct type
pub fn filter_time<'a, F>(
    batch: &'a MutableBatch,
    ranges: &'a [Range<usize>],
    mut predicate: F,
) -> Vec<Range<usize>>
where
    F: FnMut(i64) -> bool,
{
    let col_idx = *batch
        .column_names
        .get(TIME_COLUMN_NAME)
        .expect("time column");

    let col = &batch.columns[col_idx];
    let col_data = match &col.data {
        ColumnData::I64(col_data, _) => col_data,
        x => unreachable!("expected i64 got {} for time column", x),
    };

    // Time column is not nullable so can skip checking mask
    let mut ret = vec![];
    for range in ranges {
        let offset = range.start;
        ret.extend(
            filter_slice(&col_data[range.clone()], &mut predicate)
                .map(|r| (r.start + offset)..(r.end + offset)),
        )
    }
    ret
}

fn filter_slice<'a, T, F>(
    col_data: &'a [T],
    predicate: &'a mut F,
) -> impl Iterator<Item = Range<usize>> + 'a
where
    T: Copy,
    F: 'a + FnMut(T) -> bool,
{
    let mut range: Range<usize> = 0..0;
    let mut values = col_data.iter();

    std::iter::from_fn(move || loop {
        match values.next() {
            Some(value) if predicate(*value) => {
                range.end += 1;
                continue;
            }
            // Either finished or predicate failed
            _ if range.start != range.end => {
                let t = range.clone();
                range.end += 1;
                range.start = range.end;
                return Some(t);
            }
            // Predicate failed and start == end
            Some(_) => {
                range.start += 1;
                range.end += 1;
            }
            None => return None,
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
    fn test_filter_slice() {
        let collected: Vec<_> =
            filter_slice(&[0, 1, 2, 3, 4, 5, 6], &mut |x| x != 1 && x != 4).collect();
        assert_eq!(collected, vec![0..1, 2..4, 5..7]);

        let collected: Vec<_> =
            filter_slice(&[0, 1, 2, 3, 4, 5, 6], &mut |x| x == 1 || x == 2 || x == 6).collect();
        assert_eq!(collected, vec![1..3, 6..7])
    }

    #[test]
    fn test_filter_fuzz() {
        let mut rng = make_rng();
        let data: Vec<_> = std::iter::from_fn(|| Some(rng.next_u32()))
            .take(1000)
            .collect();

        let mut predicate = |x: u32| x & 1 == 0;

        let indexes: Vec<_> = filter_slice(&data, &mut predicate).flatten().collect();

        let expected: Vec<_> = data
            .iter()
            .enumerate()
            .filter_map(|(idx, x)| match predicate(*x) {
                true => Some(idx),
                false => None,
            })
            .collect();

        assert_eq!(indexes, expected);
    }

    #[test]
    fn test_filter_batch() {
        let mut batch = MutableBatch::new();
        let mut rng = make_rng();
        let data: Vec<_> = std::iter::from_fn(|| Some(rng.next_u32() as i64))
            .take(1000)
            .collect();

        let ranges = &[0..87, 90..442, 634..800];
        let mut predicate = |x: i64| x & 1 == 0;

        let mut writer = Writer::new(&mut batch, 1000);
        writer.write_time("time", data.iter().cloned()).unwrap();
        writer.commit();

        let actual: Vec<_> = filter_time(&batch, ranges, &mut predicate)
            .into_iter()
            .flatten()
            .collect();

        let expected: Vec<_> = ranges
            .iter()
            .flat_map(|r| r.clone())
            .filter(|idx| predicate(data[*idx]))
            .collect();

        assert_eq!(actual, expected);
    }
}
