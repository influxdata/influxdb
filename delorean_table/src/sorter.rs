//! The sorter module provides a sort function which will sort a collection of
//! `Packer` columns by arbitrary columns. All sorting is done in ascending
//! order.
//!
//! `sorter::sort` implements Quicksort using Hoare's partitioning scheme (how
//! you choose the pivot). This partitioning scheme typically significantly
//! reduces the number of swaps necessary but it does have some drawbacks.
//!
//! Firstly, the worse case runtime of this implementation is `O(n^2)` when the
//! input set of columns are sorted according to the desired sort order. To
//! avoid that behaviour, a heuristic is used for inputs over a certain size;
//! large inputs are first linearly scanned to determine if the input is already
//! sorted.
//!
//! Secondly, the sort produced using this partitioning scheme is not stable.
//!
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::ops::Range;

use snafu::ensure;
use snafu::Snafu;

use super::*;

#[derive(Snafu, Debug, Clone, Copy, PartialEq)]
pub enum Error {
    #[snafu(display(r#"Too many sort columns specified"#))]
    TooManyColumns,

    #[snafu(display(r#"Same column specified as sort column multiple times"#))]
    RepeatedColumns { index: usize },

    #[snafu(display(r#"Specified column index is out bounds"#))]
    OutOfBoundsColumn { index: usize },
}

/// Any Packers inputs with more than this many rows will have a linear
/// comparison scan performed on them to ensure they're not already sorted.
const SORTED_CHECK_SIZE: usize = 1000;

/// Sort a slice of `Packers` based on the provided column indexes.
///
/// All chosen columns will be sorted in ascending order; the sort is *not*
/// stable.
pub fn sort(packers: &mut [Packers], sort_by: &[usize]) -> Result<(), Error> {
    if packers.is_empty() || sort_by.is_empty() {
        return Ok(());
    }

    ensure!(sort_by.len() <= packers.len(), TooManyColumns);

    let mut col_set = BTreeSet::new();
    for &index in sort_by {
        ensure!(col_set.insert(index), RepeatedColumns { index });
    }

    // TODO(edd): map first/last still unstable https://github.com/rust-lang/rust/issues/62924
    if let Some(index) = col_set.range(packers.len()..).next() {
        return OutOfBoundsColumn { index: *index }.fail();
    }

    // Hoare's partitioning scheme can have quadratic runtime behaviour in
    // the worst case when the inputs are already sorted. To avoid this, a
    // check is added for large inputs.
    let n = packers[0].num_rows();
    if n > SORTED_CHECK_SIZE {
        let mut sorted = true;
        for i in 1..n {
            if cmp(packers, 0, i, sort_by) != Ordering::Equal {
                sorted = false;
                break;
            }
        }

        if sorted {
            return Ok(());
        }
    }

    quicksort_by(packers, 0..n - 1, sort_by);
    Ok(())
}

fn quicksort_by(packers: &mut [Packers], range: Range<usize>, sort_by: &[usize]) {
    if range.start >= range.end {
        return;
    }

    let pivot = partition(packers, &range, sort_by);
    quicksort_by(packers, range.start..pivot, sort_by);
    quicksort_by(packers, pivot + 1..range.end, sort_by);
}

fn partition(packers: &mut [Packers], range: &Range<usize>, sort_by: &[usize]) -> usize {
    let pivot = (range.start + range.end) / 2;
    let (lo, hi) = (range.start, range.end);
    if cmp(packers, pivot as usize, lo as usize, sort_by) == Ordering::Less {
        swap(packers, lo as usize, pivot as usize);
    }
    if cmp(packers, hi as usize, lo as usize, sort_by) == Ordering::Less {
        swap(packers, lo as usize, hi as usize);
    }
    if cmp(packers, pivot as usize, hi as usize, sort_by) == Ordering::Less {
        swap(packers, hi as usize, pivot as usize);
    }

    let pivot = hi;
    let mut i = range.start;
    let mut j = range.end;

    loop {
        while cmp(packers, i as usize, pivot as usize, sort_by) == Ordering::Less {
            i += 1;
        }

        while cmp(packers, j as usize, pivot as usize, sort_by) == Ordering::Greater {
            j -= 1;
        }

        if i >= j {
            return j;
        }

        swap(packers, i as usize, j as usize);
        i += 1;
        j -= 1;
    }
}

fn cmp(packers: &[Packers], a: usize, b: usize, sort_by: &[usize]) -> Ordering {
    for &idx in sort_by {
        match &packers[idx] {
            Packers::String(p) => {
                let a_val = p.get(a);
                let b_val = p.get(b);

                match (a_val, b_val) {
                    // if cmp equal then try next packer column.
                    (None, None) => continue,
                    (None, _) => return Ordering::Greater,
                    (_, None) => return Ordering::Less,
                    (Some(a_val), Some(b_val)) => {
                        let cmp = &str::cmp(a_val.as_utf8().unwrap(), b_val.as_utf8().unwrap());
                        if *cmp != Ordering::Equal {
                            // if cmp equal then try next packer column.
                            return *cmp;
                        }
                    }
                }
            }
            Packers::Integer(p) => {
                let cmp = p.get(a).cmp(&p.get(b));
                if cmp != Ordering::Equal {
                    // if cmp equal then try next packer column.
                    return cmp;
                }
            }
            _ => continue, // don't compare on non-string / timestamp cols
        }
    }
    Ordering::Equal
}

// Swap the same pair of elements in each packer column
fn swap(packers: &mut [Packers], a: usize, b: usize) {
    for p in packers {
        p.swap(a, b);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::Rng;

    #[test]
    fn packers_sort() {
        // Input Table
        //
        // 100    "a"    "cow"    1.23    100
        // NULL   "d"    "zebra"  NULL    105
        // 200    "a"    "cow"    3.22    99
        // NULL   "c"    "bee"    45.33   NULL
        let mut packer_a: Packer<i64> = Packer::new();
        packer_a.push(100);
        packer_a.push_option(None);
        packer_a.push(200);
        packer_a.push_option(None);

        let mut packer_b: Packer<ByteArray> = Packer::new();
        packer_b.push(ByteArray::from("a"));
        packer_b.push(ByteArray::from("d"));
        packer_b.push(ByteArray::from("a"));
        packer_b.push(ByteArray::from("c"));

        let mut packer_c: Packer<ByteArray> = Packer::new();
        packer_c.push(ByteArray::from("cow"));
        packer_c.push(ByteArray::from("zebra"));
        packer_c.push(ByteArray::from("cow"));
        packer_c.push(ByteArray::from("bee"));

        let mut packer_d: Packer<f64> = Packer::new();
        packer_d.push(1.23);
        packer_d.push_option(None);
        packer_d.push(3.22);
        packer_d.push(45.33);

        let mut packer_e: Packer<i64> = Packer::new();
        packer_e.push(100);
        packer_e.push(105);
        packer_e.push(99);
        packer_e.push_option(None);

        let mut packers = vec![
            Packers::Integer(packer_a),
            Packers::String(packer_b),
            Packers::String(packer_c),
            Packers::Float(packer_d),
            Packers::Integer(packer_e),
        ];

        // SORT ON COLUMN 1, 2, 4 ASC.
        sort(&mut packers, &[1, 2, 4]).unwrap();

        // Output Table
        //
        // 200    "a"    "cow"    3.22    99
        // 100    "a"    "cow"    1.23    100
        // NULL   "c"    "bee"    45.33   NULL
        // NULL   "d"    "zebra"  NULL    105

        if let Packers::Integer(p) = &packers[0] {
            assert_eq!(
                p.values(),
                vec![Some(200), Some(100), None, None,].as_slice()
            );
        };

        if let Packers::String(p) = &packers[1] {
            assert_eq!(
                p.values(),
                vec![
                    Some(ByteArray::from("a")),
                    Some(ByteArray::from("a")),
                    Some(ByteArray::from("c")),
                    Some(ByteArray::from("d"))
                ]
                .as_slice()
            );
        };

        if let Packers::String(p) = &packers[2] {
            assert_eq!(
                p.values(),
                vec![
                    Some(ByteArray::from("cow")),
                    Some(ByteArray::from("cow")),
                    Some(ByteArray::from("bee")),
                    Some(ByteArray::from("zebra"))
                ]
                .as_slice()
            );
        };

        if let Packers::Float(p) = &packers[3] {
            assert_eq!(
                p.values(),
                vec![Some(3.22), Some(1.23), Some(45.33), None].as_slice()
            );
        };

        if let Packers::Integer(p) = &packers[4] {
            assert_eq!(
                p.values(),
                vec![Some(99), Some(100), None, Some(105),].as_slice()
            );
        };
    }

    #[test]
    fn packers_sort_equal() {
        let packer: Packer<i64> = Packer::from(vec![1; 10000]);
        let mut packers = vec![Packers::Integer(packer)];

        assert_eq!(sort(&mut packers, &[0, 1]), Err(Error::TooManyColumns));

        assert_eq!(
            sorter::sort(&mut packers, &[2]),
            Err(Error::OutOfBoundsColumn { index: 2 })
        );

        sort(&mut packers, &[0]).unwrap();
    }

    #[test]
    fn packers_almost_equal() {
        let packer: Packer<i64> = Packer::from(vec![
            1588834100000000,
            1588834100000000,
            1588834100000000,
            1588834110000000,
            1588834100000000,
            1588834100000000,
            1588834100000000,
            1588834110000000,
            1588834100000000,
            1588834100000000,
            1588834100000000,
            1588834100000000,
            1588834100000000,
            1588834100000000,
            1588834100000000,
            1588834100000000,
            1588834110000000,
        ]);
        let mut packers = vec![Packers::Integer(packer)];

        sort(&mut packers, &[0]).unwrap();

        let values = packers[0].i64_packer_mut().values();

        let exp: Vec<Option<i64>> = vec![
            Some(1588834100000000),
            Some(1588834100000000),
            Some(1588834100000000),
            Some(1588834100000000),
            Some(1588834100000000),
            Some(1588834100000000),
            Some(1588834100000000),
            Some(1588834100000000),
            Some(1588834100000000),
            Some(1588834100000000),
            Some(1588834100000000),
            Some(1588834100000000),
            Some(1588834100000000),
            Some(1588834100000000),
            Some(1588834110000000),
            Some(1588834110000000),
            Some(1588834110000000),
        ];
        assert_eq!(values, exp.as_slice());
    }

    #[test]
    fn packers_random() {
        let mut rng = rand::thread_rng();

        for _ in 0..250 {
            let packer: Packer<i64> = Packer::from(
                (0..1000)
                    .map(|_| rng.gen_range(0, 20))
                    .collect::<Vec<i64>>(),
            );
            let mut packers = vec![Packers::Integer(packer)];

            sort(&mut packers, &[0]).unwrap();

            let values = packers[0].i64_packer_mut().values();
            let mut prev = values[0];
            for v in values.iter() {
                assert!(prev <= *v);
                prev = *v;
            }
        }
    }
}
