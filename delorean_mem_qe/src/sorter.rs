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

use super::column;

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
pub fn sort(vectors: &mut [column::Vector], sort_by: &[usize]) -> Result<(), Error> {
    if vectors.is_empty() || sort_by.is_empty() {
        return Ok(());
    }

    ensure!(sort_by.len() <= vectors.len(), TooManyColumns);

    let mut col_set = BTreeSet::new();
    for &index in sort_by {
        ensure!(col_set.insert(index), RepeatedColumns { index });
    }

    // TODO(edd): map first/last still unstable https://github.com/rust-lang/rust/issues/62924
    if let Some(index) = col_set.range(vectors.len()..).next() {
        return OutOfBoundsColumn { index: *index }.fail();
    }

    // Hoare's partitioning scheme can have quadratic runtime behaviour in
    // the worst case when the inputs are already sorted. To avoid this, a
    // check is added for large inputs.
    let n = vectors[0].len();
    if n > SORTED_CHECK_SIZE {
        let mut sorted = true;
        for i in 1..n {
            if cmp(vectors, i - 1, i, sort_by) == Ordering::Greater {
                sorted = false;
                break;
            }
        }

        if sorted {
            log::debug!("columns already sorted");
            return Ok(());
        }
        // if vectors_sorted_asc(vectors, n, sort_by) {
        //     return Ok(());
        // }
    }
    let now = std::time::Instant::now();
    quicksort_by(vectors, 0..n - 1, sort_by);
    log::debug!("sorted in {:?}", now.elapsed());
    Ok(())
}

fn quicksort_by(vectors: &mut [column::Vector], range: Range<usize>, sort_by: &[usize]) {
    if range.start >= range.end {
        return;
    }

    let pivot = partition(vectors, &range, sort_by);
    quicksort_by(vectors, range.start..pivot, sort_by);
    quicksort_by(vectors, pivot + 1..range.end, sort_by);
}

fn partition(vectors: &mut [column::Vector], range: &Range<usize>, sort_by: &[usize]) -> usize {
    let pivot = (range.start + range.end) / 2;
    let (lo, hi) = (range.start, range.end);
    if cmp(vectors, pivot as usize, lo as usize, sort_by) == Ordering::Less {
        swap(vectors, lo as usize, pivot as usize);
    }
    if cmp(vectors, hi as usize, lo as usize, sort_by) == Ordering::Less {
        swap(vectors, lo as usize, hi as usize);
    }
    if cmp(vectors, pivot as usize, hi as usize, sort_by) == Ordering::Less {
        swap(vectors, hi as usize, pivot as usize);
    }

    let pivot = hi;
    let mut i = range.start;
    let mut j = range.end;

    loop {
        while cmp(vectors, i as usize, pivot as usize, sort_by) == Ordering::Less {
            i += 1;
        }

        while cmp(vectors, j as usize, pivot as usize, sort_by) == Ordering::Greater {
            j -= 1;
        }

        if i >= j {
            return j;
        }

        swap(vectors, i as usize, j as usize);
        i += 1;
        j -= 1;
    }
}

fn cmp(vectors: &[column::Vector], a: usize, b: usize, sort_by: &[usize]) -> Ordering {
    for &idx in sort_by {
        match &vectors[idx] {
            column::Vector::String(p) => {
                let cmp = p.get(a).cmp(&p.get(b));
                if cmp != Ordering::Equal {
                    return cmp;
                }
                // if cmp equal then try next vector.
            }
            column::Vector::Integer(p) => {
                let cmp = p.get(a).cmp(&p.get(b));
                if cmp != Ordering::Equal {
                    return cmp;
                }
                // if cmp equal then try next vector.
            }
            _ => continue, // don't compare on non-string / timestamp cols
        }
    }
    Ordering::Equal
}

#[allow(dead_code)]
fn vectors_sorted_asc(vectors: &[column::Vector], len: usize, sort_by: &[usize]) -> bool {
    'row_wise: for i in 1..len {
        for &idx in sort_by {
            match &vectors[idx] {
                column::Vector::String(vec) => {
                    if vec[i - 1] < vec[i] {
                        continue 'row_wise;
                    } else if vec[i - 1] == vec[i] {
                        // try next column
                        continue;
                    } else {
                        // value is > so
                        return false;
                    }
                }
                column::Vector::Integer(vec) => {
                    if vec[i - 1] < vec[i] {
                        continue 'row_wise;
                    } else if vec[i - 1] == vec[i] {
                        // try next column
                        continue;
                    } else {
                        // value is > so
                        return false;
                    }
                }
                _ => continue, // don't compare on non-string / timestamp cols
            }
        }
    }
    true
}

// Swap the same pair of elements in each packer column
fn swap(vectors: &mut [column::Vector], a: usize, b: usize) {
    for p in vectors {
        p.swap(a, b);
    }
}
