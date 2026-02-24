use std::sync::Arc;

use datafusion::common::{Statistics, stats::Precision};

use super::util::make_column_statistics_inexact;

/// Applies fetch and skip to a num_rows.
///
/// Fetch is the total num of rows fetched, after the skip is applied.
///
/// This logic is extracted from [`Statistics::with_fetch`]. Refer to
/// <https://docs.rs/datafusion-common/47.0.0/src/datafusion_common/stats.rs.html#391-397>.
pub(super) fn apply_fetch(
    stats: Arc<Statistics>,
    fetch: Option<usize>,
    skip: usize,
) -> Arc<Statistics> {
    let fetch_applied = match (stats.num_rows, fetch, skip) {
        (num_rows, None, 0) => num_rows,
        (Precision::Absent, None, _) => Precision::Absent,

        // absent, but can estimate
        (Precision::Absent, Some(fetch), _) => Precision::Inexact(fetch),

        // if we are skipping more rows than we have => then 0 output row.
        (num_rows, _, skip) if skip >= *num_rows.get_value().expect("should not be absent") => {
            Precision::Exact(0)
        }

        // have exact, return exact
        (Precision::Exact(num_rows), fetch, skip) => {
            let num_rows_remaining = num_rows.saturating_sub(skip);
            let fetched = std::cmp::min(num_rows_remaining, fetch.unwrap_or(num_rows_remaining));
            Precision::Exact(fetched)
        }

        // have inexact, return inexact
        (Precision::Inexact(num_rows), fetch, skip) => {
            let num_rows_remaining = num_rows.saturating_sub(skip);
            let fetched = std::cmp::min(num_rows_remaining, fetch.unwrap_or(num_rows_remaining));
            Precision::Inexact(fetched)
        }
    };

    if stats.num_rows == fetch_applied {
        stats
    } else {
        let mut stats = Arc::unwrap_or_clone(stats);
        stats.num_rows = fetch_applied;

        // since the num_rows have to change, also need to modify other stats
        stats.total_byte_size = Precision::Absent;
        stats.column_statistics = make_column_statistics_inexact(stats.column_statistics);

        Arc::new(stats)
    }
}

#[cfg(test)]
mod tests {
    use crate::test::test_utils::single_column_schema;

    use super::*;

    fn build_stats(num_rows: Precision<usize>) -> Arc<Statistics> {
        let mut stats = Statistics::new_unknown(&single_column_schema());
        stats.num_rows = num_rows;
        stats.into()
    }

    #[test]
    fn test_apply_fetch_to_num_rows() {
        /* starting with num_rows = Precision::Absent */

        // nothing is there
        let stats_absent = build_stats(Precision::Absent);
        let actual = apply_fetch(Arc::clone(&stats_absent), None, 0);
        assert_eq!(actual.num_rows, Precision::Absent, "should still be absent");

        // absent input, + fetch
        let actual = apply_fetch(stats_absent, Some(42), 42);
        assert_eq!(
            actual.num_rows,
            Precision::Inexact(42),
            "should return an inexact(fetch) when then input rows are unknown"
        );

        /* starting with num_rows = Precision::Exact */

        // input rows=100
        let starting_num_rows = Precision::Exact(100);
        let stats = build_stats(starting_num_rows);
        let actual = apply_fetch(Arc::clone(&stats), None, 0);
        assert_eq!(
            actual.num_rows, starting_num_rows,
            "should return starting input, without fetch nor skip"
        );

        // input rows=100, + skip 25
        let actual = apply_fetch(Arc::clone(&stats), None, 25);
        assert_eq!(
            actual.num_rows,
            Precision::Exact(75),
            "should account for skipped rows"
        );

        // input rows=100, + fetch 80  => returns 80
        let actual = apply_fetch(Arc::clone(&stats), Some(80), 0);
        assert_eq!(
            actual.num_rows,
            Precision::Exact(80),
            "should account for fetched rows"
        );

        // input rows=100, + fetch 200  => returns 100
        let actual = apply_fetch(Arc::clone(&stats), Some(200), 0);
        assert_eq!(
            actual.num_rows,
            Precision::Exact(100),
            "should account for total rows, even with >fetch"
        );

        // input rows=100, + fetch 80 + skip 25   => returns 75
        let actual = apply_fetch(Arc::clone(&stats), Some(80), 25);
        assert_eq!(
            actual.num_rows,
            Precision::Exact(75),
            "should be able to return fewer rows, after skip"
        );

        // input rows=100, + fetch 80 + skip 100   => returns 0
        let actual = apply_fetch(Arc::clone(&stats), Some(80), 100);
        assert_eq!(
            actual.num_rows,
            Precision::Exact(0),
            "should have no rows to return after skip"
        );

        /* starting with num_rows = Precision::InExact */

        // input rows=100
        let starting_num_rows = Precision::Inexact(100);
        let stats = build_stats(starting_num_rows);
        let actual = apply_fetch(Arc::clone(&stats), None, 0);
        assert_eq!(
            actual.num_rows, starting_num_rows,
            "should return starting input, without fetch nor skip"
        );

        // input rows=100, + skip 25
        let actual = apply_fetch(Arc::clone(&stats), None, 25);
        assert_eq!(
            actual.num_rows,
            Precision::Inexact(75),
            "should account for skipped rows"
        );

        // input rows=100, + fetch 80  => returns 80
        let actual = apply_fetch(Arc::clone(&stats), Some(80), 0);
        assert_eq!(
            actual.num_rows,
            Precision::Inexact(80),
            "should account for fetched rows"
        );

        // input rows=100, + fetch 200  => returns 100
        let actual = apply_fetch(Arc::clone(&stats), Some(200), 0);
        assert_eq!(
            actual.num_rows,
            Precision::Inexact(100),
            "should account for total rows, even with >fetch"
        );

        // input rows=100, + fetch 80 + skip 25   => returns 75
        let actual = apply_fetch(Arc::clone(&stats), Some(80), 25);
        assert_eq!(
            actual.num_rows,
            Precision::Inexact(75),
            "should be able to return fewer rows, after skip"
        );

        // input rows=100, + fetch 80 + skip 100   => returns 0
        let actual = apply_fetch(Arc::clone(&stats), Some(80), 100);
        assert_eq!(
            actual.num_rows,
            Precision::Exact(0),
            "should have no rows to return after skip"
        );
    }
}
