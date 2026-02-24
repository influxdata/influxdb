use observability_deps::tracing::info;
use proptest::prelude::*;

use super::*;

#[test]
fn min_float_test() {
    assert_eq!(1.0, min(1.0, 2.0));
}

#[test]
fn min_num_test() {
    assert_eq!(1, min(1, 2));
}

#[test]
fn max_num_test() {
    assert_eq!(2, max(1, 2));
}

#[test]
fn max_float_test() {
    assert_eq!(2.0, max(1.0, 2.0));
}

#[test]
fn avg_num_test() {
    assert_eq!(Some(2), avg(3, 2, 4));
}

#[test_log::test(test)]
fn avg_float_test() {
    let avg_floats = avg(3, 2.0, 4.0);
    info!(avg = ?avg_floats, "average float");
    assert_eq!(Some(2.5), avg_floats);
}

#[test_log::test(test)]
fn avg_float_test_max() {
    let avg_floats = avg(usize::MAX, 2.0, 4.0);
    info!(avg = ?avg_floats, "average float");
    assert_eq!(None, avg_floats);
}

#[test_log::test(test)]
fn avg_num_test_max() {
    let avg_nums = avg(usize::MAX, 2u64, 4);
    assert_eq!(None, avg_nums);
}

#[test_log::test(test)]
fn stats_test() {
    let stats = stats(2.0, 135.5, 25.5, 37, 25.0);
    assert!(stats.is_some());
    let (min, max, avg) = stats.unwrap();
    info!(min = ?min, max = ?max, avg = ?avg, "stats >>");
    assert_eq!((2.0, 135.5, 25.486842105263158), (min, max, avg));
}

#[test_log::test(test)]
fn rollup_stats_test() {
    let stats = rollup_stats(2.0, 135.5, 25.5, 37, 25.0, 150.0, 32.0);
    assert!(stats.is_some());
    let (min, max, avg) = stats.unwrap();
    info!(min = ?min, max = ?max, avg = ?avg, "stats >>");

    assert_eq!((2.0, 150.0, 25.67105263157895), (min, max, avg));
}

#[test_log::test(test)]
fn avg_test_new_value_lower() {
    let rolling_avg = avg(2, 110, 20u64);
    assert!(rolling_avg.is_some());
    assert_eq!(80, rolling_avg.unwrap());

    let rolling_avg = avg(3, 80, 22u64);
    assert!(rolling_avg.is_some());
    assert_eq!(66, rolling_avg.unwrap());
}

#[test_log::test(test)]
fn avg_test() {
    let avg = avg(0, 4339, 0u64);
    assert!(avg.is_some());
}

proptest! {
    #[test_log::test(test)]
    fn prop_test_stats_no_panic_u64(
        min in 0u64..10000,
        max in 0u64..10000,
        curr_avg in 0u64..10000,
        num_samples in 0usize..10000,
        new_value in 0u64..100000,
    ) {
        stats(min, max, curr_avg, num_samples, new_value);
    }

    #[test]
    fn prop_test_stats_no_panic_f32(
        min in 0.0f32..10000.0,
        max in 0.0f32..10000.0,
        curr_avg in 0.0f32..10000.0,
        num_samples in 0usize..10000,
        new_value in 0.0f32..100000.0,
    ) {
        stats(min, max, curr_avg, num_samples, new_value);
    }

    #[test]
    fn prop_test_stats_no_panic_f64(
        min in 0.0f64..10000.0,
        max in 0.0f64..10000.0,
        curr_avg in 0.0f64..10000.0,
        num_samples in 0usize..10000,
        new_value in 0.0f64..100000.0,
    ) {
        stats(min, max, curr_avg, num_samples, new_value);
    }
}
