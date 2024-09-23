use num::{Num, NumCast};

pub(crate) fn stats<T: Num + Copy + NumCast + PartialOrd>(
    current_min: T,
    current_max: T,
    current_avg: T,
    current_num_samples: u64,
    new_value: T,
) -> Option<(T, T, T)> {
    let min = min(current_min, new_value);
    let max = max(current_max, new_value);
    let avg = avg(current_num_samples, current_avg, new_value)?;
    Some((min, max, avg))
}

/// Average function that returns average based on the type
/// provided. u64 for example will return avg as u64. This probably
/// is fine as we don't really need it to be a precise average.
/// For example, memory consumed measured in MB can be rounded as u64
pub(crate) fn avg<T: Num + Copy + NumCast>(
    current_num_samples: u64,
    current_avg: T,
    new_value: T,
) -> Option<T> {
    // NB: num::cast(current_num_samples).unwrap() should have been enough,
    //     given we always reset metrics. However, if we decide to not reset
    //     metrics without retrying then it is better to bubble up the `Option`
    //     to indicate this cast did not work
    let current_total = current_avg * num::cast(current_num_samples)?;
    let new_total = current_total + new_value;
    let new_num_samples = num::cast(current_num_samples.wrapping_add(1))?;
    if new_num_samples == num::cast(0).unwrap() {
        return None;
    }
    Some(new_total.div(new_num_samples))
}

fn min<T: Num + PartialOrd + Copy>(current_min: T, new_value: T) -> T {
    if new_value < current_min {
        return new_value;
    };
    current_min
}

fn max<T: Num + PartialOrd + Copy>(current_max: T, new_value: T) -> T {
    if new_value > current_max {
        return new_value;
    };
    current_max
}

#[cfg(test)]
mod tests {
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
        let avg_floats = avg(u64::MAX, 2.0, 4.0);
        info!(avg = ?avg_floats, "average float");
        assert_eq!(None, avg_floats);
    }

    #[test_log::test(test)]
    fn avg_num_test_max() {
        let avg_nums = avg(u64::MAX, 2, 4);
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

    proptest! {
        #[test_log::test(test)]
        fn prop_test_stats_no_panic_u64(
            min in 0u64..10000,
            max in 0u64..10000,
            curr_avg in 0u64..10000,
            num_samples in 0u64..10000,
            new_value in 0u64..100000,
        ) {
            stats(min, max, curr_avg, num_samples, new_value);
        }

        #[test]
        fn prop_test_stats_no_panic_f32(
            min in 0.0f32..10000.0,
            max in 0.0f32..10000.0,
            curr_avg in 0.0f32..10000.0,
            num_samples in 0u64..10000,
            new_value in 0.0f32..100000.0,
        ) {
            stats(min, max, curr_avg, num_samples, new_value);
        }

        #[test]
        fn prop_test_stats_no_panic_f64(
            min in 0.0f64..10000.0,
            max in 0.0f64..10000.0,
            curr_avg in 0.0f64..10000.0,
            num_samples in 0u64..10000,
            new_value in 0.0f64..100000.0,
        ) {
            stats(min, max, curr_avg, num_samples, new_value);
        }
    }
}
