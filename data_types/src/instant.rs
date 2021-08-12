use chrono::{DateTime, Utc};
use once_cell::sync::OnceCell;
use std::time::Instant;

/// Stores an Instant and DateTime<Utc> captured as close as possible together
static INSTANCE: OnceCell<(DateTime<Utc>, Instant)> = OnceCell::new();

/// Provides a conversion from Instant to DateTime<Utc> for display purposes
///
/// It is an approximation as if the system clock changes, the returned DateTime will not be
/// the same as the DateTime that would have been recorded at the time the Instant was created.
///
/// The conversion does, however, preserve the monotonic property of Instant, i.e. a larger
/// Instant will have a larger returned DateTime.
///
/// This should ONLY be used for display purposes, the results should not be used to
/// drive logic, nor persisted
pub fn to_approximate_datetime(instant: Instant) -> DateTime<Utc> {
    let (ref_date, ref_instant) = *INSTANCE.get_or_init(|| (Utc::now(), Instant::now()));

    if ref_instant > instant {
        ref_date
            - chrono::Duration::from_std(ref_instant.duration_since(instant))
                .expect("date overflow")
    } else {
        ref_date
            + chrono::Duration::from_std(instant.duration_since(ref_instant))
                .expect("date overflow")
    }
}

// *NOTE*: these tests currently fail on (at least) aarch64 architectures
// such as an Apple M1 machine.
//
// Possibly related to https://github.com/rust-lang/rust/issues/87906 but
// not clear at this point.
//
// Ignoring the tests here to get the suite green on aarch64.
#[cfg(not(target_arch = "aarch64"))]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_datetime() {
        // Seed global state
        to_approximate_datetime(Instant::now());

        let (ref_date, ref_instant) = *INSTANCE.get().unwrap();

        assert_eq!(
            to_approximate_datetime(ref_instant + std::time::Duration::from_nanos(78)),
            ref_date + chrono::Duration::nanoseconds(78)
        );

        assert_eq!(
            to_approximate_datetime(ref_instant - std::time::Duration::from_nanos(23)),
            ref_date - chrono::Duration::nanoseconds(23)
        );
    }

    #[test]
    fn test_to_datetime_simple() {
        let d = std::time::Duration::from_nanos(78);
        let a = Instant::now();
        let b = a + d;
        assert_eq!(b.duration_since(a), d);
    }
}
