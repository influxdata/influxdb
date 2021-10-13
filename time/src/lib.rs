#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]

use chrono::{DateTime, TimeZone, Timelike, Utc};
use parking_lot::RwLock;
use std::ops::{Add, Sub};
use std::time::Duration;

/// A UTC Timestamp returned by a [`TimeProvider`]
///
/// Purposefully does not provide [`std::convert::From`] implementations
/// as intended to be an opaque type returned by a `TimeProvider` - the construction methods
/// provided are intended for serialization/deserialization and tests only
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Time(DateTime<Utc>);

impl Add<Duration> for Time {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        let duration = chrono::Duration::from_std(rhs).unwrap();
        Self(self.0 + duration)
    }
}

impl Sub<Duration> for Time {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        let duration = chrono::Duration::from_std(rhs).unwrap();
        Self(self.0 - duration)
    }
}

impl Sub<Self> for Time {
    type Output = Duration;

    /// Calculates difference in wall-clock time
    ///
    /// **Warning: Because monotonicity is not guaranteed, `t2 - t1` might be negative
    /// even when `t2` was generated after `t1!**
    ///
    /// # Panic
    ///
    /// Panics if the result would be negative
    fn sub(self, rhs: Self) -> Self::Output {
        (self.0 - rhs.0).to_std().unwrap()
    }
}

impl std::fmt::Debug for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl std::fmt::Display for Time {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_rfc3339())
    }
}

impl Time {
    pub const MAX: Self = Self(chrono::MAX_DATETIME);
    pub const MIN: Self = Self(chrono::MIN_DATETIME);

    /// Makes a new `Time` from the number of non-leap nanoseconds
    /// since January 1, 1970 0:00:00 UTC (aka "UNIX timestamp").
    pub fn from_timestamp_nanos(nanos: i64) -> Self {
        Self(Utc.timestamp_nanos(nanos))
    }

    /// Makes a new `DateTime` from the number of non-leap milliseconds
    /// since January 1, 1970 0:00:00 UTC (aka "UNIX timestamp").
    pub fn from_timestamp_millis(millis: i64) -> Self {
        Self(Utc.timestamp_millis(millis))
    }

    /// Makes a new `DateTime` from the number of non-leap milliseconds
    /// since January 1, 1970 0:00:00 UTC (aka "UNIX timestamp").
    ///
    /// Returns None if out of range
    pub fn from_timestamp_millis_opt(millis: i64) -> Option<Self> {
        Some(Self(Utc.timestamp_millis_opt(millis).single()?))
    }

    /// Makes a new `Time` from the number of non-leap seconds
    /// since January 1, 1970 0:00:00 UTC (aka "UNIX timestamp")
    /// and the number of nanoseconds since the last whole non-leap second.
    pub fn from_timestamp(secs: i64, nanos: u32) -> Self {
        Self(Utc.timestamp(secs, nanos))
    }

    /// Makes a new `Time` from the provided [`DateTime<Utc>`]
    pub fn from_date_time(time: chrono::DateTime<Utc>) -> Self {
        Self(time)
    }

    /// Makes a new `Time` from the provided [`DateTime<Utc>`]
    pub fn from_datetime(datetime: DateTime<Utc>) -> Self {
        Self(datetime)
    }

    /// Returns an RFC 3339 and ISO 8601 date and time string such as `1996-12-19T16:39:57+00:00`.
    pub fn to_rfc3339(&self) -> String {
        self.0.to_rfc3339()
    }

    /// Returns the number of non-leap-nanoseconds since January 1, 1970 UTC
    pub fn timestamp_nanos(&self) -> i64 {
        self.0.timestamp_nanos()
    }

    /// Returns the number of seconds since January 1, 1970 UTC
    pub fn timestamp(&self) -> i64 {
        self.0.timestamp()
    }

    /// Returns the hour number from 0 to 23.
    pub fn hour(&self) -> u32 {
        self.0.hour()
    }

    /// Returns the minute number from 0 to 59.
    pub fn minute(&self) -> u32 {
        self.0.minute()
    }

    /// Returns the second number from 0 to 59.
    pub fn second(&self) -> u32 {
        self.0.second()
    }

    /// Returns the number of nanoseconds since the last second boundary
    pub fn timestamp_subsec_nanos(&self) -> u32 {
        self.0.timestamp_subsec_nanos()
    }

    /// Returns the number of non-leap-milliseconds since January 1, 1970 UTC
    pub fn timestamp_millis(&self) -> i64 {
        self.0.timestamp_millis()
    }

    /// Returns the duration since the provided time or None if it would be negative
    pub fn checked_duration_since(&self, other: Self) -> Option<Duration> {
        self.0.signed_duration_since(other.0).to_std().ok()
    }

    /// Adds given [`Duration`] to the current date and time.
    ///
    /// Returns `None` if it would result in overflow
    pub fn checked_add(&self, duration: Duration) -> Option<Self> {
        let duration = chrono::Duration::from_std(duration).ok()?;
        Some(Self(self.0.checked_add_signed(duration)?))
    }

    /// Subtracts the given [`Duration`] from the current date and time.
    ///
    /// Returns `None` if it would result in overflow
    pub fn checked_sub(&self, duration: Duration) -> Option<Self> {
        let duration = chrono::Duration::from_std(duration).ok()?;
        Some(Self(self.0.checked_sub_signed(duration)?))
    }

    /// Returns `Time` as a [`DateTime<Utc>`]
    pub fn date_time(&self) -> DateTime<Utc> {
        self.0
    }
}

pub trait TimeProvider: std::fmt::Debug + Send + Sync + 'static {
    /// Returns the current `Time`. No guarantees are made about monotonicity
    fn now(&self) -> Time;
}

/// A [`TimeProvider`] that uses [`Utc::now`] as a clock source
#[derive(Debug, Default)]
pub struct SystemProvider {}

impl SystemProvider {
    pub fn new() -> Self {
        Self::default()
    }
}

impl TimeProvider for SystemProvider {
    fn now(&self) -> Time {
        Time(Utc::now())
    }
}

/// A [`TimeProvider`] that returns a fixed `Time` that can be set by [`MockProvider::set`]
#[derive(Debug)]
pub struct MockProvider {
    now: RwLock<Time>,
}

impl MockProvider {
    pub fn new(start: Time) -> Self {
        Self {
            now: RwLock::new(start),
        }
    }

    pub fn set(&self, time: Time) {
        *self.now.write() = time
    }

    pub fn inc(&self, duration: Duration) -> Time {
        let mut now = self.now.write();
        *now = *now + duration;
        *now
    }
}

impl TimeProvider for MockProvider {
    fn now(&self) -> Time {
        *self.now.read()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_instant_provider() {
        let provider = SystemProvider::new();
        let a = provider.now();
        std::thread::sleep(Duration::from_secs(1));
        let b = provider.now();
        let c = provider.now();

        let delta = b - a;
        assert!(delta > Duration::from_millis(500));
        assert!(delta < Duration::from_secs(5));
        assert!(b <= c);
    }

    #[test]
    fn test_mock_provider() {
        let provider = MockProvider::new(Time::from_timestamp_nanos(0));
        assert_eq!(provider.now().timestamp_nanos(), 0);
        assert_eq!(provider.now().timestamp_nanos(), 0);

        provider.set(Time::from_timestamp_nanos(12));
        assert_eq!(provider.now().timestamp_nanos(), 12);
        assert_eq!(provider.now().timestamp_nanos(), 12);
    }

    #[test]
    fn test_time() {
        let verify = |date_time: DateTime<Utc>| {
            let time = Time::from_datetime(date_time);

            assert_eq!(time.hour(), date_time.hour());
            assert_eq!(time.minute(), date_time.minute());
            assert_eq!(time.second(), date_time.second());

            assert_eq!(time.date_time(), date_time);
            assert_eq!(
                time,
                Time::from_timestamp(date_time.timestamp(), date_time.timestamp_subsec_nanos())
            );
            assert_eq!(
                time,
                Time::from_timestamp_nanos(date_time.timestamp_nanos())
            );
            assert_eq!(
                Time::from_timestamp_millis(date_time.timestamp_millis()),
                Time::from_date_time(Utc.timestamp_millis(date_time.timestamp_millis()))
            );

            assert_eq!(time.timestamp_nanos(), date_time.timestamp_nanos());
            assert_eq!(time.timestamp_millis(), date_time.timestamp_millis());
            assert_eq!(time.to_rfc3339(), date_time.to_rfc3339());

            let duration = Duration::from_millis(265367345);

            assert_eq!(
                time + duration,
                Time::from_date_time(date_time + chrono::Duration::from_std(duration).unwrap())
            );

            assert_eq!(
                time - duration,
                Time::from_date_time(date_time - chrono::Duration::from_std(duration).unwrap())
            );
        };

        verify(Utc.timestamp_nanos(3406960448958394583));
        verify(Utc.timestamp_nanos(0));
        verify(Utc.timestamp_nanos(-3659396346346));
    }

    #[test]
    fn test_overflow() {
        let time = Time::MAX;
        assert!(time.checked_add(Duration::from_nanos(1)).is_none());
        assert!(time.checked_sub(Duration::from_nanos(1)).is_some());

        let time = Time::MIN;
        assert!(time.checked_add(Duration::from_nanos(1)).is_some());
        assert!(time.checked_sub(Duration::from_nanos(1)).is_none());

        let duration = Duration::from_millis(i64::MAX as u64 + 1);

        let time = Time::from_timestamp_nanos(0);
        assert!(chrono::Duration::from_std(duration).is_err());
        assert!(time.checked_add(duration).is_none());
        assert!(time.checked_sub(duration).is_none());

        assert!(Time::from_timestamp_millis_opt(i64::MAX).is_none())
    }

    #[test]
    fn test_duration_since() {
        assert_eq!(
            Time::from_timestamp_nanos(5056)
                .checked_duration_since(Time::from_timestamp_nanos(-465))
                .unwrap(),
            Duration::from_nanos(5056 + 465)
        );

        assert!(Time::MAX.checked_duration_since(Time::MIN).is_some());

        assert!(Time::from_timestamp_nanos(505)
            .checked_duration_since(Time::from_timestamp_nanos(506))
            .is_none());
    }
}
