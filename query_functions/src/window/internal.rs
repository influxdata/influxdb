//! This module contains functions and structs needed to implement
//! IOx plans, such as timestamp specific window functonality.
//!
//! The code in this module is intended to be as faithful a
//! transliteration of the original Go code into Rust as possible. It
//! does not forcing idomatic Rust when that might obscure the mapping
//! between the original code and this port.
use chrono::{prelude::*, Month::February};
use std::ops::{Add, Mul};

/// Duration is a vector representing the duration unit components.
///
/// Original: <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/values/time.go#L18>
#[derive(Debug, Clone, Copy)]
pub struct Duration {
    /// months is the number of months for the duration.
    /// This must be a positive value.
    months: i64,

    /// nsecs is the number of nanoseconds for the duration.
    /// This must be a positive value.
    nsecs: i64,

    /// negative indicates this duration is a negative value.
    negative: bool,
}

impl Duration {
    /// Port of values.ConvertDurationNsecs. Creates a Duration that
    /// representing a fixed number of nanoseconds.
    ///
    /// Original: <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/values/time.go#L40>
    pub fn from_nsecs(v: i64) -> Self {
        let (negative, nsecs) = if v < 0 { (true, -v) } else { (false, v) };

        Self {
            months: 0,
            negative,
            nsecs,
        }
    }

    // Port of values.ConvertDurationMonths. Creates a Duration that
    /// representing a fixed number of months (which vary in absolute
    /// number of nanoseconds).
    ///
    /// Original: <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/values/time.go#L52>
    pub fn from_months(v: i64) -> Self {
        let (negative, months) = if v < 0 { (true, -v) } else { (false, v) };

        Self {
            months,
            negative,
            nsecs: 0,
        }
    }

    /// create a duration from a non negative value of months and a negative
    /// flag
    pub fn from_months_with_negative(months: i64, negative: bool) -> Self {
        assert_eq!(months < 0, negative);
        Self {
            months,
            negative,
            nsecs: 0,
        }
    }

    /// IsZero returns true if this is a zero duration.
    ///
    /// Original: <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/values/time.go#L204>
    fn is_zero(&self) -> bool {
        self.months == 0 && self.nsecs == 0
    }

    /// Return the number of months in this duration
    pub fn months(&self) -> i64 {
        self.months
    }

    /// Return the number of nanoseconds in this duration
    pub fn nanoseconds(&self) -> i64 {
        self.nsecs
    }

    /// truncate the time using the duration.
    ///
    /// Porting note: this implementation was moved into Duration so
    /// we could safely assume that only month or nsec was zero, not both (as
    /// the only two ways to create a duration in this `impl` ensures
    /// that invariant)
    ///
    /// Original: <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/execute/window.go#L52>
    fn truncate(&self, t: i64) -> i64 {
        let months = self.months;
        let nsec = self.nsecs;

        match (months != 0, nsec != 0) {
            (true, false) => truncate_by_months(t, self),
            (false, true) => truncate_by_nsecs(t, self),
            // the original Go code generates runtime errors for these two cases,
            // but the way the Rust is written it Can't Happen (TM)
            (true, true) => {
                panic!("duration used as an interval cannot mix month and nanosecond units")
            }
            (false, false) => panic!("duration used as an interval cannot be zero"),
        }
    }
}

impl Mul<i64> for Duration {
    type Output = Self;

    /// Mul will multiply the Duration by a scalar.
    /// This multiplies each component of the vector.
    ///
    /// Original: <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/values/time.go#L175>
    fn mul(self, rhs: i64) -> Self {
        let mut scale = rhs;
        let mut d = self;

        // If the duration is zero, do nothing.
        // This prevents a zero value from becoming negative
        // which is not possible.
        if d.is_zero() {
            return d;
        }
        if scale < 0 {
            scale = -scale;
            d.negative = !d.negative;
        }
        d.months *= scale;
        d.nsecs *= scale;
        d
    }
}

/// Converts a nanosecond UTC timestamp into a DateTime structure
/// (which can have year, month, etc. extracted)
///
/// This is roughly equivelnt to ConvertTime
/// from <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/values/time.go#L35-L37>
fn timestamp_to_datetime(ts: i64) -> DateTime<Utc> {
    let secs = ts / 1_000_000_000;
    let nsec = ts % 1_000_000_000;
    // Note that nsec as u32 is safe here because modulo on a negative ts value
    //  still produces a positive remainder.
    let datetime = NaiveDateTime::from_timestamp_opt(secs, nsec as u32).expect("ts in range");
    DateTime::from_naive_utc_and_offset(datetime, Utc)
}

/// Original: <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/values/time.go#L491>
const LAST_DAYS: [u32; 12] = [
    31, // time.January:   31,
    28, // time.February:  28,
    31, // time.March:     31,
    30, // time.April:     30,
    31, // time.May:       31,
    30, // time.June:      30,
    31, // time.July:      31,
    31, // time.August:    31,
    30, // time.September: 30,
    31, // time.October:   31,
    30, // time.November:  30,
    31, // time.December:  31,
];

fn last_day_of_month(month: i32) -> u32 {
    // month is 1 indexed
    let idx = (month - 1) as usize;
    LAST_DAYS[idx]
}

// port of fun isLeapYear(year int) bool {
/// Original: <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/values/time.go#L506>
fn is_leap_year(year: i32) -> bool {
    year % 400 == 0 || (year % 4 == 0 && year % 100 != 0)
}

/// Convert the parts of year to nanoseconds since the epoc UTC time.
/// It mimics the combination of `time.Date` and `UnixNano`.
///
/// It is used in place of Go code such as:
/// ```golang
/// time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()
/// ```
fn to_timestamp_nanos_utc(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    min: u32,
    sec: u32,
    nano: u32,
) -> i64 {
    let ndate = NaiveDate::from_ymd_opt(year, month, day).expect("year-month-day in range");
    let ntime =
        NaiveTime::from_hms_nano_opt(hour, min, sec, nano).expect("hour-min-sec-nano in range");
    let ndatetime = NaiveDateTime::new(ndate, ntime);

    let datetime = DateTime::<Utc>::from_naive_utc_and_offset(ndatetime, Utc);
    datetime.timestamp_nanos()
}

impl Add<Duration> for i64 {
    type Output = Self;

    /// Adds a duration to a nanosecond timestamp
    ///
    /// Original: <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/values/time.go#L84>
    fn add(self, rhs: Duration) -> Self {
        let t = self;
        let d = rhs;

        let mut new_t = t;
        if d.months > 0 {
            // Determine if the number of months is positive or negative.
            let mut months = d.months;
            if d.negative {
                months = -months;
            }

            // Retrieve the current date and increment the values
            // based on the number of months.
            let ts = timestamp_to_datetime(t);
            let (mut year, mut month, mut day) = (ts.year(), ts.month() as i32, ts.day());
            year += (months / 12) as i32;
            month += (months % 12) as i32;
            // If the month overflowed or underflowed, adjust the year
            // accordingly. Because we add the modulo for the months,
            // the year will only adjust by one.
            if month > 12 {
                year += 1;
                month -= 12;
            } else if month <= 0 {
                year -= 1;
                month += 12;
            }

            // Normalize the day if we are past the end of the month.
            let mut last_day_of_month = last_day_of_month(month);
            if month == (February.number_from_month() as i32) && is_leap_year(year) {
                last_day_of_month += 1;
            }

            if day > last_day_of_month {
                day = last_day_of_month
            }

            // Retrieve the original time and construct a date
            // with the new year, month, and day.
            let (hour, min, sec) = (ts.hour(), ts.minute(), ts.second());
            let nsec = ts.nanosecond();

            let ts = to_timestamp_nanos_utc(year, month as u32, day, hour, min, sec, nsec);
            // Convert it back to our own Time implementation.
            new_t = ts;
        }

        // Add the number of nanoseconds to the time.
        let mut nsecs = d.nsecs;
        if d.negative {
            nsecs = -nsecs;
        }
        // new_t + nsecs
        // follow the golang behavior and ignore overflow
        // see https://github.com/influxdata/influxdb_iox/issues/2890
        let (v, _overflow) = new_t.overflowing_add(nsecs);
        v
    }
}

/// The bounds of a window
///
/// Original: <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/execute/bounds.go#L19>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bounds {
    pub start: i64,
    pub stop: i64,
}

/// Represents a window in time
///
/// Original: <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/execute/window.go#L11>
#[derive(Debug, Clone, Copy)]
pub struct Window {
    every: Duration,
    // The period of the window.
    period: Duration,
    offset: Duration,
}

impl Window {
    /// create a new Window with the specified duration and offset
    pub fn new(every: Duration, period: Duration, offset: Duration) -> Self {
        Self {
            every,
            period,
            offset,
        }
    }

    /// returns the bounds for the earliest window bounds
    /// that contains the given time t.  For underlapping windows that
    /// do not contain time t, the window directly after time t will be
    /// returned.
    ///
    /// Original: <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/execute/window.go#L70>
    pub fn get_earliest_bounds(&self, t: i64) -> Bounds {
        // translate to not-offset coordinate
        // t = t.Add(w.Offset.Mul(-1))
        let t = t + self.offset.mul(-1);

        // stop := w.truncate(t).Add(w.Every)
        let stop = self.truncate(t).add(self.every);

        // translate to offset coordinate
        // stop = stop.Add(w.Offset)
        let stop = stop + self.offset;

        // start := stop.Add(w.Period.Mul(-1))
        let start = stop.add(self.period.mul(-1));

        Bounds { start, stop }
    }

    /// truncate the time using the duration.
    ///
    /// Original: <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/execute/window.go#L52>
    fn truncate(&self, t: i64) -> i64 {
        self.every.truncate(t)
    }
}

/// truncateByNsecs will truncate the time to the given number
/// of nanoseconds.
///
/// Original: <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/execute/window.go#L108>
fn truncate_by_nsecs(t: i64, d: &Duration) -> i64 {
    let dur = d.nanoseconds();
    let mut remainder = t % dur;

    if remainder < 0 {
        remainder += dur;
    }

    t - remainder
}

/// truncateByMonths will truncate the time to the given
/// number of months.
///
/// Original: <https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/execute/window.go#L119>
fn truncate_by_months(t: i64, d: &Duration) -> i64 {
    let ts = timestamp_to_datetime(t);
    let (year, month) = (ts.year(), ts.month());

    // Determine the total number of months and truncate
    // the number of months by the duration amount.
    let mut total = (year * 12) + (month - 1) as i32;
    let remainder = total % d.months() as i32;
    total -= remainder;

    // Recreate a new time from the year and month combination.
    let (year, month) = ((total / 12), ((total % 12) + 1) as u32);
    to_timestamp_nanos_utc(year, month, 1, 0, 0, 0, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Month::{January, June};

    /// nanosecs per second
    const NS_SECONDS: i64 = 60 * 1_000_000_000;

    /// nanosecs per minute
    const NS_MINUTE: i64 = 60 * NS_SECONDS;

    fn make_time(v: i64) -> i64 {
        v
    }

    /// Parses an ISO timestrng to a UTC timestamp, such as:
    ///
    /// t: mustParseTime("1970-02-01T00:00:00Z"),
    fn must_parse_time(s: &str) -> i64 {
        let datetime = DateTime::parse_from_rfc3339(s).unwrap();
        datetime.timestamp_nanos()
    }

    /// TestWindow_GetEarliestBounds
    ///
    /// Original: https://github.com/influxdata/flux/blob/1e9bfd49f21c0e679b42acf6fc515ce05c6dec2b/execute/window_test.go#L84
    #[test]
    fn get_earliest_bounds() {
        struct TestCase {
            name: &'static str,
            w: Window,
            t: i64, // time
            want: Bounds,
        }

        #[allow(clippy::identity_op)]
        let testcases = vec![
            TestCase {
                name: "simple",
                w: Window::new(
                    Duration::from_nsecs(5 * NS_MINUTE),
                    Duration::from_nsecs(5 * NS_MINUTE),
                    Duration::from_nsecs(0),
                ),
                t: make_time(6 * NS_MINUTE),
                want: Bounds {
                    start: make_time(5 * NS_MINUTE),
                    stop: make_time(10 * NS_MINUTE),
                },
            },
            TestCase {
                name: "simple with offset",
                w: Window::new(
                    Duration::from_nsecs(5 * NS_MINUTE),
                    Duration::from_nsecs(5 * NS_MINUTE),
                    Duration::from_nsecs(30 * NS_SECONDS),
                ),
                t: make_time(5 * NS_MINUTE),
                want: Bounds {
                    start: make_time(30 * NS_SECONDS),
                    stop: make_time(5 * NS_MINUTE + 30 * NS_SECONDS),
                },
            },
            TestCase {
                name: "simple months",
                w: Window::new(
                    Duration::from_months(5),
                    Duration::from_months(5),
                    Duration::from_months(0),
                ),
                t: to_timestamp_nanos_utc(1970, January.number_from_month(), 1, 0, 0, 0, 0),
                want: Bounds {
                    start: to_timestamp_nanos_utc(1970, January.number_from_month(), 1, 0, 0, 0, 0),
                    stop: to_timestamp_nanos_utc(1970, June.number_from_month(), 1, 0, 0, 0, 0),
                },
            },
            TestCase {
                name: "underlapping",
                w: Window::new(
                    Duration::from_nsecs(2 * NS_MINUTE),
                    Duration::from_nsecs(1 * NS_MINUTE),
                    Duration::from_nsecs(30 * NS_SECONDS),
                ),
                t: make_time(3 * NS_MINUTE),
                want: Bounds {
                    start: make_time(3 * NS_MINUTE + 30 * NS_SECONDS),
                    stop: make_time(4 * NS_MINUTE + 30 * NS_SECONDS),
                },
            },
            TestCase {
                name: "underlapping not contained",
                w: Window::new(
                    Duration::from_nsecs(2 * NS_MINUTE),
                    Duration::from_nsecs(1 * NS_MINUTE),
                    Duration::from_nsecs(30 * NS_SECONDS),
                ),
                t: make_time(2 * NS_MINUTE + 45 * NS_SECONDS),
                want: Bounds {
                    start: make_time(3 * NS_MINUTE + 30 * NS_SECONDS),
                    stop: make_time(4 * NS_MINUTE + 30 * NS_SECONDS),
                },
            },
            TestCase {
                name: "overlapping",
                w: Window::new(
                    Duration::from_nsecs(1 * NS_MINUTE),
                    Duration::from_nsecs(2 * NS_MINUTE),
                    Duration::from_nsecs(30 * NS_SECONDS),
                ),
                t: make_time(30 * NS_SECONDS),
                want: Bounds {
                    start: make_time(-30 * NS_SECONDS),
                    stop: make_time(1 * NS_MINUTE + 30 * NS_SECONDS),
                },
            },
            TestCase {
                name: "partially overlapping",
                w: Window::new(
                    Duration::from_nsecs(1 * NS_MINUTE),
                    Duration::from_nsecs(3 * NS_MINUTE + 30 * NS_SECONDS),
                    Duration::from_nsecs(30 * NS_SECONDS),
                ),
                t: make_time(5 * NS_MINUTE + 45 * NS_SECONDS),
                want: Bounds {
                    start: make_time(3 * NS_MINUTE),
                    stop: make_time(6 * NS_MINUTE + 30 * NS_SECONDS),
                },
            },
            TestCase {
                name: "partially overlapping (t on boundary)",
                w: Window::new(
                    Duration::from_nsecs(1 * NS_MINUTE),
                    Duration::from_nsecs(3 * NS_MINUTE + 30 * NS_SECONDS),
                    Duration::from_nsecs(30 * NS_SECONDS),
                ),
                t: make_time(5 * NS_MINUTE),
                want: Bounds {
                    start: make_time(2 * NS_MINUTE),
                    stop: make_time(5 * NS_MINUTE + 30 * NS_SECONDS),
                },
            },
            TestCase {
                name: "truncate before offset",
                w: Window::new(
                    Duration::from_nsecs(5 * NS_SECONDS),
                    Duration::from_nsecs(5 * NS_SECONDS),
                    Duration::from_nsecs(2 * NS_SECONDS),
                ),
                t: make_time(1 * NS_SECONDS),
                want: Bounds {
                    start: make_time(-3 * NS_SECONDS),
                    stop: make_time(2 * NS_SECONDS),
                },
            },
            TestCase {
                name: "truncate after offset",
                w: Window::new(
                    Duration::from_nsecs(5 * NS_SECONDS),
                    Duration::from_nsecs(5 * NS_SECONDS),
                    Duration::from_nsecs(2 * NS_SECONDS),
                ),
                t: make_time(3 * NS_SECONDS),
                want: Bounds {
                    start: make_time(2 * NS_SECONDS),
                    stop: make_time(7 * NS_SECONDS),
                },
            },
            TestCase {
                name: "truncate before calendar offset",
                w: Window::new(
                    Duration::from_months(5),
                    Duration::from_months(5),
                    Duration::from_months(2),
                ),
                t: must_parse_time("1970-02-01T00:00:00Z"),
                want: Bounds {
                    start: must_parse_time("1969-10-01T00:00:00Z"),
                    stop: must_parse_time("1970-03-01T00:00:00Z"),
                },
            },
            TestCase {
                name: "truncate after calendar offset",
                w: Window::new(
                    Duration::from_months(5),
                    Duration::from_months(5),
                    Duration::from_months(2),
                ),
                t: must_parse_time("1970-04-01T00:00:00Z"),
                want: Bounds {
                    start: must_parse_time("1970-03-01T00:00:00Z"),
                    stop: must_parse_time("1970-08-01T00:00:00Z"),
                },
            },
            TestCase {
                name: "negative calendar offset",
                w: Window::new(
                    Duration::from_months(5),
                    Duration::from_months(5),
                    Duration::from_months(-2),
                ),
                t: must_parse_time("1970-02-01T00:00:00Z"),
                want: Bounds {
                    start: must_parse_time("1969-11-01T00:00:00Z"),
                    stop: must_parse_time("1970-04-01T00:00:00Z"),
                },
            },
        ];

        for tc in testcases {
            let got = tc.w.get_earliest_bounds(tc.t);

            assert_eq!(
                tc.want, got,
                "'{}' did not get expected bounds; want:\n{:?}\ngot:\n{:?}",
                tc.name, tc.want, got
            );
        }
    }

    #[test]
    fn test_timestamp_to_datetime() {
        assert_eq!(
            timestamp_to_datetime(1591894320000000000).to_rfc3339(),
            "2020-06-11T16:52:00+00:00"
        );
        assert_eq!(
            timestamp_to_datetime(159189432).to_rfc3339(),
            "1970-01-01T00:00:00.159189432+00:00"
        );
    }

    #[test]
    #[should_panic]
    fn test_timestamp_to_datetime_negative() {
        // Note while testing to make sure a negative timestamp doesn't overflow, it
        // turns out that the chrono library itself didn't handle parsing
        // negative timestamps:
        //
        // thread 'window::tests::test_timestamp_to_datetime' panicked at 'invalid or
        // out-of-range datetime', src/github.com-1ecc6299db9ec823/chrono-0.4.
        // 19/src/naive/datetime.rs:117:18
        assert_eq!(timestamp_to_datetime(-1568756160).to_rfc3339(), "foo");
    }
}
