use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone};
use datafusion::common::{DataFusionError, Result};

/// Parse the timestamp string and return a DateTime in UTC.
fn parse_timestamp_utc(s: &str) -> Result<DateTime<FixedOffset>> {
    // 1a. Try a date time format string with nanosecond precision and then without
    //    https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L3661
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        // 1b. Try a date time format string without nanosecond precision
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%"))
        // 2. Try RFC3339 with nano precision
        //    https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L3664
        .or_else(|_| DateTime::parse_from_str(s, "%+").map(|ts| ts.naive_utc()))
        // 3. Try a date string
        //    https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L3671
        .or_else(
            |_| // Parse as a naive date, add a midnight time and then interpret the result in
                NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map(|nd| nd.and_time(NaiveTime::default())),
        )
        .map(|ts| DateTime::from_utc(ts, chrono::Utc.fix()))
        .map_err(|_| DataFusionError::Plan("invalid timestamp string".into()))
}

/// Parse the timestamp string and return a DateTime in the specified timezone.
fn parse_timestamp_tz(s: &str, tz: chrono_tz::Tz) -> Result<DateTime<FixedOffset>> {
    // 1a. Try a date time format string with nanosecond precision
    //    https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L3661
    tz.datetime_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        // 1a. Try a date time format string without nanosecond precision
        .or_else(|_| tz.datetime_from_str(s, "%Y-%m-%d %H:%M:%S"))
        // 2. Try RFC3339 with nano precision
        //    https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L3664
        .or_else(|_| {
            DateTime::parse_from_str(s, "%+").map(|ts| tz.from_utc_datetime(&ts.naive_utc()))
        })
        // 3. Try a date string
        //    https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L3671
        .or_else(|_| {
            // Parse as a naive date, add a midnight time and then interpret the result in
            // timezone "tz"
            NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map(|nd| nd.and_time(NaiveTime::default()).and_local_timezone(tz))
                .map_err(|_| ())?
                // When converted to the target timezone, tz, it is possible the
                // date is ambiguous due to time shifts. In this case, rather than
                // fail, choose the earliest valid date.
                .earliest()
                // if there is no valid date, return an error
                .ok_or(())
        })
        .map(|ts| ts.with_timezone(&ts.offset().fix()))
        .map_err(|_| DataFusionError::Plan("invalid timestamp string".into()))
}

/// Parse the string and return a `DateTime` using a fixed offset.
///
/// Based on the [`ToTimeLiteral`] function.
///
/// [`ToTimeLiteral`]: https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L3654-L3655
///
pub fn parse_timestamp(s: &str, tz: Option<chrono_tz::Tz>) -> Result<DateTime<FixedOffset>> {
    match tz {
        Some(tz) => parse_timestamp_tz(s, tz),
        // We could have mapped None => Utc and called parse_timestamp_tz, however,
        // this implementation is able to use the simplified NaiveDateTime type,
        // which is more efficient, as it does not have to perform transitions between
        // arbitrary timezones.
        None => parse_timestamp_utc(s),
    }
}

#[cfg(test)]
mod test {
    use super::parse_timestamp;

    #[test]
    fn test_parse_timestamp() {
        use chrono_tz::America::New_York; // a timezone in the Western hemisphere
        use chrono_tz::Australia::Hobart; // a timezone in the Eastern hemisphere

        //
        // No timezone specified
        //

        //
        // Step 1: Date-time format
        // https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/parser.go#L19
        //
        // These timestamps should be interpreted in UTC
        //

        // Nanosecond precision
        let res = parse_timestamp("2004-04-09 10:30:45.123456789", None).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 10:30:45.123456789 +00:00");
        // Nanosecond precision, 7 digits
        let res = parse_timestamp("2004-04-09 10:30:45.1234567", None).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 10:30:45.123456700 +00:00");
        // Microsecond precision
        let res = parse_timestamp("2004-04-09 10:30:45.123456", None).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 10:30:45.123456 +00:00");
        // No fractional seconds
        let res = parse_timestamp("2004-04-09 10:30:45", None).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 10:30:45 +00:00");

        //
        // Step 2: RFC3339Nano format
        // https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L3664
        //
        // These timestamps should be interpreted in whatever timezone is specified in the
        // string literal.

        let res = parse_timestamp("2004-04-09T10:30:45.123456789Z", None).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 10:30:45.123456789 +00:00");
        // With an offset in Eastern hemisphere
        let res = parse_timestamp("2004-04-09T10:30:45.123456789+10:00", None).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 00:30:45.123456789 +00:00");
        // With an offset in Western hemisphere
        let res = parse_timestamp("2004-04-09T10:30:45.123456789-05:00", None).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 15:30:45.123456789 +00:00");

        //
        // Step 3: Date format
        // https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L3671
        //
        //
        // These timestamps should be interpreted at midnight in UTC
        //

        let res = parse_timestamp("2004-04-09", None).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 00:00:00 +00:00");

        //
        // Timezone specified, therefore unspecified timezone strings should be interpreted
        // in the provided timezone.
        //

        //
        // Step 1: Date-time format
        // https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/parser.go#L19
        //

        // Nanosecond precision
        let res = parse_timestamp("2004-04-09 10:30:45.123456789", Some(Hobart)).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 10:30:45.123456789 +10:00");
        let res = parse_timestamp("2004-04-09 10:30:45.123456789", Some(New_York)).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 10:30:45.123456789 -04:00");
        // Nanosecond precision, 7 digits
        let res = parse_timestamp("2004-04-09 10:30:45.1234567", Some(Hobart)).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 10:30:45.123456700 +10:00");
        // Microsecond precision
        let res = parse_timestamp("2004-04-09 10:30:45.123456", Some(Hobart)).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 10:30:45.123456 +10:00");
        // No fractional seconds
        let res = parse_timestamp("2004-04-09 10:30:45", Some(Hobart)).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 10:30:45 +10:00");
        // No fractional seconds
        let res = parse_timestamp("2004-04-09 00:00:00", Some(Hobart)).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 00:00:00 +10:00");

        //
        // Step 2: RFC3339Nano format
        // https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L3664
        //
        // Timestamps are interpreted in the timezone specified in the timestamp string and then converted to the
        // desired timezone.

        let res = parse_timestamp("2004-04-09T10:30:45.123456789Z", Some(Hobart)).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 20:30:45.123456789 +10:00");
        // With an offset in the same timezone
        let res = parse_timestamp("2004-04-09T10:30:45.123456789+10:00", Some(Hobart)).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 10:30:45.123456789 +10:00");
        // With an offset in another timezone
        let res = parse_timestamp("2004-04-09T10:30:45.123456789-05:00", Some(Hobart)).unwrap();
        assert_eq!(res.to_string(), "2004-04-10 01:30:45.123456789 +10:00");

        //
        // Step 3: Date format
        // https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L3671
        //

        let res = parse_timestamp("2004-04-09", Some(Hobart)).unwrap();
        assert_eq!(res.to_string(), "2004-04-09 00:00:00 +10:00");
        // 2004-04-09 should be the same as parsing a full date-time at midnight in the specified timezone
        assert_eq!(
            res,
            parse_timestamp("2004-04-09 00:00:00", Some(Hobart)).unwrap()
        );
    }
}
