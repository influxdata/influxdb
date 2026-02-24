#![no_main]

use influxdb_line_protocol::{Error, parse_lines};
use libfuzzer_sys::fuzz_target;

use workspace_hack as _;

fuzz_target!(|line_protocol: &str| {
    for line_result in parse_lines(line_protocol) {
        match line_result {
            Ok(_)
            | Err(Error::MeasurementValueInvalid)
            | Err(Error::EndsWithBackslash)
            | Err(Error::ExpectedTagKey { .. })
            | Err(Error::ExpectedTagValue { .. })
            | Err(Error::CannotParseEntireLine { .. })
            | Err(Error::TimestampValueInvalid { .. }) => {}
            Err(Error::Context {
                context: "field set",
                ref inner,
            }) if matches!(
                **inner,
                Error::ExpectedSpace { .. }
                    | Error::FieldSetMissing
                    | Error::EndsWithBackslash
                    | Error::IntegerValueInvalid { .. }
            ) => {}
            Err(Error::TagSetMalformedWithContext {
                context: "tag equals sign",
                ..
            }) => {}
            Err(other) => panic!("{other} ({other:?})"),
        }
    }
});
