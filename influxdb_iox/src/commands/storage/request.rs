pub mod generated_types {
    pub use generated_types::influxdata::platform::storage::{read_group_request::Group, *};
}

use snafu::Snafu;

use self::generated_types::*;
use super::response::{
    FIELD_TAG_KEY_BIN, FIELD_TAG_KEY_TEXT, MEASUREMENT_TAG_KEY_BIN, MEASUREMENT_TAG_KEY_TEXT,
};
use ::generated_types::{aggregate::AggregateType, google::protobuf::*};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("duration {:?} too large", d))]
    Duration { d: std::time::Duration },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn measurement_fields(
    org_bucket: Any,
    measurement: String,
    start: i64,
    stop: i64,
    predicate: std::option::Option<Predicate>,
) -> MeasurementFieldsRequest {
    generated_types::MeasurementFieldsRequest {
        source: Some(org_bucket),
        measurement,
        range: Some(TimestampRange { start, end: stop }),
        predicate,
    }
}

pub fn measurement_tag_keys(
    org_bucket: Any,
    measurement: String,
    start: i64,
    stop: i64,
    predicate: std::option::Option<Predicate>,
) -> MeasurementTagKeysRequest {
    generated_types::MeasurementTagKeysRequest {
        source: Some(org_bucket),
        measurement,
        range: Some(TimestampRange { start, end: stop }),
        predicate,
    }
}

pub fn read_filter(
    org_bucket: Any,
    start: i64,
    stop: i64,
    predicate: std::option::Option<Predicate>,
) -> ReadFilterRequest {
    generated_types::ReadFilterRequest {
        predicate,
        read_source: Some(org_bucket),
        range: Some(TimestampRange { start, end: stop }),
        key_sort: read_filter_request::KeySort::Unspecified as i32, // IOx doesn't support any other sort
        tag_key_meta_names: TagKeyMetaNames::Binary as i32,
    }
}

pub fn read_group(
    org_bucket: Any,
    start: i64,
    stop: i64,
    predicate: std::option::Option<Predicate>,
    aggregate: std::option::Option<AggregateType>,
    group: Group,
    group_keys: Vec<String>,
) -> ReadGroupRequest {
    generated_types::ReadGroupRequest {
        predicate,
        read_source: Some(org_bucket),
        range: Some(TimestampRange { start, end: stop }),
        aggregate: aggregate.map(|a| Aggregate { r#type: a as i32 }),
        group: group as i32,
        group_keys,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn read_window_aggregate(
    org_bucket: Any,
    start: i64,
    stop: i64,
    predicate: std::option::Option<Predicate>,
    every: std::time::Duration,
    offset: std::time::Duration,
    aggregates: Vec<AggregateType>,
    window: std::option::Option<Window>,
) -> Result<ReadWindowAggregateRequest, Error> {
    let window_every = if every.as_nanos() > i64::MAX as u128 {
        return DurationSnafu { d: every }.fail();
    } else {
        every.as_nanos() as i64
    };

    let offset = if offset.as_nanos() > i64::MAX as u128 {
        return DurationSnafu { d: offset }.fail();
    } else {
        offset.as_nanos() as i64
    };

    // wrap in the PB message type for aggregates.
    let aggregate = aggregates
        .into_iter()
        .map(|a| Aggregate { r#type: a as i32 })
        .collect::<Vec<_>>();

    Ok(generated_types::ReadWindowAggregateRequest {
        predicate,
        read_source: Some(org_bucket),
        range: Some(TimestampRange { start, end: stop }),
        window_every,
        offset,
        aggregate,
        window,
        tag_key_meta_names: TagKeyMetaNames::Text as i32,
    })
}

pub fn tag_values(
    org_bucket: Any,
    start: i64,
    stop: i64,
    predicate: std::option::Option<Predicate>,
    tag_key: String,
) -> TagValuesRequest {
    let tag_key = if tag_key_is_measurement(tag_key.as_bytes()) {
        MEASUREMENT_TAG_KEY_BIN.to_vec()
    } else if tag_key_is_field(tag_key.as_bytes()) {
        FIELD_TAG_KEY_BIN.to_vec()
    } else {
        tag_key.as_bytes().to_vec()
    };

    generated_types::TagValuesRequest {
        predicate,
        tags_source: Some(org_bucket),
        range: Some(TimestampRange { start, end: stop }),
        tag_key,
    }
}

pub(crate) fn tag_key_is_measurement(key: &[u8]) -> bool {
    (key == MEASUREMENT_TAG_KEY_TEXT) || (key == MEASUREMENT_TAG_KEY_BIN)
}

pub(crate) fn tag_key_is_field(key: &[u8]) -> bool {
    (key == FIELD_TAG_KEY_TEXT) || (key == FIELD_TAG_KEY_BIN)
}

#[cfg(test)]
mod test_super {
    use std::num::NonZeroU64;

    use influxdb_storage_client::{Client, OrgAndBucket};

    use super::*;

    #[test]
    fn test_read_window_aggregate_durations() {
        let org_bucket = Client::read_source(
            &OrgAndBucket::new(
                NonZeroU64::new(123_u64).unwrap(),
                NonZeroU64::new(456_u64).unwrap(),
            ),
            0,
        );

        let got = read_window_aggregate(
            org_bucket.clone(),
            1,
            10,
            None,
            std::time::Duration::from_millis(3),
            std::time::Duration::from_millis(2),
            vec![],
            None,
        )
        .unwrap();

        assert_eq!(got.window_every, 3_000_000);
        assert_eq!(got.offset, 2_000_000);

        let got = read_window_aggregate(
            org_bucket.clone(),
            1,
            10,
            None,
            std::time::Duration::from_secs(u64::MAX),
            std::time::Duration::from_millis(2),
            vec![],
            None,
        );
        assert!(got.is_err());

        let got = read_window_aggregate(
            org_bucket,
            1,
            10,
            None,
            std::time::Duration::from_secs(3),
            std::time::Duration::from_secs(u64::MAX),
            vec![],
            None,
        );
        assert!(got.is_err());
    }
}

// TODO Add the following helpers for building requests:
//
// * read_group
// * tag_keys
// * tag_values_with_measurement_and_key
// * measurement_names
// * measurement_tag_keys
// * measurement_tag_values
