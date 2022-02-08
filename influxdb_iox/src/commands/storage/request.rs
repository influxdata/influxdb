pub mod generated_types {
    pub use generated_types::influxdata::platform::storage::*;
}

use self::generated_types::*;
use super::response::{
    tag_key_is_field, tag_key_is_measurement, FIELD_TAG_KEY_BIN, MEASUREMENT_TAG_KEY_BIN,
};
use ::generated_types::google::protobuf::*;

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
        tag_key_meta_names: TagKeyMetaNames::Text as i32,
    }
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

// TODO Add the following helpers for building requests:
//
// * read_group
// * read_window_aggregate
// * tag_keys
// * tag_values_with_measurement_and_key
// * measurement_names
// * measurement_tag_keys
// * measurement_tag_values
// * measurement_fields
