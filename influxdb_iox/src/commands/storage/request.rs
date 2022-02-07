pub mod generated_types {
    pub use generated_types::influxdata::platform::storage::*;
}

use self::generated_types::*;
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

// TODO Add the following helpers for building requests:
//
// * read_group
// * read_window_aggregate
// * tag_keys
// * tag_values
// * tag_values_with_measurement_and_key
// * measurement_names
// * measurement_tag_keys
// * measurement_tag_values
// * measurement_fields
