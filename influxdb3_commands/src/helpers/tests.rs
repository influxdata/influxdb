use influxdb3_server::all_paths;

use crate::helpers::{DISABLED_AUTHZ_INVALID_VALUE_ERR, DISABLED_AUTHZ_TOO_MANY_VALUES_ERR};

use super::DisableAuthzList;

#[test]
fn test_parses_disabled_authz() {
    let list: DisableAuthzList = "health,ping,metrics".parse().expect("parseable");
    let all_mapped = list.get_mapped_endpoints();
    assert_eq!(4, all_mapped.len());
    assert_eq!(*all_mapped.first().unwrap(), all_paths::API_V3_HEALTH);
    assert_eq!(*all_mapped.get(1).unwrap(), all_paths::API_V1_HEALTH);
    assert_eq!(*all_mapped.get(2).unwrap(), all_paths::API_PING);
    assert_eq!(*all_mapped.get(3).unwrap(), all_paths::API_METRICS);
}

#[test]
fn test_fails_to_parse_disabled_authz_list_invalid_values_err() {
    let list = "health,foo,metrics"
        .parse::<DisableAuthzList>()
        .unwrap_err();
    assert_eq!(list, DISABLED_AUTHZ_INVALID_VALUE_ERR);
}

#[test]
fn test_fails_to_parse_disabled_authz_list_too_many_values_err() {
    let list = "health,foo,metrics,boo,zoo"
        .parse::<DisableAuthzList>()
        .unwrap_err();
    assert_eq!(list, DISABLED_AUTHZ_TOO_MANY_VALUES_ERR);
}

#[test]
fn test_fails_to_parse_disabled_authz_list_too_many_allowed_values_err() {
    let list = "health,metrics,ping,health"
        .parse::<DisableAuthzList>()
        .unwrap_err();
    assert_eq!(list, DISABLED_AUTHZ_TOO_MANY_VALUES_ERR);
}
