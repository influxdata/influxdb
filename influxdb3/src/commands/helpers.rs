// common fruitilities for cli

use std::{str::FromStr, sync::OnceLock};

use influxdb3_server::all_paths;
use observability_deps::tracing::trace;

const DISABLED_AUTHZ_TOO_MANY_VALUES_ERR: &str = "--disable-authz cannot take more than 3 items";
const DISABLED_AUTHZ_INVALID_VALUE_ERR: &str =
    "invalid value passed in for --disable-authz, allowed values are health, ping, and metrics";

static AUTHZ_DISABLED_RESOURCES: OnceLock<Vec<&'static str>> = OnceLock::new();

// This custom type is used to parse `--disable-authz health,ping,metrics`, it wasn't straight
// forward to setup num_args and also collect the values into a list with a value_delimiter set.
// Even if it's possible with a bit of clap-fu, still requires mapping those resource names to
// endpoints. This custom type pulls both parsing and mapping logic together, easier to setup
// and test
#[derive(Debug, Clone)]
pub struct DisableAuthzList;

impl Default for DisableAuthzList {
    fn default() -> Self {
        AUTHZ_DISABLED_RESOURCES.get_or_init(Vec::new);
        Self {}
    }
}

impl DisableAuthzList {
    pub(crate) fn get_mapped_endpoints(&self) -> &'static Vec<&'static str> {
        let all_paths_without_authz = AUTHZ_DISABLED_RESOURCES
            .get()
            .expect("disabled resource paths to have been loaded");
        trace!(paths_without_authz = ?all_paths_without_authz, "paths setup without authz");
        all_paths_without_authz
    }
}

impl FromStr for DisableAuthzList {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let resources: Vec<String> = s.split(',').map(|s| s.trim().to_string()).collect();
        if resources.len() > 3 {
            return Err(DISABLED_AUTHZ_TOO_MANY_VALUES_ERR);
        }

        for r in &resources {
            if !["health", "ping", "metrics"]
                .iter()
                .any(|resource| resource == r)
            {
                return Err(DISABLED_AUTHZ_INVALID_VALUE_ERR);
            }
        }

        let resources_static = resources
            .iter()
            .flat_map(|path| {
                if path == "health" {
                    return vec![all_paths::API_V3_HEALTH, all_paths::API_V1_HEALTH];
                }

                if path == "ping" {
                    return vec![all_paths::API_PING];
                }

                if path == "metrics" {
                    return vec![all_paths::API_METRICS];
                }
                vec![]
            })
            .collect();

        AUTHZ_DISABLED_RESOURCES.get_or_init(|| resources_static);

        Ok(Self)
    }
}

#[cfg(test)]
mod tests {
    use influxdb3_server::all_paths;

    use crate::commands::helpers::{
        DISABLED_AUTHZ_INVALID_VALUE_ERR, DISABLED_AUTHZ_TOO_MANY_VALUES_ERR,
    };

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
}
