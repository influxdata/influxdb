// common fruitilities for cli

use std::{str::FromStr, sync::OnceLock};

use influxdb3_server::all_paths;
use observability_deps::tracing::trace;

const DISABLED_AUTHZ_TOO_MANY_VALUES_ERR: &str = "--disable-authz cannot take more than 4 items";
const DISABLED_AUTHZ_INVALID_VALUE_ERR: &str = "invalid value passed in for --disable-authz, allowed values are health, ping, metrics, and ready";

static AUTHZ_DISABLED_RESOURCES: OnceLock<Vec<&'static str>> = OnceLock::new();

// This custom type is used to parse `--disable-authz health,ping,metrics`, it wasn't straight
// forward to setup num_args and also collect the values into a list with a value_delimiter set.
// Even if it's possible with a bit of clap-fu, still requires mapping those resource names to
// endpoints. This custom type pulls both parsing and mapping logic together, easier to setup
// and test
#[derive(Debug, Clone, Copy)]
pub struct DisableAuthzList;

impl Default for DisableAuthzList {
    fn default() -> Self {
        AUTHZ_DISABLED_RESOURCES.get_or_init(Vec::new);
        Self {}
    }
}

impl DisableAuthzList {
    pub fn get_mapped_endpoints(&self) -> &'static Vec<&'static str> {
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
        if resources.len() > 4 {
            return Err(DISABLED_AUTHZ_TOO_MANY_VALUES_ERR);
        }

        for r in &resources {
            if !["health", "ping", "metrics", "ready"]
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

                if path == "ready" {
                    return vec![all_paths::API_V3_READY];
                }
                vec![]
            })
            .collect();

        AUTHZ_DISABLED_RESOURCES.get_or_init(|| resources_static);

        Ok(Self)
    }
}

#[cfg(test)]
mod tests;
