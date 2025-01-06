//! Request structs for the /api/v3/plugin_test API

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Request definition for `POST /api/v3/plugin_test/wal` API
#[derive(Debug, Serialize, Deserialize)]
pub struct WalPluginTestRequest {
    pub name: String,
    pub input_lp: Option<String>,
    pub input_file: Option<String>,
    pub input_arguments: Option<HashMap<String, String>>,
}

/// Response definition for `POST /api/v3/plugin_test/wal` API
#[derive(Debug, Serialize, Deserialize)]
pub struct WalPluginTestResponse {
    pub log_lines: Vec<String>,
    pub database_writes: HashMap<String, Vec<String>>,
    pub errors: Vec<String>,
}
