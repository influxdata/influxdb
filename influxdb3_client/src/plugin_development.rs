//! Request structs for the /api/v3/plugin_test API

use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

/// Request definition for `POST /api/v3/plugin_test/wal` API
#[derive(Debug, Serialize, Deserialize)]
pub struct WalPluginTestRequest {
    pub filename: String,
    pub database: String,
    pub input_lp: String,
    pub input_arguments: Option<HashMap<String, String>>,
}

/// Response definition for `POST /api/v3/plugin_test/wal` API
#[derive(Debug, Serialize, Deserialize)]
pub struct WalPluginTestResponse {
    pub log_lines: Vec<String>,
    pub database_writes: HashMap<String, Vec<String>>,
    pub errors: Vec<String>,
}

/// Request definition for `POST /api/v3/plugin_test/cron` API
#[derive(Debug, Serialize, Deserialize)]
pub struct CronPluginTestRequest {
    pub filename: String,
    pub database: String,
    pub schedule: Option<String>,
    pub input_arguments: Option<HashMap<String, String>>,
}

/// Response definition for `POST /api/v3/plugin_test/cron` API
#[derive(Debug, Serialize, Deserialize)]
pub struct CronPluginTestResponse {
    pub trigger_time: Option<String>,
    pub log_lines: Vec<String>,
    pub database_writes: HashMap<String, Vec<String>>,
    pub errors: Vec<String>,
}
