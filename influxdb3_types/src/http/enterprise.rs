use serde::{Deserialize, Serialize};

/// Request definition for the `POST /api/v3/pro/configure/file_index` API
#[derive(Debug, Deserialize, Serialize)]
pub struct FileIndexCreateRequest {
    pub db: String,
    pub table: Option<String>,
    pub columns: Vec<String>,
}

/// Request definition for the `DELETE /api/v3/pro/configure/file_index` API
#[derive(Debug, Deserialize, Serialize)]
pub struct FileIndexDeleteRequest {
    pub db: String,
    pub table: Option<String>,
}
