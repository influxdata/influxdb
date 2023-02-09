//! Dialect

use serde::{Deserialize, Serialize};

/// Dialect are options to change the default CSV output format;
/// <https://www.w3.org/TR/2015/REC-tabular-metadata-20151217/#dialect-descriptions>
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Dialect {
    /// If true, the results will contain a header row
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header: Option<bool>,
    /// Separator between cells; the default is ,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    /// <https://www.w3.org/TR/2015/REC-tabular-data-model-20151217/#columns>
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Vec<Annotations>>,
    /// Character prefixed to comment strings
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment_prefix: Option<String>,
    /// Format of timestamps
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_time_format: Option<DateTimeFormat>,
}

impl Dialect {
    /// Dialect are options to change the default CSV output format;
    /// <https://www.w3.org/TR/2015/REC-tabular-metadata-20151217/#dialect-descriptions>
    pub fn new() -> Self {
        Self::default()
    }
}

/// <https://www.w3.org/TR/2015/REC-tabular-data-model-20151217/#columns>
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Annotations {
    /// Group Annotation
    Group,
    /// Datatype Annotation
    Datatype,
    /// Default Annotation
    Default,
}

/// Timestamp Format
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum DateTimeFormat {
    /// RFC3339
    Rfc3339,
    /// RFC3339Nano
    Rfc3339Nano,
}
