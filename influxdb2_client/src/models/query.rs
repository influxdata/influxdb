//! Query

use crate::models::ast::Package;
use crate::models::File;
use serde::{Deserialize, Serialize};
use serde_json::Number;
use std::collections::HashMap;

/// Query influx using the Flux language
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Query {
    /// Query Script
    #[serde(rename = "extern", skip_serializing_if = "Option::is_none")]
    pub r#extern: Option<File>,
    /// Query script to execute.
    pub query: String,
    /// The type of query. Must be \"flux\".
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<Type>,
    /// Dialect
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dialect: Option<crate::models::ast::Dialect>,
    /// Specifies the time that should be reported as "now" in the query.
    /// Default is the server's now time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub now: Option<String>,

    /// Params for use in query via params.param_name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<HashMap<String, Param>>,
}

/// Query Param Enum for Flux
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
pub enum Param {
    /// A number param
    Number(Number),
    /// A string param
    String(String),
}

impl Query {
    /// Query influx using the Flux language
    pub fn new(query: String) -> Self {
        Self {
            query,
            ..Default::default()
        }
    }
}

/// The type of query. Must be \"flux\".
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    /// Query Type
    Flux,
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
/// Flux Query Suggestion
pub struct FluxSuggestion {
    /// Suggestion Name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Suggestion Params
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<HashMap<String, String>>,
}

impl FluxSuggestion {
    /// Returns an instance FluxSuggestion
    pub fn new() -> Self {
        Self::default()
    }
}

/// FluxSuggestions
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FluxSuggestions {
    /// List of Flux Suggestions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub funcs: Vec<crate::models::FluxSuggestion>,
}

impl FluxSuggestions {
    /// Return an instance of FluxSuggestions
    pub fn new() -> Self {
        Self::default()
    }
}

/// AnalyzeQueryResponse
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AnalyzeQueryResponse {
    /// List of QueryResponseErrors
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<AnalyzeQueryResponseErrors>,
}

impl AnalyzeQueryResponse {
    /// Return an instance of AnanlyzeQueryResponse
    pub fn new() -> Self {
        Self::default()
    }
}

/// AnalyzeQueryResponseErrors
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AnalyzeQueryResponseErrors {
    /// Error line
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<i32>,
    /// Error column
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<i32>,
    /// Error char
    #[serde(skip_serializing_if = "Option::is_none")]
    pub character: Option<i32>,
    /// Error message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl AnalyzeQueryResponseErrors {
    /// Return an instance of AnalyzeQueryResponseErrors
    pub fn new() -> Self {
        Self::default()
    }
}

/// AstResponse : Contains the AST for the supplied Flux query
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AstResponse {
    /// AST of Flux query
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ast: Option<Package>,
}

impl AstResponse {
    /// Contains the AST for the supplied Flux query
    pub fn new() -> Self {
        Self::default()
    }
}

/// LanguageRequest : Flux query to be analyzed.
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LanguageRequest {
    /// Flux query script to be analyzed
    pub query: String,
}

impl LanguageRequest {
    /// Flux query to be analyzed.
    pub fn new(query: String) -> Self {
        Self { query }
    }
}
