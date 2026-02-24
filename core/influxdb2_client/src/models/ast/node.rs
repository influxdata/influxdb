//! Node

use serde::{Deserialize, Serialize};

/// Node
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Node {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Elements of the dictionary
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub elements: Vec<crate::models::ast::DictItem>,
    /// Function parameters
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<crate::models::ast::Property>,
    /// Block body
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub body: Vec<crate::models::ast::Statement>,
    /// Node Operator
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    /// Left left node
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left: Option<Box<crate::models::ast::Expression>>,
    /// Right right node
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right: Option<Box<crate::models::ast::Expression>>,
    /// Parent node
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callee: Option<Box<crate::models::ast::Expression>>,
    /// Function arguments
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub arguments: Vec<crate::models::ast::Expression>,
    /// Test Expr
    #[serde(skip_serializing_if = "Option::is_none")]
    pub test: Option<Box<crate::models::ast::Expression>>,
    /// Alternate Expr
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alternate: Option<Box<crate::models::ast::Expression>>,
    /// Consequent Expr
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consequent: Option<Box<crate::models::ast::Expression>>,
    /// Object Expr
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<Box<crate::models::ast::Expression>>,
    /// PropertyKey
    #[serde(skip_serializing_if = "Option::is_none")]
    pub property: Option<crate::models::ast::PropertyKey>,
    /// Array Expr
    #[serde(skip_serializing_if = "Option::is_none")]
    pub array: Option<Box<crate::models::ast::Expression>>,
    /// Index Expr
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<Box<crate::models::ast::Expression>>,
    /// Object properties
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub properties: Vec<crate::models::ast::Property>,
    /// Expression
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<Box<crate::models::ast::Expression>>,
    /// Node arguments
    #[serde(skip_serializing_if = "Option::is_none")]
    pub argument: Option<Box<crate::models::ast::Expression>>,
    /// Call Expr
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call: Option<crate::models::ast::CallExpression>,
    /// Node Value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Duration values
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<crate::models::ast::Duration>,
    /// Node name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl Node {
    /// Return instance of Node
    pub fn new() -> Self {
        Self::default()
    }
}
