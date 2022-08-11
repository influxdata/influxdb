//! Expression

use serde::{Deserialize, Serialize};

/// Expression AST
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Expression {
    /// Type of AST node
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Elements of the dictionary
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub elements: Vec<crate::models::ast::DictItem>,
    /// Function parameters
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<crate::models::ast::Property>,
    /// Node
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<crate::models::ast::Node>,
    /// Operator
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    /// Left leaf
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left: Option<Box<crate::models::ast::Expression>>,
    /// Right leaf
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right: Option<Box<crate::models::ast::Expression>>,
    /// Parent Expression
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callee: Option<Box<crate::models::ast::Expression>>,
    /// Function arguments
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
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
    /// PropertyKey Expr
    #[serde(skip_serializing_if = "Option::is_none")]
    pub property: Option<Box<crate::models::ast::PropertyKey>>,
    /// Array Expr
    #[serde(skip_serializing_if = "Option::is_none")]
    pub array: Option<Box<crate::models::ast::Expression>>,
    /// Index Expr
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<Box<crate::models::ast::Expression>>,
    /// Properties
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub properties: Vec<crate::models::ast::Property>,
    /// Expression
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<Box<crate::models::ast::Expression>>,
    /// Argument
    #[serde(skip_serializing_if = "Option::is_none")]
    pub argument: Option<Box<crate::models::ast::Expression>>,
    /// Call Expr
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call: Option<crate::models::ast::CallExpression>,
    /// Expression Value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Duration values
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<crate::models::ast::Duration>,
    /// Expression Name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl Expression {
    /// Return instance of expression
    pub fn new() -> Self {
        Self::default()
    }
}
