use crate::generated_types::{
    node::{self, Comparison, Logical, Value},
    Node, Predicate,
};

use crate::storage::StorageError;
use croaring::Treemap;
use snafu::Snafu;
use std::iter::Peekable;
use std::str::Chars;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Predicate parse error: {}", description))]
    Parse { description: String },

    #[snafu(display("Predicate evaluation error: {}", description))]
    Evaluation { description: String },
}

// TODO: remove once we have made all modules have their own errors
impl From<Error> for StorageError {
    fn from(e: Error) -> Self {
        Self {
            description: e.to_string(),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn parse_predicate(val: &str) -> Result<Predicate> {
    let mut chars = val.chars().peekable();

    let mut predicate = Predicate { root: None };
    let node = parse_node(&mut chars)?;
    predicate.root = Some(node);

    // Err(StorageError{description: "couldn't parse".to_string()})
    Ok(predicate)
}

fn parse_node(chars: &mut Peekable<Chars<'_>>) -> Result<Node> {
    eat_whitespace(chars);

    let left = parse_key(chars)?;
    eat_whitespace(chars);

    let comparison = parse_comparison(chars)?;
    let right = parse_value(chars)?;

    let mut node = Node {
        children: vec![
            Node {
                value: Some(node::Value::TagRefValue(left)),
                children: vec![],
            },
            Node {
                value: Some(right),
                children: vec![],
            },
        ],
        value: Some(node::Value::Comparison(comparison as i32)),
    };

    if let Some(logical) = parse_logical(chars)? {
        let right = parse_node(chars)?;
        node = Node {
            children: vec![node, right],
            value: Some(Value::Logical(logical as i32)),
        }
    }

    Ok(node)
}

fn parse_key(chars: &mut Peekable<Chars<'_>>) -> Result<String> {
    let mut key = String::new();

    loop {
        let ch = chars.peek();
        if ch == None {
            break;
        }
        let ch = ch.unwrap();

        if ch.is_alphanumeric() || *ch == '_' || *ch == '-' {
            key.push(chars.next().unwrap());
        } else {
            return Ok(key);
        }
    }
    Parse {
        description: "reached end of predicate without a comparison operator",
    }
    .fail()
}

fn parse_comparison(chars: &mut Peekable<Chars<'_>>) -> Result<Comparison> {
    if let Some(ch) = chars.next() {
        let comp = match ch {
            '>' => match chars.peek() {
                Some('=') => {
                    chars.next();
                    node::Comparison::Gte
                }
                _ => node::Comparison::Gt,
            },
            '<' => match chars.peek() {
                Some('=') => {
                    chars.next();
                    node::Comparison::Lte
                }
                _ => node::Comparison::Lt,
            },
            '=' => node::Comparison::Equal,
            '!' => match chars.next() {
                Some('=') => Comparison::NotEqual,
                Some(ch) => {
                    return Parse {
                        description: format!("unhandled comparator !{}", ch),
                    }
                    .fail();
                }
                None => {
                    return Parse {
                        description:
                            "reached end of string without finishing not equals comparator",
                    }
                    .fail();
                }
            },
            _ => {
                return Parse {
                    description: format!("unhandled comparator {}", ch),
                }
                .fail()
            }
        };

        return Ok(comp);
    }
    Parse {
        description: "reached end of string without finding a comparison operator".to_string(),
    }
    .fail()
}

fn parse_value(chars: &mut Peekable<Chars<'_>>) -> Result<Value> {
    eat_whitespace(chars);
    let mut val = String::new();

    match chars.next() {
        Some('"') => {
            for ch in chars {
                if ch == '"' {
                    return Ok(Value::StringValue(val));
                }
                val.push(ch);
            }
        }
        Some(ch) => {
            return Parse {
                description: format!("unable to parse non-string values like '{}'", ch),
            }
            .fail()
        }
        None => (),
    }

    Parse {
        description: "reached end of predicate without a closing quote for the string value",
    }
    .fail()
}

fn parse_logical(chars: &mut Peekable<Chars<'_>>) -> Result<Option<node::Logical>> {
    eat_whitespace(chars);

    if let Some(ch) = chars.next() {
        match ch {
            'a' | 'A' => {
                match chars.next() {
                    Some('n') | Some('N') => (),
                    Some(ch) => {
                        return Parse {
                            description: format!(r#"expected "and" but found a{}"#, ch),
                        }
                        .fail()
                    }
                    _ => {
                        return Parse {
                            description: "unexpectedly reached end of string".to_string(),
                        }
                        .fail()
                    }
                }
                match chars.next() {
                    Some('d') | Some('D') => (),
                    Some(ch) => {
                        return Parse {
                            description: format!(r#"expected "and" but found an{}"#, ch),
                        }
                        .fail()
                    }
                    _ => {
                        return Parse {
                            description: "unexpectedly reached end of string".to_string(),
                        }
                        .fail()
                    }
                }
                return Ok(Some(node::Logical::And));
            }
            'o' | 'O' => match chars.next() {
                Some('r') | Some('R') => return Ok(Some(node::Logical::Or)),
                Some(ch) => {
                    return Parse {
                        description: format!(r#"expected "or" but found o{}"#, ch),
                    }
                    .fail()
                }
                _ => {
                    return Parse {
                        description: "unexpectedly reached end of string".to_string(),
                    }
                    .fail()
                }
            },
            _ => {
                return Parse {
                    description: format!(
                        "unexpected character {} trying parse logical expression",
                        ch
                    ),
                }
                .fail()
            }
        }
    }

    Ok(None)
}

fn eat_whitespace(chars: &mut Peekable<Chars<'_>>) {
    while let Some(&ch) = chars.peek() {
        if ch.is_whitespace() {
            let _ = chars.next();
        } else {
            break;
        }
    }
}

pub trait EvaluateVisitor {
    fn equal(&mut self, left: &str, right: &str) -> Result<Treemap>;
}

#[derive(Debug)]
pub struct Evaluate<V: EvaluateVisitor>(V);

impl<V: EvaluateVisitor> Evaluate<V> {
    pub fn evaluate(visitor: V, node: &Node) -> Result<Treemap> {
        Self(visitor).node(node)
    }

    fn node(&mut self, n: &Node) -> Result<Treemap> {
        if n.children.len() != 2 {
            return Evaluation {
                description: format!(
                    "expected only two children of node but found {}",
                    n.children.len()
                ),
            }
            .fail();
        }

        match &n.value {
            Some(node_value) => match node_value {
                Value::Logical(l) => {
                    let l = Logical::from_i32(*l).unwrap();
                    self.logical(&n.children[0], &n.children[1], l)
                }
                Value::Comparison(c) => {
                    let c = Comparison::from_i32(*c).unwrap();
                    self.comparison(&n.children[0], &n.children[1], c)
                }
                val => Evaluation {
                    description: format!("Evaluate::node called on wrong type {:?}", val),
                }
                .fail(),
            },
            None => Evaluation {
                description: "emtpy node value".to_string(),
            }
            .fail(),
        }
    }

    fn logical(&mut self, left: &Node, right: &Node, op: Logical) -> Result<Treemap> {
        let mut left_result = self.node(left)?;
        let right_result = self.node(right)?;

        match op {
            Logical::And => left_result.and_inplace(&right_result),
            Logical::Or => left_result.or_inplace(&right_result),
        };

        Ok(left_result)
    }

    fn comparison(&mut self, left: &Node, right: &Node, op: Comparison) -> Result<Treemap> {
        let left = match &left.value {
            Some(Value::TagRefValue(s)) => s,
            _ => {
                return Evaluation {
                    description: "expected left operand to be a TagRefValue".to_string(),
                }
                .fail()
            }
        };

        let right = match &right.value {
            Some(Value::StringValue(s)) => s,
            _ => {
                return Evaluation {
                    description: "unable to run comparison against anything other than a string"
                        .to_string(),
                }
                .fail()
            }
        };

        match op {
            Comparison::Equal => self.0.equal(left, right),
            comp => Evaluation {
                description: format!("unable to handle comparison {:?}", comp),
            }
            .fail(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_predicate() {
        let pred = super::parse_predicate(r#"host = "foo""#).unwrap();
        assert_eq!(
            pred,
            Predicate {
                root: Some(Node {
                    value: Some(node::Value::Comparison(node::Comparison::Equal as i32)),
                    children: vec![
                        Node {
                            value: Some(node::Value::TagRefValue("host".to_string())),
                            children: vec![]
                        },
                        Node {
                            value: Some(node::Value::StringValue("foo".to_string())),
                            children: vec![]
                        },
                    ],
                },)
            }
        );

        let pred = super::parse_predicate(r#"host != "serverA" AND region="west""#).unwrap();
        assert_eq!(
            pred,
            Predicate {
                root: Some(Node {
                    value: Some(Value::Logical(node::Logical::And as i32)),
                    children: vec![
                        Node {
                            value: Some(Value::Comparison(Comparison::NotEqual as i32)),
                            children: vec![
                                Node {
                                    value: Some(Value::TagRefValue("host".to_string())),
                                    children: vec![]
                                },
                                Node {
                                    value: Some(Value::StringValue("serverA".to_string())),
                                    children: vec![]
                                },
                            ],
                        },
                        Node {
                            value: Some(Value::Comparison(Comparison::Equal as i32)),
                            children: vec![
                                Node {
                                    value: Some(Value::TagRefValue("region".to_string())),
                                    children: vec![]
                                },
                                Node {
                                    value: Some(Value::StringValue("west".to_string())),
                                    children: vec![]
                                },
                            ],
                        }
                    ],
                },)
            }
        );
    }
}
