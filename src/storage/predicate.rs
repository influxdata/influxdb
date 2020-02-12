use crate::delorean::node::{Comparison, Value};
use crate::delorean::{node, Node, Predicate};
use crate::storage::StorageError;

use std::iter::Peekable;
use std::str::Chars;

pub fn parse_predicate(val: &str) -> Result<Predicate, StorageError> {
    let mut chars = val.chars().peekable();

    let mut predicate = Predicate { root: None };
    let node = parse_node(&mut chars)?;
    predicate.root = Some(node);

    // Err(StorageError{description: "couldn't parse".to_string()})
    Ok(predicate)
}

fn parse_node(chars: &mut Peekable<Chars<'_>>) -> Result<Node, StorageError> {
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

fn parse_key(chars: &mut Peekable<Chars<'_>>) -> Result<String, StorageError> {
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

    Err(StorageError {
        description: "reached end of predicate without a comparison operator".to_string(),
    })
}

fn parse_comparison(chars: &mut Peekable<Chars<'_>>) -> Result<Comparison, StorageError> {
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
                    return Err(StorageError {
                        description: format!("unhandled comparator !{}", ch),
                    })
                }
                None => {
                    return Err(StorageError {
                        description:
                            "reached end of string without finishing not equals comparator"
                                .to_string(),
                    })
                }
            },
            _ => {
                return Err(StorageError {
                    description: format!("unhandled comparator {}", ch),
                })
            }
        };

        return Ok(comp);
    }
    Err(StorageError {
        description: "reached end of string without finding a comparison operator".to_string(),
    })
}

fn parse_value(chars: &mut Peekable<Chars<'_>>) -> Result<Value, StorageError> {
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
            return Err(StorageError {
                description: format!("unable to parse non-string values like '{}'", ch),
            })
        }
        None => (),
    }

    Err(StorageError {
        description: "reached end of predicate without a closing quote for the string value"
            .to_string(),
    })
}

fn parse_logical(chars: &mut Peekable<Chars<'_>>) -> Result<Option<node::Logical>, StorageError> {
    eat_whitespace(chars);

    if let Some(ch) = chars.next() {
        match ch {
            'a' | 'A' => {
                match chars.next() {
                    Some('n') | Some('N') => (),
                    Some(ch) => {
                        return Err(StorageError {
                            description: format!("expected \"and\" but found a{}", ch),
                        })
                    }
                    _ => {
                        return Err(StorageError {
                            description: "unexpectedly reached end of string".to_string(),
                        })
                    }
                }
                match chars.next() {
                    Some('d') | Some('D') => (),
                    Some(ch) => {
                        return Err(StorageError {
                            description: format!("expected \"and\" but found an{}", ch),
                        })
                    }
                    _ => {
                        return Err(StorageError {
                            description: "unexpectedly reached end of string".to_string(),
                        })
                    }
                }
                return Ok(Some(node::Logical::And));
            }
            'o' | 'O' => match chars.next() {
                Some('r') | Some('R') => return Ok(Some(node::Logical::Or)),
                Some(ch) => {
                    return Err(StorageError {
                        description: format!("expected \"or\" but found o{}", ch),
                    })
                }
                _ => {
                    return Err(StorageError {
                        description: "unexpectedly reached end of string".to_string(),
                    })
                }
            },
            _ => {
                return Err(StorageError {
                    description: format!(
                        "unexpected character {} trying parse logical expression",
                        ch
                    ),
                })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_predicate() {
        let pred = super::parse_predicate("host = \"foo\"").unwrap();
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

        let pred = super::parse_predicate("host != \"serverA\" AND region=\"west\"").unwrap();
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
