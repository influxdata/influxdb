//! This module contains the actual code generation logic

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::io::{Result, Write};

use crate::descriptor::TypePath;

mod enumeration;
mod message;

pub use enumeration::generate_enum;
pub use message::generate_message;

#[derive(Debug, Clone, Copy)]
struct Indent(usize);

impl Display for Indent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for _ in 0..self.0 {
            write!(f, "    ")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Config {
    pub extern_types: BTreeMap<TypePath, String>,
}

impl Config {
    fn rust_type(&self, path: &TypePath) -> String {
        if let Some(t) = self.extern_types.get(path) {
            return t.clone();
        }

        let mut ret = String::new();
        let path = path.path();
        assert!(!path.is_empty(), "path cannot be empty");

        for i in &path[..(path.len() - 1)] {
            ret.push_str(i.to_snake_case().as_str());
            ret.push_str("::");
        }
        ret.push_str(path.last().unwrap().to_camel_case().as_str());
        ret
    }

    fn rust_variant(&self, enumeration: &TypePath, variant: &str) -> String {
        use heck::CamelCase;
        assert!(
            variant
                .chars()
                .all(|c| matches!(c, '0'..='9' | 'A'..='Z' | '_')),
            "illegal variant - {}",
            variant
        );

        // TODO: Config to disable stripping prefix

        let enumeration_name = enumeration.path().last().unwrap().to_shouty_snake_case();
        let variant = match variant.strip_prefix(&enumeration_name) {
            Some("") => variant,
            Some(stripped) => stripped,
            None => variant,
        };
        variant.to_camel_case()
    }
}

fn write_fields_array<'a, W: Write, I: Iterator<Item = &'a str>>(
    writer: &mut W,
    indent: usize,
    variants: I,
) -> Result<()> {
    writeln!(writer, "{}const FIELDS: &[&str] = &[", Indent(indent))?;
    for name in variants {
        writeln!(writer, "{}\"{}\",", Indent(indent + 1), name)?;
    }
    writeln!(writer, "{}];", Indent(indent))?;
    writeln!(writer)
}

fn write_serialize_start<W: Write>(indent: usize, rust_type: &str, writer: &mut W) -> Result<()> {
    writeln!(
        writer,
        r#"{indent}impl serde::Serialize for {rust_type} {{
{indent}    #[allow(deprecated)]
{indent}    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
{indent}    where
{indent}        S: serde::Serializer,
{indent}    {{"#,
        indent = Indent(indent),
        rust_type = rust_type
    )
}

fn write_serialize_end<W: Write>(indent: usize, writer: &mut W) -> Result<()> {
    writeln!(
        writer,
        r#"{indent}    }}
{indent}}}"#,
        indent = Indent(indent),
    )
}

fn write_deserialize_start<W: Write>(indent: usize, rust_type: &str, writer: &mut W) -> Result<()> {
    writeln!(
        writer,
        r#"{indent}impl<'de> serde::Deserialize<'de> for {rust_type} {{
{indent}    #[allow(deprecated)]
{indent}    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
{indent}    where
{indent}        D: serde::Deserializer<'de>,
{indent}    {{"#,
        indent = Indent(indent),
        rust_type = rust_type
    )
}

fn write_deserialize_end<W: Write>(indent: usize, writer: &mut W) -> Result<()> {
    writeln!(
        writer,
        r#"{indent}    }}
{indent}}}"#,
        indent = Indent(indent),
    )
}
