//! This module contains code to parse and extract the protobuf descriptor
//! format for use by the rest of the codebase

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::io::{Error, ErrorKind, Result};

use itertools::{EitherOrBoth, Itertools};
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorSet, MessageOptions, OneofDescriptorProto,
};

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Package(String);

impl Display for Package {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Package {
    pub fn new(s: impl Into<String>) -> Self {
        let s = s.into();
        assert!(
            !s.starts_with('.'),
            "package cannot start with \'.\', got \"{}\"",
            s
        );
        Self(s)
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct TypeName(String);

impl Display for TypeName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl TypeName {
    pub fn new(s: impl Into<String>) -> Self {
        let s = s.into();
        assert!(
            !s.contains('.'),
            "type name cannot contain \'.\', got \"{}\"",
            s
        );
        Self(s)
    }

    pub fn to_snake_case(&self) -> String {
        use heck::SnakeCase;
        self.0.to_snake_case()
    }

    pub fn to_camel_case(&self) -> String {
        use heck::CamelCase;
        self.0.to_camel_case()
    }

    pub fn to_shouty_snake_case(&self) -> String {
        use heck::ShoutySnakeCase;
        self.0.to_shouty_snake_case()
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct TypePath {
    package: Package,
    path: Vec<TypeName>,
}

impl Display for TypePath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.package.fmt(f)?;
        for element in &self.path {
            write!(f, ".{}", element)?;
        }
        Ok(())
    }
}

impl TypePath {
    pub fn new(package: Package) -> Self {
        Self {
            package,
            path: Default::default(),
        }
    }

    pub fn package(&self) -> &Package {
        &self.package
    }

    pub fn path(&self) -> &[TypeName] {
        self.path.as_slice()
    }

    pub fn child(&self, name: TypeName) -> Self {
        let path = self
            .path
            .iter()
            .cloned()
            .chain(std::iter::once(name))
            .collect();
        Self {
            package: self.package.clone(),
            path,
        }
    }

    pub fn matches_prefix(&self, prefix: &str) -> bool {
        let prefix = match prefix.strip_prefix('.') {
            Some(prefix) => prefix,
            None => return false,
        };

        if prefix.len() <= self.package.0.len() {
            return self.package.0.starts_with(prefix);
        }

        match prefix.strip_prefix(&self.package.0) {
            Some(prefix) => {
                let split = prefix.split('.').skip(1);
                for zipped in self.path.iter().zip_longest(split) {
                    match zipped {
                        EitherOrBoth::Both(a, b) if a.0.as_str() == b => continue,
                        EitherOrBoth::Left(_) => return true,
                        _ => return false,
                    }
                }
                true
            }
            None => false,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct DescriptorSet {
    descriptors: BTreeMap<TypePath, Descriptor>,
}

impl DescriptorSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_encoded(&mut self, encoded: &[u8]) -> Result<()> {
        let descriptors: FileDescriptorSet =
            prost::Message::decode(encoded).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        for file in descriptors.file {
            let syntax = match file.syntax.as_deref() {
                None | Some("proto2") => Syntax::Proto2,
                Some("proto3") => Syntax::Proto3,
                Some(s) => panic!("unknown syntax: {}", s),
            };

            let package = Package::new(file.package.expect("expected package"));
            let path = TypePath::new(package);

            for descriptor in file.message_type {
                self.register_message(&path, descriptor, syntax)
            }

            for descriptor in file.enum_type {
                self.register_enum(&path, descriptor)
            }
        }

        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&TypePath, &Descriptor)> {
        self.descriptors.iter()
    }

    fn register_message(&mut self, path: &TypePath, descriptor: DescriptorProto, syntax: Syntax) {
        let name = TypeName::new(descriptor.name.expect("expected name"));
        let child_path = path.child(name);

        for child_descriptor in descriptor.enum_type {
            self.register_enum(&child_path, child_descriptor)
        }

        for child_descriptor in descriptor.nested_type {
            self.register_message(&child_path, child_descriptor, syntax)
        }

        self.register_descriptor(
            child_path.clone(),
            Descriptor::Message(MessageDescriptor {
                path: child_path,
                options: descriptor.options,
                one_of: descriptor.oneof_decl,
                fields: descriptor.field,
                syntax,
            }),
        );
    }

    fn register_enum(&mut self, path: &TypePath, descriptor: EnumDescriptorProto) {
        let name = TypeName::new(descriptor.name.expect("expected name"));
        self.register_descriptor(
            path.child(name),
            Descriptor::Enum(EnumDescriptor {
                values: descriptor.value,
            }),
        );
    }

    fn register_descriptor(&mut self, path: TypePath, descriptor: Descriptor) {
        match self.descriptors.entry(path) {
            Entry::Occupied(o) => panic!("descriptor already registered for {}", o.key()),
            Entry::Vacant(v) => v.insert(descriptor),
        };
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Syntax {
    Proto2,
    Proto3,
}

#[derive(Debug, Clone)]
pub enum Descriptor {
    Enum(EnumDescriptor),
    Message(MessageDescriptor),
}

#[derive(Debug, Clone)]
pub struct EnumDescriptor {
    pub values: Vec<EnumValueDescriptorProto>,
}

#[derive(Debug, Clone)]
pub struct MessageDescriptor {
    pub path: TypePath,
    pub options: Option<MessageOptions>,
    pub one_of: Vec<OneofDescriptorProto>,
    pub fields: Vec<FieldDescriptorProto>,
    pub syntax: Syntax,
}

impl MessageDescriptor {
    /// Whether this is an auto-generated type for the map field
    pub fn is_map(&self) -> bool {
        self.options
            .as_ref()
            .and_then(|options| options.map_entry)
            .unwrap_or(false)
    }
}
