#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

//! This module is used to represent the abstract "schema" of a set of line
//! protocol data records, as defined in the
//! [documentation](https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial).
//!
//! The line protocol format has an inherently "flexible" schema
//! (e.g. the tags and fields for a measurement can and do change over
//! time), the schema only makes sense for a given set of rows (not
//! all possible rows in that measurement).
//!
//! The line protocol schema consists of a series of columns, each with a
//! specific type, indexed by 0.
//!
//! ```
//! use delorean_table_schema::{SchemaBuilder, DataType, ColumnDefinition};
//! let schema = SchemaBuilder::new(String::from("my_measurement"))
//!     .tag("tag1")
//!     .field("field1", DataType::Float)
//!     .field("field2", DataType::Boolean)
//!     .tag("tag2")
//!     .build();
//!
//! let cols = schema.get_col_defs();
//! assert_eq!(cols.len(), 5);
//! assert_eq!(cols[0], ColumnDefinition::new("tag1", 0, DataType::String));
//! assert_eq!(cols[1], ColumnDefinition::new("tag2", 1, DataType::String));
//! assert_eq!(cols[2], ColumnDefinition::new("field1", 2, DataType::Float));
//! assert_eq!(cols[3], ColumnDefinition::new("field2", 3, DataType::Boolean));
//! assert_eq!(cols[4], ColumnDefinition::new("timestamp", 4, DataType::Timestamp));
//! ```
use delorean_tsm::BlockType;

use log::warn;
use std::collections::BTreeMap;
use std::convert::From;

/// Represents a specific Line Protocol Tag name
#[derive(Debug, PartialEq)]
pub struct Tag {
    pub name: String,
    index: u32,
}

impl Tag {
    pub fn new(name: impl Into<String>, index: u32) -> Self {
        Self {
            name: name.into(),
            index,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
/// Line Protocol Data Types as defined in [the InfluxData documentation][influx]
///
/// [influx]: https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/#data-types
pub enum DataType {
    /// 64-bit floating point number (TDB if NULLs / Nans are allowed)
    Float,
    /// 64-bit signed integer
    Integer,
    /// UTF-8 encoded string
    String,
    /// true or false
    Boolean,
    /// 64 bit timestamp "UNIX timestamps" representing nanosecods
    /// since the UNIX epoch (00:00:00 UTC on 1 January 1970).
    Timestamp,
}

impl From<&BlockType> for DataType {
    fn from(value: &BlockType) -> Self {
        match value {
            BlockType::Float => Self::Float,
            BlockType::Integer => Self::Integer,
            BlockType::Bool => Self::Boolean,
            BlockType::Str => Self::String,
            BlockType::Unsigned => Self::Integer,
        }
    }
}

/// Represents a specific Line Protocol Field name
#[derive(Debug, PartialEq)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    index: u32,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType, index: u32) -> Self {
        Self {
            name: name.into(),
            data_type,
            index,
        }
    }
}

/// Represents a column of the line protocol data (specifically how to
/// find tag values and field values in a set of columns)
#[derive(Debug, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub index: u32,
    pub data_type: DataType,
}

impl ColumnDefinition {
    pub fn new(name: impl Into<String>, index: u32, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            index,
            data_type,
        }
    }
}

/// Represents the overall "schema" of line protocol data. See the
/// module definition for more details and example of how to construct
/// and access a `Schema` object.
#[derive(Debug)]
pub struct Schema {
    measurement: String,
    tags: BTreeMap<String, Tag>,
    fields: BTreeMap<String, Field>,
    timestamp_name: String,
    timestamp_index: u32,
}

impl Schema {
    pub fn measurement(&self) -> &str {
        &self.measurement
    }

    /// Return true if `col_def` holds values for a Tag (as opposed to Field or Timestamp)
    pub fn is_tag(&self, col_def: &ColumnDefinition) -> bool {
        self.tags.contains_key(&col_def.name)
    }

    /// Return the name of the column used to store timestamps
    pub fn timestamp(&self) -> &String {
        &self.timestamp_name
    }

    // Return a Vec of `ColumnDefinition`s such that
    // `v[idx].index == idx` for all columns
    // (aka that the vec is in the same order as the columns of the schema
    // FIXME : consider pre-computing this on schema directly.
    pub fn get_col_defs(&self) -> Vec<ColumnDefinition> {
        let mut cols = Vec::with_capacity(self.tags.len() + self.fields.len() + 1);
        cols.extend(self.tags.iter().map(|(name, tag)| ColumnDefinition {
            name: name.clone(),
            index: tag.index,
            data_type: DataType::String,
        }));
        cols.extend(self.fields.iter().map(|(name, field)| ColumnDefinition {
            name: name.clone(),
            index: field.index,
            data_type: field.data_type,
        }));
        cols.push(ColumnDefinition {
            name: self.timestamp_name.clone(),
            index: self.timestamp_index,
            data_type: DataType::Timestamp,
        });

        cols.sort_by_key(|col| col.index);
        cols
    }
}

/// Used to create new `Schema` objects
#[derive(Debug)]
pub struct SchemaBuilder {
    measurement_name: String,
    tag_names: Vec<String>,
    field_defs: Vec<(String, DataType)>,
}

impl SchemaBuilder {
    /// Begin building the schema for a named measurement
    pub fn new(measurement_name: impl Into<String>) -> Self {
        Self {
            measurement_name: measurement_name.into(),
            tag_names: Vec::new(),
            field_defs: Vec::new(),
        }
    }

    pub fn get_measurement_name(&self) -> &String {
        &self.measurement_name
    }

    /// Add a new tag name to the schema.
    pub fn tag(mut self, name: &str) -> Self {
        // check for existing tag (FIXME make this faster)
        if self.tag_names.iter().find(|&s| s == name).is_none() {
            self.tag_names.push(name.to_string());
        }
        self
    }

    /// Add a new typed field to the schema. Field names can not be repeated
    pub fn field(mut self, name: &str, data_type: DataType) -> Self {
        // check for existing fields (FIXME make this faster)
        match self
            .field_defs
            .iter()
            .find(|(existing_name, _)| existing_name == name)
        {
            Some((_, existing_type)) => {
                if *existing_type != data_type {
                    warn!("Ignoring new type for field '{}': Previously it had type {:?}, attempted to set type {:?}.",
                          name, existing_type, data_type);
                }
            }
            None => {
                let new_field_def = (name.to_string(), data_type);
                self.field_defs.push(new_field_def);
            }
        }
        self
    }

    /// Create a new schema from a list of tag names and (field_name, data_type) pairs
    pub fn build(self) -> Schema {
        // assign column indexes to all columns, starting at 0
        let mut indexer = 0..;

        Schema {
            measurement: self.measurement_name.to_string(),
            tags: self
                .tag_names
                .iter()
                .map(|name| {
                    (
                        name.clone(),
                        Tag::new(name.clone(), indexer.next().unwrap()),
                    )
                })
                .collect(),
            fields: self
                .field_defs
                .iter()
                .map(|(name, typ)| {
                    (
                        name.clone(),
                        Field::new(name.clone(), *typ, indexer.next().unwrap()),
                    )
                })
                .collect(),
            timestamp_index: indexer.next().unwrap(),
            timestamp_name: String::from("timestamp"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn construct() {
        let builder = SchemaBuilder::new(String::from("my_measurement"))
            .tag("tag1")
            .field("field1", DataType::Float)
            .field("field2", DataType::Boolean)
            .tag("tag2");

        let schema = builder.build();

        assert_eq!(schema.measurement, "my_measurement");
        assert_eq!(schema.tags.len(), 2);
        assert_eq!(
            schema.tags.get("tag1"),
            Some(&Tag::new(String::from("tag1"), 0))
        );
        assert_eq!(
            schema.tags.get("tag2"),
            Some(&Tag::new(String::from("tag2"), 1))
        );
        assert_eq!(
            schema.fields.get("field1"),
            Some(&Field::new(String::from("field1"), DataType::Float, 2))
        );
        assert_eq!(
            schema.fields.get("field2"),
            Some(&Field::new(String::from("field2"), DataType::Boolean, 3))
        );
        assert_eq!(schema.timestamp_index, 4);
    }

    #[test]
    fn duplicate_tag_names() {
        let schema = SchemaBuilder::new(String::from("my_measurement"))
            .tag("tag1")
            .tag("tag1")
            .build();

        let cols = schema.get_col_defs();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0], ColumnDefinition::new("tag1", 0, DataType::String));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("timestamp", 1, DataType::Timestamp)
        );
    }

    #[test]
    fn duplicate_field_name_same_type() {
        let schema = SchemaBuilder::new(String::from("my_measurement"))
            .field("field1", DataType::Float)
            .field("field1", DataType::Float)
            .build();

        let cols = schema.get_col_defs();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0], ColumnDefinition::new("field1", 0, DataType::Float));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("timestamp", 1, DataType::Timestamp)
        );
    }

    #[test]
    fn duplicate_field_name_different_type() {
        let schema = SchemaBuilder::new(String::from("my_measurement"))
            .field("field1", DataType::Float)
            .field("field1", DataType::Integer)
            .build();
        // second Integer definition should be ignored, and type remains float
        let cols = schema.get_col_defs();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0], ColumnDefinition::new("field1", 0, DataType::Float));
        assert_eq!(
            cols[1],
            ColumnDefinition::new("timestamp", 1, DataType::Timestamp)
        );
    }

    #[test]
    fn get_col_defs() {
        let schema = SchemaBuilder::new(String::from("my_measurement"))
            .tag("tag1")
            .field("field1", DataType::Float)
            .field("field2", DataType::Boolean)
            .tag("tag2")
            .build();

        let cols = schema.get_col_defs();
        assert_eq!(cols.len(), 5);
        assert_eq!(cols[0], ColumnDefinition::new("tag1", 0, DataType::String));
        assert_eq!(cols[1], ColumnDefinition::new("tag2", 1, DataType::String));
        assert_eq!(cols[2], ColumnDefinition::new("field1", 2, DataType::Float));
        assert_eq!(
            cols[3],
            ColumnDefinition::new("field2", 3, DataType::Boolean)
        );
        assert_eq!(
            cols[4],
            ColumnDefinition::new("timestamp", 4, DataType::Timestamp)
        );
    }

    #[test]
    fn get_col_defs_empty() {
        let schema = SchemaBuilder::new(String::from("my_measurement")).build();

        let cols = schema.get_col_defs();
        assert_eq!(cols.len(), 1);
        assert_eq!(
            cols[0],
            ColumnDefinition::new("timestamp", 0, DataType::Timestamp)
        );
    }

    #[test]
    fn get_col_defs_sort() {
        // Test that get_col_defs sorts its output
        let mut schema = SchemaBuilder::new(String::from("my_measurement"))
            .tag("tag1")
            .field("field1", DataType::Float)
            .build();

        let cols = schema.get_col_defs();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0], ColumnDefinition::new("tag1", 0, DataType::String));
        assert_eq!(cols[1], ColumnDefinition::new("field1", 1, DataType::Float));
        assert_eq!(
            cols[2],
            ColumnDefinition::new("timestamp", 2, DataType::Timestamp)
        );

        // Now, if we somehow have changed how the indexes are
        // assigned, the columns should still appear in order
        schema.tags.get_mut("tag1").unwrap().index = 2;
        schema.timestamp_index = 0;

        let cols = schema.get_col_defs();
        assert_eq!(cols.len(), 3);
        assert_eq!(
            cols[0],
            ColumnDefinition::new("timestamp", 0, DataType::Timestamp)
        );
        assert_eq!(cols[1], ColumnDefinition::new("field1", 1, DataType::Float));
        assert_eq!(cols[2], ColumnDefinition::new("tag1", 2, DataType::String));
    }

    #[test]
    fn is_tag() {
        let schema = SchemaBuilder::new("my_measurement")
            .tag("tag1")
            .field("field1", DataType::Float)
            .build();
        let cols = schema.get_col_defs();

        assert!(schema.is_tag(&cols[0]));
        assert!(!schema.is_tag(&cols[1]));
        assert!(!schema.is_tag(&cols[2]));
    }
}
