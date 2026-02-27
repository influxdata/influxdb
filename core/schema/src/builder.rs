use std::convert::TryInto;

use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use snafu::{ResultExt, Snafu};

#[cfg(feature = "v3")]
use crate::{MissingSeriesKeyColumnSnafu, TagsNotInSeriesKeySnafu};

use super::{InfluxColumnType, InfluxFieldType, Schema, TIME_COLUMN_NAME};

/// Namespace schema creation / validation errors.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error validating schema: {}", source))]
    ValidatingSchema { source: super::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Builder for a Schema
#[derive(Debug, Default, Clone)]
pub struct SchemaBuilder {
    /// Optional measurement name
    measurement: Option<String>,

    /// The fields, in order
    fields: Vec<(ArrowField, Option<InfluxColumnType>)>,

    /// If the builder has been consumed
    finished: bool,

    /// The series key, if defined
    #[cfg(feature = "v3")]
    series_key: Option<Vec<String>>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(n: usize) -> Self {
        Self {
            measurement: Default::default(),
            fields: Vec::with_capacity(n),
            finished: Default::default(),
            #[cfg(feature = "v3")]
            series_key: None,
        }
    }

    #[cfg(feature = "v3")]
    pub fn series_key(&self) -> Option<&[String]> {
        self.series_key.as_deref()
    }

    #[cfg(feature = "v3")]
    pub fn with_series_key<I>(&mut self, columns: I) -> &mut Self
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        self.series_key = Some(
            columns
                .into_iter()
                .map(|c| c.as_ref().to_string())
                .collect(),
        );
        self
    }

    /// Add an arbitrary column with the specified Arrow datatype.
    #[cfg(feature = "v3")]
    pub fn arrow(
        &mut self,
        column_name: impl Into<String>,
        arrow_type: ArrowDataType,
        nullable: bool,
    ) -> &mut Self {
        self.add_column(column_name, nullable, None, arrow_type)
    }

    /// Add a new tag column to this schema. By default tags are
    /// potentially nullable as they are not guaranteed to be present
    /// for all rows
    pub fn tag(&mut self, column_name: impl Into<String>) -> &mut Self {
        let influxdb_column_type = InfluxColumnType::Tag;
        let arrow_type = (&influxdb_column_type).into();

        self.add_column(column_name, true, Some(influxdb_column_type), arrow_type)
    }

    /// Add a new field column with the specified InfluxDB data model type
    pub fn influx_field(
        &mut self,
        column_name: impl Into<String>,
        influxdb_field_type: InfluxFieldType,
    ) -> &mut Self {
        let arrow_type: ArrowDataType = influxdb_field_type.into();
        self.add_column(
            column_name,
            true,
            Some(InfluxColumnType::Field(influxdb_field_type)),
            arrow_type,
        )
    }

    /// Add a new field column with the specified InfluxDB data model type
    pub fn influx_column(
        &mut self,
        column_name: impl Into<String>,
        column_type: InfluxColumnType,
    ) -> &mut Self {
        match column_type {
            InfluxColumnType::Tag => self.tag(column_name),
            InfluxColumnType::Field(influx_field_type) => self
                .field(column_name, influx_field_type.into())
                .expect("just converted this from a valid type"),
            InfluxColumnType::Timestamp => self.timestamp(),
        }
    }

    /// Add a new nullable field column with the specified Arrow datatype.
    pub fn field(
        &mut self,
        column_name: impl Into<String>,
        arrow_type: ArrowDataType,
    ) -> Result<&mut Self, &'static str> {
        let influxdb_column_type = arrow_type.clone().try_into().map(InfluxColumnType::Field)?;

        Ok(self.add_column(column_name, true, Some(influxdb_column_type), arrow_type))
    }

    /// Add the InfluxDB data model timestamp column
    pub fn timestamp(&mut self) -> &mut Self {
        let influxdb_column_type = InfluxColumnType::Timestamp;
        let arrow_type = (&influxdb_column_type).into();
        self.add_column(
            TIME_COLUMN_NAME,
            false,
            Some(influxdb_column_type),
            arrow_type,
        )
    }

    /// Set optional InfluxDB data model measurement name
    pub fn measurement(&mut self, measurement_name: impl Into<String>) -> &mut Self {
        self.measurement = Some(measurement_name.into());
        self
    }

    /// Creates an Arrow schema with embedded metadata.
    /// All schema validation happens at this time.
    /// ```
    /// use schema::{builder::SchemaBuilder, InfluxColumnType, InfluxFieldType};
    ///
    /// let schema = SchemaBuilder::new()
    ///   .tag("region")
    ///   .influx_field("counter", InfluxFieldType::Float)
    ///   .timestamp()
    ///   .build()
    ///   .unwrap();
    ///
    /// let (influxdb_column_type, arrow_field) = schema.field(0);
    /// assert_eq!(arrow_field.name(), "region");
    /// assert_eq!(influxdb_column_type, InfluxColumnType::Tag);
    ///
    /// let (influxdb_column_type, arrow_field) = schema.field(1);
    /// assert_eq!(arrow_field.name(), "counter");
    /// assert_eq!(influxdb_column_type, InfluxColumnType::Field(InfluxFieldType::Float));
    ///
    /// let (influxdb_column_type, arrow_field) = schema.field(2);
    /// assert_eq!(arrow_field.name(), "time");
    /// assert_eq!(influxdb_column_type, InfluxColumnType::Timestamp);
    /// ```
    pub fn build(&mut self) -> Result<Schema> {
        assert!(!self.finished, "build called multiple times");
        self.finished = true;

        #[cfg(feature = "v3")]
        if let Some(sk) = self.series_key.as_ref() {
            if sk
                .iter()
                .any(|k| self.fields.iter().all(|(f, _)| f.name() != k))
            {
                return MissingSeriesKeyColumnSnafu
                    .fail()
                    .context(ValidatingSchemaSnafu);
            }

            if self
                .fields
                .iter()
                .any(|(f, t)| matches!(t, Some(InfluxColumnType::Tag)) && !sk.contains(f.name()))
            {
                return TagsNotInSeriesKeySnafu
                    .fail()
                    .context(ValidatingSchemaSnafu);
            }
        }

        Schema::new_from_parts(
            self.measurement.take(),
            self.fields.drain(..),
            false,
            #[cfg(feature = "v3")]
            self.series_key.take(),
        )
        .context(ValidatingSchemaSnafu)
    }

    /// Internal helper method to add a column definition
    fn add_column(
        &mut self,
        column_name: impl Into<String>,
        nullable: bool,
        column_type: Option<InfluxColumnType>,
        arrow_type: ArrowDataType,
    ) -> &mut Self {
        let field = ArrowField::new(column_name, arrow_type, nullable);

        self.fields.push((field, column_type));
        self
    }
}

#[expect(missing_docs)]
#[derive(Debug, Snafu)]
pub enum InvalidInsertionError {
    #[snafu(display(
        "Existing table schema specifies column {column} is type {table_type}, but type {given} was given"
    ))]
    TableSchemaConflict {
        column: String,
        table_type: InfluxColumnType,
        given: InfluxColumnType,
    },
}

/// A type capable of checking the validity of a column insertion into a Batch using information
/// external to the writer.
pub trait ColumnInsertValidator {
    /// Validates whether a new column with `col_name` and `col_type` can be added to the writer's
    /// Batch
    fn validate_insertion(
        &self,
        col_name: &str,
        col_type: InfluxColumnType,
    ) -> std::result::Result<(), InvalidInsertionError>;
}

impl<T> ColumnInsertValidator for &T
where
    T: ColumnInsertValidator,
{
    fn validate_insertion(
        &self,
        col_name: &str,
        col_type: InfluxColumnType,
    ) -> std::result::Result<(), InvalidInsertionError> {
        T::validate_insertion(self, col_name, col_type)
    }
}

/// A no-op [`ColumnInsertValidator`] implementation that always allows an
/// insert to proceed.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopColumnInsertValidator;

impl ColumnInsertValidator for NoopColumnInsertValidator {
    fn validate_insertion(
        &self,
        _col_name: &str,
        _col_type: InfluxColumnType,
    ) -> Result<(), InvalidInsertionError> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use InfluxColumnType::*;
    use InfluxFieldType::*;

    use crate::assert_column_eq;

    use super::*;

    #[test]
    fn test_builder_basic() {
        let s = SchemaBuilder::new()
            .influx_field("str_field", String)
            .tag("the_tag")
            .influx_field("int_field", Integer)
            .influx_field("uint_field", UInteger)
            .influx_field("bool_field", Boolean)
            .influx_field("float_field", Float)
            .tag("the_second_tag")
            .timestamp()
            .measurement("the_measurement")
            .build()
            .unwrap();

        assert_column_eq!(s, 0, Field(String), "str_field");
        assert_column_eq!(s, 1, Tag, "the_tag");
        assert_column_eq!(s, 2, Field(Integer), "int_field");
        assert_column_eq!(s, 3, Field(UInteger), "uint_field");
        assert_column_eq!(s, 4, Field(Boolean), "bool_field");
        assert_column_eq!(s, 5, Field(Float), "float_field");
        assert_column_eq!(s, 6, Tag, "the_second_tag");
        assert_column_eq!(s, 7, Timestamp, "time");

        assert_eq!(s.measurement().unwrap(), "the_measurement");
        assert_eq!(s.len(), 8);
    }

    #[test]
    fn test_builder_tag() {
        let s = SchemaBuilder::new()
            .tag("the_tag")
            .tag("the_other_tag")
            .build()
            .unwrap();

        let (influxdb_column_type, field) = s.field(0);
        assert_eq!(field.name(), "the_tag");
        assert_eq!(
            field.data_type(),
            &ArrowDataType::Dictionary(
                Box::new(ArrowDataType::Int32),
                Box::new(ArrowDataType::Utf8)
            )
        );
        assert!(field.is_nullable());
        assert_eq!(influxdb_column_type, Tag);

        let (influxdb_column_type, field) = s.field(1);
        assert_eq!(field.name(), "the_other_tag");
        assert_eq!(
            field.data_type(),
            &ArrowDataType::Dictionary(
                Box::new(ArrowDataType::Int32),
                Box::new(ArrowDataType::Utf8)
            )
        );
        assert!(field.is_nullable());
        assert_eq!(influxdb_column_type, Tag);

        assert_eq!(s.len(), 2);
    }

    #[test]
    fn test_builder_field() {
        let s = SchemaBuilder::new()
            .field("the_influx_field", ArrowDataType::Float64)
            .unwrap()
            .field("the_other_influx_field", ArrowDataType::Int64)
            .unwrap()
            .build()
            .unwrap();

        let (influxdb_column_type, field) = s.field(0);
        assert_eq!(field.name(), "the_influx_field");
        assert_eq!(field.data_type(), &ArrowDataType::Float64);
        assert!(field.is_nullable());
        assert_eq!(influxdb_column_type, Field(Float));

        let (influxdb_column_type, field) = s.field(1);
        assert_eq!(field.name(), "the_other_influx_field");
        assert_eq!(field.data_type(), &ArrowDataType::Int64);
        assert!(field.is_nullable());
        assert_eq!(influxdb_column_type, Field(Integer));

        assert_eq!(s.len(), 2);
    }

    #[test]
    fn test_builder_influx_field() {
        let s = SchemaBuilder::new()
            .influx_field("the_influx_field", InfluxFieldType::Float)
            .build()
            .unwrap();

        let (influxdb_column_type, field) = s.field(0);
        assert_eq!(field.name(), "the_influx_field");
        assert_eq!(field.data_type(), &ArrowDataType::Float64);
        assert!(field.is_nullable());
        assert_eq!(influxdb_column_type, Field(Float));

        assert_eq!(s.len(), 1);
    }

    #[test]
    fn test_builder_no_measurement() {
        let s = SchemaBuilder::new().tag("the tag").build().unwrap();

        assert_eq!(s.measurement(), None);
    }

    #[test]
    fn test_builder_dupe_tag() {
        let res = SchemaBuilder::new().tag("the tag").tag("the tag").build();

        assert_eq!(
            res.unwrap_err().to_string(),
            "Error validating schema: Internal Error: Duplicate column name found in schema: 'the tag'"
        );
    }

    #[test]
    fn test_builder_dupe_field_and_tag() {
        let res = SchemaBuilder::new()
            .tag("the name")
            .influx_field("the name", Integer)
            .build();

        assert_eq!(
            res.unwrap_err().to_string(),
            "Error validating schema: Internal Error: Duplicate column name found in schema: 'the name'"
        );
    }

    #[test]
    fn test_builder_dupe_field_and_timestamp() {
        let res = SchemaBuilder::new().tag("time").timestamp().build();

        assert_eq!(
            res.unwrap_err().to_string(),
            "Error validating schema: Internal Error: Duplicate column name found in schema: 'time'"
        );
    }

    #[cfg(feature = "v3")]
    #[test]
    fn test_builder_mismatch_series_key_and_tags() {
        SchemaBuilder::new()
            .with_series_key(["a", "b"])
            .influx_field("f1", Float)
            .timestamp()
            .measurement("foo")
            .build()
            .expect_err("no tag columns, but specified series key, should not be able to build");
        SchemaBuilder::new()
            .with_series_key(["a", "b"])
            .tag("a")
            .influx_field("f1", Float)
            .timestamp()
            .measurement("foo")
            .build()
            .expect_err("missing tag 'b' from the series key, should not be able to build");
        SchemaBuilder::new()
            .with_series_key(["a"])
            .tag("a")
            .tag("b")
            .influx_field("f1", Float)
            .timestamp()
            .measurement("foo")
            .build()
            .expect_err("extra tag 'b' that is not in the series key, should not be able to build");
    }
}
