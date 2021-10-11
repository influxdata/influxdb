use std::convert::TryInto;

use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use snafu::{ResultExt, Snafu};

use crate::sort::SortKey;

use super::{InfluxColumnType, InfluxFieldType, Schema, TIME_COLUMN_NAME};

/// Database schema creation / validation errors.
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
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a new tag column to this schema. By default tags are
    /// potentially nullable as they are not guaranteed to be present
    /// for all rows
    pub fn tag(&mut self, column_name: &str) -> &mut Self {
        let influxdb_column_type = InfluxColumnType::Tag;
        let arrow_type = (&influxdb_column_type).into();

        self.add_column(column_name, true, Some(influxdb_column_type), arrow_type)
    }

    /// Add a new tag column to this schema that is known (somehow) to
    /// have no nulls for all rows
    pub fn non_null_tag(&mut self, column_name: &str) -> &mut Self {
        let influxdb_column_type = InfluxColumnType::Tag;
        let arrow_type = (&influxdb_column_type).into();

        self.add_column(column_name, false, Some(influxdb_column_type), arrow_type)
    }

    /// Add a new field column with the specified InfluxDB data model type
    pub fn influx_field(
        &mut self,
        column_name: &str,
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
    pub fn influx_column(&mut self, column_name: &str, column_type: InfluxColumnType) -> &mut Self {
        match column_type {
            InfluxColumnType::IOx(iox_value_type) => self.field(column_name, iox_value_type.into()),
            InfluxColumnType::Tag => self.tag(column_name),
            InfluxColumnType::Field(influx_field_type) => {
                self.field(column_name, influx_field_type.into())
            }
            InfluxColumnType::Timestamp => self.timestamp(),
        }
    }

    /// Add a new nullable field column with the specified Arrow datatype.
    pub fn field(&mut self, column_name: &str, arrow_type: ArrowDataType) -> &mut Self {
        let influxdb_column_type = arrow_type
            .clone()
            .try_into()
            .map(InfluxColumnType::Field)
            .ok();

        self.add_column(column_name, true, influxdb_column_type, arrow_type)
    }

    /// Add a new field column with the specified Arrow datatype that can not be
    /// null
    pub fn non_null_field(&mut self, column_name: &str, arrow_type: ArrowDataType) -> &mut Self {
        let influxdb_column_type = arrow_type
            .clone()
            .try_into()
            .map(InfluxColumnType::Field)
            .ok();

        self.add_column(column_name, false, influxdb_column_type, arrow_type)
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
    /// assert_eq!(influxdb_column_type, Some(InfluxColumnType::Tag));
    ///
    /// let (influxdb_column_type, arrow_field) = schema.field(1);
    /// assert_eq!(arrow_field.name(), "counter");
    /// assert_eq!(influxdb_column_type, Some(InfluxColumnType::Field(InfluxFieldType::Float)));
    ///
    /// let (influxdb_column_type, arrow_field) = schema.field(2);
    /// assert_eq!(arrow_field.name(), "time");
    /// assert_eq!(influxdb_column_type, Some(InfluxColumnType::Timestamp));
    /// ```
    pub fn build(&mut self) -> Result<Schema> {
        self.build_with_sort_key(&Default::default())
    }

    pub fn build_with_sort_key(&mut self, sort_key: &SortKey<'_>) -> Result<Schema> {
        assert!(!self.finished, "build called multiple times");
        self.finished = true;

        Schema::new_from_parts(
            self.measurement.take(),
            self.fields.drain(..),
            sort_key,
            false,
        )
        .context(ValidatingSchema)
    }

    /// Internal helper method to add a column definition
    fn add_column(
        &mut self,
        column_name: &str,
        nullable: bool,
        column_type: Option<InfluxColumnType>,
        arrow_type: ArrowDataType,
    ) -> &mut Self {
        let field = ArrowField::new(column_name, arrow_type, nullable);

        self.fields.push((field, column_type));
        self
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
            .non_null_tag("the_non_null_tag")
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
        assert_eq!(influxdb_column_type, Some(Tag));

        let (influxdb_column_type, field) = s.field(1);
        assert_eq!(field.name(), "the_non_null_tag");
        assert_eq!(
            field.data_type(),
            &ArrowDataType::Dictionary(
                Box::new(ArrowDataType::Int32),
                Box::new(ArrowDataType::Utf8)
            )
        );
        assert!(!field.is_nullable());
        assert_eq!(influxdb_column_type, Some(Tag));

        assert_eq!(s.len(), 2);
    }

    #[test]
    fn test_builder_field() {
        let s = SchemaBuilder::new()
            .field("the_influx_field", ArrowDataType::Float64)
            // can't represent with lp
            .field("the_no_influx_field", ArrowDataType::Decimal(10, 0))
            .build()
            .unwrap();

        let (influxdb_column_type, field) = s.field(0);
        assert_eq!(field.name(), "the_influx_field");
        assert_eq!(field.data_type(), &ArrowDataType::Float64);
        assert!(field.is_nullable());
        assert_eq!(influxdb_column_type, Some(Field(Float)));

        let (influxdb_column_type, field) = s.field(1);
        assert_eq!(field.name(), "the_no_influx_field");
        assert_eq!(field.data_type(), &ArrowDataType::Decimal(10, 0));
        assert!(field.is_nullable());
        assert_eq!(influxdb_column_type, None);

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
        assert_eq!(influxdb_column_type, Some(Field(Float)));

        assert_eq!(s.len(), 1);
    }

    #[test]
    fn test_builder_non_field() {
        let s = SchemaBuilder::new()
            .non_null_field("the_influx_field", ArrowDataType::Float64)
            // can't represent with lp
            .non_null_field("the_no_influx_field", ArrowDataType::Decimal(10, 0))
            .build()
            .unwrap();

        let (influxdb_column_type, field) = s.field(0);
        assert_eq!(field.name(), "the_influx_field");
        assert_eq!(field.data_type(), &ArrowDataType::Float64);
        assert!(!field.is_nullable());
        assert_eq!(influxdb_column_type, Some(Field(Float)));

        let (influxdb_column_type, field) = s.field(1);
        assert_eq!(field.name(), "the_no_influx_field");
        assert_eq!(field.data_type(), &ArrowDataType::Decimal(10, 0));
        assert!(!field.is_nullable());
        assert_eq!(influxdb_column_type, None);

        assert_eq!(s.len(), 2);
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
            "Error validating schema: Error: Duplicate column name found in schema: 'the tag'"
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
            "Error validating schema: Error: Duplicate column name found in schema: 'the name'"
        );
    }

    #[test]
    fn test_builder_dupe_field_and_timestamp() {
        let res = SchemaBuilder::new().tag("time").timestamp().build();

        assert_eq!(
            res.unwrap_err().to_string(),
            "Error validating schema: Error: Duplicate column name found in schema: 'time'"
        );
    }
}
