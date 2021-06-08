use std::convert::TryInto;

use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use snafu::{ResultExt, Snafu};

use crate::schema::COLUMN_METADATA_KEY;

use super::{InfluxColumnType, InfluxFieldType, Schema, TIME_COLUMN_NAME};

/// Database schema creation / validation errors.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error validating schema: {}", source))]
    ValidatingSchema { source: super::Error },

    #[snafu(display("Error while merging schemas: {}", source))]
    MergingSchemas { source: super::Error },

    #[snafu(display("No schemas found when building merged schema",))]
    NoSchemas {},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Builder for a Schema
#[derive(Debug, Default, Clone)]
pub struct SchemaBuilder {
    /// Optional measurement name
    measurement: Option<String>,

    /// The fields, in order
    fields: Vec<ArrowField>,
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
            InfluxColumnType::Tag => self.tag(column_name),
            InfluxColumnType::Field(field) => self.field(column_name, field.into()),
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

    /// Creates an Arrow schema with embedded metadata, resetting the
    /// builder back to `default`. All schema validation happens at
    /// this time.
    ///
    /// ```
    /// use internal_types::schema::{builder::SchemaBuilder, InfluxColumnType, InfluxFieldType};
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
        Schema::new_from_parts(self.measurement.take(), std::mem::take(&mut self.fields))
            .context(ValidatingSchema)
    }

    /// Internal helper method to add a column definition
    fn add_column(
        &mut self,
        column_name: &str,
        nullable: bool,
        influxdb_column_type: Option<InfluxColumnType>,
        arrow_type: ArrowDataType,
    ) -> &mut Self {
        let mut field = ArrowField::new(column_name, arrow_type, nullable);
        if let Some(column_type) = influxdb_column_type {
            field.set_metadata(Some(
                vec![(COLUMN_METADATA_KEY.to_string(), column_type.to_string())]
                    .into_iter()
                    .collect(),
            ))
        }

        self.fields.push(field);
        self
    }
}

/// Schema Merger
///
/// The usecase for merging schemas is when different chunks have
/// different schemas. This struct can be used to build a combined
/// schema by mergeing Schemas together according to the following
/// rules:
///
/// 1. New columns may be added in subsequent schema, but the types of
///    the columns (including any metadata) must be the same
///
/// 2. The measurement names must be consistent: one or both can be
///    `None`, or they can both be  are `Some(name`)
#[derive(Debug, Default)]
pub struct SchemaMerger {
    inner: Option<Schema>,
}

impl SchemaMerger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends the schema to the merged schema being built,
    /// validating that no columns are added.
    ///
    /// O(n^2) in the number of fields (columns)
    pub fn merge(mut self, new_schema: Schema) -> Result<Self> {
        self.inner = match self.inner.take() {
            None => Some(new_schema),
            Some(existing_schema) => {
                let merged_schema = existing_schema
                    .try_merge(new_schema)
                    .context(MergingSchemas)?;
                Some(merged_schema)
            }
        };
        Ok(self)
    }

    /// Returns the schema that was built, consuming the builder
    pub fn build(self) -> Result<Schema> {
        self.inner.map(Ok).unwrap_or_else(|| NoSchemas {}.fail())
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
        assert_eq!(field.is_nullable(), true);
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
        assert_eq!(field.is_nullable(), false);
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
        assert_eq!(field.is_nullable(), true);
        assert_eq!(influxdb_column_type, Some(Field(Float)));

        let (influxdb_column_type, field) = s.field(1);
        assert_eq!(field.name(), "the_no_influx_field");
        assert_eq!(field.data_type(), &ArrowDataType::Decimal(10, 0));
        assert_eq!(field.is_nullable(), true);
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
        assert_eq!(field.is_nullable(), true);
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
        assert_eq!(field.is_nullable(), false);
        assert_eq!(influxdb_column_type, Some(Field(Float)));

        let (influxdb_column_type, field) = s.field(1);
        assert_eq!(field.name(), "the_no_influx_field");
        assert_eq!(field.data_type(), &ArrowDataType::Decimal(10, 0));
        assert_eq!(field.is_nullable(), false);
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

    #[test]
    fn test_merge_schema_empty() {
        let merged_schema_error = SchemaMerger::new().build().unwrap_err();

        assert_eq!(
            merged_schema_error.to_string(),
            "No schemas found when building merged schema"
        );
    }

    #[test]
    fn test_merge_same_schema() {
        let schema1 = SchemaBuilder::new()
            .tag("the_tag")
            .influx_field("int_field", Integer)
            .build()
            .unwrap();

        let schema2 = SchemaBuilder::new()
            .tag("the_tag")
            .influx_field("int_field", Integer)
            .build()
            .unwrap();

        let merged_schema = SchemaMerger::new()
            .merge(schema1.clone())
            .unwrap()
            .merge(schema2.clone())
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(merged_schema, schema1);
        assert_eq!(merged_schema, schema2);
    }
}
