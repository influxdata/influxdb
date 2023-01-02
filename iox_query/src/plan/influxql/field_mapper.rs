#![allow(dead_code)]

use crate::plan::influxql::var_ref::field_type_to_var_ref_data_type;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::common::{DataFusionError, Result};
use influxdb_influxql_parser::expression::VarRefDataType;
use schema::{InfluxColumnType, Schema};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(crate) type FieldTypeMap = HashMap<String, VarRefDataType>;
pub(crate) type TagSet = HashSet<String>;

/// Represents an InfluxQL schema for determining the fields and tags
/// of a measurement.
pub(crate) trait FieldMapper {
    /// Determine the fields and tags for the given measurement name.
    fn field_and_dimensions(&self, name: &str) -> Result<Option<(FieldTypeMap, TagSet)>>;
    /// Determine the [`VarRefDataType`] for the given field name.
    fn map_type(&self, name: &str, field: &str) -> Result<Option<VarRefDataType>>;
}

pub(crate) struct SchemaFieldMapper {
    schema: Arc<dyn SchemaProvider>,
}

impl SchemaFieldMapper {
    pub(crate) fn new(schema: Arc<dyn SchemaProvider>) -> Self {
        Self { schema }
    }

    fn get_schema(&self, name: &str) -> Result<Option<Schema>> {
        Ok(Some(
            Schema::try_from(match self.schema.table(name) {
                Some(t) => t.schema(),
                None => return Ok(None),
            })
            .map_err(|e| {
                DataFusionError::Internal(format!("Unable to create IOx schema: {}", e))
            })?,
        ))
    }
}

impl FieldMapper for SchemaFieldMapper {
    fn field_and_dimensions(&self, name: &str) -> Result<Option<(FieldTypeMap, TagSet)>> {
        match self.get_schema(name)? {
            Some(iox) => Ok(Some((
                FieldTypeMap::from_iter(iox.iter().filter_map(|(col_type, f)| match col_type {
                    InfluxColumnType::Field(ft) => {
                        Some((f.name().clone(), field_type_to_var_ref_data_type(ft)))
                    }
                    _ => None,
                })),
                iox.tags_iter()
                    .map(|f| f.name().clone())
                    .collect::<TagSet>(),
            ))),
            None => Ok(None),
        }
    }

    fn map_type(&self, measurement_name: &str, field: &str) -> Result<Option<VarRefDataType>> {
        match self.get_schema(measurement_name)? {
            Some(iox) => Ok(match iox.find_index_of(field) {
                Some(i) => match iox.field(i).0 {
                    InfluxColumnType::Field(ft) => Some(field_type_to_var_ref_data_type(ft)),
                    InfluxColumnType::Tag => Some(VarRefDataType::Tag),
                    InfluxColumnType::Timestamp => None,
                },
                None => None,
            }),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::plan::influxql::field_mapper::{
        FieldMapper, FieldTypeMap, SchemaFieldMapper, TagSet,
    };
    use crate::plan::influxql::test_utils::MockSchemaProvider;
    use assert_matches::assert_matches;
    use influxdb_influxql_parser::expression::VarRefDataType;

    #[test]
    fn test_schema_field_mapper() {
        let fm =
            &SchemaFieldMapper::new(MockSchemaProvider::new_schema_provider()) as &dyn FieldMapper;

        // Measurement exists
        let (field_set, tag_set) = fm.field_and_dimensions("cpu").unwrap().unwrap();
        assert_eq!(
            field_set,
            FieldTypeMap::from([
                ("usage_user".to_string(), VarRefDataType::Float),
                ("usage_system".to_string(), VarRefDataType::Float),
                ("usage_idle".to_string(), VarRefDataType::Float),
            ])
        );
        assert_eq!(
            tag_set,
            TagSet::from(["host".to_string(), "region".to_string()])
        );

        // Measurement does not exist
        assert!(fm.field_and_dimensions("cpu2").unwrap().is_none());

        // `map_type` API calls

        // Returns expected type
        assert_matches!(
            fm.map_type("cpu", "usage_user").unwrap(),
            Some(VarRefDataType::Float)
        );
        assert_matches!(
            fm.map_type("cpu", "host").unwrap(),
            Some(VarRefDataType::Tag)
        );
        // Returns None for nonexistent field
        assert!(fm.map_type("cpu", "nonexistent").unwrap().is_none());
        // Returns None for nonexistent measurement
        assert!(fm.map_type("nonexistent", "usage").unwrap().is_none());
    }
}
