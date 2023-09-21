use crate::plan::ir::TagSet;
use crate::plan::var_ref::{field_type_to_var_ref_data_type, influx_type_to_var_ref_data_type};
use crate::plan::SchemaProvider;
use influxdb_influxql_parser::expression::VarRefDataType;
use schema::InfluxColumnType;
use std::collections::HashMap;

pub(crate) type FieldTypeMap = HashMap<String, VarRefDataType>;

pub(crate) fn field_and_dimensions(
    s: &dyn SchemaProvider,
    name: &str,
) -> Option<(FieldTypeMap, TagSet)> {
    s.table_schema(name).map(|iox| {
        let mut field_set = FieldTypeMap::new();
        let mut tag_set = TagSet::new();

        for col in iox.iter() {
            match col {
                (InfluxColumnType::Field(ft), f) => {
                    field_set.insert(f.name().to_owned(), field_type_to_var_ref_data_type(ft));
                }
                (InfluxColumnType::Tag, f) => {
                    tag_set.insert(f.name().to_owned());
                }
                (InfluxColumnType::Timestamp, _) => {}
            }
        }
        (field_set, tag_set)
    })
}

pub(crate) fn map_type(
    s: &dyn SchemaProvider,
    measurement_name: &str,
    field: &str,
) -> Option<VarRefDataType> {
    s.table_schema(measurement_name).and_then(|iox| {
        iox.field_by_name(field)
            .and_then(|(dt, _)| influx_type_to_var_ref_data_type(Some(dt)))
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::plan::test_utils::MockSchemaProvider;
    use assert_matches::assert_matches;

    #[test]
    fn test_schema_field_mapper() {
        let namespace = MockSchemaProvider::default();

        // Measurement exists
        let (field_set, tag_set) = field_and_dimensions(&namespace, "cpu").unwrap();
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
            TagSet::from(["cpu".to_string(), "host".to_string(), "region".to_string()])
        );

        // Measurement does not exist
        assert!(field_and_dimensions(&namespace, "cpu2").is_none());

        // `map_type` API calls

        // Returns expected type
        assert_matches!(
            map_type(&namespace, "cpu", "usage_user"),
            Some(VarRefDataType::Float)
        );
        assert_matches!(
            map_type(&namespace, "cpu", "host"),
            Some(VarRefDataType::Tag)
        );
        assert_matches!(
            map_type(&namespace, "cpu", "time"),
            Some(VarRefDataType::Timestamp)
        );
        // Returns None for nonexistent field
        assert!(map_type(&namespace, "cpu", "nonexistent").is_none());
        // Returns None for nonexistent measurement
        assert!(map_type(&namespace, "nonexistent", "usage").is_none());
    }
}
