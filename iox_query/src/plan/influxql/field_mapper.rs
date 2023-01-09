#![allow(dead_code)]

use crate::plan::influxql::var_ref::field_type_to_var_ref_data_type;
use datafusion::common::Result;
use influxdb_influxql_parser::expression::VarRefDataType;
use predicate::rpc_predicate::QueryNamespaceMeta;
use schema::InfluxColumnType;
use std::collections::{HashMap, HashSet};

pub(crate) type FieldTypeMap = HashMap<String, VarRefDataType>;
pub(crate) type TagSet = HashSet<String>;

pub(crate) fn field_and_dimensions(
    namespace: &dyn QueryNamespaceMeta,
    name: &str,
) -> Result<Option<(FieldTypeMap, TagSet)>> {
    match namespace.table_schema(name) {
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

pub(crate) fn map_type(
    namespace: &dyn QueryNamespaceMeta,
    measurement_name: &str,
    field: &str,
) -> Result<Option<VarRefDataType>> {
    match namespace.table_schema(measurement_name) {
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::plan::influxql::test_utils::MockNamespace;
    use assert_matches::assert_matches;

    #[test]
    fn test_schema_field_mapper() {
        let namespace = MockNamespace::default();

        // Measurement exists
        let (field_set, tag_set) = field_and_dimensions(&namespace, "cpu").unwrap().unwrap();
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
        assert!(field_and_dimensions(&namespace, "cpu2").unwrap().is_none());

        // `map_type` API calls

        // Returns expected type
        assert_matches!(
            map_type(&namespace, "cpu", "usage_user").unwrap(),
            Some(VarRefDataType::Float)
        );
        assert_matches!(
            map_type(&namespace, "cpu", "host").unwrap(),
            Some(VarRefDataType::Tag)
        );
        // Returns None for nonexistent field
        assert!(map_type(&namespace, "cpu", "nonexistent")
            .unwrap()
            .is_none());
        // Returns None for nonexistent measurement
        assert!(map_type(&namespace, "nonexistent", "usage")
            .unwrap()
            .is_none());
    }
}
