use influxdb_influxql_parser::expression::VarRefDataType;
use schema::InfluxFieldType;

#[allow(dead_code)]
/// Maps a [`VarRefDataType`] to an [`InfluxFieldType`], or `None` if no such mapping exists.
pub(crate) fn var_ref_data_type_to_field_type(v: VarRefDataType) -> Option<InfluxFieldType> {
    match v {
        VarRefDataType::Integer => Some(InfluxFieldType::Integer),
        VarRefDataType::Unsigned => Some(InfluxFieldType::UInteger),
        VarRefDataType::Float => Some(InfluxFieldType::Float),
        VarRefDataType::String => Some(InfluxFieldType::String),
        VarRefDataType::Boolean => Some(InfluxFieldType::Boolean),
        VarRefDataType::Tag | VarRefDataType::Field | VarRefDataType::Timestamp => None,
    }
}

/// Maps an [`InfluxFieldType`] to a [`VarRefDataType`].
pub(crate) fn field_type_to_var_ref_data_type(v: InfluxFieldType) -> VarRefDataType {
    match v {
        InfluxFieldType::Integer => VarRefDataType::Integer,
        InfluxFieldType::UInteger => VarRefDataType::Unsigned,
        InfluxFieldType::Float => VarRefDataType::Float,
        InfluxFieldType::String => VarRefDataType::String,
        InfluxFieldType::Boolean => VarRefDataType::Boolean,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use assert_matches::assert_matches;

    #[test]
    fn test_var_ref_data_type_to_field_type() {
        assert_matches!(
            var_ref_data_type_to_field_type(VarRefDataType::Float),
            Some(InfluxFieldType::Float)
        );
        assert_matches!(
            var_ref_data_type_to_field_type(VarRefDataType::Integer),
            Some(InfluxFieldType::Integer)
        );
        assert_matches!(
            var_ref_data_type_to_field_type(VarRefDataType::Unsigned),
            Some(InfluxFieldType::UInteger)
        );
        assert_matches!(
            var_ref_data_type_to_field_type(VarRefDataType::String),
            Some(InfluxFieldType::String)
        );
        assert_matches!(
            var_ref_data_type_to_field_type(VarRefDataType::Boolean),
            Some(InfluxFieldType::Boolean)
        );
        assert!(var_ref_data_type_to_field_type(VarRefDataType::Field).is_none());
        assert!(var_ref_data_type_to_field_type(VarRefDataType::Tag).is_none());
        assert!(var_ref_data_type_to_field_type(VarRefDataType::Timestamp).is_none());
    }

    #[test]
    fn test_field_type_to_var_ref_data_type() {
        assert_matches!(
            field_type_to_var_ref_data_type(InfluxFieldType::Float),
            VarRefDataType::Float
        );
        assert_matches!(
            field_type_to_var_ref_data_type(InfluxFieldType::Integer),
            VarRefDataType::Integer
        );
        assert_matches!(
            field_type_to_var_ref_data_type(InfluxFieldType::UInteger),
            VarRefDataType::Unsigned
        );
        assert_matches!(
            field_type_to_var_ref_data_type(InfluxFieldType::String),
            VarRefDataType::String
        );
        assert_matches!(
            field_type_to_var_ref_data_type(InfluxFieldType::Boolean),
            VarRefDataType::Boolean
        );
    }
}
