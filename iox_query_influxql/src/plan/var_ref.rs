use arrow::datatypes::DataType;
use influxdb_influxql_parser::expression::VarRefDataType;
use schema::{InfluxColumnType, InfluxFieldType};

pub(crate) fn var_ref_data_type_to_data_type(v: VarRefDataType) -> Option<DataType> {
    match v {
        VarRefDataType::Float => Some(DataType::Float64),
        VarRefDataType::Integer => Some(DataType::Int64),
        VarRefDataType::Unsigned => Some(DataType::UInt64),
        VarRefDataType::String => Some(DataType::Utf8),
        VarRefDataType::Boolean => Some(DataType::Boolean),
        VarRefDataType::Tag => Some(DataType::Utf8),
        VarRefDataType::Field | VarRefDataType::Timestamp => None,
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

/// Maps an [`InfluxColumnType`] to a [`VarRefDataType`].
pub(crate) fn column_type_to_var_ref_data_type(v: InfluxColumnType) -> VarRefDataType {
    match v {
        InfluxColumnType::Tag => VarRefDataType::Tag,
        InfluxColumnType::Field(ft) => field_type_to_var_ref_data_type(ft),
        InfluxColumnType::Timestamp => VarRefDataType::Timestamp,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use assert_matches::assert_matches;

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
