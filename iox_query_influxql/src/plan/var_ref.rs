use arrow::datatypes::DataType;
use influxdb_influxql_parser::expression::VarRefDataType;
use schema::{InfluxColumnType, InfluxFieldType};

/// Map a field-like data type to an equivalent Arrow data type.
pub(crate) fn var_ref_data_type_to_data_type(v: VarRefDataType) -> Option<DataType> {
    match v {
        VarRefDataType::Float => Some(DataType::Float64),
        VarRefDataType::Integer => Some(DataType::Int64),
        VarRefDataType::Unsigned => Some(DataType::UInt64),
        VarRefDataType::String => Some(DataType::Utf8),
        VarRefDataType::Boolean => Some(DataType::Boolean),
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

/// Maps an [`InfluxFieldType`] to a [`VarRefDataType`].
pub(crate) fn influx_type_to_var_ref_data_type(
    v: Option<InfluxColumnType>,
) -> Option<VarRefDataType> {
    match v {
        None => None,
        Some(InfluxColumnType::Tag) => Some(VarRefDataType::Tag),
        Some(InfluxColumnType::Field(ft)) => Some(field_type_to_var_ref_data_type(ft)),
        Some(InfluxColumnType::Timestamp) => Some(VarRefDataType::Timestamp),
    }
}

/// Maps an [`VarRefDataType`] to an [`InfluxColumnType`].
pub(crate) fn var_ref_data_type_to_influx_type(
    v: Option<VarRefDataType>,
) -> Option<InfluxColumnType> {
    match v {
        Some(VarRefDataType::Float) => Some(InfluxColumnType::Field(InfluxFieldType::Float)),
        Some(VarRefDataType::Integer) => Some(InfluxColumnType::Field(InfluxFieldType::Integer)),
        Some(VarRefDataType::Unsigned) => Some(InfluxColumnType::Field(InfluxFieldType::UInteger)),
        Some(VarRefDataType::String) => Some(InfluxColumnType::Field(InfluxFieldType::String)),
        Some(VarRefDataType::Boolean) => Some(InfluxColumnType::Field(InfluxFieldType::Boolean)),
        Some(VarRefDataType::Tag) => Some(InfluxColumnType::Tag),
        Some(VarRefDataType::Timestamp) => Some(InfluxColumnType::Timestamp),
        Some(VarRefDataType::Field) | None => None,
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
