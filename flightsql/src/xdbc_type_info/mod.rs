//! Represents the response to FlightSQL `GetXdbcTypeInfo` requests and
//! handles the conversion to/from the format specified in the
//! [Arrow FlightSQL Specification].
//!
//! <
//!    type_name: utf8 not null (The name of the data type, for example: VARCHAR, INTEGER, etc),
//!    data_type: int32 not null (The SQL data type),
//!    column_size: int32 (The maximum size supported by that column.
//!                        In case of exact numeric types, this represents the maximum precision.
//!                        In case of string types, this represents the character length.
//!                        In case of datetime data types, this represents the length in characters of the string representation.
//!                        NULL is returned for data types where column size is not applicable.),
//!    literal_prefix: utf8 (Character or characters used to prefix a literal, NULL is returned for
//!                          data types where a literal prefix is not applicable.),
//!    literal_suffix: utf8 (Character or characters used to terminate a literal,
//!                          NULL is returned for data types where a literal suffix is not applicable.),
//!    create_params: list< utf8 not null >
//!                         (A list of keywords corresponding to which parameters can be used when creating
//!                          a column for that specific type.
//!                          NULL is returned if there are no parameters for the data type definition.),
//!    nullable: int32 not null (Shows if the data type accepts a NULL value. The possible values can be seen in the
//!                              Nullable enum.),
//!    case_sensitive: bool not null (Shows if a character data type is case-sensitive in collations and comparisons),
//!    searchable: int32 not null (Shows how the data type is used in a WHERE clause. The possible values can be seen in the
//!                                Searchable enum.),
//!    unsigned_attribute: bool (Shows if the data type is unsigned. NULL is returned if the attribute is
//!                              not applicable to the data type or the data type is not numeric.),
//!    fixed_prec_scale: bool not null (Shows if the data type has predefined fixed precision and scale.),
//!    auto_increment: bool (Shows if the data type is auto incremental. NULL is returned if the attribute
//!                          is not applicable to the data type or the data type is not numeric.),
//!    local_type_name: utf8 (Localized version of the data source-dependent name of the data type. NULL
//!                           is returned if a localized name is not supported by the data source),
//!    minimum_scale: int32 (The minimum scale of the data type on the data source.
//!                          If a data type has a fixed scale, the MINIMUM_SCALE and MAXIMUM_SCALE
//!                          columns both contain this value. NULL is returned if scale is not applicable.),
//!    maximum_scale: int32 (The maximum scale of the data type on the data source.
//!                          NULL is returned if scale is not applicable.),
//!    sql_data_type: int32 not null (The value of the SQL DATA TYPE which has the same values
//!                                   as data_type value. Except for interval and datetime, which
//!                                   uses generic values. More info about those types can be
//!                                   obtained through datetime_subcode. The possible values can be seen
//!                                   in the XdbcDataType enum.),
//!    datetime_subcode: int32 (Only used when the SQL DATA TYPE is interval or datetime. It contains
//!                             its sub types. For type different from interval and datetime, this value
//!                             is NULL. The possible values can be seen in the XdbcDatetimeSubcode enum.),
//!    num_prec_radix: int32 (If the data type is an approximate numeric type, this column contains
//!                           the value 2 to indicate that COLUMN_SIZE specifies a number of bits. For
//!                           exact numeric types, this column contains the value 10 to indicate that
//!                           column size specifies a number of decimal digits. Otherwise, this column is NULL.),
//!    interval_precision: int32 (If the data type is an interval data type, then this column contains the value
//!                               of the interval leading precision. Otherwise, this column is NULL. This fields
//!                               is only relevant to be used by ODBC).
//! >
//! The returned data should be ordered by data_type and then by type_name.
//!
//!
//! [Arrow FlightSQL Specification]: https://github.com/apache/arrow/blob/9588da967c756b2923e213ccc067378ba6c90a86/format/FlightSql.proto#L1064-L1113

use arrow_flight::sql::metadata::{XdbcTypeInfo, XdbcTypeInfoData, XdbcTypeInfoDataBuilder};
use arrow_flight::sql::{Nullable, Searchable, XdbcDataType, XdbcDatetimeSubcode};
use once_cell::sync::Lazy;

pub(crate) fn xdbc_type_info_data() -> &'static XdbcTypeInfoData {
    &XDBC_TYPE_INFO_DATA
}

/// Data Types supported by DataFusion
/// <https://arrow.apache.org/datafusion/user-guide/sql/data_types.html>
static XDBC_TYPE_INFO_DATA: Lazy<XdbcTypeInfoData> = Lazy::new(|| {
    let mut builder = XdbcTypeInfoDataBuilder::new();
    builder.append(XdbcTypeInfo {
        type_name: "VARCHAR".to_string(),
        data_type: XdbcDataType::XdbcVarchar,
        column_size: Some(i32::MAX), // https://github.com/apache/arrow-datafusion/blob/3801d45fe5ea3d9b207488527b758a0264665263/datafusion/core/src/catalog/information_schema.rs#L532
        literal_prefix: Some("'".to_string()),
        literal_suffix: Some("'".to_string()),
        create_params: Some(vec!["length".to_string()]),
        nullable: Nullable::NullabilityNullable,
        case_sensitive: true,
        searchable: Searchable::Full,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_increment: None,
        local_type_name: Some("VARCHAR".to_string()),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: XdbcDataType::XdbcVarchar,
        datetime_subcode: None,
        num_prec_radix: None,
        interval_precision: None,
    });
    builder.append(XdbcTypeInfo {
        type_name: "INTEGER".to_string(),
        data_type: XdbcDataType::XdbcInteger,
        column_size: Some(32), // https://github.com/apache/arrow-datafusion/blob/3801d45fe5ea3d9b207488527b758a0264665263/datafusion/core/src/catalog/information_schema.rs#L563
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: Nullable::NullabilityNullable,
        case_sensitive: false,
        searchable: Searchable::Full,
        unsigned_attribute: Some(false),
        fixed_prec_scale: false,
        auto_increment: Some(false),
        local_type_name: Some("INTEGER".to_string()),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: XdbcDataType::XdbcInteger,
        datetime_subcode: None,
        num_prec_radix: Some(2), // https://github.com/apache/arrow-datafusion/blob/3801d45fe5ea3d9b207488527b758a0264665263/datafusion/core/src/catalog/information_schema.rs#L563
        interval_precision: None,
    });
    builder.append(XdbcTypeInfo {
        type_name: "FLOAT".to_string(),
        data_type: XdbcDataType::XdbcFloat,
        column_size: Some(24), // https://github.com/apache/arrow-datafusion/blob/3801d45fe5ea3d9b207488527b758a0264665263/datafusion/core/src/catalog/information_schema.rs#L568
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: Nullable::NullabilityNullable,
        case_sensitive: false,
        searchable: Searchable::Full,
        unsigned_attribute: Some(false),
        fixed_prec_scale: false,
        auto_increment: Some(false),
        local_type_name: Some("FLOAT".to_string()),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: XdbcDataType::XdbcFloat,
        datetime_subcode: None,
        num_prec_radix: Some(2), // https://github.com/apache/arrow-datafusion/blob/3801d45fe5ea3d9b207488527b758a0264665263/datafusion/core/src/catalog/information_schema.rs#L568
        interval_precision: None,
    });
    builder.append(XdbcTypeInfo {
        type_name: "TIMESTAMP".to_string(),
        data_type: XdbcDataType::XdbcTimestamp,
        column_size: Some(i32::MAX), // https://github.com/apache/arrow-datafusion/blob/4297547df6dc297d692ca82566cfdf135d4730b5/datafusion/proto/src/generated/prost.rs#L894
        literal_prefix: Some("'".to_string()),
        literal_suffix: Some("'".to_string()),
        create_params: None,
        nullable: Nullable::NullabilityNullable,
        case_sensitive: false,
        searchable: Searchable::Full,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_increment: None,
        local_type_name: Some("TIMESTAMP".to_string()),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: XdbcDataType::XdbcTimestamp,
        datetime_subcode: None,
        num_prec_radix: None,
        interval_precision: None,
    });
    builder.append(XdbcTypeInfo {
        type_name: "INTERVAL".to_string(),
        data_type: XdbcDataType::XdbcInterval,
        column_size: Some(i32::MAX), // https://github.com/apache/arrow-datafusion/blob/4297547df6dc297d692ca82566cfdf135d4730b5/datafusion/proto/src/generated/prost.rs#L1031-L1038
        literal_prefix: Some("'".to_string()),
        literal_suffix: Some("'".to_string()),
        create_params: None,
        nullable: Nullable::NullabilityNullable,
        case_sensitive: false,
        searchable: Searchable::Full,
        unsigned_attribute: None,
        fixed_prec_scale: false,
        auto_increment: None,
        local_type_name: Some("INTERVAL".to_string()),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: XdbcDataType::XdbcInterval,
        datetime_subcode: Some(XdbcDatetimeSubcode::XdbcSubcodeUnknown),
        num_prec_radix: None,
        interval_precision: None, // https://github.com/apache/arrow-datafusion/blob/6be75ff2dcc47128b78a695477512ba86c46373f/datafusion/core/src/catalog/information_schema.rs#L581-L582
    });

    builder.build().expect("created XdbcTypeInfo")
});
