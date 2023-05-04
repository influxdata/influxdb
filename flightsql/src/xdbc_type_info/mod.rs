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

mod value;

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BooleanArray, Int32Array, ListArray, ListBuilder, StringArray, StringBuilder,
    },
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use once_cell::sync::Lazy;

use value::{XdbcTypeInfo, ALL_DATA_TYPES};

/// The schema for GetXdbcTypeInfo
static GET_XDBC_TYPE_INFO_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("type_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Int32, false),
        Field::new("column_size", DataType::Int32, true),
        Field::new("literal_prefix", DataType::Utf8, true),
        Field::new("literal_suffix", DataType::Utf8, true),
        Field::new(
            "create_params",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            true,
        ),
        Field::new("nullable", DataType::Int32, false), // Nullable enum: https://github.com/apache/arrow/blob/9588da967c756b2923e213ccc067378ba6c90a86/format/FlightSql.proto#L1014-L1029
        Field::new("case_sensitive", DataType::Boolean, false),
        Field::new("searchable", DataType::Int32, false), // Searchable enum: https://github.com/apache/arrow/blob/9588da967c756b2923e213ccc067378ba6c90a86/format/FlightSql.proto#L1031-L1056
        Field::new("unsigned_attribute", DataType::Boolean, true),
        Field::new("fixed_prec_scale", DataType::Boolean, false),
        Field::new("auto_increment", DataType::Boolean, true),
        Field::new("local_type_name", DataType::Utf8, true),
        Field::new("minimum_scale", DataType::Int32, true),
        Field::new("maximum_scale", DataType::Int32, true),
        Field::new("sql_data_type", DataType::Int32, false),
        Field::new("datetime_subcode", DataType::Int32, true), // XdbcDatetimeSubcode value: https://github.com/apache/arrow/blob/9588da967c756b2923e213ccc067378ba6c90a86/format/FlightSql.proto#L978-L1012
        Field::new("num_prec_radix", DataType::Int32, true),
        Field::new("interval_precision", DataType::Int32, true),
    ]))
});

pub static TYPE_INFO_RECORD_BATCH: Lazy<RecordBatch> = Lazy::new(|| {
    let type_names: Vec<&str> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.type_name)
        .collect();
    let type_name = Arc::new(StringArray::from(type_names)) as ArrayRef;

    let data_types: Vec<i32> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.data_type.clone() as i32) // case XdbcDataType enum to i32
        .collect();
    let data_type = Arc::new(Int32Array::from(data_types)) as ArrayRef;

    let column_sizes: Vec<Option<i32>> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.column_size)
        .collect();
    let column_size = Arc::new(Int32Array::from(column_sizes)) as ArrayRef;

    let literal_prefixes: Vec<Option<&str>> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.literal_prefix as Option<&str>)
        .collect();
    let literal_prefix = Arc::new(StringArray::from(literal_prefixes)) as ArrayRef;

    let literal_suffixes: Vec<Option<&str>> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.literal_suffix as Option<&str>)
        .collect();
    let literal_suffix = Arc::new(StringArray::from(literal_suffixes)) as ArrayRef;

    let mut create_params_builder: ListBuilder<StringBuilder> =
        ListBuilder::new(StringBuilder::new());
    ALL_DATA_TYPES.iter().for_each(|entry: &XdbcTypeInfo| {
        match &entry.create_params {
            Some(params) => {
                params
                    .iter()
                    .for_each(|value| create_params_builder.values().append_value(value));
                create_params_builder.append(true);
            }
            None => create_params_builder.append(false), // create_params is nullable
        }
    });
    let (field, offsets, values, nulls) = create_params_builder.finish().into_parts();
    // Re-defined the field to be non-nullable
    let new_field = Arc::new(field.as_ref().clone().with_nullable(false));
    let create_params = Arc::new(ListArray::new(new_field, offsets, values, nulls)) as ArrayRef;

    let nullabilities: Vec<i32> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.nullable.clone() as i32) // cast Nullable enum to i32
        .collect();
    let nullable = Arc::new(Int32Array::from(nullabilities)) as ArrayRef;

    let case_sensitivities: Vec<bool> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.case_sensitive)
        .collect();
    let case_sensitive = Arc::new(BooleanArray::from(case_sensitivities));

    let searchabilities: Vec<i32> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.searchable.clone() as i32) // cast Searchable enum to i32
        .collect();
    let searchable = Arc::new(Int32Array::from(searchabilities)) as ArrayRef;

    let unsigned_attributes: Vec<Option<bool>> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.unsigned_attribute as Option<bool>)
        .collect();
    let unsigned_attribute = Arc::new(BooleanArray::from(unsigned_attributes)) as ArrayRef;

    let fixed_prec_scales: Vec<bool> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.fixed_prec_scale)
        .collect();
    let fixed_prec_scale = Arc::new(BooleanArray::from(fixed_prec_scales)) as ArrayRef;

    let auto_increments: Vec<Option<bool>> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.auto_increment)
        .collect();
    let auto_increment = Arc::new(BooleanArray::from(auto_increments)) as ArrayRef;

    let local_type_names: Vec<Option<&str>> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.local_type_name)
        .collect();
    let local_type_name = Arc::new(StringArray::from(local_type_names)) as ArrayRef;

    let minimum_scales: Vec<Option<i32>> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.minimum_scale)
        .collect();
    let minimum_scale = Arc::new(Int32Array::from(minimum_scales)) as ArrayRef;

    let maximum_scales: Vec<Option<i32>> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.maximum_scale)
        .collect();
    let maximum_scale = Arc::new(Int32Array::from(maximum_scales)) as ArrayRef;

    let sql_data_types: Vec<i32> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.sql_data_type.clone() as i32) // case XdbcDataType enum to i32
        .collect();
    let sql_data_type = Arc::new(Int32Array::from(sql_data_types)) as ArrayRef;

    let datetime_subcodes: Vec<Option<i32>> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.datetime_subcode)
        .collect();
    let datetime_subcode = Arc::new(Int32Array::from(datetime_subcodes)) as ArrayRef;

    let num_prec_radices: Vec<Option<i32>> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.num_prec_radix)
        .collect();
    let num_prec_radix = Arc::new(Int32Array::from(num_prec_radices)) as ArrayRef;

    let interval_precisions: Vec<Option<i32>> = ALL_DATA_TYPES
        .iter()
        .map(|entry: &XdbcTypeInfo| entry.interval_precision)
        .collect();
    let interval_precision = Arc::new(Int32Array::from(interval_precisions)) as ArrayRef;

    RecordBatch::try_new(
        Arc::clone(&GET_XDBC_TYPE_INFO_SCHEMA),
        vec![
            type_name,
            data_type,
            column_size,
            literal_prefix,
            literal_suffix,
            create_params,
            nullable,
            case_sensitive,
            searchable,
            unsigned_attribute,
            fixed_prec_scale,
            auto_increment,
            local_type_name,
            minimum_scale,
            maximum_scale,
            sql_data_type,
            datetime_subcode,
            num_prec_radix,
            interval_precision,
        ],
    )
    .unwrap()
});
