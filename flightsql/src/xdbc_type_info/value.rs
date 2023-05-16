use once_cell::sync::Lazy;

/// [Arrow FlightSQL Specification]: https://github.com/apache/arrow/blob/9588da967c756b2923e213ccc067378ba6c90a86/format/FlightSql.proto#L948-L973
///
/// Note: Some of the data types are not supported by DataFusion yet:
/// <https://arrow.apache.org/datafusion/user-guide/sql/data_types.html#unsupported-sql-types>
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum XdbcDataType {
    UnknownType = 0,
    Char = 1,
    Numeric = 2,
    Decimal = 3,
    Integer = 4,
    Smallint = 5,
    Float = 6,
    Real = 7,
    Double = 8,
    Datetime = 9, // Not yet supported by DataFusion
    Interval = 10,
    Varchar = 12,
    Date = 91,
    Time = 92,
    Timestamp = 93,
    Longvarchar = -1,
    Binary = -2,    // Not yet supported by DataFusion
    Varbinary = -3, // Not yet supported by DataFusion
    Longvarbinary = -4,
    Bigint = -5,
    Tinyint = -6,
    Bit = -7,
    Wchar = -8,
    Wvarchar = -9, // Not yet supported by DataFusion
}

/// [Arrow FlightSQL Specification]: https://github.com/apache/arrow/blob/9588da967c756b2923e213ccc067378ba6c90a86/format/FlightSql.proto#L1014-L1029
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum Nullability {
    // Indicates that the fields does not allow the use of null values.
    NoNulls = 0,
    // Indicates that the fields allow the use of null values.
    Nullable = 1,
    // Indicates that nullability of the fields can not be determined.
    Unknown = 2,
}

/// [Arrow FlightSQL Specification]: https://github.com/apache/arrow/blob/9588da967c756b2923e213ccc067378ba6c90a86/format/FlightSql.proto#L1031-L1056
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum Searchable {
    // Indicates that column can not be used in a WHERE clause.
    None = 0,

    // Indicates that the column can be used in a WHERE clause if it is using a
    // LIKE operator.
    Char = 1,

    // Indicates that the column can be used In a WHERE clause with any
    // operator other than LIKE.
    // - Allowed operators: comparison, quantified comparison, BETWEEN,
    //                      DISTINCT, IN, MATCH, and UNIQUE.
    Basic = 2,

    // Indicates that the column can be used in a WHERE clause using any operator.
    Full = 3,
}

/// Detailed subtype information for XDBC_TYPE_DATETIME and XDBC_TYPE_INTERVAL.
///
/// [Arrow FlightSQL Specification]: https://github.com/apache/arrow/blob/9588da967c756b2923e213ccc067378ba6c90a86/format/FlightSql.proto#L978-L1012
#[allow(dead_code)]
struct XdbcDatetimeSubcodeType {
    // option allow_alias = true; // TODO chunchun: what to do with it?
    unknown: i32,
    year: i32,
    date: i32,
    time: i32,
    month: i32,
    timestamp: i32,
    day: i32,
    time_with_timezone: i32,
    hour: i32,
    timestamp_with_timezone: i32,
    minute: i32,
    second: i32,
    year_to_month: i32,
    day_to_hour: i32,
    day_to_minute: i32,
    day_to_second: i32,
    hour_to_minute: i32,
    hour_to_second: i32,
    minute_to_second: i32,
    interval_year: i32,
    interval_month: i32,
    interval_day: i32,
    interval_hour: i32,
    interval_minute: i32,
    interval_second: i32,
    interval_year_to_month: i32,
    interval_day_to_hour: i32,
    interval_day_to_minute: i32,
    interval_day_to_second: i32,
    interval_hour_to_minute: i32,
    interval_hour_to_second: i32,
    interval_minute_to_second: i32,
}

#[allow(dead_code)]
static XDBC_DATETIME_SUBCODE: Lazy<XdbcDatetimeSubcodeType> =
    Lazy::new(|| XdbcDatetimeSubcodeType {
        unknown: 0,
        year: 1,
        date: 1,
        time: 2,
        month: 2,
        timestamp: 3,
        day: 3,
        time_with_timezone: 4,
        hour: 4,
        timestamp_with_timezone: 5,
        minute: 5,
        second: 6,
        year_to_month: 7,
        day_to_hour: 8,
        day_to_minute: 9,
        day_to_second: 10,
        hour_to_minute: 11,
        hour_to_second: 12,
        minute_to_second: 13,
        interval_year: 101,
        interval_month: 102,
        interval_day: 103,
        interval_hour: 104,
        interval_minute: 105,
        interval_second: 106,
        interval_year_to_month: 107,
        interval_day_to_hour: 108,
        interval_day_to_minute: 109,
        interval_day_to_second: 110,
        interval_hour_to_minute: 111,
        interval_hour_to_second: 112,
        interval_minute_to_second: 113,
    });

pub struct XdbcTypeInfo {
    pub type_name: &'static str,
    pub data_type: XdbcDataType,
    // column_size: int32 (The maximum size supported by that column.
    //              In case of exact numeric types, this represents the maximum precision.
    //              In case of string types, this represents the character length.
    //              In case of datetime data types, this represents the length in characters of the string representation.
    //              NULL is returned for data types where column size is not applicable.)
    pub column_size: Option<i32>,
    pub literal_prefix: Option<&'static str>,
    pub literal_suffix: Option<&'static str>,
    pub create_params: Option<Vec<&'static str>>,
    pub nullable: Nullability,
    pub case_sensitive: bool,
    pub searchable: Searchable,
    pub unsigned_attribute: Option<bool>,
    pub fixed_prec_scale: bool,
    pub auto_increment: Option<bool>,
    pub local_type_name: Option<&'static str>,
    pub minimum_scale: Option<i32>,
    pub maximum_scale: Option<i32>,
    pub sql_data_type: XdbcDataType,
    pub datetime_subcode: Option<i32>, // values are from XDBC_DATETIME_SUBCODE
    pub num_prec_radix: Option<i32>,
    pub interval_precision: Option<i32>,
}

/// Data Types supported by DataFusion
/// <https://arrow.apache.org/datafusion/user-guide/sql/data_types.html>
pub static ALL_DATA_TYPES: Lazy<Vec<XdbcTypeInfo>> = Lazy::new(|| {
    vec![
        XdbcTypeInfo {
            type_name: "VARCHAR",
            data_type: XdbcDataType::Varchar,
            column_size: Some(i32::MAX), // https://github.com/apache/arrow-datafusion/blob/3801d45fe5ea3d9b207488527b758a0264665263/datafusion/core/src/catalog/information_schema.rs#L532
            literal_prefix: Some("'"),
            literal_suffix: Some("'"),
            create_params: Some(vec!["length"]),
            nullable: Nullability::Nullable,
            case_sensitive: true,
            searchable: Searchable::Full,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_increment: None,
            local_type_name: Some("VARCHAR"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::Varchar,
            datetime_subcode: None,
            num_prec_radix: None,
            interval_precision: None,
        },
        XdbcTypeInfo {
            type_name: "INTEGER",
            data_type: XdbcDataType::Integer,
            column_size: Some(32), // https://github.com/apache/arrow-datafusion/blob/3801d45fe5ea3d9b207488527b758a0264665263/datafusion/core/src/catalog/information_schema.rs#L563
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: Nullability::Nullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            local_type_name: Some("INTEGER"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::Integer,
            datetime_subcode: None,
            num_prec_radix: Some(2), // https://github.com/apache/arrow-datafusion/blob/3801d45fe5ea3d9b207488527b758a0264665263/datafusion/core/src/catalog/information_schema.rs#L563
            interval_precision: None,
        },
        XdbcTypeInfo {
            type_name: "FLOAT",
            data_type: XdbcDataType::Float,
            column_size: Some(24), // https://github.com/apache/arrow-datafusion/blob/3801d45fe5ea3d9b207488527b758a0264665263/datafusion/core/src/catalog/information_schema.rs#L568
            literal_prefix: None,
            literal_suffix: None,
            create_params: None,
            nullable: Nullability::Nullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: Some(false),
            fixed_prec_scale: false,
            auto_increment: Some(false),
            local_type_name: Some("FLOAT"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::Float,
            datetime_subcode: None,
            num_prec_radix: Some(2), // https://github.com/apache/arrow-datafusion/blob/3801d45fe5ea3d9b207488527b758a0264665263/datafusion/core/src/catalog/information_schema.rs#L568
            interval_precision: None,
        },
        XdbcTypeInfo {
            type_name: "TIMESTAMP",
            data_type: XdbcDataType::Timestamp,
            column_size: Some(i32::MAX), // https://github.com/apache/arrow-datafusion/blob/4297547df6dc297d692ca82566cfdf135d4730b5/datafusion/proto/src/generated/prost.rs#L894
            literal_prefix: Some("'"),
            literal_suffix: Some("'"),
            create_params: None,
            nullable: Nullability::Nullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_increment: None,
            local_type_name: Some("TIMESTAMP"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::Timestamp,
            datetime_subcode: None,
            num_prec_radix: None,
            interval_precision: None,
        },
        XdbcTypeInfo {
            type_name: "INTERVAL",
            data_type: XdbcDataType::Interval,
            column_size: Some(i32::MAX), // https://github.com/apache/arrow-datafusion/blob/4297547df6dc297d692ca82566cfdf135d4730b5/datafusion/proto/src/generated/prost.rs#L1031-L1038
            literal_prefix: Some("'"),
            literal_suffix: Some("'"),
            create_params: None,
            nullable: Nullability::Nullable,
            case_sensitive: false,
            searchable: Searchable::Full,
            unsigned_attribute: None,
            fixed_prec_scale: false,
            auto_increment: None,
            local_type_name: Some("INTERVAL"),
            minimum_scale: None,
            maximum_scale: None,
            sql_data_type: XdbcDataType::Interval,
            datetime_subcode: Some(XDBC_DATETIME_SUBCODE.unknown),
            num_prec_radix: None,
            interval_precision: None, // https://github.com/apache/arrow-datafusion/blob/6be75ff2dcc47128b78a695477512ba86c46373f/datafusion/core/src/catalog/information_schema.rs#L581-L582
        },
    ]
});
