use data_types::{
    ColumnSummary, InfluxDbType, StatValues, Statistics, TableSummary, TimestampMinMax,
};
use schema::{InfluxColumnType, InfluxFieldType, Schema};

/// Create basic table summary.
///
/// This contains:
/// - correct column types
/// - [total count](StatValues::total_count) for all columns
/// - [min](StatValues::min)/[max](StatValues::max) for the timestamp column
pub fn create_basic_summary(
    row_count: u64,
    schema: &Schema,
    ts_min_max: TimestampMinMax,
) -> TableSummary {
    let mut columns = Vec::with_capacity(schema.len());
    for i in 0..schema.len() {
        let (t, field) = schema.field(i);
        let t = t.expect("influx column type must be known");

        let influxdb_type = match t {
            InfluxColumnType::Tag => InfluxDbType::Tag,
            InfluxColumnType::Field(_) => InfluxDbType::Field,
            InfluxColumnType::Timestamp => InfluxDbType::Timestamp,
        };

        let stats = match t {
            InfluxColumnType::Tag | InfluxColumnType::Field(InfluxFieldType::String) => {
                Statistics::String(StatValues {
                    min: None,
                    max: None,
                    total_count: row_count,
                    null_count: None,
                    distinct_count: None,
                })
            }
            InfluxColumnType::Timestamp => Statistics::I64(StatValues {
                min: Some(ts_min_max.min),
                max: Some(ts_min_max.max),
                total_count: row_count,
                null_count: None,
                distinct_count: None,
            }),
            InfluxColumnType::Field(InfluxFieldType::Integer) => Statistics::I64(StatValues {
                min: None,
                max: None,
                total_count: row_count,
                null_count: None,
                distinct_count: None,
            }),
            InfluxColumnType::Field(InfluxFieldType::UInteger) => Statistics::U64(StatValues {
                min: None,
                max: None,
                total_count: row_count,
                null_count: None,
                distinct_count: None,
            }),
            InfluxColumnType::Field(InfluxFieldType::Float) => Statistics::F64(StatValues {
                min: None,
                max: None,
                total_count: row_count,
                null_count: None,
                distinct_count: None,
            }),
            InfluxColumnType::Field(InfluxFieldType::Boolean) => Statistics::Bool(StatValues {
                min: None,
                max: None,
                total_count: row_count,
                null_count: None,
                distinct_count: None,
            }),
        };

        columns.push(ColumnSummary {
            name: field.name().clone(),
            influxdb_type: Some(influxdb_type),
            stats,
        })
    }

    TableSummary { columns }
}

#[cfg(test)]
mod tests {
    use schema::builder::SchemaBuilder;

    use super::*;

    #[test]
    fn test_create_basic_summary_no_columns_no_rows() {
        let schema = SchemaBuilder::new().build().unwrap();
        let row_count = 0;
        let ts_min_max = TimestampMinMax { min: 10, max: 20 };

        let actual = create_basic_summary(row_count, &schema, ts_min_max);
        let expected = TableSummary { columns: vec![] };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_create_basic_summary_no_rows() {
        let schema = full_schema();
        let row_count = 0;
        let ts_min_max = TimestampMinMax { min: 10, max: 20 };

        let actual = create_basic_summary(row_count, &schema, ts_min_max);
        let expected = TableSummary {
            columns: vec![
                ColumnSummary {
                    name: String::from("tag"),
                    influxdb_type: Some(InfluxDbType::Tag),
                    stats: Statistics::String(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_bool"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::Bool(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_float"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::F64(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_integer"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::I64(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_string"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::String(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_uinteger"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::U64(StatValues {
                        min: None,
                        max: None,
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("time"),
                    influxdb_type: Some(InfluxDbType::Timestamp),
                    stats: Statistics::I64(StatValues {
                        min: Some(10),
                        max: Some(20),
                        total_count: 0,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
            ],
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_create_basic_summary() {
        let schema = full_schema();
        let row_count = 3;
        let ts_min_max = TimestampMinMax { min: 42, max: 42 };

        let actual = create_basic_summary(row_count, &schema, ts_min_max);
        let expected = TableSummary {
            columns: vec![
                ColumnSummary {
                    name: String::from("tag"),
                    influxdb_type: Some(InfluxDbType::Tag),
                    stats: Statistics::String(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_bool"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::Bool(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_float"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::F64(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_integer"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::I64(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_string"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::String(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("field_uinteger"),
                    influxdb_type: Some(InfluxDbType::Field),
                    stats: Statistics::U64(StatValues {
                        min: None,
                        max: None,
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: String::from("time"),
                    influxdb_type: Some(InfluxDbType::Timestamp),
                    stats: Statistics::I64(StatValues {
                        min: Some(42),
                        max: Some(42),
                        total_count: 3,
                        null_count: None,
                        distinct_count: None,
                    }),
                },
            ],
        };
        assert_eq!(actual, expected);
    }

    fn full_schema() -> Schema {
        SchemaBuilder::new()
            .tag("tag")
            .influx_field("field_bool", InfluxFieldType::Boolean)
            .influx_field("field_float", InfluxFieldType::Float)
            .influx_field("field_integer", InfluxFieldType::Integer)
            .influx_field("field_string", InfluxFieldType::String)
            .influx_field("field_uinteger", InfluxFieldType::UInteger)
            .timestamp()
            .build()
            .unwrap()
    }
}
