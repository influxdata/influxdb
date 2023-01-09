//! Code to translate IOx statistics to DataFusion statistics

use data_types::{ColumnSummary, InfluxDbType, Statistics as IOxStatistics, TableSummary};
use datafusion::{
    physical_plan::{ColumnStatistics, Statistics as DFStatistics},
    scalar::ScalarValue,
};

/// Converts stats.min and an appropriate `ScalarValue`
pub(crate) fn min_to_scalar(
    influx_type: &InfluxDbType,
    stats: &IOxStatistics,
) -> Option<ScalarValue> {
    match stats {
        IOxStatistics::I64(v) => {
            if InfluxDbType::Timestamp == *influx_type {
                v.min
                    .map(|x| ScalarValue::TimestampNanosecond(Some(x), None))
            } else {
                v.min.map(ScalarValue::from)
            }
        }
        IOxStatistics::U64(v) => v.min.map(ScalarValue::from),
        IOxStatistics::F64(v) => v.min.map(ScalarValue::from),
        IOxStatistics::Bool(v) => v.min.map(ScalarValue::from),
        IOxStatistics::String(v) => v.min.as_deref().map(ScalarValue::from),
    }
}

/// Converts stats.max to an appropriate `ScalarValue`
pub(crate) fn max_to_scalar(
    influx_type: &InfluxDbType,
    stats: &IOxStatistics,
) -> Option<ScalarValue> {
    match stats {
        IOxStatistics::I64(v) => {
            if InfluxDbType::Timestamp == *influx_type {
                v.max
                    .map(|x| ScalarValue::TimestampNanosecond(Some(x), None))
            } else {
                v.max.map(ScalarValue::from)
            }
        }
        IOxStatistics::U64(v) => v.max.map(ScalarValue::from),
        IOxStatistics::F64(v) => v.max.map(ScalarValue::from),
        IOxStatistics::Bool(v) => v.max.map(ScalarValue::from),
        IOxStatistics::String(v) => v.max.as_deref().map(ScalarValue::from),
    }
}

/// Creates a DataFusion `Statistics` object from an IOx `TableSummary`
pub(crate) fn df_from_iox(
    schema: &arrow::datatypes::Schema,
    summary: &TableSummary,
) -> DFStatistics {
    let column_by_name = summary
        .columns
        .iter()
        .map(|c| (&c.name, c))
        .collect::<hashbrown::HashMap<_, _>>();

    // compute statistics for all columns in the schema, in order
    let column_statistics = schema
        .fields()
        .iter()
        .map(|field| {
            column_by_name
                .get(field.name())
                .map(|c| df_from_iox_col(c))
                // use default statisics of none available  for this column
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();

    DFStatistics {
        num_rows: Some(summary.total_count() as usize),
        total_byte_size: Some(summary.size()),
        column_statistics: Some(column_statistics),
        is_exact: true,
    }
}

/// Convert IOx `ColumnSummary` to DataFusion's `ColumnStatistics`
fn df_from_iox_col(col: &ColumnSummary) -> ColumnStatistics {
    let stats = &col.stats;
    let col_data_type = &col.influxdb_type;

    let distinct_count = stats.distinct_count().map(|v| {
        let v: u64 = v.into();
        v as usize
    });

    let null_count = stats.null_count().map(|x| x as usize);

    ColumnStatistics {
        null_count,
        max_value: max_to_scalar(col_data_type, stats),
        min_value: min_to_scalar(col_data_type, stats),
        distinct_count,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use data_types::{InfluxDbType, StatValues};
    use schema::{builder::SchemaBuilder, InfluxFieldType};
    use std::num::NonZeroU64;

    macro_rules! assert_nice_eq {
        ($actual:ident, $expected:ident) => {
            assert_eq!(
                $actual, $expected,
                "\n\nactual:\n\n{:#?}\n\nexpected:\n\n{:#?}",
                $actual, $expected,
            );
        };
    }

    #[test]
    fn convert() {
        let c1_stats = StatValues {
            min: Some(11),
            max: Some(11),
            total_count: 3,
            null_count: Some(1),
            distinct_count: None,
        };
        let c1_summary = ColumnSummary {
            name: "c1".to_string(),
            influxdb_type: InfluxDbType::Tag,
            stats: IOxStatistics::I64(c1_stats),
        };

        let c2_stats = StatValues {
            min: Some(-5),
            max: Some(6),
            total_count: 3,
            null_count: Some(0),
            distinct_count: Some(NonZeroU64::new(33).unwrap()),
        };
        let c2_summary = ColumnSummary {
            name: "c2".to_string(),
            influxdb_type: InfluxDbType::Field,
            stats: IOxStatistics::I64(c2_stats),
        };

        let table_summary = TableSummary {
            columns: vec![c1_summary, c2_summary],
        };

        let df_c1_stats = ColumnStatistics {
            null_count: Some(1),
            max_value: Some(ScalarValue::Int64(Some(11))),
            min_value: Some(ScalarValue::Int64(Some(11))),
            distinct_count: None,
        };

        let df_c2_stats = ColumnStatistics {
            null_count: Some(0),
            max_value: Some(ScalarValue::Int64(Some(6))),
            min_value: Some(ScalarValue::Int64(Some(-5))),
            distinct_count: Some(33),
        };

        // test 1: columns in c1, c2 order

        let schema = SchemaBuilder::new()
            .tag("c1")
            .influx_field("c2", InfluxFieldType::Integer)
            .build()
            .unwrap();

        let expected = DFStatistics {
            num_rows: Some(3),
            total_byte_size: Some(444),
            column_statistics: Some(vec![df_c1_stats.clone(), df_c2_stats.clone()]),
            is_exact: true,
        };

        let actual = df_from_iox(schema.inner(), &table_summary);
        assert_nice_eq!(actual, expected);

        // test 1: columns in c1, c2 order in shcema (in c1, c2 in table_summary)

        let schema = SchemaBuilder::new()
            .tag("c2")
            .influx_field("c1", InfluxFieldType::Integer)
            .build()
            .unwrap();

        let expected = DFStatistics {
            // in c2, c1 order
            column_statistics: Some(vec![df_c2_stats.clone(), df_c1_stats.clone()]),
            // other fields the same
            ..expected
        };

        let actual = df_from_iox(schema.inner(), &table_summary);
        assert_nice_eq!(actual, expected);

        // test 3: columns in c1 tag with stats, c3 (tag no stats) and c2column without statistics
        let schema = SchemaBuilder::new()
            .tag("c2")
            .influx_field("c1", InfluxFieldType::Integer)
            .tag("c3")
            .build()
            .unwrap();

        let expected = DFStatistics {
            // in c2, c1, c3 w/ default stats
            column_statistics: Some(vec![df_c2_stats, df_c1_stats, ColumnStatistics::default()]),
            // other fields the same
            ..expected
        };

        let actual = df_from_iox(schema.inner(), &table_summary);
        assert_nice_eq!(actual, expected);
    }

    #[test]
    fn null_ts() {
        let c_stats = StatValues {
            min: None,
            max: None,
            total_count: 3,
            null_count: None,
            distinct_count: None,
        };
        let c_summary = ColumnSummary {
            name: "time".to_string(),
            influxdb_type: InfluxDbType::Timestamp,
            stats: IOxStatistics::I64(c_stats),
        };

        let table_summary = TableSummary {
            columns: vec![c_summary],
        };

        let df_c_stats = ColumnStatistics {
            null_count: None,
            // Note min/max values should be `None` (not known)
            // NOT `Some(None)` (known to be null)
            max_value: None,
            min_value: None,
            distinct_count: None,
        };

        let schema = SchemaBuilder::new().timestamp().build().unwrap();

        let expected = DFStatistics {
            num_rows: Some(3),
            total_byte_size: Some(236),
            column_statistics: Some(vec![df_c_stats]),
            is_exact: true,
        };

        let actual = df_from_iox(schema.inner(), &table_summary);
        assert_nice_eq!(actual, expected);
    }
}
