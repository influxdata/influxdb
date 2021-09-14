//! Code to translate IOx statistics to DataFusion statistics

use data_types::partition_metadata::{ColumnSummary, Statistics as IOxStatistics, TableSummary};
use datafusion::{
    physical_plan::{ColumnStatistics, Statistics as DFStatistics},
    scalar::ScalarValue,
};
use internal_types::schema::Schema;

/// Converts stats.min and an appropriate `ScalarValue`
pub(crate) fn min_to_scalar(stats: &IOxStatistics) -> Option<ScalarValue> {
    match stats {
        IOxStatistics::I64(v) => v.min.map(ScalarValue::from),
        IOxStatistics::U64(v) => v.min.map(ScalarValue::from),
        IOxStatistics::F64(v) => v.min.map(ScalarValue::from),
        IOxStatistics::Bool(v) => v.min.map(ScalarValue::from),
        IOxStatistics::String(v) => v.min.as_deref().map(ScalarValue::from),
    }
}

/// Converts stats.max to an appropriate `ScalarValue`
pub(crate) fn max_to_scalar(stats: &IOxStatistics) -> Option<ScalarValue> {
    match stats {
        IOxStatistics::I64(v) => v.max.map(ScalarValue::from),
        IOxStatistics::U64(v) => v.max.map(ScalarValue::from),
        IOxStatistics::F64(v) => v.max.map(ScalarValue::from),
        IOxStatistics::Bool(v) => v.max.map(ScalarValue::from),
        IOxStatistics::String(v) => v.max.as_deref().map(ScalarValue::from),
    }
}

/// Creates a DataFusion `Statistics` object from an IOx `TableSummary`
pub(crate) fn df_from_iox(schema: &Schema, summary: &TableSummary) -> DFStatistics {
    // reorder the column statistics so DF sees them in the same order
    // as the schema. Form map of field_name-->column_index
    let order_map = schema
        .iter()
        .enumerate()
        .map(|(i, (_, field))| (field.name(), i))
        .collect::<hashbrown::HashMap<_, _>>();

    let mut columns: Vec<(&ColumnSummary, &usize)> = summary
        .columns
        .iter()
        // as there may be more columns in the summary than are in the
        // schema, filter them out prior to sorting
        .filter_map(|s| order_map.get(&s.name).map(|order_index| (s, order_index)))
        .collect();

    // sort columns by schema order
    columns.sort_by_key(|s| s.1);

    let column_statistics = columns
        .into_iter()
        .map(|(c, _)| df_from_iox_col(c))
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

    let distinct_count = stats.distinct_count().map(|v| {
        let v: u64 = v.into();
        v as usize
    });

    let null_count = Some(stats.null_count() as usize);

    ColumnStatistics {
        null_count,
        max_value: max_to_scalar(stats),
        min_value: min_to_scalar(stats),
        distinct_count,
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZeroU64;

    use super::*;
    use data_types::partition_metadata::{InfluxDbType, StatValues};
    use internal_types::schema::{builder::SchemaBuilder, InfluxFieldType};

    #[test]
    fn convert() {
        let c1_stats = StatValues {
            min: Some(11),
            max: Some(11),
            total_count: 3,
            null_count: 1,
            distinct_count: None,
        };
        let c1_summary = ColumnSummary {
            name: "c1".to_string(),
            influxdb_type: Some(InfluxDbType::Tag),
            stats: IOxStatistics::I64(c1_stats),
        };

        let c2_stats = StatValues {
            min: Some(-5),
            max: Some(6),
            total_count: 3,
            null_count: 0,
            distinct_count: Some(NonZeroU64::new(33).unwrap()),
        };
        let c2_summary = ColumnSummary {
            name: "c2".to_string(),
            influxdb_type: Some(InfluxDbType::Field),
            stats: IOxStatistics::I64(c2_stats),
        };

        let table_summary = TableSummary {
            name: "t1".to_string(),
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
            total_byte_size: Some(438),
            column_statistics: Some(vec![df_c1_stats.clone(), df_c2_stats.clone()]),
            is_exact: true,
        };

        let actual = df_from_iox(&schema, &table_summary);
        assert_eq!(
            actual, expected,
            "\n\nactual:\n\n{:#?}\n\nexpected:\n\n{:#?}",
            actual, expected
        );

        // test 1: columns in c1, c2 order in shcema (in c1, c2 in table_summary)

        let schema = SchemaBuilder::new()
            .tag("c2")
            .influx_field("c1", InfluxFieldType::Integer)
            .build()
            .unwrap();

        let expected = DFStatistics {
            // in c2, c1 order
            column_statistics: Some(vec![df_c2_stats, df_c1_stats]),
            // other fields the same
            ..expected
        };

        let actual = df_from_iox(&schema, &table_summary);
        assert_eq!(
            actual, expected,
            "\n\nactual:\n\n{:#?}\n\nexpected:\n\n{:#?}",
            actual, expected
        );
    }
}
