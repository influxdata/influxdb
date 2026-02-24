use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::scalar::ScalarValue;

use crate::QueryChunk;

pub(crate) struct SchemaBoundStatistics {
    schema: SchemaRef,
    // Map from column name to column index in schema.
    // We want to compute this once and reuse it for all chunks.
    // Have both schema and this map in the same struct to keep them in sync.
    col_name_to_col_index_map: HashMap<String, usize>,
}

impl SchemaBoundStatistics {
    pub fn new(schema: SchemaRef) -> Self {
        // Build a map from col_name to col_idx for the schema
        let col_name_to_col_index_map: HashMap<String, usize> = schema
            .fields
            .iter()
            .enumerate()
            .map(|(idx, field)| (field.name().to_string(), idx))
            .collect();

        Self {
            schema,
            col_name_to_col_index_map,
        }
    }

    /// Return column statstics of the given chunk but for the schema of this struct.
    /// Columns that are not part of the chunk are marked as unknown/absent.
    pub fn chunk_column_statistics(&self, chunk: &Arc<dyn QueryChunk>) -> Statistics {
        // Statistics of columns in the chunk
        let chunk_stats = chunk.stats();
        let chunk_schema = chunk.schema();

        // A vector of absent stats for all columns in the schema
        let mut schema_col_stats_for_chunk = self
            .schema
            .fields()
            .iter()
            .map(|f| {
                let null_val =
                    ScalarValue::try_from(f.data_type()).expect("works for all IOx types");

                ColumnStatistics {
                    null_count: chunk_stats.num_rows,
                    max_value: Precision::Exact(null_val.clone()),
                    min_value: Precision::Exact(null_val),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                }
            })
            .collect::<Vec<_>>();

        // Fill stats of columns in the chunk, or fallback values
        let arrow_schema = chunk_schema.as_arrow();
        let fields = arrow_schema.fields();
        fields.iter().enumerate().for_each(|(idx, f)| {
            let col_name = f.name();

            let schema_idx = self.col_name_to_col_index_map.get(col_name);
            if let Some(schema_idx) = schema_idx {
                schema_col_stats_for_chunk[*schema_idx] =
                    chunk_stats.column_statistics[idx].clone();
            }
        });

        Statistics {
            num_rows: chunk_stats.num_rows,
            total_byte_size: chunk_stats.total_byte_size,
            column_statistics: schema_col_stats_for_chunk,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{CHUNK_ORDER_COLUMN_NAME, test::TestChunk};
    use schema::{InfluxFieldType, SchemaBuilder};

    #[test]
    fn test_stats_for_one_chunk_on_super_schema() {
        // parquet chunk with columns in schema: tag, field, time, CHUNK_ORDER_COLUMN_NAME
        let parquet_chunk = Arc::new(
            TestChunk::new("t")
                .with_row_count(42)
                .with_tag_column_with_stats("tag", Some("AL"), Some("MT"))
                .with_i64_field_column_with_stats("field", Some(0), Some(100))
                .with_time_column_with_stats(Some(10), Some(20))
                .with_i64_field_column_with_stats(CHUNK_ORDER_COLUMN_NAME, Some(5), Some(6))
                .with_dummy_parquet_file(),
        );

        // create a super schema with superset columns but without the CHUNK_ORDER_COLUMN_NAME: another_tag, tag, field, another_field, time
        let super_schema: SchemaRef = SchemaBuilder::new()
            .tag("another_tag")
            .tag("tag")
            .influx_field("field", InfluxFieldType::Float)
            .influx_field("another_field", InfluxFieldType::Integer)
            .timestamp()
            .build()
            .unwrap()
            .into();

        let schema_bound = SchemaBoundStatistics::new(super_schema);

        let parquet_chunk = Arc::clone(&parquet_chunk) as Arc<dyn QueryChunk>;
        let actual_stats = schema_bound.chunk_column_statistics(&parquet_chunk);

        insta::assert_debug_snapshot!(actual_stats, @r#"
        Statistics {
            num_rows: Exact(42),
            total_byte_size: Absent,
            column_statistics: [
                ColumnStatistics {
                    null_count: Exact(42),
                    max_value: Exact(Dictionary(Int32, Utf8(NULL))),
                    min_value: Exact(Dictionary(Int32, Utf8(NULL))),
                    sum_value: Absent,
                    distinct_count: Absent,
                },
                ColumnStatistics {
                    null_count: Exact(0),
                    max_value: Exact(Dictionary(Int32, Utf8("MT"))),
                    min_value: Exact(Dictionary(Int32, Utf8("AL"))),
                    sum_value: Absent,
                    distinct_count: Absent,
                },
                ColumnStatistics {
                    null_count: Absent,
                    max_value: Exact(Int64(100)),
                    min_value: Exact(Int64(0)),
                    sum_value: Absent,
                    distinct_count: Absent,
                },
                ColumnStatistics {
                    null_count: Exact(42),
                    max_value: Exact(Int64(NULL)),
                    min_value: Exact(Int64(NULL)),
                    sum_value: Absent,
                    distinct_count: Absent,
                },
                ColumnStatistics {
                    null_count: Exact(0),
                    max_value: Exact(TimestampNanosecond(20, None)),
                    min_value: Exact(TimestampNanosecond(10, None)),
                    sum_value: Absent,
                    distinct_count: Absent,
                },
            ],
        }
        "#);
    }
}
