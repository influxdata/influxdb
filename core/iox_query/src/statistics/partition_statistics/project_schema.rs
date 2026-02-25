use std::{borrow::Borrow, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::projection::ProjectionExpr;
use datafusion::{
    common::{ColumnStatistics, Result, Statistics, internal_datafusion_err, stats::Precision},
    physical_plan::expressions::{Column, Literal},
    scalar::ScalarValue,
};
use itertools::Itertools;

use crate::{
    CHUNK_ORDER_FIELD, chunk_order_field, statistics::partition_statistics::util::pretty_fmt_fields,
};

/// Takes in a datasource schema and statistics, and projects the scan schema on top.
///
/// This handles use cases where we may be either:
/// (1) scanning a subset of columns, (b) adding a __chunk_order column, (c) resolving different schema across files.
///
/// This will error if the src_schema (e.g. file schema) has a field without a corresponding column_statistics.
pub(crate) fn project_schema_onto_datasrc_statistics(
    src_statistics: &Statistics,
    src_schema: &SchemaRef,
    project_schema: &SchemaRef,
) -> Result<Arc<Statistics>> {
    // (1) scanning a subset of columns (a.k.a. project_schema.fields)
    let column_statistics = project_schema
        .fields()
        .iter()
        .map(|field| {
            let Ok(col_idx) = src_schema.index_of(field.name()) else {
                // (b) adding a __chunk_order colum
                if *field == *CHUNK_ORDER_FIELD {
                    return Ok(ColumnStatistics::new_unknown());
                } else {
                    // (c) adding an empty column (e.g. different parquet files could have slightly different schema).
                    return Ok(ColumnStatistics {
                        null_count: src_statistics.num_rows,
                        max_value: Precision::Exact(ScalarValue::Null),
                        min_value: Precision::Exact(ScalarValue::Null),
                        distinct_count: Precision::Absent,
                        sum_value: Precision::Absent,
                    });
                }
            };

            let Some(col_stats) = src_statistics.column_statistics.get(col_idx) else {
                return Err(internal_datafusion_err!(
                    "missing column statistics for {}@{}",
                    field.name(),
                    col_idx
                ));
            };
            Ok(col_stats.clone())
        })
        .try_collect()?;

    Ok(Arc::new(Statistics {
        num_rows: src_statistics.num_rows,
        total_byte_size: src_statistics.total_byte_size,
        column_statistics,
    }))
}

/// Determine the output statistics, handling the [`chunk_order_field`] if missing.
///
/// This will error for any other missing field, except for the chunk order column being optionally added.
pub(super) fn project_statistics_with_chunk_order<
    T: Borrow<Statistics> + Clone + Into<Arc<Statistics>> + std::fmt::Debug,
>(
    src_statistics: &T,
    src_schema: &SchemaRef,
    project_schema: &SchemaRef,
) -> Result<Arc<Statistics>> {
    // The src_statistics columns does not match the src schema.
    if src_statistics.borrow().column_statistics.len() != src_schema.fields().len() {
        return Err(internal_datafusion_err!(
            "statistics do not have the same number of columns as the schema, the schema has {:?} fields whereas the statistics have {:?} columns",
            src_schema.fields().len(),
            src_statistics.borrow().column_statistics.len()
        ));
    }

    // Check if column schema is the same for input and output.
    if project_schema.fields() == src_schema.fields() {
        return Ok(src_statistics.clone().into()); // in cases where this is a Arc ref, it's a cheap clone
    }

    // Select a subset of columns, erroring if the output column does not exist in the input schema.
    let column_statistics = project_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(project_col_idx, project_field)| {
            // confirm project_schema field is in the source schema
            let Some(src_field) = src_schema.fields.get(project_col_idx) else {
                if *project_field == chunk_order_field() {
                    return Ok(ColumnStatistics::new_unknown());
                } else {
                    return Err(internal_datafusion_err!(
                        "field `{}`@{} does not exist in input schema, found fields [{}]",
                        project_field.name(),
                        project_col_idx,
                        pretty_fmt_fields(src_schema)
                    ));
                }
            };

            // aliasing not permitted
            if src_field.name() != project_field.name() {
                return Err(internal_datafusion_err!(
                    "field `{}`@{} does not exist in input schema, found fields [{}]",
                    project_field.name(),
                    project_col_idx,
                    pretty_fmt_fields(src_schema)
                ));
            }

            // confirm idx is in the col_stats
            let Some(col_stats) = src_statistics
                .borrow()
                .column_statistics
                .get(project_col_idx)
            else {
                return Err(internal_datafusion_err!(
                    "missing column statistics for {}@{}",
                    project_field.name(),
                    project_col_idx
                ));
            };
            Ok(col_stats.clone())
        })
        .try_collect()?;

    Ok(Arc::new(Statistics {
        num_rows: src_statistics.borrow().num_rows,
        total_byte_size: src_statistics.borrow().total_byte_size,
        column_statistics,
    }))
}

/// Determine the output statistics based upon a subset of columns.
///
/// [`Statistics`] have column_statistics indexed to the plan's schema fields.
/// When the output projected schema has fewer columns than the input schema,
/// (a.k.a. it takes a subselection of columns),
/// then we also need to take a subselection of column stats.
///
/// This does not have a concept of aliasing, as it requires a `subset_selected` of columns.
///
/// This will error if the src_schema has a field without a corresponding src column_statistics.
pub(super) fn project_select_subset_of_column_statistics<
    T: Borrow<Statistics> + Clone + Into<Arc<Statistics>> + std::fmt::Debug,
>(
    src_statistics: &T,
    src_schema: &SchemaRef,
    subset_selected: &Vec<usize>,
    project_schema: &SchemaRef,
) -> Result<Arc<Statistics>> {
    // The src_statistics columns does not match the src schema.
    if src_statistics.borrow().column_statistics.len() != src_schema.fields().len() {
        return Err(internal_datafusion_err!(
            "statistics do not have the same number of columns as the schema, the schema has {:?} fields whereas the statistics have {:?} columns",
            src_schema.fields().len(),
            src_statistics.borrow().column_statistics.len()
        ));
    }

    // Check if projection schema matches the len of the subset_selected.
    // Do this before using the subset_selected for projecting onto the src_schema.
    if project_schema.fields().len() != subset_selected.len() {
        return Err(internal_datafusion_err!(
            "projected schema cannot be a subset of columns, the schema has {:?} fields whereas the projection indices have {:?} columns",
            project_schema.fields().len(),
            subset_selected.len()
        ));
    }

    // Select a subset of columns, erroring if the output column does not exist in the input schema.
    let column_statistics = src_schema
        .project(subset_selected)?
        .fields()
        .iter()
        .zip(subset_selected)
        .enumerate()
        .map(|(proj_col_idx, (src_field, src_col_idx))| {
            let project_field = &project_schema.fields()[proj_col_idx];

            // aliasing not permitted
            if src_field.name() != project_field.name() {
                return Err(internal_datafusion_err!(
                    "field `{}`@{} does not exist in input schema, found fields [{}]",
                    project_field.name(),
                    src_col_idx,
                    pretty_fmt_fields(src_schema)
                ));
            }

            // confirm idx is in the col_stats
            let Some(col_stats) = src_statistics.borrow().column_statistics.get(*src_col_idx)
            else {
                return Err(internal_datafusion_err!(
                    "missing column statistics for {}@{}",
                    src_field.name(),
                    src_col_idx
                ));
            };
            Ok(col_stats.clone())
        })
        .try_collect()?;

    Ok(Arc::new(Statistics {
        num_rows: src_statistics.borrow().num_rows,
        total_byte_size: src_statistics.borrow().total_byte_size,
        column_statistics,
    }))
}

/// For [`ProjectionExec`](datafusion::physical_plan::projection::ProjectionExec),
/// selectively cast schema projection on [`Statistics`].
///
/// This differs from other schema projections, as non-[`Column`] expressions may be present
/// or aliasing may be used.
/// In these circumstances, instead of erroring it returns unknown/absent column_statistics.
///
/// This code is pulled from a private internal DataFusion function:
/// <https://github.com/influxdata/arrow-datafusion/blob/b754eb4b9ccadc03d13838ac6e416c407975b405/datafusion/physical-plan/src/projection.rs#L294-L326>
pub(super) fn proj_exec_stats<'a>(
    mut stats: Statistics,
    exprs: impl Iterator<Item = &'a ProjectionExpr>,
    projexec_schema: &SchemaRef,
) -> Result<Arc<Statistics>> {
    let mut primitive_row_size = 0;
    let mut primitive_row_size_possible = true;
    let mut column_statistics = vec![];
    for projection_expr in exprs {
        let expr = &projection_expr.expr;
        let col_stats = if let Some(col) = expr.as_any().downcast_ref::<Column>() {
            // handle columns in schema
            let col_index = col.index();
            if col_index >= stats.column_statistics.len() {
                return Err(internal_datafusion_err!(
                    "Column index {} out of bounds in partition statistics projection \
                     (available columns: {}, column name: '{}'). \
                     This indicates a schema mismatch between projection expressions and input statistics.",
                    col_index,
                    stats.column_statistics.len(),
                    col.name()
                ));
            } else {
                stats.column_statistics[col_index].clone()
            }
        } else if let Some(lit_expr) = expr.as_any().downcast_ref::<Literal>() {
            // handle constants
            match lit_expr.value() {
                &ScalarValue::Null => ColumnStatistics {
                    null_count: Precision::Absent, // we don't know row count
                    min_value: Precision::Exact(ScalarValue::Null),
                    max_value: Precision::Exact(ScalarValue::Null),
                    distinct_count: Precision::Exact(1),
                    sum_value: Precision::Absent,
                },
                val => ColumnStatistics {
                    null_count: Precision::Exact(0),
                    min_value: Precision::Exact(val.clone()),
                    max_value: Precision::Exact(val.to_owned()),
                    distinct_count: Precision::Exact(1),
                    sum_value: Precision::Absent,
                },
            }
        } else {
            // TODO stats: estimate more statistics from expressions
            // (expressions should compute their statistics themselves)
            ColumnStatistics::new_unknown()
        };
        column_statistics.push(col_stats);
        if let Ok(data_type) = expr.data_type(projexec_schema)
            && let Some(value) = data_type.primitive_width()
        {
            primitive_row_size += value;
            continue;
        }
        primitive_row_size_possible = false;
    }

    if primitive_row_size_possible {
        stats.total_byte_size = Precision::Exact(primitive_row_size).multiply(&stats.num_rows);
    }
    stats.column_statistics = column_statistics;
    Ok(Arc::new(stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CHUNK_ORDER_COLUMN_NAME;
    use arrow::datatypes::{Field, Schema};
    use datafusion::physical_plan::expressions::{NoOp, col, lit};

    fn build_schema(col_names: Vec<&str>) -> SchemaRef {
        let fields = col_names
            .into_iter()
            .map(|col| Field::new(col, arrow::datatypes::DataType::Int64, false))
            .collect_vec()
            .into();
        Arc::new(Schema {
            fields,
            metadata: Default::default(),
        })
    }

    fn build_col_stats(values: Vec<(&str, i64, i64, usize)>) -> (Arc<Statistics>, SchemaRef) {
        let (column_statistics, fields) = values.into_iter().fold(
            (vec![], vec![]),
            |(mut col_stats, mut fields), (col_name, min, max, nulls)| {
                let col_stat = if col_name == CHUNK_ORDER_COLUMN_NAME
                    || col_name == "I am an alias"
                    || col_name == "I am an alias for possible idx bounds-failure"
                {
                    ColumnStatistics::new_unknown()
                } else {
                    ColumnStatistics {
                        min_value: Precision::Exact(ScalarValue::Int64(Some(min))),
                        max_value: Precision::Exact(ScalarValue::Int64(Some(max))),
                        null_count: Precision::Exact(nulls),
                        distinct_count: Precision::Absent,
                        sum_value: Precision::Absent,
                    }
                };

                col_stats.push(col_stat);
                fields.push(col_name);
                (col_stats, fields)
            },
        );

        (
            Arc::new(Statistics {
                column_statistics,
                num_rows: Precision::Absent,
                total_byte_size: Precision::Absent,
            }),
            build_schema(fields),
        )
    }

    #[test]
    fn test_retain_all_columns() {
        let (src_stats, src_schema) = build_col_stats(vec![
            ("col_a", 0, 24, 1),
            ("col_b", 354, 242650, 2),
            ("col_c", 67, 1111, 3),
        ]);
        let (expected_stats, project_schema) = build_col_stats(vec![
            ("col_a", 0, 24, 1),
            ("col_b", 354, 242650, 2),
            ("col_c", 67, 1111, 3),
        ]);

        /* Test: works for subset projection */
        let actual = project_select_subset_of_column_statistics(
            &src_stats,
            &src_schema,
            &vec![0, 1, 2],
            &project_schema,
        )
        .expect("ok");
        assert_eq!(
            actual, expected_stats,
            "should be able to project all columns"
        );

        /* Test: works for datasource projection */
        let actual =
            project_schema_onto_datasrc_statistics(&src_stats, &src_schema, &project_schema)
                .expect("ok");
        assert_eq!(
            actual, expected_stats,
            "should be able to project all columns"
        );

        /* Test: works for chunk order projection */
        let actual = project_statistics_with_chunk_order(&src_stats, &src_schema, &project_schema)
            .expect("ok");
        assert_eq!(
            actual, expected_stats,
            "should be able to project all columns"
        );

        /* Test: works for projection_exec */
        let exprs = [
            ProjectionExpr::new(col("col_a", &src_schema).unwrap(), "col_a".into()),
            ProjectionExpr::new(col("col_b", &src_schema).unwrap(), "col_b".into()),
            ProjectionExpr::new(col("col_c", &src_schema).unwrap(), "col_c".into()),
        ];
        let actual = proj_exec_stats(
            Arc::unwrap_or_clone(src_stats),
            exprs.iter(),
            &project_schema,
        )
        .unwrap();
        assert_eq!(
            actual, expected_stats,
            "should be able to project all columns"
        );
    }

    #[test]
    fn test_remove_and_reorder_columns() {
        let (src_stats, src_schema) = build_col_stats(vec![
            ("col_a", 0, 24, 1),
            ("col_b", 354, 242650, 2),
            ("col_c", 67, 1111, 3),
        ]);
        let (expected_stats, project_schema) = build_col_stats(vec![
            ("col_c", 67, 1111, 3),
            // projection removed col_b
            ("col_a", 0, 24, 1),
        ]);

        /* Test: works for subset projection */
        let actual = project_select_subset_of_column_statistics(
            &src_stats,
            &src_schema,
            &vec![2, 0],
            &project_schema,
        )
        .expect("ok");
        assert_eq!(
            actual, expected_stats,
            "should be able to remove and re-order columns"
        );

        /* Test: correctly errors for subset projection, if the subset_selected doesn't match the projection schema */
        let err = project_select_subset_of_column_statistics(
            &src_stats,
            &src_schema,
            &vec![0, 2], // this is incorrect
            &project_schema,
        )
        .unwrap_err();
        assert!(err.message().contains(
            "field `col_c`@0 does not exist in input schema, found fields [col_a,col_b,col_c]"
        ));

        /* Test: works for datasource projection */
        let actual =
            project_schema_onto_datasrc_statistics(&src_stats, &src_schema, &project_schema)
                .expect("ok");
        assert_eq!(
            actual, expected_stats,
            "should be able to remove and re-order columns"
        );

        /* Test: errors for chunk order projection */
        let err = project_statistics_with_chunk_order(&src_stats, &src_schema, &project_schema)
            .unwrap_err();
        assert!(err.message().contains(
            "field `col_c`@0 does not exist in input schema, found fields [col_a,col_b,col_c]"
        ));

        /* Test: works for projection_exec */
        let exprs = [
            ProjectionExpr::new(col("col_c", &src_schema).unwrap(), "col_c".into()),
            ProjectionExpr::new(col("col_a", &src_schema).unwrap(), "col_a".into()),
        ];
        let actual = proj_exec_stats(
            Arc::unwrap_or_clone(src_stats),
            exprs.iter(),
            &project_schema,
        )
        .unwrap();
        assert_eq!(
            actual, expected_stats,
            "should be able to remove and re-order columns"
        );
    }

    #[test]
    fn test_projection_with_aliases() {
        /* Test: can handle schema with aliases */
        let (src_stats, src_schema) =
            build_col_stats(vec![("col_a", 0, 24, 1), ("col_b", 354, 242650, 2)]);
        let (expected_stats, project_schema) = build_col_stats(vec![
            ("col_a", 0, 24, 1),
            ("I am an alias", 0, 0, 0),
            ("I am an alias for possible idx bounds-failure", 0, 0, 0),
        ]);

        /* Test: errors for subset projection, when number of subset_selected don't match the number of projected fields */
        let err = project_select_subset_of_column_statistics(
            &src_stats,
            &src_schema,
            &vec![0],
            &project_schema,
        )
        .unwrap_err();
        assert!(err.message().contains(
            "projected schema cannot be a subset of columns, the schema has 3 fields whereas the projection indices have 1 columns"
        ));

        /* Test: errors for subset projection, when field names in the src vs projected schema do not match */
        let err = project_select_subset_of_column_statistics(
            &src_stats,
            &src_schema,
            &vec![0, 1, 1],
            &project_schema,
        )
        .unwrap_err();
        assert!(err.message().contains(
            "field `I am an alias`@1 does not exist in input schema, found fields [col_a,col_b]."
        ));

        /* Test: works for datasource projection */
        // ** SPECIAL FOR DATASRC = fills in nulls for aliases missing in filegroup schema **
        let actual =
            project_schema_onto_datasrc_statistics(&src_stats, &src_schema, &project_schema)
                .expect("ok");
        let datasrc_null_columns = ColumnStatistics {
            null_count: expected_stats.num_rows,
            max_value: Precision::Exact(ScalarValue::Null),
            min_value: Precision::Exact(ScalarValue::Null),
            distinct_count: Precision::Absent,
            sum_value: Precision::Absent,
        };
        let expected_stats_with_nulls_at_datasrc = Arc::new(Statistics {
            num_rows: expected_stats.num_rows,
            total_byte_size: expected_stats.total_byte_size,
            column_statistics: vec![
                expected_stats.column_statistics[0].clone(),
                datasrc_null_columns.clone(),
                datasrc_null_columns.clone(),
            ],
        });
        assert_eq!(
            actual, expected_stats_with_nulls_at_datasrc,
            "should be able to handle schema with aliases"
        );
        assert_eq!(
            actual.column_statistics[1], datasrc_null_columns,
            "aliased column should have NULL min/max"
        );

        /* Test: errors for chunk order projection */
        let err = project_statistics_with_chunk_order(&src_stats, &src_schema, &project_schema)
            .unwrap_err();
        assert!(err.message().contains(
            "field `I am an alias`@1 does not exist in input schema, found fields [col_a,col_b]"
        ));

        /* Test: works for projection_exec */
        let exprs = [
            ProjectionExpr::new(col("col_a", &src_schema).unwrap(), "col_a".into()),
            ProjectionExpr::new(Arc::new(NoOp::new()), "I am an alias".into()),
            ProjectionExpr::new(
                Arc::new(NoOp::new()),
                "I am an alias for possible idx bounds-failure".into(),
            ),
        ];
        let actual = proj_exec_stats(
            Arc::unwrap_or_clone(src_stats),
            exprs.iter(),
            &project_schema,
        )
        .unwrap();
        assert_eq!(
            actual, expected_stats,
            "should be able to handle schema with aliases"
        );
        assert_eq!(
            actual.column_statistics[1],
            ColumnStatistics::new_unknown(),
            "aliased column should have unknown statistics"
        );
    }

    #[test]
    fn test_missing_chunk_order_column() {
        let (src_stats, src_schema) = build_col_stats(vec![("col_a", 0, 24, 1)]);
        let (expected_stats, project_schema) = build_col_stats(vec![
            ("col_a", 0, 24, 1),
            (CHUNK_ORDER_COLUMN_NAME, 0, 0, 0),
        ]);

        /* Test: errors for subset projection, when subset=[0,1] */
        let err = project_select_subset_of_column_statistics(
            &src_stats,
            &src_schema,
            &vec![0, 1],
            &project_schema,
        )
        .unwrap_err();
        assert!(
            err.message()
                .contains("project index 1 out of bounds, max field 1")
        );

        /* Test: errors for subset projection, when subset=[0] */
        let err = project_select_subset_of_column_statistics(
            &src_stats,
            &src_schema,
            &vec![0],
            &project_schema,
        )
        .unwrap_err();
        assert!(err.message().contains(
            "projected schema cannot be a subset of columns, the schema has 2 fields whereas the projection indices have 1 columns."
        ));

        /* Test: works for chunk order projection */
        let actual = project_statistics_with_chunk_order(&src_stats, &src_schema, &project_schema)
            .expect("ok");
        assert_eq!(
            actual, expected_stats,
            "should be able to handle _chunk_order column"
        );
        assert_eq!(
            actual.column_statistics[1],
            ColumnStatistics::new_unknown(),
            "_chunk_order should have unknown statistics"
        );

        /* Test: works for datasource projection */
        let actual =
            project_schema_onto_datasrc_statistics(&src_stats, &src_schema, &project_schema)
                .expect("ok");
        assert_eq!(
            actual, expected_stats,
            "should be able to handle _chunk_order column"
        );
        assert_eq!(
            actual.column_statistics[1],
            ColumnStatistics::new_unknown(),
            "_chunk_order should have unknown statistics"
        );
    }

    #[test]
    fn test_src_statistics_mismatches_src_schema() {
        /* Test: will error if src-statistics and src-schema have a different number of columns, for a non-scan node */
        let (src_stats_2_cols, _) = build_col_stats(vec![
            ("col_a", 0, 24, 1),
            ("make an extra column stats", 0, 24, 1),
        ]);
        let (_, src_schema_1_col) = build_col_stats(vec![("col_a", 0, 24, 1)]);
        let (_expected_stats, project_schema) = build_col_stats(vec![("col_a", 0, 24, 1)]);
        let expected_error = "statistics do not have the same number of columns as the schema";
        let actual_err = project_select_subset_of_column_statistics(
            &src_stats_2_cols,
            &src_schema_1_col,
            &vec![0, 1],
            &project_schema,
        )
        .unwrap_err();
        assert!(actual_err.message().contains(expected_error));
    }

    /// col_a (0), col_a (1) -->  col_a (0), col_a (1)
    #[test]
    fn test_projection_with_2_fields_having_same_name_and_same_projection_order() {
        let (src_stats, src_schema) = build_col_stats(vec![
            ("col_a", 0, 24, 1),    // first field with smaller stats
            ("col_a", 10, 100, 42), // second field with larger stats
        ]);
        let (expected_stats, project_schema) =
            build_col_stats(vec![("col_a", 0, 24, 1), ("col_a", 10, 100, 42)]);

        /* Test: works for subset projection  */
        let actual = project_select_subset_of_column_statistics(
            &src_stats,
            &src_schema,
            &vec![0, 1],
            &project_schema,
        )
        .expect("ok");
        assert_eq!(
            actual, expected_stats,
            "should be able to handle schema with same-named fields"
        );
        assert_eq!(
            actual.column_statistics[0].null_count.get_value(),
            Some(&1),
            "smaller stats col_a should come first"
        );
        assert_eq!(
            actual.column_statistics[1].null_count.get_value(),
            Some(&42),
            "larger stats col_a should come second"
        );

        /* Test: DOES NOT WORK for datasource projection, we take the first matching field_name in the schema */
        let actual =
            project_schema_onto_datasrc_statistics(&src_stats, &src_schema, &project_schema)
                .expect("ok");
        assert_eq!(
            actual.column_statistics[0].null_count.get_value(),
            Some(&1),
            "smaller stats col_a should come first"
        );
        assert_eq!(
            actual.column_statistics[1].null_count.get_value(),
            Some(&1),
            "** should take the first matching, which is again the smaller stats **"
        );

        /* Test: works chunk order projection */
        let actual = project_statistics_with_chunk_order(&src_stats, &src_schema, &project_schema)
            .expect("ok");
        assert_eq!(
            actual, expected_stats,
            "should be able to handle schema with same-named fields"
        );
        assert_eq!(
            actual.column_statistics[0].null_count.get_value(),
            Some(&1),
            "smaller stats col_a should come first"
        );
        assert_eq!(
            actual.column_statistics[1].null_count.get_value(),
            Some(&42),
            "larger stats col_a should come second"
        );

        /* Test: works for projection_exec */
        let exprs = [
            ProjectionExpr::new(Arc::new(Column::new("col_a", 0)) as _, "col_a_idx0".into()),
            ProjectionExpr::new(Arc::new(Column::new("col_a", 1)) as _, "col_a_idx1".into()),
        ];
        let actual = proj_exec_stats(
            Arc::unwrap_or_clone(src_stats),
            exprs.iter(),
            &project_schema,
        )
        .unwrap();
        assert_eq!(
            actual, expected_stats,
            "should be able to handle schema with same-named fields"
        );
        assert_eq!(
            actual.column_statistics[0].null_count.get_value(),
            Some(&1),
            "smaller stats col_a should come first"
        );
        assert_eq!(
            actual.column_statistics[1].null_count.get_value(),
            Some(&42),
            "larger stats col_a should come second"
        );
    }

    /// col_a (0), col_a (1) -->  col_a (1), col_a (0)
    #[test]
    fn test_projection_with_2_fields_having_same_name_and_inverse_projection_order() {
        let (src_stats, src_schema) = build_col_stats(vec![
            ("col_a", 0, 24, 1),    // first field with smaller stats
            ("col_a", 10, 100, 42), // second field with larger stats
        ]);
        // project as [1,0] a.k.a. reverse the columns
        let (expected_stats, project_schema) =
            build_col_stats(vec![("col_a", 10, 100, 42), ("col_a", 0, 24, 1)]);

        /* Test: works for subset projection  */
        let actual = project_select_subset_of_column_statistics(
            &src_stats,
            &src_schema,
            &vec![1, 0], // reverse the columns
            &project_schema,
        )
        .expect("ok");
        assert_eq!(
            actual, expected_stats,
            "should be able to handle schema with same-named fields, reversed ordering"
        );
        assert_eq!(
            actual.column_statistics[0].null_count.get_value(),
            Some(&42),
            "bigger stats col_a should come first"
        );

        /* Test: DOES NOT WORK for datasource projection, we take the first matching field_name in the schema */
        let actual =
            project_schema_onto_datasrc_statistics(&src_stats, &src_schema, &project_schema)
                .expect("ok");
        assert_eq!(
            actual.column_statistics[0].null_count.get_value(),
            Some(&1),
            "smaller stats col_a should come first"
        );
        assert_eq!(
            actual.column_statistics[1].null_count.get_value(),
            Some(&1),
            "** should take the first matching, which is again the smaller stats **"
        );

        /* Test: NOT POSSIBLE for chunk order projection, since it's a pass-thru with no re-ordering of columns */

        /* Test: works for projection_exec */
        let exprs = [
            ProjectionExpr::new(Arc::new(Column::new("col_a", 1)) as _, "col_a_idx1".into()),
            ProjectionExpr::new(Arc::new(Column::new("col_a", 0)) as _, "col_a_idx0".into()),
        ];
        let actual = proj_exec_stats(
            Arc::unwrap_or_clone(src_stats),
            exprs.iter(),
            &project_schema,
        )
        .unwrap();
        assert_eq!(
            actual, expected_stats,
            "should be able to handle schema with same-named fields, reversed ordering"
        );
        assert_eq!(
            actual.column_statistics[0].null_count.get_value(),
            Some(&42),
            "bigger stats col_a should come first"
        );
    }

    #[test]
    fn test_projection_with_constants() {
        // build any kinds of stats
        let (src_stats, src_schema) =
            build_col_stats(vec![("col_a", 0, 24, 1), ("col_b", 10, 100, 42)]);

        /* Test: if use columns col_a and col_b, you should get back their stats: */
        let exprs = [
            ProjectionExpr::new(col("col_a", &src_schema).unwrap(), "col_a".to_string()),
            ProjectionExpr::new(col("col_b", &src_schema).unwrap(), "col_b".to_string()),
        ];
        let actual = proj_exec_stats(
            Arc::unwrap_or_clone(Arc::clone(&src_stats)),
            exprs.iter(),
            &src_schema,
        )
        .unwrap();
        assert_eq!(
            actual, src_stats,
            "proj_exec_stats should extract the proper columns from the physical exprs"
        );

        /* Test: if use constants, then should get back constant stats: */
        let exprs = [
            ProjectionExpr::new(lit(10_000), "col_a".to_string()),
            ProjectionExpr::new(lit(1_000_000), "col_b".to_string()),
        ];
        let actual = proj_exec_stats(
            Arc::unwrap_or_clone(Arc::clone(&src_stats)),
            exprs.iter(),
            &src_schema,
        )
        .unwrap();
        // min/max are the constants
        assert_eq!(
            actual.column_statistics[0].min_value.get_value(),
            Some(&ScalarValue::Int32(Some(10_000))),
            "min value should be the constant"
        );
        assert_eq!(
            actual.column_statistics[0].max_value.get_value(),
            Some(&ScalarValue::Int32(Some(10_000))),
            "max value should be the constant"
        );
        assert_eq!(
            actual.column_statistics[1].min_value.get_value(),
            Some(&ScalarValue::Int32(Some(1_000_000))),
            "min value should be the constant"
        );
        assert_eq!(
            actual.column_statistics[1].max_value.get_value(),
            Some(&ScalarValue::Int32(Some(1_000_000))),
            "max value should be the constant"
        );
        // should have no nulls
        assert_eq!(
            actual.column_statistics[0].null_count.get_value(),
            Some(&0),
            "non-null constants should have no nulls"
        );
        assert_eq!(
            actual.column_statistics[1].null_count.get_value(),
            Some(&0),
            "non-null constants should have no nulls"
        );
        // should have only 1 distinct value
        assert_eq!(
            actual.column_statistics[0].distinct_count.get_value(),
            Some(&1),
            "constants should have 1 distinct value"
        );
        assert_eq!(
            actual.column_statistics[1].distinct_count.get_value(),
            Some(&1),
            "constants should have 1 distinct value"
        );

        /* Test: if use NULL as a constant, then should get back constant stats: */
        let exprs = [
            ProjectionExpr::new(lit(ScalarValue::Null), "col_a".to_string()),
            ProjectionExpr::new(lit(ScalarValue::Null), "col_b".to_string()),
        ];
        let actual =
            proj_exec_stats(Arc::unwrap_or_clone(src_stats), exprs.iter(), &src_schema).unwrap();
        // min/max are the constants
        assert_eq!(
            actual.column_statistics[0].min_value.get_value(),
            Some(&ScalarValue::Null),
            "min value should be the constant"
        );
        assert_eq!(
            actual.column_statistics[0].max_value.get_value(),
            Some(&ScalarValue::Null),
            "max value should be the constant"
        );
        assert_eq!(
            actual.column_statistics[1].min_value.get_value(),
            Some(&ScalarValue::Null),
            "min value should be the constant"
        );
        assert_eq!(
            actual.column_statistics[1].max_value.get_value(),
            Some(&ScalarValue::Null),
            "max value should be the constant"
        );
        // should have absent stats for Null Count
        // (since it will be non-zero, and equaling whatever is the row count after projection onto the batch stream)
        assert_eq!(
            actual.column_statistics[0].null_count,
            Precision::Absent,
            "null constants should have unknown stats for total nulls"
        );
        assert_eq!(
            actual.column_statistics[1].null_count,
            Precision::Absent,
            "null constants should have unknown stats for total nulls"
        );
        // should have only 1 distinct value
        assert_eq!(
            actual.column_statistics[0].distinct_count.get_value(),
            Some(&1),
            "constants should have 1 distinct value"
        );
        assert_eq!(
            actual.column_statistics[1].distinct_count.get_value(),
            Some(&1),
            "constants should have 1 distinct value"
        );
    }
}
