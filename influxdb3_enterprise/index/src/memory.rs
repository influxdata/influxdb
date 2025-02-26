//! This is the in-memory file index that is kept in the `CompactedData` struct. It is used to
//! quickly look up which parquet files contain a given column value.

use datafusion::{
    logical_expr::{Operator, expr::InList},
    prelude::Expr,
    scalar::ScalarValue,
};
use hashbrown::HashMap;
use influxdb3_id::ParquetFileId;
use influxdb3_write::{ChunkFilter, ParquetFile};
use std::{cmp::Ordering, sync::Arc};

use crate::hash_for_index;

#[derive(Debug, Eq, PartialEq, Default)]
pub struct FileIndex {
    index: HashMap<(u64, u64), Vec<ParquetFileMeta>>,
    parquet_files: HashMap<ParquetFileId, Arc<ParquetFile>>,
}

impl FileIndex {
    pub fn append(
        &mut self,
        column_name: &str,
        value: &str,
        gen_min_time_ns: i64,
        gen_max_time_ns: i64,
        parquet_file_ids: &[ParquetFileId],
    ) {
        let column = hash_for_index(column_name.as_bytes());
        let value = hash_for_index(value.as_bytes());
        self.append_with_hashed_values(
            column,
            value,
            gen_min_time_ns,
            gen_max_time_ns,
            parquet_file_ids,
        )
    }

    pub fn append_with_hashed_values(
        &mut self,
        column: u64,
        value: u64,
        gen_min_time_ns: i64,
        gen_max_time_ns: i64,
        parquet_file_ids: &[ParquetFileId],
    ) {
        let parquet_file_metas = self.index.entry((column, value)).or_default();
        for id in parquet_file_ids {
            parquet_file_metas.push(ParquetFileMeta {
                id: *id,
                min_time: gen_min_time_ns,
                max_time: gen_max_time_ns,
            });
        }
        parquet_file_metas.sort();
    }

    pub fn add_files(&mut self, files: &[Arc<ParquetFile>]) {
        for file in files {
            self.parquet_files.insert(file.id, Arc::clone(file));
        }
    }

    // here we just remove the files from the parquet file map. Their ids will still
    // show up in the index, but will get filtered out when we look up the files for a filter
    pub fn remove_files(&mut self, files: &[Arc<ParquetFile>]) {
        for file in files {
            self.parquet_files.remove(&file.id);
        }
    }

    /// Given the filter expression, return the list of parquet file that match the index,
    /// if the expression contains a column that is indexed. Also filter by time range if the
    /// expression contains a time range. If no indexed column appears in the expression, return
    /// all parquet files.
    pub fn parquet_files_for_filter(&self, filter: &ChunkFilter<'_>) -> Vec<Arc<ParquetFile>> {
        let ids = self.ids_for_filter(filter);
        match ids {
            Some(ids) => ids
                .iter()
                .filter_map(|id| self.parquet_files.get(id).cloned())
                // NB: don't need to filter on time here as already done by ids_for_filter
                .collect(),
            None => self
                .parquet_files
                .values()
                .filter(|f| filter.test_time_stamp_min_max(f.min_time, f.max_time))
                .cloned()
                .collect(),
        }
    }

    /// If the provided `filter` contains columns that are indexed, return the list of
    /// `ParquetFileId`s in the index that satisfy the `filter`; this will also filter
    /// `ParquetFileId`s by time range if the `filter` has one. If no indexed column appears in the
    /// expression, return None.
    pub fn ids_for_filter(&self, filter: &ChunkFilter<'_>) -> Option<Vec<ParquetFileId>> {
        let mut filtered_ids = Option::<Vec<ParquetFileId>>::None;

        // Step through each Expr, and for each, walk that Expr tree to match `ParquetFileIds` in
        // the index that satisfy the Expr.
        //
        // The list of Exprs from DataFusion are split on AND conjunctions, so if there are multiple
        // Exprs, we need to take the intersection of the `ParquetFileId`s in the index from each.
        for expr in filter.original_filters() {
            if let Some(ids) = self.walk_expr(expr, filter) {
                if let Some(ref f) = filtered_ids {
                    filtered_ids.replace(intersection(f, &ids));
                } else {
                    filtered_ids.replace(ids);
                }
            }
        }

        filtered_ids
    }

    fn walk_expr(&self, expr: &Expr, filter: &ChunkFilter<'_>) -> Option<Vec<ParquetFileId>> {
        if let Expr::BinaryExpr(b) = expr {
            if let Expr::Column(c) = b.left.as_ref() {
                if let (Operator::Eq, Some(val)) = (b.op, b.right.as_string_literal()) {
                    let col_hashed = hash_for_index(c.name().as_bytes());
                    let val_hashed = hash_for_index(val.as_bytes());
                    return self
                        .parquet_file_ids_for_hashed_column_value(col_hashed, val_hashed, filter);
                }
            } else {
                let left = self.walk_expr(&b.left, filter);
                let right = self.walk_expr(&b.right, filter);
                match (left, right, b.op) {
                    (Some(left), Some(right), datafusion::logical_expr::Operator::And) => {
                        return Some(intersection(&left, &right));
                    }
                    (Some(left), Some(right), datafusion::logical_expr::Operator::Or) => {
                        return Some(union(&left, &right));
                    }
                    (Some(l), _, _) => return Some(l),
                    (_, Some(r), _) => return Some(r),
                    _ => {}
                }
            }
        } else if let Expr::InList(InList {
            expr,
            list,
            // NB: this ignores NOT IN predicates; the assumption is that they would not be specific
            // and therefore using the index will provide negligible gain over scanning all files.
            //
            // The draw-back is that if a user provides a query like:
            // ```
            // foo IN ('a', 'b') AND foo NOT IN ('b')
            // ```
            // ...which would simplify to:
            // ```
            // foo IN ('a')
            // ```
            // This method will take the `IN` but ignore the `NOT IN` and be evaluated as
            // ```
            // foo IN ('a', 'b')
            // ```
            // The implication is that the index is less specific than it could be, but DataFusion
            // will just ignore data from the false positive files, so the end result will still be
            // correct. Furthermore, such a predicate is nonsensical.
            negated: false,
        }) = expr
        {
            if let Expr::Column(c) = expr.as_ref() {
                let col_hashed = hash_for_index(c.name().as_bytes());
                let values_hashed = list
                    .iter()
                    .filter_map(AsStringLiteral::as_string_literal)
                    .map(|s| hash_for_index(s.as_bytes()))
                    .collect::<Vec<u64>>();
                let mut ids = Vec::new();
                for val_hashed in values_hashed {
                    if let Some(new_ids) = self
                        .parquet_file_ids_for_hashed_column_value(col_hashed, val_hashed, filter)
                    {
                        ids = union(&ids, &new_ids);
                    }
                }
                return Some(ids);
            }
        }

        None
    }

    /// Removes the file id from the map of files, but doesn't remove it from postings lists
    /// it appears in as it would be too costly. We will vacume the file index periodically
    /// to clean up the postings lists of stale file ids.
    pub fn remove_file(&mut self, file_id: ParquetFileId) {
        self.parquet_files.remove(&file_id);
    }

    fn parquet_file_ids_for_hashed_column_value(
        &self,
        column: u64,
        value: u64,
        filter: &ChunkFilter<'_>,
    ) -> Option<Vec<ParquetFileId>> {
        self.index.get(&(column, value)).map(|v| {
            v.iter()
                .filter(|p| filter.test_time_stamp_min_max(p.min_time, p.max_time))
                .map(|p| p.id)
                .collect()
        })
    }
}

/// Helper trait for reducing DataFusion `Expr`s and `ScalarValue`s to string literals
trait AsStringLiteral {
    fn as_string_literal(&self) -> Option<&str>;
}

impl AsStringLiteral for Expr {
    fn as_string_literal(&self) -> Option<&str> {
        match self {
            Expr::Literal(literal) => literal.as_string_literal(),
            _ => None,
        }
    }
}

impl AsStringLiteral for ScalarValue {
    fn as_string_literal(&self) -> Option<&str> {
        match self {
            ScalarValue::Utf8(Some(val))
            | ScalarValue::Utf8View(Some(val))
            | ScalarValue::LargeUtf8(Some(val)) => Some(val.as_str()),
            ScalarValue::Dictionary(_, scalar_value) => scalar_value.as_string_literal(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct ParquetFileMeta {
    id: ParquetFileId,
    min_time: i64,
    max_time: i64,
}

// impl Ord and PartialOrd for ParquetFileMeta so that we can sort the list of ParquetFileMeta by id
impl Ord for ParquetFileMeta {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialOrd for ParquetFileMeta {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn intersection(vec1: &[ParquetFileId], vec2: &[ParquetFileId]) -> Vec<ParquetFileId> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);

    while i < vec1.len() && j < vec2.len() {
        match vec1[i].cmp(&vec2[j]) {
            Ordering::Less => {
                i += 1;
            }
            Ordering::Equal => {
                result.push(vec1[i]);
                i += 1;
                j += 1;
            }
            Ordering::Greater => {
                j += 1;
            }
        }
    }

    result
}

fn union(vec1: &[ParquetFileId], vec2: &[ParquetFileId]) -> Vec<ParquetFileId> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);

    while i < vec1.len() && j < vec2.len() {
        match vec1[i].cmp(&vec2[j]) {
            Ordering::Less => {
                result.push(vec1[i]);
                i += 1;
            }
            Ordering::Equal => {
                result.push(vec1[i]);
                i += 1;
                j += 1;
            }
            Ordering::Greater => {
                result.push(vec2[j]);
                j += 1;
            }
        }
    }

    // Add remaining elements from vec1, if any
    result.extend_from_slice(&vec1[i..]);

    // Add remaining elements from vec2, if any
    result.extend_from_slice(&vec2[j..]);

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;
    use datafusion::{arrow::datatypes::DataType, scalar::ScalarValue};
    use influxdb3_catalog::catalog::TableDefinition;
    use influxdb3_id::{ColumnId, TableId};
    use schema::InfluxColumnType;

    #[test_log::test]
    fn test_file_index() {
        // Create a table def, this is used when the index analyzes filter expressions:
        let table_def = Arc::new(
            TableDefinition::new(
                TableId::new(),
                "test-table".into(),
                vec![
                    (ColumnId::from(0), "host".into(), InfluxColumnType::Tag),
                    (ColumnId::from(1), "region".into(), InfluxColumnType::Tag),
                    (
                        ColumnId::from(2),
                        "time".into(),
                        InfluxColumnType::Timestamp,
                    ),
                ],
                vec![ColumnId::from(0), ColumnId::from(1)],
            )
            .unwrap(),
        );

        // create a file index and populate it with a bunch of parquet file ids; this sends in
        // a column name/value pair, e.g., "host"/"A", along with a min time and max time of the
        // generation indexed, as well as a set of parquet file ids that are in that generation
        // that are associated with the column/name value pair:
        let mut file_index = FileIndex::default();
        for (column, value, min_time, max_time, file_ids) in [
            ("host", "A", 0, 100, vec![1]),
            ("host", "A", 101, 200, vec![2]),
            ("host", "B", 101, 200, vec![2, 3]),
            ("host", "B", 201, 300, vec![4]),
            ("host", "A", 301, 400, vec![5]),
            ("host", "B", 301, 400, vec![6]),
            ("host", "C", 301, 400, vec![7]),
            ("host", "B", 401, 500, vec![8]),
            ("host", "C", 401, 500, vec![9]),
            ("host", "A", 501, 600, vec![10]),
            ("region", "east", 0, 300, vec![1, 2]),
            ("region", "west", 0, 300, vec![3, 4]),
            ("region", "east", 301, 600, vec![5, 6, 8, 10]),
            ("region", "west", 301, 600, vec![7, 9]),
        ] {
            file_index.append(
                column,
                value,
                min_time,
                max_time,
                file_ids
                    .iter()
                    .copied()
                    .map(ParquetFileId::from)
                    .collect::<Vec<_>>()
                    .as_slice(),
            );
        }

        // run a set of test cases that utilize the index to get an expected set of parquet file IDs:
        {
            #[derive(Debug)]
            struct TestCase<'a> {
                description: &'a str,
                /// List of DataFusion filter expressions
                exprs: &'a [Expr],
                /// The set of `ParquetFileId`s expected in the index for the given set of
                /// expressions. Expressed as a list of `u64` here for brevity and converted below
                expected_ids: &'a [u64],
            }

            let test_cases = [
                TestCase {
                    description: "basic equality on host",
                    exprs: &[col("host").eq(lit("A"))],
                    expected_ids: &[1, 2, 5, 10],
                },
                TestCase {
                    description: "expression matching host with dictionary not string literal",
                    exprs: &[col("host").eq(lit(ScalarValue::Dictionary(
                        Box::new(DataType::Int32),
                        Box::new(ScalarValue::from("A")),
                    )))],
                    expected_ids: &[1, 2, 5, 10],
                },
                TestCase {
                    description: "basic equality on host with time bounds",
                    exprs: &[
                        col("host").eq(lit("A")),
                        col("time")
                            .gt(lit_timestamp_nano(101))
                            .and(col("time").lt_eq(lit_timestamp_nano(300))),
                    ],
                    expected_ids: &[2],
                },
                TestCase {
                    description: "equality on host with OR conjunction",
                    exprs: &[col("host").eq(lit("A")).or(col("host").eq(lit("B")))],
                    expected_ids: &[1, 2, 3, 4, 5, 6, 8, 10],
                },
                TestCase {
                    description: "ids are returned in order even if not insterted in order",
                    exprs: &[col("region")
                        .eq(lit("east"))
                        .or(col("region").eq(lit("west")))],
                    expected_ids: &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                },
                TestCase {
                    description: "returns correct ids even with column names not indexed",
                    exprs: &[col("host").eq(lit("B")), col("foo").gt(lit(1))],
                    expected_ids: &[2, 3, 4, 6, 8],
                },
                TestCase {
                    description: "empty set if time bounds disqualify",
                    exprs: &[
                        col("host").eq(lit("C")),
                        col("time").lt(lit_timestamp_nano(301)),
                    ],
                    expected_ids: &[],
                },
                TestCase {
                    description: "empty set from multiple expressions uses intersection",
                    exprs: &[col("host").eq(lit("A")), col("host").eq(lit("B"))],
                    expected_ids: &[2],
                },
                TestCase {
                    description: "multiple expressions",
                    // NB: this does not remove `2`, because although "B" is in `2`, we still need
                    // to produce that ID from the index given that "A" is also in `2`.
                    exprs: &[col("host").eq(lit("A")), col("host").not_eq(lit("B"))],
                    expected_ids: &[1, 2, 5, 10],
                },
                TestCase {
                    description: "OR of two different columns",
                    exprs: &[col("host").eq(lit("C")).or(col("region").eq(lit("west")))],
                    expected_ids: &[3, 4, 7, 9],
                },
                TestCase {
                    description: "basic in list",
                    exprs: &[col("host").in_list(vec![lit("A")], false)],
                    expected_ids: &[1, 2, 5, 10],
                },
            ];

            for t in test_cases {
                println!("test case: {}", t.description);
                let filter = ChunkFilter::new(&table_def, t.exprs).unwrap();
                let ids = file_index.ids_for_filter(&filter).unwrap();
                assert_eq!(
                    ids,
                    t.expected_ids
                        .iter()
                        .copied()
                        .map(ParquetFileId::from)
                        .collect::<Vec<_>>(),
                    "test failed: {}",
                    t.description
                );
            }
        }

        // test cases where the index is ignored:
        {
            struct TestCase<'a> {
                exprs: &'a [Expr],
            }

            let test_cases = [
                // column not in the index should be ignored:
                TestCase {
                    exprs: &[col("foo").eq(lit("bar"))],
                },
                // NOT IN on its own is ignored:
                TestCase {
                    exprs: &[col("host").in_list(vec![lit("A")], true)],
                },
            ];

            for t in test_cases {
                let filter = ChunkFilter::new(&table_def, t.exprs).unwrap();
                let ids = file_index.ids_for_filter(&filter);
                assert!(ids.is_none());
            }
        }
    }
}
