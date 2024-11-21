//! This is the in-memory file index that is kept in the `CompactedData` struct. It is used to
//! quickly look up which parquet files contain a given column value.

use datafusion::common::ScalarValue;
use datafusion::logical_expr::Expr;
use hashbrown::HashMap;
use influxdb3_id::ParquetFileId;
use influxdb3_write::ParquetFile;
use schema::TIME_COLUMN_NAME;
use std::sync::Arc;
use xxhash_rust::xxh64::xxh64;

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
        let column = xxh64(column_name.as_bytes(), 0);
        let value = xxh64(value.as_bytes(), 0);
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
    pub fn parquet_files_for_filter(&self, filter: &[Expr]) -> Vec<Arc<ParquetFile>> {
        let ids = self.ids_for_filter(filter);
        match ids {
            Some(ids) => ids
                .iter()
                .filter_map(|id| self.parquet_files.get(id).cloned())
                .collect(),
            None => self.parquet_files.values().cloned().collect(),
        }
    }

    /// Given the filter expression, return the list of parquet file ids that match the index,
    /// if the expression contains a column that is indexed. Also filter by time range if the
    /// expression contains a time range. If no indexed column appears in the expression, return None.
    pub fn ids_for_filter(&self, filter: &[Expr]) -> Option<Vec<ParquetFileId>> {
        let (min_time, max_time) = get_timestamp_range_from_exprs(filter);

        let mut filtered_ids: Option<Vec<ParquetFileId>> = None;

        for expr in filter {
            if let Some(ids) = self.walk_expr(expr, min_time, max_time) {
                if let Some(f) = filtered_ids {
                    filtered_ids = Some(union(&f, &ids));
                } else {
                    filtered_ids = Some(ids);
                }
            }
        }

        filtered_ids
    }

    /// Removes the file id from the map of files, but doesn't remove it from postings lists
    /// it appears in as it would be too costly. We will vacume the file index periodically
    /// to clean up the postings lists of stale file ids.
    pub fn remove_file(&mut self, file_id: ParquetFileId) {
        self.parquet_files.remove(&file_id);
    }

    fn contains_value(&self, col: &str, val: &str) -> bool {
        self.index
            .contains_key(&(xxh64(col.as_bytes(), 0), xxh64(val.as_bytes(), 0)))
    }

    fn walk_expr(&self, expr: &Expr, min_time: i64, max_time: i64) -> Option<Vec<ParquetFileId>> {
        if let Expr::BinaryExpr(b) = expr {
            if let Expr::Column(c) = b.left.as_ref() {
                match b.right.as_ref() {
                    Expr::Literal(ScalarValue::Utf8(Some(val))) => {
                        if self.contains_value(&c.name, val)
                            && b.op == datafusion::logical_expr::Operator::Eq
                        {
                            return self.ids_for_column_value(&c.name, val, min_time, max_time);
                        }
                    }
                    Expr::Literal(ScalarValue::Dictionary(_, val)) => {
                        if let ScalarValue::Utf8(Some(val)) = val.as_ref() {
                            if self.contains_value(&c.name, val)
                                && b.op == datafusion::logical_expr::Operator::Eq
                            {
                                return self.ids_for_column_value(&c.name, val, min_time, max_time);
                            }
                        }
                    }
                    _ => {}
                }
            } else {
                let left = self.walk_expr(&b.left, min_time, max_time);
                let right = self.walk_expr(&b.right, min_time, max_time);
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
        }

        None
    }

    fn ids_for_column_value(
        &self,
        column: &str,
        value: &str,
        min_time: i64,
        max_time: i64,
    ) -> Option<Vec<ParquetFileId>> {
        let (column, value) = (xxh64(column.as_bytes(), 0), xxh64(value.as_bytes(), 0));
        self.index.get(&(column, value)).map(|v| {
            v.iter()
                .filter(|p| p.contains_time_range(min_time, max_time))
                .map(|p| p.id)
                .collect()
        })
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct ParquetFileMeta {
    id: ParquetFileId,
    min_time: i64,
    max_time: i64,
}

impl ParquetFileMeta {
    fn contains_time_range(&self, min_time: i64, max_time: i64) -> bool {
        self.min_time <= max_time && self.max_time >= min_time
    }
}

// impl Ord and PartialOrd for ParquetFileMeta so that we can sort the list of ParquetFileMeta by id
impl Ord for ParquetFileMeta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialOrd for ParquetFileMeta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn get_timestamp_range_from_exprs(filter: &[Expr]) -> (i64, i64) {
    let mut time_min = None;
    let mut time_max = None;

    for expr in filter {
        walk_for_time(expr, &mut time_min, &mut time_max);
    }

    match (time_min, time_max) {
        (Some(min), Some(max)) => (min, max),
        (Some(min), None) => (min, i64::MAX),
        (None, Some(max)) => (i64::MIN, max),
        _ => (i64::MIN, i64::MAX),
    }
}

fn walk_for_time(expr: &Expr, tmin: &mut Option<i64>, tmax: &mut Option<i64>) {
    if let Expr::BinaryExpr(b) = expr {
        if let Expr::Column(c) = b.left.as_ref() {
            if c.name == TIME_COLUMN_NAME {
                if let Expr::Literal(ScalarValue::TimestampNanosecond(Some(t), _)) =
                    b.right.as_ref()
                {
                    match b.op {
                        datafusion::logical_expr::Operator::GtEq => {
                            *tmin = Some(*t);
                        }
                        datafusion::logical_expr::Operator::Gt => {
                            *tmin = Some(*t + 1);
                        }
                        datafusion::logical_expr::Operator::LtEq => {
                            *tmax = Some(*t + 1);
                        }
                        datafusion::logical_expr::Operator::Lt => {
                            *tmax = Some(*t);
                        }
                        datafusion::logical_expr::Operator::Eq => {
                            *tmin = Some(*t);
                            *tmax = Some(*t + 1);
                        }
                        _ => {}
                    }
                }
            }
        } else {
            walk_for_time(&b.left, tmin, tmax);
            walk_for_time(&b.right, tmin, tmax);
        }
    }
}

fn intersection(vec1: &[ParquetFileId], vec2: &[ParquetFileId]) -> Vec<ParquetFileId> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);

    #[allow(clippy::comparison_chain)]
    while i < vec1.len() && j < vec2.len() {
        if vec1[i] < vec2[j] {
            i += 1;
        } else if vec1[i] > vec2[j] {
            j += 1;
        } else {
            result.push(vec1[i]);
            i += 1;
            j += 1;
        }
    }

    result
}

fn union(vec1: &[ParquetFileId], vec2: &[ParquetFileId]) -> Vec<ParquetFileId> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);

    #[allow(clippy::comparison_chain)]
    while i < vec1.len() && j < vec2.len() {
        if vec1[i] < vec2[j] {
            result.push(vec1[i]);
            i += 1;
        } else if vec1[i] > vec2[j] {
            result.push(vec2[j]);
            j += 1;
        } else {
            result.push(vec1[i]);
            i += 1;
            j += 1;
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
    use datafusion::arrow::datatypes::DataType;
    use datafusion::prelude::*;

    #[test]
    fn test_file_index() {
        let mut file_index = FileIndex::default();
        let one = ParquetFileMeta {
            id: ParquetFileId::from(1),
            min_time: 0,
            max_time: 100,
        };
        let two = ParquetFileMeta {
            id: ParquetFileId::from(2),
            min_time: 101,
            max_time: 200,
        };
        let three = ParquetFileMeta {
            id: ParquetFileId::from(3),
            min_time: 201,
            max_time: 300,
        };
        let five = ParquetFileMeta {
            id: ParquetFileId::from(5),
            min_time: 601,
            max_time: 700,
        };
        file_index.append("host", "A", 0, 100, &[one.id]);
        file_index.append("host", "A", 201, 300, &[three.id]);
        file_index.append("host", "B", 101, 200, &[two.id]);
        file_index.append("region", "us-west", 0, 300, &[one.id, two.id, five.id]);
        file_index.append("region", "us-west", 301, 600, &[three.id]);

        let expr = col("host").eq(lit("A"));
        let ids = file_index.ids_for_filter(&[expr]);
        assert_eq!(ids, Some(vec![one.id, three.id]));

        // build an expression matching host with a dictionary not a string literal
        let expr = col("host").eq(lit(ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::from("A")),
        )));
        let ids = file_index.ids_for_filter(&[expr]);
        assert_eq!(ids, Some(vec![one.id, three.id]));

        let expr = col("host").eq(lit("A")).and(
            col("time")
                .gt(lit_timestamp_nano(101))
                .and(col("time").lt_eq(lit_timestamp_nano(300))),
        );
        let ids = file_index.ids_for_filter(&[expr]);
        assert_eq!(ids, Some(vec![three.id]));

        let expr = col("host").eq(lit("A")).or(col("host").eq(lit("B")));
        let ids = file_index.ids_for_filter(&[expr]);
        assert_eq!(ids, Some(vec![one.id, two.id, three.id]));

        // test that the ids are returned in order even if not inserted that way
        let expr = col("region").eq(lit("us-west"));
        let ids = file_index.ids_for_filter(&[expr]);
        assert_eq!(ids, Some(vec![one.id, two.id, three.id, five.id]));

        // test that index returns the file ids for a filter that contains a column that is not indexed
        let expr = col("host").eq(lit("B")).and(col("foo").gt(lit(1)));
        let ids = file_index.ids_for_filter(&[expr]);
        assert_eq!(ids, Some(vec![two.id]));

        // test that we get an empty set if the time disqualifies it
        let expr = col("host")
            .eq(lit("A"))
            .and(col("time").gt(lit_timestamp_nano(301)));
        let ids = file_index.ids_for_filter(&[expr]);
        assert_eq!(ids, Some(vec![]));

        // test that we get an empty set if an AND clause filters out all files
        let expr = col("host").eq(lit("A")).and(col("host").eq(lit("B")));
        let ids = file_index.ids_for_filter(&[expr]);
        assert_eq!(ids, Some(vec![]));

        // test that we get None if the index doesn't contain any of the columns in the filter (thus all files are valid)
        let expr = col("foo").eq(lit("bar"));
        let ids = file_index.ids_for_filter(&[expr]);
        assert_eq!(ids, None);
    }
}
