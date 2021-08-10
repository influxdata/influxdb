use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    convert::{TryFrom, TryInto},
    fmt::Display,
    sync::Arc,
};

use hashbrown::{hash_map, HashMap};
use itertools::Itertools;
use snafu::{ResultExt, Snafu};

use crate::column::{self, cmp::Operator, Column, RowIDs, RowIDsOption};
use crate::schema;
use crate::schema::{AggregateType, LogicalDataType, ResultSchema};
use crate::value::{
    AggregateVec, EncodedValues, OwnedValue, Scalar, Value, Values, ValuesIterator,
};
use arrow::{
    array,
    array::ArrayRef,
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
};
use datafusion::{
    logical_plan::Expr as DfExpr, logical_plan::Operator as DFOperator,
    scalar::ScalarValue as DFScalarValue,
};
use internal_types::schema::{InfluxColumnType, Schema};
use internal_types::selection::Selection;
use std::num::NonZeroU64;

/// The name used for a timestamp column.
pub const TIME_COLUMN_NAME: &str = internal_types::schema::TIME_COLUMN_NAME;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("arrow conversion error: {}", source))]
    ArrowConversion { source: arrow::error::ArrowError },

    #[snafu(display("schema conversion error: {}", source))]
    SchemaConversion {
        source: internal_types::schema::builder::Error,
    },

    #[snafu(display("unsupported operation: {}", msg))]
    UnsupportedOperation { msg: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A `RowGroup` is an immutable horizontal chunk of a single `Table`. By
/// definition it has the same schema as all the other row groups in the table.
/// All the columns within the `RowGroup` must have the same number of logical
/// rows.
pub struct RowGroup {
    meta: MetaData,

    columns: Vec<Column>,
    all_columns_by_name: BTreeMap<String, usize>,
    time_column: usize,
}

impl RowGroup {
    pub fn new(rows: u32, columns: Vec<(String, ColumnType)>) -> Self {
        let mut meta = MetaData {
            rows,
            ..MetaData::default()
        };

        let mut all_columns = vec![];
        let mut all_columns_by_name = BTreeMap::new();
        let mut time_column = None;

        for (name, ct) in columns {
            match ct {
                ColumnType::Tag(c) => {
                    assert_eq!(c.num_rows(), rows);

                    meta.add_column(
                        &name,
                        c.size(),
                        schema::ColumnType::Tag(name.clone()),
                        c.logical_datatype(),
                        c.column_range(),
                        c.storage_stats().nulls,
                        c.cardinality(),
                    );

                    all_columns_by_name.insert(name.clone(), all_columns.len());
                    all_columns.push(c);
                }
                ColumnType::Field(c) => {
                    assert_eq!(c.num_rows(), rows);

                    meta.add_column(
                        &name,
                        c.size(),
                        schema::ColumnType::Field(name.clone()),
                        c.logical_datatype(),
                        c.column_range(),
                        c.storage_stats().nulls,
                        c.cardinality(),
                    );
                    all_columns_by_name.insert(name.clone(), all_columns.len());
                    all_columns.push(c);
                }
                ColumnType::Time(c) => {
                    assert_eq!(c.num_rows(), rows);

                    meta.add_column(
                        &name,
                        c.size(),
                        schema::ColumnType::Timestamp(name.clone()),
                        c.logical_datatype(),
                        c.column_range(),
                        c.storage_stats().nulls,
                        c.cardinality(),
                    );

                    all_columns_by_name.insert(name.clone(), all_columns.len());
                    time_column = Some(all_columns.len());
                    all_columns.push(c);
                }
            }
        }

        // Meta data should have same columns for types and ranges.
        assert_eq!(meta.columns.keys().len(), all_columns.len());

        Self {
            meta,
            columns: all_columns,
            all_columns_by_name,
            time_column: time_column.unwrap(),
        }
    }

    /// The total estimated size in bytes of the row group in memory
    pub fn size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>()
            + self
                .all_columns_by_name
                .keys()
                .map(|key| key.len() + std::mem::size_of::<usize>())
                .sum::<usize>();
        base_size + self.meta.size()
    }

    /// Returns an iterator of (column_name, estimated_size) for all
    /// columns in this row_group
    pub fn column_sizes(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        self.all_columns_by_name.iter().map(move |(name, idx)| {
            let column = &self.columns[*idx];
            (name.as_str(), column.size())
        })
    }

    /// The total estimated size in bytes of all columns in the row group if
    /// the values within were stored contiguously with no compression.
    /// `include_nulls` controls whether to size NULL values according to the
    /// base size of the data-type or to ignore them from the calculation.
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        self.columns
            .iter()
            .map(|c| c.size_raw(include_nulls))
            .sum::<usize>()
    }

    /// The number of rows in the `RowGroup` (all columns have the same number
    /// of rows).
    pub fn rows(&self) -> u32 {
        self.meta.rows
    }

    /// The number of columns in the `RowGroup`.
    pub fn columns(&self) -> usize {
        self.columns.len()
    }

    // The row group's meta data.
    pub fn metadata(&self) -> &MetaData {
        &self.meta
    }

    // Returns a reference to a column from the column name.
    //
    // It is the caller's responsibility to ensure the column exists in the read
    // group. Panics if the column doesn't exist.
    fn column_by_name(&self, name: ColumnName<'_>) -> &Column {
        &self.columns[*self.all_columns_by_name.get(name).unwrap()]
    }

    // Takes a `ColumnName`, looks up that column in the `RowGroup`, and
    // returns a reference to that column's name owned by the `RowGroup` along
    // with a reference to the column itself. The returned column name will have
    // the lifetime of `self`, not the lifetime of the input.
    fn column_name_and_column(&self, name: ColumnName<'_>) -> (&str, &Column) {
        let (column_name, column_index) = self.all_columns_by_name.get_key_value(name).unwrap();
        (column_name, &self.columns[*column_index])
    }

    // Returns a reference to the timestamp column.
    fn time_column(&self) -> &Column {
        &self.columns[self.time_column]
    }

    /// Efficiently determines if the row group _might_ satisfy all of the
    /// provided binary expressions, when conjunctively applied.
    ///
    /// `false` indicates that one or more of the expressions would not match
    /// any rows in the row group.
    pub fn could_satisfy_conjunctive_binary_expressions<'a>(
        &self,
        exprs: impl IntoIterator<Item = &'a BinaryExpr>,
    ) -> bool {
        // if a single expression returns `false` then the whole operation
        // returns `false` because the expressions are conjunctively applied.
        exprs
            .into_iter()
            .all(|expr| self.meta.column_could_satisfy_binary_expr(expr))
    }

    /// Determines if the row group contains one or more rows that satisfy all
    /// of the provided binary expressions, when conjunctively applied.
    ///
    /// `satisfies_predicate` currently constructs a set of row ids for all
    /// rows that satisfy the predicate, but does not materialise any
    /// values. There are some optimisation opportunities here, but I don't
    /// think they're at all worth it at the moment.
    ///
    /// They could be:
    ///  * for predicates with single expression just find a matching value in
    ///    the column;
    ///  * in some cases perhaps work row by row rather than column by column.
    pub fn satisfies_predicate(&self, predicate: &Predicate) -> bool {
        if !self.could_satisfy_conjunctive_binary_expressions(predicate.iter()) {
            return false;
        }

        // return false if there were no rows ids returned that satisfy the
        // predicate.
        !matches!(
            self.row_ids_from_predicate(predicate),
            RowIDsOption::None(_)
        )
    }

    //
    // Methods for reading the `RowGroup`
    //

    /// Returns a set of materialised column values that optionally satisfy a
    /// predicate.
    ///
    /// TODO(edd): this should probably return an Option and the caller can
    /// filter None results.
    pub fn read_filter(
        &self,
        columns: &[ColumnName<'_>],
        predicates: &Predicate,
    ) -> ReadFilterResult<'_> {
        let select_columns = self.meta.schema_for_column_names(columns);
        assert_eq!(select_columns.len(), columns.len());

        let schema = ResultSchema {
            select_columns,
            ..Default::default()
        };

        // apply predicates to determine candidate rows.
        let row_ids = self.row_ids_from_predicate(predicates);
        let col_data = self.materialise_rows(&schema, row_ids);
        ReadFilterResult {
            schema,
            data: col_data,
        }
    }

    fn materialise_rows(&self, schema: &ResultSchema, row_ids: RowIDsOption) -> Vec<Values<'_>> {
        let mut col_data = Vec::with_capacity(schema.len());
        match row_ids {
            RowIDsOption::None(_) => col_data, // nothing to materialise
            RowIDsOption::Some(row_ids) => {
                // TODO(edd): causes an allocation. Implement a way to pass a
                // pooled buffer to the croaring Bitmap API.
                let row_ids = row_ids.to_vec();
                for (ct, _) in &schema.select_columns {
                    let (_, col) = self.column_name_and_column(ct.as_str());
                    if let schema::ColumnType::Tag(_) = ct {
                        col_data.push(col.values_as_dictionary(row_ids.as_slice()));
                    } else {
                        col_data.push(col.values(row_ids.as_slice()));
                    }
                }
                col_data
            }

            RowIDsOption::All(_) => {
                for (ct, _) in &schema.select_columns {
                    let (_, col) = self.column_name_and_column(ct.as_str());
                    if let schema::ColumnType::Tag(_) = ct {
                        col_data.push(col.all_values_as_dictionary());
                    } else {
                        col_data.push(col.all_values());
                    }
                }
                col_data
            }
        }
    }

    // Determines the set of row ids that satisfy the provided predicate.
    fn row_ids_from_predicate(&self, predicate: &Predicate) -> RowIDsOption {
        // TODO(edd): perf - potentially pool this so we can re-use it once rows
        // have been materialised and it's no longer needed. Initialise a bitmap
        // RowIDs because it's like that set operations will be necessary.
        let mut result_row_ids = RowIDs::new_bitmap();

        // TODO(edd): perf - pool the dst buffer so we can re-use it across
        // subsequent calls to `row_ids_from_predicates`. Right now this buffer
        // will be re-used across all columns in the `RowGroup` but not re-used
        // for subsequent calls _to_ the `RowGroup`.
        let mut dst = RowIDs::new_bitmap();

        let mut predicate = Cow::Borrowed(predicate);

        // If there is a time-range in the predicate (two time expressions),
        // then execute an optimised version that will use a range based
        // filter on the time column, effectively avoiding a scan and
        // intersection.
        if predicate.contains_time_range() {
            predicate = predicate.to_owned(); // We need to modify the predicate

            let time_range = predicate
                .to_mut()
                .remove_expr_by_column_name(TIME_COLUMN_NAME);

            // removes time expression from predicate
            let time_pred_row_ids = self.row_ids_from_time_range(&time_range, dst);
            match time_pred_row_ids {
                // No matching rows based on time range
                RowIDsOption::None(_) => return time_pred_row_ids,

                // all rows match - continue to apply other predicates
                RowIDsOption::All(_dst) => {
                    dst = _dst; // hand buffer back
                }

                // some rows match - continue to apply predicates
                RowIDsOption::Some(row_ids) => {
                    // fill the result row id set with the matching rows from
                    // the time column.
                    result_row_ids.union(&row_ids);
                    dst = row_ids // hand buffer back
                }
            }
        }

        for expr in predicate.iter() {
            // N.B column should always exist because validation of predicates
            // should happen at the `Table` level.
            let (_, col) = self.column_name_and_column(expr.column());

            // Explanation of how this buffer pattern works. The idea is that
            // the buffer should be returned to the caller so it can be re-used
            // on other columns. Each call to `row_ids_filter` returns the
            // buffer back enabling it to be re-used.
            match col.row_ids_filter(&expr.op, &expr.literal_as_value(), dst) {
                // No rows will be returned for the `RowGroup` because this
                // column does not match any rows.
                RowIDsOption::None(_dst) => return RowIDsOption::None(_dst),

                // Intersect the row ids found at this column with all those
                // found on other column predicates.
                RowIDsOption::Some(row_ids) => {
                    if result_row_ids.is_empty() {
                        result_row_ids.union(&row_ids)
                    } else {
                        result_row_ids.intersect(&row_ids);
                    }

                    // before evaluating the next expression check if we have
                    // ruled out all rows
                    if result_row_ids.is_empty() {
                        return RowIDsOption::None(result_row_ids);
                    }

                    dst = row_ids; // hand buffer back
                }

                // This is basically a no-op because all rows match the
                // predicate on this column.
                RowIDsOption::All(_dst) => {
                    dst = _dst; // hand buffer back
                }
            }
        }

        if result_row_ids.is_empty() {
            // All rows matched all expressions in the predicate.
            return RowIDsOption::All(result_row_ids);
        }
        RowIDsOption::Some(result_row_ids)
    }

    // An optimised function for applying two comparison predicates to a time
    // column at once.
    fn row_ids_from_time_range(&self, time_range: &[BinaryExpr], dst: RowIDs) -> RowIDsOption {
        assert_eq!(time_range.len(), 2);
        self.time_column().row_ids_filter_range(
            &(time_range[0].op, time_range[0].literal_as_value()), // min time
            &(time_range[1].op, time_range[1].literal_as_value()), // max time
            dst,
        )
    }

    /// Materialises a collection of data in group columns and aggregate
    /// columns, optionally filtered by the provided predicate.
    ///
    /// Collectively, row-wise values in the group columns comprise a "group
    /// key", and each value in the same row for the aggregate columns contains
    /// aggregate values for those group keys.
    ///
    /// Note: `read_aggregate` currently only supports "tag" columns.
    /// Note: `read_aggregate` does not order results.
    pub fn read_aggregate(
        &self,
        predicate: &Predicate,
        group_columns: &[ColumnName<'_>],
        aggregates: &[(ColumnName<'_>, AggregateType)],
    ) -> ReadAggregateResult<'_> {
        let schema = ResultSchema {
            select_columns: vec![],
            group_columns: self.meta.schema_for_column_names(group_columns),
            aggregate_columns: self.meta.schema_for_aggregate_column_names(aggregates),
        };

        let mut result = ReadAggregateResult {
            schema,
            ..ReadAggregateResult::default()
        };

        // Pure column aggregates - no grouping.
        if group_columns.is_empty() {
            self.aggregate_columns(predicate, &mut result);
            return result;
        }

        // All of the below assume grouping by columns.

        // Handle case where there are no predicates and all the columns being
        // grouped support constant-time expression of the row_ids belonging to
        // each grouped value.
        let all_group_cols_pre_computed = result.schema.group_column_names_iter().all(|name| {
            self.column_by_name(name)
                .properties()
                .has_pre_computed_row_ids
        });
        if predicate.is_empty() && all_group_cols_pre_computed {
            self.read_group_all_rows_all_rle(&mut result);
            return result;
        }

        // There are predicates. The next stage is apply them and determine the
        // intermediate set of row ids.
        let row_ids = self.row_ids_from_predicate(predicate);
        let filter_row_ids = match row_ids {
            RowIDsOption::None(_) => {
                return result;
            } // no matching rows
            RowIDsOption::Some(row_ids) => Some(row_ids.to_vec()),
            RowIDsOption::All(_) => None,
        };

        let agg_cols_num = result.schema.aggregate_columns.len();

        // materialise all *encoded* values for each column we are grouping on.
        // These will not be the logical (typically string) values, but will be
        // vectors of integers representing the physical values.
        let groupby_encoded_ids: Vec<_> = result
            .schema
            .group_column_names_iter()
            .map(|name| {
                let col = self.column_by_name(name);
                let mut encoded_values_buf =
                    EncodedValues::with_capacity_u32(col.num_rows() as usize);

                // Do we want some rows for the column (predicate filtered some
                // rows) or all of them (predicates filtered no rows).
                match &filter_row_ids {
                    Some(row_ids) => {
                        encoded_values_buf = col.encoded_values(row_ids, encoded_values_buf);
                    }
                    None => {
                        // None here means "no partial set of row ids" meaning
                        // get all of them.
                        encoded_values_buf = col.all_encoded_values(encoded_values_buf);
                    }
                }
                encoded_values_buf.take_u32()
            })
            .collect();

        // Materialise values in aggregate columns.
        let mut aggregate_columns_data = Vec::with_capacity(agg_cols_num);
        for (col_type, _, _) in &result.schema.aggregate_columns {
            let col = self.column_by_name(col_type.as_str());

            // TODO(edd): this materialises a column per aggregate. If there are
            // multiple aggregates for the same column then this will
            // over-allocate

            // Do we want some rows for the column or all of them?
            let column_values = match &filter_row_ids {
                Some(row_ids) => col.values(row_ids),
                None => {
                    // None here means "no partial set of row ids", i.e., get
                    // all of the row ids because they all satisfy the
                    // predicates.
                    col.all_values()
                }
            };
            aggregate_columns_data.push(column_values);
        }

        // Perform the group by using a hashmap
        self.read_group_with_hashing(&mut result, &groupby_encoded_ids, aggregate_columns_data);
        result
    }

    // read_group_hash executes a read-group-aggregate operation on the
    // `RowGroup` using a hashmap to build up a collection of group keys and
    // aggregates.
    //
    // read_group_hash accepts a set of conjunctive predicates.
    fn read_group_with_hashing<'a>(
        &'a self,
        dst: &mut ReadAggregateResult<'a>,
        groupby_encoded_ids: &[Vec<u32>],
        aggregate_columns_data: Vec<Values<'a>>,
    ) {
        // An optimised approach to building the hashmap of group keys using a
        // single 128-bit integer as the group key. If grouping is on more than
        // four columns then a fallback to using an vector as a key will happen.
        if dst.schema.group_columns.len() <= 4 {
            self.read_group_hash_with_u128_key(dst, groupby_encoded_ids, aggregate_columns_data);
            return;
        }

        self.read_group_hash_with_vec_key(dst, groupby_encoded_ids, aggregate_columns_data);
    }

    // This function is used with `read_group_hash` when the number of columns
    // being grouped on requires the use of a `Vec<u32>` as the group key in the
    // hash map.
    fn read_group_hash_with_vec_key<'a>(
        &'a self,
        dst: &mut ReadAggregateResult<'a>,
        groupby_encoded_ids: &[Vec<u32>],
        aggregate_input_columns: Vec<Values<'a>>,
    ) {
        let total_rows = groupby_encoded_ids[0].len();
        assert!(groupby_encoded_ids.iter().all(|x| x.len() == total_rows));

        // These vectors will hold the decoded values of each part of each
        // group key. They are the output columns of the input columns used for
        // the grouping operation.
        let mut group_cols_out = vec![vec![]; groupby_encoded_ids.len()];

        // Each of these vectors will be used to store each aggregate row-value
        // for a specific aggregate result column.
        let mut agg_cols_out = dst
            .schema
            .aggregate_columns
            .iter()
            .map(|(_, agg_type, data_type)| AggregateVec::from((agg_type, data_type)))
            .collect::<Vec<_>>();

        // Maps each group key to an ordinal offset on output columns. This
        // offset is used to update aggregate values for each group key and to
        // store the decoded representations of the group keys themselves in
        // the associated output columns.
        let mut group_keys: HashMap<Vec<u32>, usize> = HashMap::default();

        // reference back to underlying group columns for fetching decoded group
        // key values.
        let input_group_columns = dst
            .schema
            .group_column_names_iter()
            .map(|name| self.column_by_name(name))
            .collect::<Vec<_>>();

        // key_buf will be used as a temporary buffer for group keys represented
        // as a `Vec<u32>`.
        let mut key_buf = vec![0; dst.schema.group_columns.len()];
        let mut next_ordinal_id = 0; // assign a position for each group key in output columns.
        for row in 0..total_rows {
            // update the group key buffer with the group key for this row
            for (j, col_ids) in groupby_encoded_ids.iter().enumerate() {
                key_buf[j] = col_ids[row];
            }

            match group_keys.raw_entry_mut().from_key(&key_buf) {
                hash_map::RawEntryMut::Occupied(entry) => {
                    let ordinal_id = entry.get();

                    // Update each aggregate column at this ordinal offset
                    // with the values present in the input columns at the
                    // current row.
                    for (agg_col_i, aggregate_result) in agg_cols_out.iter_mut().enumerate() {
                        aggregate_result.update(
                            &aggregate_input_columns[agg_col_i],
                            row,
                            *ordinal_id,
                        )
                    }
                }
                // group key does not exist, so create it.
                hash_map::RawEntryMut::Vacant(entry) => {
                    // Update each aggregate column at this ordinal offset
                    // with the values present in the input columns at the
                    // current row.
                    for (agg_col_i, aggregate_result) in agg_cols_out.iter_mut().enumerate() {
                        aggregate_result.update(
                            &aggregate_input_columns[agg_col_i],
                            row,
                            next_ordinal_id,
                        )
                    }

                    // Add decoded group key values to the output group columns.
                    for (group_col_i, group_key_col) in group_cols_out.iter_mut().enumerate() {
                        if group_key_col.len() >= next_ordinal_id {
                            group_key_col.resize(next_ordinal_id + 1, None);
                        }
                        let decoded_value = input_group_columns[group_col_i]
                            .decode_id(groupby_encoded_ids[group_col_i][row]);
                        group_key_col[next_ordinal_id] = match decoded_value {
                            Value::Null => None,
                            Value::String(s) => Some(s),
                            _ => panic!("currently unsupported group column"),
                        };
                    }

                    // update the hashmap with the encoded group key and the
                    // associated ordinal offset.
                    entry.insert(key_buf.clone(), next_ordinal_id);
                    next_ordinal_id += 1;
                }
            }
        }

        dst.group_key_cols = group_cols_out;
        dst.aggregate_cols = agg_cols_out;
    }

    // This function is similar to `read_group_hash_with_vec_key` in that it
    // calculates groups keys and aggregates for a read-group-aggregate
    // operation using a hashmap.
    //
    // This function can be invoked when fewer than four columns are being
    // grouped. In this case the key to the hashmap can be a `u128` integer,
    // which is significantly more performant than using a `Vec<u32>`.
    fn read_group_hash_with_u128_key<'a>(
        &'a self,
        dst: &mut ReadAggregateResult<'a>,
        groupby_encoded_ids: &[Vec<u32>],
        aggregate_input_columns: Vec<Values<'a>>,
    ) {
        let total_rows = groupby_encoded_ids[0].len();
        assert!(groupby_encoded_ids.iter().all(|x| x.len() == total_rows));
        assert!(dst.schema.group_columns.len() <= 4);

        // These vectors will hold the decoded values of each part of each
        // group key. They are the output columns derived from the input
        // grouping columns.
        let mut group_cols_out: Vec<Vec<Option<ColumnName<'a>>>> = vec![];
        group_cols_out.resize(groupby_encoded_ids.len(), vec![]);

        // Each of these vectors will be used to store each aggregate row-value
        // for a specific aggregate result column.
        let mut agg_cols_out = dst
            .schema
            .aggregate_columns
            .iter()
            .map(|(_, agg_type, data_type)| AggregateVec::from((agg_type, data_type)))
            .collect::<Vec<_>>();

        // Maps each group key to an ordinal offset on output columns. This
        // offset is used to update aggregate values for each group key and to
        // store the decoded representations of the group keys themselves in
        // the associated output columns.
        let mut group_keys: HashMap<u128, usize> = HashMap::default();

        // reference back to underlying group columns for fetching decoded group
        // key values.
        let input_group_columns = dst
            .schema
            .group_column_names_iter()
            .map(|name| self.column_by_name(name))
            .collect::<Vec<_>>();

        let mut next_ordinal_id = 0; // assign a position for each group key in output columns.
        for row in 0..total_rows {
            // pack each column's encoded value for the row into a packed
            // group key.
            let mut group_key_packed = 0_u128;
            for (i, col_ids) in groupby_encoded_ids.iter().enumerate() {
                group_key_packed = pack_u32_in_u128(group_key_packed, col_ids[row], i);
            }

            match group_keys.raw_entry_mut().from_key(&group_key_packed) {
                hash_map::RawEntryMut::Occupied(entry) => {
                    let ordinal_id = entry.get();

                    // Update each aggregate column at this ordinal offset
                    // with the values present in the input columns at the
                    // current row.
                    for (agg_col_i, aggregate_result) in agg_cols_out.iter_mut().enumerate() {
                        aggregate_result.update(
                            &aggregate_input_columns[agg_col_i],
                            row,
                            *ordinal_id,
                        )
                    }
                }
                hash_map::RawEntryMut::Vacant(entry) => {
                    // Update each aggregate column at this ordinal offset
                    // with the values present in the input columns at the
                    // current row.
                    for (agg_col_i, aggregate_result) in agg_cols_out.iter_mut().enumerate() {
                        aggregate_result.update(
                            &aggregate_input_columns[agg_col_i],
                            row,
                            next_ordinal_id,
                        )
                    }

                    // Add decoded group key values to the output group columns.
                    for (group_col_i, group_key_col) in group_cols_out.iter_mut().enumerate() {
                        if group_key_col.len() >= next_ordinal_id {
                            group_key_col.resize(next_ordinal_id + 1, None);
                        }
                        let decoded_value = input_group_columns[group_col_i]
                            .decode_id(groupby_encoded_ids[group_col_i][row]);
                        group_key_col[next_ordinal_id] = match decoded_value {
                            Value::Null => None,
                            Value::String(s) => Some(s),
                            _ => panic!("currently unsupported group column"),
                        };
                    }

                    // update the hashmap with the encoded group key and the
                    // associated ordinal offset.
                    entry.insert(group_key_packed, next_ordinal_id);
                    next_ordinal_id += 1;
                }
            }
        }

        dst.group_key_cols = group_cols_out;
        dst.aggregate_cols = agg_cols_out;
    }

    // Optimised `read_group` method when there are no predicates and all the
    // group columns are RLE-encoded.
    //
    // In this case all the grouping columns pre-computed bitsets for each
    // distinct value.
    fn read_group_all_rows_all_rle<'a>(&'a self, dst: &mut ReadAggregateResult<'a>) {
        // References to the columns to be used as input for producing the
        // output aggregates.
        let input_group_columns = dst
            .schema
            .group_column_names_iter()
            .map(|name| self.column_by_name(name))
            .collect::<Vec<_>>();

        // References to the columns to be used as input for producing the
        // output aggregates. Also returns the required aggregate type.
        let input_aggregate_columns = dst
            .schema
            .aggregate_columns
            .iter()
            .map(|(col_type, agg_type, _)| (self.column_by_name(col_type.as_str()), *agg_type))
            .collect::<Vec<_>>();

        let groupby_encoded_ids = dst
            .schema
            .group_column_names_iter()
            .map(|col_type| {
                self.column_by_name(col_type.as_str())
                    .grouped_row_ids()
                    .unwrap_left()
            })
            .collect::<Vec<_>>();

        // These vectors will hold the decoded values of each part of each
        // group key. They are the output columns derived from the input
        // grouping columns.
        let mut group_cols_out: Vec<Vec<Option<ColumnName<'a>>>> = vec![];
        group_cols_out.resize(groupby_encoded_ids.len(), vec![]);

        // Each of these vectors will be used to store each aggregate row-value
        // for a specific aggregate result column.
        let mut agg_cols_out = dst
            .schema
            .aggregate_columns
            .iter()
            .map(|(_, agg_type, data_type)| AggregateVec::from((agg_type, data_type)))
            .collect::<Vec<_>>();

        let mut output_rows = 0;

        // multi_cartesian_product will create the cartesian product of all
        // grouping-column values. This is likely going to be more group keys
        // than there exists row-data for, so don't materialise them yet...
        //
        // For example, we have two columns like:
        //
        //    [0, 1, 1, 2, 2, 3, 4] // column encodes the values as integers
        //    [3, 3, 3, 3, 4, 2, 1] // column encodes the values as integers
        //
        // The columns have these distinct values:
        //
        //    [0, 1, 2, 3, 4] [1, 2, 3, 4]
        //
        // We will produce the following "group key" candidates:
        //
        //    [0, 1], [0, 2], [0, 3], [0, 4] [1, 1], [1, 2], [1, 3], [1, 4]
        //    [2, 1], [2, 2], [2, 3], [2, 4] [3, 1], [3, 2], [3, 3], [3, 4]
        //    [4, 1], [4, 2], [4, 3], [4, 4]
        //
        // Based on the columns we can see that we only have data for the
        // following group keys:
        //
        //    [0, 3], [1, 3], [2, 3], [2, 4], [3, 2], [4, 1]
        //
        // We figure out which group keys have data and which don't in the loop
        // below, by intersecting row_id bitsets for each encoded id, and
        // checking for non-empty sets.
        let candidate_group_keys = groupby_encoded_ids
            .iter()
            .map(|ids| (0..ids.len()))
            .multi_cartesian_product();

        // Let's figure out which of the candidate group keys are actually group
        // keys with data.
        'outer: for group_key_buf in candidate_group_keys {
            let mut group_key_row_ids =
                Cow::Borrowed(groupby_encoded_ids[0][group_key_buf[0]].unwrap_bitmap());

            if group_key_row_ids.is_empty() {
                continue;
            }

            for i in 1..group_key_buf.len() {
                let other = groupby_encoded_ids[i][group_key_buf[i]].unwrap_bitmap();

                if group_key_row_ids.and_cardinality(other) > 0 {
                    group_key_row_ids = Cow::Owned(group_key_row_ids.and(other));
                } else {
                    continue 'outer;
                }
            }

            // There exist rows for this group key combination. Materialise the
            // group key and calculate the aggregates for this key using set
            // of row IDs.
            output_rows += 1;

            // Add decoded group key values to the output group columns.
            for (group_col_i, col) in group_cols_out.iter_mut().enumerate() {
                let decoded_value =
                    input_group_columns[group_col_i].decode_id(group_key_buf[group_col_i] as u32);

                col.push(match decoded_value {
                    Value::Null => None,
                    Value::String(s) => Some(s),
                    _ => panic!("currently unsupported group column"),
                });
            }

            // Calculate an aggregate from each input aggregate column and
            // set it at the relevant offset in the output column.
            for (agg_col_i, (agg_col, typ)) in input_aggregate_columns.iter().enumerate() {
                match typ {
                    AggregateType::Count => {
                        let agg = agg_col.count(&group_key_row_ids.to_vec()) as u64;
                        agg_cols_out[agg_col_i].push(Value::Scalar(Scalar::U64(agg)))
                    }
                    AggregateType::First => {}
                    AggregateType::Last => {}
                    AggregateType::Min => {
                        let agg = agg_col.min(&group_key_row_ids.to_vec());
                        agg_cols_out[agg_col_i].push(agg);
                    }
                    AggregateType::Max => {
                        let agg = agg_col.max(&group_key_row_ids.to_vec());
                        agg_cols_out[agg_col_i].push(agg);
                    }
                    AggregateType::Sum => {
                        let agg = agg_col.sum(&group_key_row_ids.to_vec());
                        agg_cols_out[agg_col_i].push(Value::Scalar(agg));
                    }
                }
            }
        }

        for col in &group_cols_out {
            assert_eq!(col.len(), output_rows);
        }
        for col in &agg_cols_out {
            assert_eq!(col.len(), output_rows);
        }

        dst.group_key_cols = group_cols_out;
        dst.aggregate_cols = agg_cols_out;
    }

    // Optimised `read_group` method for cases where the columns being grouped
    // are already totally ordered in the `RowGroup`.
    //
    // In this case the rows are already in "group key order" and the aggregates
    // can be calculated by reading the rows in order.
    fn _read_group_sorted_stream(
        &self,
        _predicates: &Predicate,
        _group_column: ColumnName<'_>,
        _aggregates: &[(ColumnName<'_>, AggregateType)],
    ) {
        todo!()
    }

    // Applies aggregates on multiple columns with an optional predicate.
    fn aggregate_columns<'a>(&'a self, predicate: &Predicate, dst: &mut ReadAggregateResult<'a>) {
        let row_ids = match predicate.is_empty() {
            true => {
                // TODO(edd): PERF - teach each column encoding how to produce
                // an aggregate for all its rows without needed
                // to see the entire set of row ids. Currently
                // column encodings aggregate based on the slice
                // of row ids they see.
                (0..self.rows()).into_iter().collect::<Vec<u32>>()
            }
            false => match self.row_ids_from_predicate(predicate) {
                RowIDsOption::Some(row_ids) => row_ids.to_vec(),
                RowIDsOption::None(_) => vec![],
                RowIDsOption::All(_) => {
                    // see above comment.
                    (0..self.rows()).into_iter().collect::<Vec<u32>>()
                }
            },
        };

        dst.aggregate_cols = dst
            .schema
            .aggregate_columns
            .iter()
            .map(|(col_type, agg_type, data_type)| {
                let col = self.column_by_name(col_type.as_str()); // input aggregate column
                let mut agg_vec = AggregateVec::from((agg_type, data_type));

                // produce single aggregate for the input column subject to a
                // predicate filter.
                match agg_type {
                    AggregateType::Count => {
                        let value = Value::Scalar(Scalar::U64(col.count(&row_ids) as u64));
                        agg_vec.push(value);
                    }
                    AggregateType::First => unimplemented!("First not yet implemented"),
                    AggregateType::Last => unimplemented!("Last not yet implemented"),
                    AggregateType::Min => agg_vec.push(col.min(&row_ids)),
                    AggregateType::Max => agg_vec.push(col.max(&row_ids)),
                    AggregateType::Sum => agg_vec.push(Value::Scalar(col.sum(&row_ids))),
                }
                agg_vec
            })
            .collect::<Vec<_>>();
    }

    /// Given the predicate (which may be empty), determine a set of rows
    /// contained in this row group that satisfy it. Any column that contains a
    /// non-null value at any of these row positions is then included in the
    /// results, which are added to `dst`.
    ///
    /// As an optimisation, the contents of `dst` are checked before execution
    /// and any columns already existing in the set are not interrogated.
    ///
    /// If you are familiar with InfluxDB, this is essentially an implementation
    /// of `SHOW TAG KEYS`.
    pub fn column_names(
        &self,
        predicate: &Predicate,
        columns: Selection<'_>,
        dst: &mut BTreeSet<String>,
    ) {
        // Determine the set of columns in this row group that are not already
        // present in `dst`, i.e., they haven't been identified in other row
        // groups already.
        let candidate_columns = self
            .all_columns_by_name
            .iter()
            .filter_map(|(name, &id)| match dst.contains(name) {
                // N.B there is bool::then() but it's currently unstable.
                true => None,
                false => match columns {
                    Selection::All => Some((name, &self.columns[id])),
                    Selection::Some(names) => {
                        if names.iter().any(|selection| name == selection) {
                            Some((name, &self.columns[id]))
                        } else {
                            None
                        }
                    }
                },
            })
            .collect::<Vec<_>>();

        match self.row_ids_from_predicate(predicate) {
            RowIDsOption::None(_) => {} // nothing matches predicate
            RowIDsOption::Some(row_ids) => {
                let row_ids = row_ids.to_vec();

                for (name, column) in candidate_columns {
                    if column.has_non_null_value(&row_ids) {
                        dst.insert(name.to_owned());
                    }
                }
            }
            RowIDsOption::All(_) => {
                for (name, column) in candidate_columns {
                    if column.has_any_non_null_value() {
                        dst.insert(name.to_owned());
                    }
                }
            }
        }
    }

    /// Returns the distinct set of values for the selected columns, constrained
    /// by an optional predicate.
    pub fn column_values<'a>(
        &'a self,
        predicate: &Predicate,
        columns: &[ColumnName<'_>],
        mut dst: BTreeMap<String, BTreeSet<String>>,
    ) -> BTreeMap<String, BTreeSet<String>> {
        // Build up candidate columns
        let candidate_columns = self
            .all_columns_by_name
            .iter()
            // Filter any columns that are not present in the `Selection`.
            .filter_map(|(name, &id)| {
                if columns.iter().any(|selection| name == selection) {
                    Some((name, &self.columns[id]))
                } else {
                    None
                }
            })
            // Further filter candidate columns by removing any columns that we
            // can prove we already have all the distinct values for.
            .filter(|(name, column)| {
                match dst.get(*name) {
                    // process the column if we haven't got all the distinct
                    // values.
                    Some(values) => column.has_other_non_null_string_values(values),
                    // no existing values for this column - we will need to
                    // process it.
                    None => true,
                }
            })
            .collect::<Vec<_>>();

        let row_ids = self.row_ids_from_predicate(predicate);
        for (name, column) in candidate_columns {
            // If no rows match there is nothing to do, if some rows match then
            // extract an iterator of those IDs. If all rows match then create
            // an iterator of all rows without materialising them.
            let row_itr: Box<dyn Iterator<Item = u32>> = match &row_ids {
                RowIDsOption::None(_) => return dst,
                RowIDsOption::Some(row_ids) => Box::new(row_ids.iter()),
                RowIDsOption::All(_) => Box::new(0..self.rows()),
            };

            let results = dst.entry(name.clone()).or_default();
            for v in column.distinct_values(row_itr).into_iter().flatten() {
                if !results.contains(v) {
                    results.insert(v.to_owned());
                }
            }
        }

        dst
    }

    pub(crate) fn column_storage_statistics(&self) -> Vec<column::Statistics> {
        self.columns.iter().map(|c| c.storage_stats()).collect()
    }
}

impl std::fmt::Display for &RowGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.metadata().fmt(f)?;

        writeln!(f, "[DATA]")?;

        for (name, idx) in &self.all_columns_by_name {
            writeln!(
                f,
                "'{}' (pos {}): size: {} {}",
                name,
                idx,
                self.size(),
                self.columns[*idx]
            )?;
        }

        Ok(())
    }
}

/// Initialise a `RowGroup` from an Arrow RecordBatch.
///
/// Presently this requires the RecordBatch to contain meta-data that specifies
/// the semantic meaning of each column in terms of an Influx time-series
/// use-case, i.e., whether the column is a tag column, field column or a time
/// column.
impl From<RecordBatch> for RowGroup {
    fn from(rb: RecordBatch) -> Self {
        let rows = rb.num_rows();
        // TODO proper error handling here if the input schema is bad
        let schema: Schema = rb
            .schema()
            .try_into()
            .expect("Valid time-series schema when creating row group");

        let mut columns = vec![];
        for (i, arrow_column) in rb.columns().iter().enumerate() {
            let (lp_type, field) = schema.field(i);
            let col_name = field.name();

            match lp_type {
                Some(InfluxColumnType::Tag) => {
                    let column_data = match arrow_column.data_type() {
                        DataType::Utf8 => Column::from(arrow::array::StringArray::from(
                            arrow_column.data().clone(),
                        )),
                        DataType::Dictionary(key, value)
                            if key.as_ref() == &DataType::Int32
                                && value.as_ref() == &DataType::Utf8 =>
                        {
                            Column::from(
                                arrow::array::DictionaryArray::<arrow::datatypes::Int32Type>::from(
                                    arrow_column.data().clone(),
                                ),
                            )
                        }
                        _ => panic!("invalid tag column type"),
                    };

                    columns.push((col_name.to_owned(), ColumnType::Tag(column_data)));
                }
                Some(InfluxColumnType::Field(_)) => {
                    let column_data = match arrow_column.data_type() {
                        DataType::Int64 => Column::from(arrow::array::Int64Array::from(
                            arrow_column.data().clone(),
                        )),
                        DataType::Float64 => Column::from(arrow::array::Float64Array::from(
                            arrow_column.data().clone(),
                        )),
                        DataType::UInt64 => Column::from(arrow::array::UInt64Array::from(
                            arrow_column.data().clone(),
                        )),
                        DataType::Boolean => Column::from(arrow::array::BooleanArray::from(
                            arrow_column.data().clone(),
                        )),
                        DataType::Utf8 => Column::from(arrow::array::StringArray::from(
                            arrow_column.data().clone(),
                        )),
                        dt => unimplemented!(
                            "data type {:?} currently not supported for field columns",
                            dt
                        ),
                    };

                    columns.push((col_name.to_owned(), ColumnType::Field(column_data)));
                }
                Some(InfluxColumnType::Timestamp) => {
                    assert_eq!(col_name, TIME_COLUMN_NAME);

                    let column_data =
                        Column::from(arrow::array::Int64Array::from(arrow_column.data().clone()));

                    columns.push((col_name.to_owned(), ColumnType::Time(column_data)));
                }
                _ => panic!("unknown column type"),
            }
        }

        Self::new(rows as u32, columns)
    }
}

// Packs an encoded values into a `u128` at `pos`, which must be `[0,4)`.
#[inline(always)]
fn pack_u32_in_u128(packed_value: u128, encoded_id: u32, pos: usize) -> u128 {
    packed_value | (encoded_id as u128) << (32 * pos)
}

// Given a packed encoded group key, unpacks them into `n` individual `u32`
// group keys, and stores them in `dst`. It is the caller's responsibility to
// ensure n <= 4.
#[cfg(test)]
fn unpack_u128_group_key(group_key_packed: u128, n: usize, mut dst: Vec<u32>) -> Vec<u32> {
    dst.resize(n, 0);

    for (i, encoded_id) in dst.iter_mut().enumerate() {
        *encoded_id = (group_key_packed >> (i * 32)) as u32;
    }

    dst
}

#[derive(Clone, Default, Debug, PartialEq)]
pub struct Predicate(Vec<BinaryExpr>);

impl Predicate {
    pub fn new(expr: Vec<BinaryExpr>) -> Self {
        Self(expr)
    }

    /// Constructs a `Predicate` based on the provided collection of expressions
    /// and explicit time bounds.
    ///
    /// The `from` and `to` values will be converted into appropriate
    /// expressions, which result in the `Predicate` expressing the following:
    ///
    /// time >= from AND time < to
    pub fn with_time_range(exprs: &[BinaryExpr], from: i64, to: i64) -> Self {
        let mut time_exprs = vec![
            BinaryExpr::from((TIME_COLUMN_NAME, ">=", from)),
            BinaryExpr::from((TIME_COLUMN_NAME, "<", to)),
        ];

        time_exprs.extend_from_slice(exprs);
        Self(time_exprs)
    }

    /// A `Predicate` is empty if it has no expressions.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, BinaryExpr> {
        self.0.iter()
    }

    /// Returns a vector of all expressions on the predicate.
    pub fn expressions(&self) -> &[BinaryExpr] {
        &self.0
    }

    // Removes all expressions for specified column from the predicate and
    // returns them.
    //
    // The current use-case for this to separate processing the time column on
    // its own using an optimised filtering function (because the time column is
    // very likely to have two expressions in the predicate).
    fn remove_expr_by_column_name(&mut self, name: ColumnName<'_>) -> Vec<BinaryExpr> {
        let mut exprs = vec![];
        while let Some(i) = self.0.iter().position(|expr| expr.col == name) {
            exprs.push(self.0.remove(i));
        }

        exprs
    }

    // Returns true if the Predicate contains two time expressions.
    fn contains_time_range(&self) -> bool {
        self.0
            .iter()
            .filter(|expr| expr.col == TIME_COLUMN_NAME)
            .count()
            == 2
    }
}

/// Supported literal values for expressions. These map to a sub-set of logical
/// datatypes supported by the `ReadBuffer`.
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    String(String),
    Integer(i64),
    Unsigned(u64),
    Float(f64),
    Boolean(bool),
}

impl<'a> TryFrom<&DFScalarValue> for Literal {
    type Error = String;

    fn try_from(value: &DFScalarValue) -> Result<Self, Self::Error> {
        match value {
            DFScalarValue::Boolean(v) => match v {
                Some(v) => Ok(Self::Boolean(*v)),
                None => Err("NULL literal not supported".to_owned()),
            },
            DFScalarValue::Float64(v) => match v {
                Some(v) => Ok(Self::Float(*v)),
                None => Err("NULL literal not supported".to_owned()),
            },
            DFScalarValue::Int64(v) => match v {
                Some(v) => Ok(Self::Integer(*v)),
                None => Err("NULL literal not supported".to_owned()),
            },
            DFScalarValue::UInt64(v) => match v {
                Some(v) => Ok(Self::Unsigned(*v)),
                None => Err("NULL literal not supported".to_owned()),
            },
            DFScalarValue::Utf8(v) => match v {
                Some(v) => Ok(Self::String(v.clone())),
                None => Err("NULL literal not supported".to_owned()),
            },
            DFScalarValue::TimestampNanosecond(v) => match v {
                Some(v) => Ok(Self::Integer(*v)),
                None => Err("NULL literal not supported".to_owned()),
            },
            _ => Err("scalar type not supported".to_owned()),
        }
    }
}

/// An expression that contains a column name on the left side, an operator, and
/// a literal value on the right side.
#[derive(Clone, Debug, PartialEq)]
pub struct BinaryExpr {
    col: String,
    op: Operator,
    value: Literal,
}

impl BinaryExpr {
    pub fn new(column_name: impl Into<String>, op: Operator, value: Literal) -> Self {
        Self {
            col: column_name.into(),
            op,
            value,
        }
    }

    pub fn column(&self) -> ColumnName<'_> {
        self.col.as_str()
    }

    pub fn op(&self) -> Operator {
        self.op
    }

    pub fn literal(&self) -> &Literal {
        &self.value
    }

    fn literal_as_value(&self) -> Value<'_> {
        match self.literal() {
            Literal::String(v) => Value::String(v),
            Literal::Integer(v) => Value::Scalar(Scalar::I64(*v)),
            Literal::Unsigned(v) => Value::Scalar(Scalar::U64(*v)),
            Literal::Float(v) => Value::Scalar(Scalar::F64(*v)),
            Literal::Boolean(v) => Value::Boolean(*v),
        }
    }
}

impl From<(&str, &str, &str)> for BinaryExpr {
    fn from(expr: (&str, &str, &str)) -> Self {
        Self::new(
            expr.0,
            Operator::try_from(expr.1).unwrap(),
            Literal::String(expr.2.to_owned()),
        )
    }
}

// These From implementations are useful for expressing expressions easily in
// tests by allowing for example:
//
//    BinaryExpr::from("region", ">=", "east")
//    BinaryExpr::from("counter", "=", 321.3)
macro_rules! binary_expr_from_impls {
    ($(($type:ident, $variant:ident),)*) => {
        $(
            impl From<(&str, &str, $type)> for BinaryExpr {
                fn from(expr: (&str, &str, $type)) -> Self {
                    Self::new(
                        expr.0,
                        Operator::try_from(expr.1).unwrap(),
                        Literal::$variant(expr.2.to_owned()),
                    )
                }
            }
        )*
    };
}

binary_expr_from_impls! {
    (String, String),
    (i64, Integer),
    (f64, Float),
    (u64, Unsigned),
    (bool, Boolean),
}

impl TryFrom<&DfExpr> for BinaryExpr {
    type Error = String;

    fn try_from(df_expr: &DfExpr) -> Result<Self, Self::Error> {
        match df_expr {
            DfExpr::BinaryExpr { left, op, right } => {
                match (&**left, &**right) {
                    (DfExpr::Column(c), DfExpr::Literal(scalar)) => Ok(Self::new(
                        &c.name,
                        Operator::try_from(op)?,
                        Literal::try_from(scalar)?,
                    )),
                    (DfExpr::Literal(_), DfExpr::Column(_)) => {
                        // In this case we may have been give (literal, op, column).
                        // Swap left and right around and retry.
                        Self::try_from(&DfExpr::BinaryExpr {
                            left: right.clone(),
                            // since relation has been swapped we need the
                            // converse operator, e.g., `a < b` becomes `b > a`.
                            op: match op {
                                DFOperator::Eq => DFOperator::Eq,
                                DFOperator::NotEq => DFOperator::NotEq,
                                DFOperator::Lt => DFOperator::Gt,
                                DFOperator::LtEq => DFOperator::GtEq,
                                DFOperator::Gt => DFOperator::Lt,
                                DFOperator::GtEq => DFOperator::LtEq,
                                op => return Err(format!("unsupported DF operator {:?}", op)),
                            },
                            right: left.clone(),
                        })
                    }
                    (_, _) => {
                        return Err(format!(
                            "unsupported expression {:?} {:?} {:?}",
                            *left, op, *right
                        ))
                    }
                }
            }
            _ => return Err(format!("unsupported expression type {:?}", df_expr)),
        }
    }
}

// A representation of a column name.
pub type ColumnName<'a> = &'a str;

/// The InfluxDB-specific semantic meaning of a column.
pub enum ColumnType {
    Tag(Column),
    Field(Column),
    Time(Column),
}

impl ColumnType {
    /// The total size in bytes of the column
    pub fn size(&self) -> usize {
        match &self {
            Self::Tag(c) => c.size(),
            Self::Field(c) => c.size(),
            Self::Time(c) => c.size(),
        }
    }

    /// The number of distinct values if known
    pub fn distinct_count(&self) -> Option<NonZeroU64> {
        match &self {
            Self::Tag(c) => c.cardinality(),
            Self::Field(c) => c.cardinality(),
            Self::Time(c) => c.cardinality(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub typ: crate::schema::ColumnType,
    pub logical_data_type: LogicalDataType,
    pub range: (OwnedValue, OwnedValue),
    /// How many values in this column are nulls
    pub null_count: u32,
    pub distinct_count: Option<NonZeroU64>,
}

impl ColumnMeta {
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.range.0.size() + self.range.1.size()
    }
}

impl Display for &ColumnMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Can't rely on ColumnType Display impl.
        let semantic_type = match self.typ {
            schema::ColumnType::Tag(_) => "TAG",
            schema::ColumnType::Field(_) => "FIELD",
            schema::ColumnType::Timestamp(_) => "TIMESTAMP",
            schema::ColumnType::Other(_) => "IOX",
        };

        write!(
            f,
            "sem_type: {}, log_type: {}, range: ({}, {})",
            semantic_type, self.logical_data_type, &self.range.0, &self.range.1
        )
    }
}
// column metadata is equivalent for two columns if their logical type and
// semantic type are equivalent.
impl PartialEq for ColumnMeta {
    fn eq(&self, other: &Self) -> bool {
        self.typ == other.typ && self.logical_data_type == other.logical_data_type
    }
}

#[derive(Default, Debug)]
pub struct MetaData {
    // The total size in bytes of all column data in the `RowGroup`.
    pub columns_size: usize,

    // The total number of rows in the `RowGroup`.
    pub rows: u32,

    // The distinct set of columns for this `RowGroup` (all of these columns
    // will appear in all of the `Table`'s `RowGroup`s) and the range of values
    // for each of those columns.
    //
    // This can be used to skip the table entirely if a logical predicate can't
    // possibly match based on the range of values a column has.
    pub columns: BTreeMap<String, ColumnMeta>,

    pub column_names: Vec<String>,
}

impl std::fmt::Display for &MetaData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "[META] rows: {}, columns: {}",
            self.rows,
            self.columns.len()
        )?;

        for (name, meta) in &self.columns {
            writeln!(f, "'{}': {}", name, meta)?;
        }
        Ok(())
    }
}

impl MetaData {
    /// Returns the estimated size in bytes of the meta data and all column data
    /// associated with a `RowGroup`.
    fn size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();

        (base_size
            // account for contents of meta data
            + self
                .columns
                .iter()
                .map(|(k, v)| k.len() + v.size())
                .sum::<usize>())
            + self.columns_size
    }

    // helper function to determine if the provided binary expression could be
    // satisfied in the `RowGroup`, If this function returns `false` then there
    // no rows in the `RowGroup` would ever match the expression.
    //
    pub fn column_could_satisfy_binary_expr(&self, expr: &BinaryExpr) -> bool {
        let (column_min, column_max) = match self.columns.get(expr.column()) {
            Some(schema) => &schema.range,
            None => return false, // column doesn't exist.
        };

        let (op, value) = (expr.op(), &expr.literal_as_value());
        match op {
            // If the column range covers the value then it could contain that
            // value.
            Operator::Equal => column_min <= value && column_max >= value,

            // If every value in the column is equal to "value" then this will
            // be false, otherwise it must be satisfied
            Operator::NotEqual => (column_min != column_max) || column_max != value,

            // if the column max is larger than value then the column could
            // contain the value.
            Operator::GT => column_max > value,

            // if the column max is at least as large as `value` then the column
            // could contain the value.
            Operator::GTE => column_max >= value,

            // if the column min is smaller than value then the column could
            // contain the value.
            Operator::LT => column_min < value,

            // if the column min is at least as small as value then the column
            // could contain the value.
            Operator::LTE => column_min <= value,
        }
    }

    pub fn add_column(
        &mut self,
        name: &str,
        column_size: usize,
        col_type: schema::ColumnType,
        logical_data_type: LogicalDataType,
        range: (OwnedValue, OwnedValue),
        null_count: u32,
        distinct_count: Option<NonZeroU64>,
    ) {
        self.column_names.push(name.to_owned());
        self.columns.insert(
            name.to_owned(),
            ColumnMeta {
                typ: col_type,
                logical_data_type,
                range,
                null_count,
                distinct_count,
            },
        );
        self.columns_size += column_size;
    }

    // Extract schema information for a set of columns.
    fn schema_for_column_names(
        &self,
        names: &[ColumnName<'_>],
    ) -> Vec<(crate::schema::ColumnType, LogicalDataType)> {
        names
            .iter()
            .map(|&name| {
                let schema = self.columns.get(name).unwrap();
                (schema.typ.clone(), schema.logical_data_type)
            })
            .collect::<Vec<_>>()
    }

    // Extract the schema information for a set of aggregate columns
    fn schema_for_aggregate_column_names(
        &self,
        columns: &[(ColumnName<'_>, AggregateType)],
    ) -> Vec<(crate::schema::ColumnType, AggregateType, LogicalDataType)> {
        columns
            .iter()
            .map(|(name, agg_type)| {
                let schema = self.columns.get(*name).unwrap();

                // TODO(edd): this check happens because an aggregate does
                // not have to have the same physical type as the logical
                // type of the column it is aggregating on. An example of
                // this is Count. I'm going to fix this by associated data
                // types with the aggregate itself.
                let physical_data_type = if let AggregateType::Count = agg_type {
                    LogicalDataType::Unsigned
                } else {
                    schema.logical_data_type
                };

                (schema.typ.clone(), *agg_type, physical_data_type)
            })
            .collect::<Vec<_>>()
    }
}

/// Encapsulates results from `RowGroup`s with a structure that makes them
/// easier to work with and display.
pub struct ReadFilterResult<'row_group> {
    schema: ResultSchema,
    data: Vec<Values<'row_group>>,
}

impl ReadFilterResult<'_> {
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn schema(&self) -> &ResultSchema {
        &self.schema
    }
}

impl TryFrom<ReadFilterResult<'_>> for RecordBatch {
    type Error = Error;

    fn try_from(result: ReadFilterResult<'_>) -> Result<Self, Self::Error> {
        let schema = internal_types::schema::Schema::try_from(result.schema())
            .map_err(|source| Error::SchemaConversion { source })?;

        let columns: Vec<ArrayRef> = result
            .data
            .into_iter()
            .enumerate()
            .map(|(i, values)| {
                // Note: here we are special-casing columns that have been
                // specified as being represented by `TimestampNanosecondArray`
                // according to the Arrow schema. Currently this is done so that
                // when they're fed into a data-fusion query engine, it will
                // emit a friendlier representation of them.
                if let DataType::Timestamp(TimeUnit::Nanosecond, timestamp) =
                    schema.field(i).1.data_type()
                {
                    return match values {
                        Values::I64(arr) => {
                            Ok(Arc::new(arrow::array::TimestampNanosecondArray::from_vec(
                                arr,
                                timestamp.clone(),
                            )) as arrow::array::ArrayRef)
                        }
                        Values::I64N(arr) => Ok(Arc::new(
                            arrow::array::TimestampNanosecondArray::from_opt_vec(
                                arr,
                                timestamp.clone(),
                            ),
                        )
                            as arrow::array::ArrayRef),
                        t => UnsupportedOperation {
                            msg: format!("cannot convert {:?} to TimestampNanosecondArray", t),
                        }
                        .fail(),
                    };
                }

                Ok(arrow::array::ArrayRef::from(values))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let arrow_schema: arrow::datatypes::SchemaRef = schema.into();

        // try_new only returns an error if the schema is invalid or the number
        // of rows on columns differ. We have full control over both so there
        // should never be an error to return...
        Self::try_new(arrow_schema, columns).map_err(|source| Error::ArrowConversion { source })
    }
}

impl std::fmt::Debug for &ReadFilterResult<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Display the header
        Display::fmt(self.schema(), f)?;
        writeln!(f)?;

        // Display the rest of the values.
        Display::fmt(&self, f)
    }
}

impl Display for &ReadFilterResult<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            return Ok(());
        }

        let expected_rows = self.data[0].len();
        let mut rows = 0;

        let mut iter_map = self
            .data
            .iter()
            .map(|v| ValuesIterator::new(v))
            .collect::<Vec<_>>();

        let columns = iter_map.len();
        while rows < expected_rows {
            if rows > 0 {
                writeln!(f)?;
            }

            for (i, data) in iter_map.iter_mut().enumerate() {
                write!(f, "{}", data.next().unwrap())?;
                if i < columns - 1 {
                    write!(f, ",")?;
                }
            }

            rows += 1;
        }
        writeln!(f)
    }
}

#[derive(Default, Clone)]
pub struct ReadAggregateResult<'row_group> {
    // a schema describing the columns in the results and their types.
    pub(crate) schema: ResultSchema,

    // The collection of columns forming the group keys.
    pub(crate) group_key_cols: Vec<Vec<Option<&'row_group str>>>,

    // The collection of aggregate columns. Each value in each column is an
    // aggregate associated with the group key built from values in the group
    // columns and the same ordinal position.
    pub(crate) aggregate_cols: Vec<AggregateVec>,

    pub(crate) group_keys_sorted: bool,
}

impl<'row_group> ReadAggregateResult<'row_group> {
    pub fn new(schema: ResultSchema) -> Self {
        Self {
            schema,
            ..Default::default()
        }
    }

    /// A `ReadAggregateResult` is empty if there are no aggregate columns.
    pub fn is_empty(&self) -> bool {
        self.aggregate_cols.is_empty()
    }

    pub fn schema(&self) -> &ResultSchema {
        &self.schema
    }

    /// The number of rows in the result.
    pub fn rows(&self) -> usize {
        if self.aggregate_cols.is_empty() {
            return 0;
        }
        self.aggregate_cols[0].len()
    }

    // The number of distinct group keys in the result. Not the same as `rows()`
    // because a `ReadAggregateResult` can have no group keys and have a single
    // aggregate row.
    pub fn cardinality(&self) -> usize {
        if self.group_key_cols.is_empty() {
            return 0;
        }
        self.group_key_cols[0].len()
    }

    // Is this result for a grouped aggregate?
    pub fn is_grouped_aggregate(&self) -> bool {
        !self.group_key_cols.is_empty()
    }

    // The number of grouping columns.
    pub fn group_key_columns(&self) -> usize {
        self.group_key_cols.len()
    }

    // Whether or not the rows in the results are sorted by group keys or not.
    pub fn group_keys_sorted(&self) -> bool {
        self.group_key_cols.is_empty() || self.group_keys_sorted
    }

    /// Merges `other` and self, returning a new set of results.
    pub fn merge(
        mut self,
        mut other: ReadAggregateResult<'row_group>,
    ) -> ReadAggregateResult<'row_group> {
        assert_eq!(self.schema(), other.schema());

        if self.is_empty() {
            return other;
        } else if other.is_empty() {
            return self;
        }

        // `read_aggregate` uses a variety of ways to generate results. It is
        // not safe to assume any particular ordering, so we will sort self and
        // other and do a merge.
        if !self.group_keys_sorted() {
            self.sort();
        }

        if !other.group_keys_sorted() {
            other.sort();
        }

        let mut result = Self::new(self.schema.clone());
        // Allocate output grouping columns
        result
            .group_key_cols
            .resize(result.schema.group_columns.len(), vec![]);

        // Allocate output aggregate columns
        result.aggregate_cols = result
            .schema
            .aggregate_columns
            .iter()
            .map(|(_, agg_type, data_type)| AggregateVec::from((agg_type, data_type)))
            .collect::<Vec<_>>();

        let mut self_i = 0;
        let mut other_i = 0;
        while self_i < self.rows() || other_i < other.rows() {
            if self_i == self.rows() {
                // drained self, add the rest of other's group key columns
                for (col_i, col) in result.group_key_cols.iter_mut().enumerate() {
                    col.extend(other.group_key_cols[col_i].iter().skip(other_i));
                }

                // add the rest of other's aggregate columns
                //
                // N.B - by checking the data type of the aggregate columns here
                // we can do type checking on a column basis (once) rather than
                // for each row. This allows us to extract an aggregate vec
                // and an iterator of the same type to extend the aggregate vec.
                for (col_i, (_, _, data_type)) in result.schema.aggregate_columns.iter().enumerate()
                {
                    match data_type {
                        LogicalDataType::Integer => {
                            let arr = other.aggregate_cols.remove(0);
                            result.aggregate_cols[col_i]
                                .extend_with_i64(arr.take_as_i64().into_iter());
                        }
                        LogicalDataType::Unsigned => {
                            let arr = other.aggregate_cols.remove(0);
                            result.aggregate_cols[col_i]
                                .extend_with_u64(arr.take_as_u64().into_iter());
                        }
                        LogicalDataType::Float => {
                            let arr = other.aggregate_cols.remove(0);
                            result.aggregate_cols[col_i]
                                .extend_with_f64(arr.take_as_f64().into_iter());
                        }
                        LogicalDataType::String => {
                            let arr = other.aggregate_cols.remove(0);
                            result.aggregate_cols[col_i]
                                .extend_with_str(arr.take_as_str().into_iter());
                        }
                        LogicalDataType::Binary => {
                            let arr = other.aggregate_cols.remove(0);
                            result.aggregate_cols[col_i]
                                .extend_with_bytes(arr.take_as_bytes().into_iter());
                        }
                        LogicalDataType::Boolean => {
                            let arr = other.aggregate_cols.remove(0);
                            result.aggregate_cols[col_i]
                                .extend_with_bool(arr.take_as_bool().into_iter());
                        }
                    }
                }

                return result;
            } else if other_i == other.rows() {
                // drained other, add the rest of self's group key columns
                for (col_i, col) in result.group_key_cols.iter_mut().enumerate() {
                    col.extend(self.group_key_cols[col_i].iter().skip(self_i));
                }

                // add the rest of self's aggregate columns
                for (col_i, (_, _, data_type)) in result.schema.aggregate_columns.iter().enumerate()
                {
                    match data_type {
                        LogicalDataType::Integer => {
                            let arr = self.aggregate_cols.remove(0);
                            result.aggregate_cols[col_i]
                                .extend_with_i64(arr.take_as_i64().into_iter());
                        }
                        LogicalDataType::Unsigned => {
                            let arr = self.aggregate_cols.remove(0);
                            result.aggregate_cols[col_i]
                                .extend_with_u64(arr.take_as_u64().into_iter());
                        }
                        LogicalDataType::Float => {
                            let arr = self.aggregate_cols.remove(0);
                            result.aggregate_cols[col_i]
                                .extend_with_f64(arr.take_as_f64().into_iter());
                        }
                        LogicalDataType::String => {
                            let arr = self.aggregate_cols.remove(0);
                            result.aggregate_cols[col_i]
                                .extend_with_str(arr.take_as_str().into_iter());
                        }
                        LogicalDataType::Binary => {
                            let arr = self.aggregate_cols.remove(0);
                            result.aggregate_cols[col_i]
                                .extend_with_bytes(arr.take_as_bytes().into_iter());
                        }
                        LogicalDataType::Boolean => {
                            let arr = self.aggregate_cols.remove(0);
                            result.aggregate_cols[col_i]
                                .extend_with_bool(arr.take_as_bool().into_iter());
                        }
                    }
                }

                return result;
            }

            // compare the next row in self and other and determine if there is
            // a clear lexicographic order.
            let mut ord = Ordering::Equal;
            for i in 0..result.schema.group_columns.len() {
                match self.group_key_cols[i][self_i].partial_cmp(&other.group_key_cols[i][other_i])
                {
                    Some(o) => {
                        ord = o;
                        if !matches!(ord, Ordering::Equal) {
                            break;
                        }
                    }
                    None => continue,
                }
            }

            match ord {
                Ordering::Less => {
                    // move the next row for each of self's columns onto result.
                    for (col_i, col) in result.group_key_cols.iter_mut().enumerate() {
                        col.push(self.group_key_cols[col_i][self_i]);
                    }
                    for (col_i, col) in result.aggregate_cols.iter_mut().enumerate() {
                        col.push(self.aggregate_cols[col_i].value(self_i));
                    }
                    self_i += 1;
                }
                Ordering::Equal => {
                    // move the next row for each of self's columns onto result.
                    for (col_i, col) in result.group_key_cols.iter_mut().enumerate() {
                        col.push(self.group_key_cols[col_i][self_i]);
                    }

                    // merge all the aggregates for this group key.
                    for (col_i, col) in result.aggregate_cols.iter_mut().enumerate() {
                        let self_value = self.aggregate_cols[col_i].value(self_i);
                        let other_value = other.aggregate_cols[col_i].value(other_i);
                        let (_, agg_type, _) = &self.schema.aggregate_columns[col_i];
                        col.push(match agg_type {
                            AggregateType::Count => self_value + other_value,
                            AggregateType::Min => match self_value.partial_cmp(&other_value) {
                                Some(ord) => match ord {
                                    Ordering::Less => self_value,
                                    Ordering::Equal => self_value,
                                    Ordering::Greater => other_value,
                                },
                                None => self_value,
                            },
                            AggregateType::Max => match self_value.partial_cmp(&other_value) {
                                Some(ord) => match ord {
                                    Ordering::Less => other_value,
                                    Ordering::Equal => other_value,
                                    Ordering::Greater => self_value,
                                },
                                None => self_value,
                            },
                            AggregateType::Sum => self_value + other_value,
                            _ => unimplemented!("first/last not implemented"),
                        });
                    }
                    self_i += 1;
                    other_i += 1;
                }
                Ordering::Greater => {
                    // move the next row for each of other's columns onto result.
                    for (col_i, col) in result.group_key_cols.iter_mut().enumerate() {
                        col.push(other.group_key_cols[col_i][other_i]);
                    }
                    for (col_i, col) in result.aggregate_cols.iter_mut().enumerate() {
                        col.push(other.aggregate_cols[col_i].value(other_i));
                    }
                    other_i += 1;
                }
            }
        }
        result
    }

    // Executes a mutable sort of the results based on the lexicographic order
    // of each group key columns.
    //
    // Given these group key columns:
    //
    //    [foo  [zoo  [zoo
    //     foo   bar   zoo
    //     bar   bar   bar
    //     bar]  bar]  zoo]
    //
    //    `sort` would result them becoming:
    //
    //    [bar  [bar  [bar
    //     bar   bar   zoo
    //     foo   bar   zoo
    //     foo]  zoo]  zoo]
    //
    // The same permutation is also applied to the aggregate columns.
    //
    pub fn sort(&mut self) {
        if self.group_keys_sorted {
            return;
        }

        // Create a vector of group keys, which allows us to determine a
        // permutation by which we should sort all columns.
        let mut group_keys = (0..self.rows())
            .map(|i| GroupKey::new(&self.group_key_cols, i))
            .collect::<Vec<_>>();

        // sort the vector of group keys, which will give us a permutation
        // that we can apply to all of the columns.
        group_keys.sort_unstable_by(|a, b| {
            let cols = a.len();
            for i in 0..cols {
                match a.columns[i][a.row_offset].partial_cmp(&b.columns[i][b.row_offset]) {
                    Some(ord) => {
                        if matches!(ord, Ordering::Equal) {
                            continue;
                        }
                        return ord;
                    }
                    None => continue,
                }
            }

            std::cmp::Ordering::Equal
        });

        // Now create a permutation by looking at how the row_offsets have been
        // ordered in the `group_keys` array.
        let perm = permutation::Permutation::from_vec(
            group_keys
                .iter()
                .map(|gk| gk.row_offset)
                .collect::<Vec<_>>(),
        );
        assert_eq!(perm.len(), self.rows());

        // Apply that permutation to all of the columns.
        for col in self.group_key_cols.iter_mut() {
            *col = perm.apply_slice(col.as_slice());
        }

        for col in self.aggregate_cols.iter_mut() {
            col.sort_with_permutation(&perm);
        }

        self.group_keys_sorted = true;
    }
}

// The `GroupKey` struct is a wrapper over a specific row of data in grouping
// columns.
//
// Rather than pivot the columns into a row-wise orientation to sort them, we
// can effectively sort a projection across them (`row_offset`) storing
// `GroupKey`s in a vector and sorting that.
struct GroupKey<'a> {
    columns: &'a [Vec<Option<&'a str>>],
    row_offset: usize,
}

impl<'a> GroupKey<'a> {
    fn new(columns: &'a [Vec<Option<&'a str>>], offset: usize) -> Self {
        Self {
            columns,
            row_offset: offset,
        }
    }

    // The number of columns comprising the `GroupKey`.
    fn len(&self) -> usize {
        self.columns.len()
    }
}

impl TryFrom<ReadAggregateResult<'_>> for RecordBatch {
    type Error = Error;

    fn try_from(mut result: ReadAggregateResult<'_>) -> Result<Self, Self::Error> {
        let schema = internal_types::schema::Schema::try_from(result.schema())
            .map_err(|source| Error::SchemaConversion { source })?;
        let arrow_schema: arrow::datatypes::SchemaRef = schema.into();

        // Add the group columns to the set of column data for the record batch.
        let mut columns: Vec<Arc<dyn arrow::array::Array>> =
            Vec::with_capacity(result.schema.len());

        for (_, data_type) in &result.schema.group_columns {
            match data_type {
                LogicalDataType::String => {
                    columns.push(Arc::new(array::StringArray::from(
                        result.group_key_cols.remove(0), // move column out of result
                    )));
                }
                _ => panic!("only String currently supported as group column"),
            }
        }

        for (_, _, data_type) in &result.schema.aggregate_columns {
            match data_type {
                LogicalDataType::Integer => {
                    columns.push(Arc::new(array::Int64Array::from(
                        result.aggregate_cols.remove(0).take_as_i64(),
                    )));
                }
                LogicalDataType::Unsigned => {
                    columns.push(Arc::new(array::UInt64Array::from(
                        result.aggregate_cols.remove(0).take_as_u64(),
                    )));
                }
                LogicalDataType::Float => {
                    columns.push(Arc::new(array::Float64Array::from(
                        result.aggregate_cols.remove(0).take_as_f64(),
                    )));
                }
                LogicalDataType::String => {
                    columns.push(Arc::new(array::StringArray::from(
                        result
                            .aggregate_cols
                            .remove(0)
                            .take_as_str()
                            .iter()
                            .map(|x| x.as_deref())
                            .collect::<Vec<_>>(),
                    )));
                }
                LogicalDataType::Binary => {
                    columns.push(Arc::new(array::BinaryArray::from(
                        result
                            .aggregate_cols
                            .remove(0)
                            .take_as_bytes()
                            .iter()
                            .map(|x| x.as_deref())
                            .collect::<Vec<_>>(),
                    )));
                }
                LogicalDataType::Boolean => {
                    columns.push(Arc::new(array::BooleanArray::from(
                        result.aggregate_cols.remove(0).take_as_bool(),
                    )));
                }
            }
        }

        // everything has been moved and copied into record batch.
        assert!(result.group_key_cols.is_empty());
        assert!(result.aggregate_cols.is_empty());

        // try_new only returns an error if the schema is invalid or the number
        // of rows on columns differ. We have full control over both so there
        // should never be an error to return...
        Self::try_new(arrow_schema, columns).context(ArrowConversion)
    }
}

// `group_keys_sorted` does not contribute to a result's equality with another
impl PartialEq for ReadAggregateResult<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.schema() == other.schema()
            && self.group_key_cols == other.group_key_cols
            && self.aggregate_cols == other.aggregate_cols
    }
}

/// The Debug implementation emits both the schema and the column data for the
/// results.
impl std::fmt::Debug for ReadAggregateResult<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Display the schema
        Display::fmt(&self.schema(), f)?;

        // Display the rest of the values.
        Display::fmt(&self, f)
    }
}

/// The Display implementation emits all of the column data for the results, but
/// omits the schema.
impl Display for ReadAggregateResult<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            return Ok(());
        }

        // There may or may not be group keys
        let expected_rows = self.rows();
        for row in 0..expected_rows {
            if row > 0 {
                writeln!(f)?;
            }

            // write row for group by columns
            if self.is_grouped_aggregate() {
                for col in &self.group_key_cols {
                    match col[row] {
                        Some(v) => write!(f, "{},", v)?,
                        None => write!(f, "NULL,")?,
                    }
                }
            }

            // write row for aggregate columns
            for (i, col) in self.aggregate_cols.iter().enumerate() {
                col.write_value(row, f)?;
                if i < self.aggregate_cols.len() - 1 {
                    write!(f, ",")?
                }
            }
        }

        writeln!(f)
    }
}

/// Test helpers that are only available when compiled in test mode
#[cfg(test)]
impl ColumnType {
    /// Helper function to construct a `Tag` column from a slice of `&str`
    pub fn create_tag(values: &[&str]) -> Self {
        Self::Tag(Column::from(values))
    }

    /// Helper function to construct a `Tag` column from a slice of `Option<&str>`
    pub fn create_tag_opt(values: &[Option<&str>]) -> Self {
        Self::Tag(Column::from(values))
    }

    /// Helper function to construct a `Time` column from a slice of `i64`
    pub fn create_time(values: &[i64]) -> Self {
        Self::Time(Column::from(values))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::schema;

    // Helper function that creates a predicate from a single expression
    fn col_pred(expr: BinaryExpr) -> Predicate {
        Predicate::new(vec![expr])
    }

    #[test]
    fn size() {
        let mut columns = vec![];
        let rc = ColumnType::Tag(Column::from(&[Some("west"), Some("west"), None, None][..]));
        columns.push(("region".to_string(), rc));
        let tc = ColumnType::Time(Column::from(&[100_i64, 200, 500, 600][..]));
        columns.push(("time".to_string(), tc));

        let row_group = RowGroup::new(4, columns);

        let rg_size = row_group.size();
        assert!(rg_size > 0);

        let mut columns = vec![];

        let track = ColumnType::Tag(Column::from(
            &[Some("Thinking"), Some("of"), Some("a"), Some("place")][..],
        ));
        columns.push(("track".to_string(), track));
        let tc = ColumnType::Time(Column::from(&[100_i64, 200, 500, 600][..]));
        columns.push(("time".to_string(), tc));

        let row_group = RowGroup::new(4, columns);

        assert!(row_group.size() > rg_size);
    }

    #[test]
    fn row_ids_from_predicates() {
        let mut columns = vec![];
        let tc = ColumnType::Time(Column::from(&[100_i64, 200, 500, 600, 300, 300][..]));
        columns.push(("time".to_string(), tc));
        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        columns.push(("region".to_string(), rc));
        let row_group = RowGroup::new(6, columns);

        // Closed partially covering "time range" predicate
        let row_ids = row_group.row_ids_from_predicate(&Predicate::with_time_range(&[], 200, 600));
        assert_eq!(row_ids.unwrap().to_vec(), vec![1, 2, 4, 5]);

        // Fully covering "time range" predicate
        let row_ids = row_group.row_ids_from_predicate(&Predicate::with_time_range(&[], 10, 601));
        assert!(matches!(row_ids, RowIDsOption::All(_)));

        // Open ended "time range" predicate
        let row_ids = row_group.row_ids_from_predicate(&col_pred(BinaryExpr::from((
            TIME_COLUMN_NAME,
            ">=",
            300_i64,
        ))));
        assert_eq!(row_ids.unwrap().to_vec(), vec![2, 3, 4, 5]);

        // Closed partially covering "time range" predicate and other column
        // predicate
        let row_ids = row_group.row_ids_from_predicate(&Predicate::with_time_range(
            &[BinaryExpr::from(("region", "=", "south"))],
            200,
            600,
        ));
        assert_eq!(row_ids.unwrap().to_vec(), vec![4]);

        // Fully covering "time range" predicate and other column predicate
        let row_ids = row_group.row_ids_from_predicate(&Predicate::with_time_range(
            &[BinaryExpr::from(("region", "=", "west"))],
            10,
            601,
        ));
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 1, 3]);

        // "time range" predicate and other column predicate that doesn't match
        let row_ids = row_group.row_ids_from_predicate(&Predicate::with_time_range(
            &[BinaryExpr::from(("region", "=", "nope"))],
            200,
            600,
        ));
        assert!(matches!(row_ids, RowIDsOption::None(_)));

        // predicate where both expressions match rows, but not the same rows
        let row_ids = row_group.row_ids_from_predicate(&Predicate::with_time_range(
            &[BinaryExpr::from(("region", "=", "west"))], // only matches rows outside of time-range
            300,
            301,
        ));
        assert!(matches!(row_ids, RowIDsOption::None(_)));

        // predicate where both expressions match rows, but not the same rows
        let row_ids = row_group.row_ids_from_predicate(&Predicate::with_time_range(
            &[BinaryExpr::from(("region", "=", "south"))], // only matches rows outside of time-range
            100,
            200,
        ));
        assert!(matches!(row_ids, RowIDsOption::None(_)));

        // Just a column predicate
        let row_ids =
            row_group.row_ids_from_predicate(&col_pred(BinaryExpr::from(("region", "=", "east"))));
        assert_eq!(row_ids.unwrap().to_vec(), vec![2]);

        // Predicate can matches all the rows
        let row_ids =
            row_group.row_ids_from_predicate(&col_pred(BinaryExpr::from(("region", "!=", "abba"))));
        assert!(matches!(row_ids, RowIDsOption::All(_)));

        // No predicates
        let row_ids = row_group.row_ids_from_predicate(&Predicate::default());
        assert!(matches!(row_ids, RowIDsOption::All(_)));
    }

    #[test]
    fn read_filter() {
        let mut columns = vec![];
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        columns.push(("time".to_string(), tc));

        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        columns.push(("region".to_string(), rc));

        let mc = ColumnType::Tag(Column::from(
            &["GET", "POST", "POST", "POST", "PUT", "GET"][..],
        ));
        columns.push(("method".to_string(), mc));

        let fc = ColumnType::Field(Column::from(&[100_u64, 101, 200, 203, 203, 10][..]));
        columns.push(("count".to_string(), fc));

        let row_group = RowGroup::new(6, columns);

        let cases = vec![
            (
                vec!["count", "region", "time"],
                Predicate::with_time_range(&[], 1, 6),
                "count,region,time
100,west,1
101,west,2
200,east,3
203,west,4
203,south,5
",
            ),
            (
                vec!["time", "region", "method"],
                Predicate::with_time_range(&[], -19, 2),
                "time,region,method
1,west,GET
",
            ),
            (
                vec!["time"],
                Predicate::with_time_range(&[], 0, 3),
                "time
1
2
",
            ),
            (
                vec!["method"],
                Predicate::with_time_range(&[], 0, 3),
                "method
GET
POST
",
            ),
            (
                vec!["count", "method", "time"],
                Predicate::with_time_range(&[BinaryExpr::from(("method", "=", "POST"))], 0, 6),
                "count,method,time
101,POST,2
200,POST,3
203,POST,4
",
            ),
            (
                vec!["region", "time"],
                Predicate::with_time_range(&[BinaryExpr::from(("method", "=", "POST"))], 0, 6),
                "region,time
west,2
east,3
west,4
",
            ),
        ];

        for (cols, predicates, expected) in cases {
            let results = row_group.read_filter(&cols, &predicates);
            assert_eq!(format!("{:?}", &results), expected);
        }

        // test no matching rows
        let results = row_group.read_filter(
            &["method", "region", "time"],
            &Predicate::with_time_range(&[], -19, 1),
        );
        assert!(results.is_empty());
    }

    #[test]
    fn read_filter_dictionaries() {
        let mut columns = vec![];
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        columns.push(("time".to_string(), tc));

        // Tag column that will be dictionary encoded when materialised
        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        columns.push(("region".to_string(), rc));

        // Field column that will be stored as a string array when materialised
        let mc = ColumnType::Field(Column::from(
            &["GET", "POST", "POST", "POST", "PUT", "GET"][..],
        ));
        columns.push(("method".to_string(), mc));

        let row_group = RowGroup::new(6, columns);

        let cases = vec![
            (
                vec!["method", "region", "time"],
                Predicate::default(),
                "method,region,time
GET,west,1
POST,west,2
POST,east,3
POST,west,4
PUT,south,5
GET,north,6
",
            ),
            (
                vec!["method", "region", "time"],
                Predicate::with_time_range(&[], -1, 3),
                "method,region,time
GET,west,1
POST,west,2
",
            ),
        ];

        for (cols, predicates, expected) in cases {
            let results = row_group.read_filter(&cols, &predicates);
            assert_eq!(format!("{:?}", &results), expected);
        }
    }

    // Test coverage for https://github.com/influxdata/influxdb_iox/issues/1860
    #[test]
    fn read_filter_bug_1860() {
        let mut columns = vec![];
        let tc = ColumnType::Time(Column::from(&[200_i64, 300, 400, 500, 600][..]));
        columns.push(("time".to_string(), tc));

        let city = ColumnType::Tag(Column::from(
            &[Some("Boston"), None, None, Some("NYC"), Some("NYC")][..],
        ));
        columns.push(("city".to_string(), city));

        let state = ColumnType::Tag(Column::from(
            &[Some("MA"), Some("CA"), Some("NY"), Some("NY"), Some("NY")][..],
        ));
        columns.push(("state".to_string(), state));

        let borough = ColumnType::Tag(Column::from(
            &[None, None, None, None, Some("Brooklyn")][..],
        ));
        columns.push(("borough".to_string(), borough));

        let temp = ColumnType::Field(Column::from(&[50.4, 79.0, 60.8, 61.0, 61.0][..]));
        columns.push(("temp".to_string(), temp));

        let row_group = RowGroup::new(5, columns);

        let results = row_group.read_filter(
            &["state"],
            &Predicate::with_time_range(&[BinaryExpr::from(("state", "=", "NY"))], 1, 300),
        );
        assert!(results.is_empty());
    }

    #[test]
    fn read_aggregate() {
        let mut columns = vec![];
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        columns.push(("time".to_string(), tc));

        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        columns.push(("region".to_string(), rc));

        let mc = ColumnType::Tag(Column::from(
            &["GET", "POST", "POST", "POST", "PUT", "GET"][..],
        ));
        columns.push(("method".to_string(), mc));

        let ec = ColumnType::Tag(Column::from(
            &[
                Some("prod"),
                Some("prod"),
                Some("stag"),
                Some("prod"),
                None,
                None,
            ][..],
        ));
        columns.push(("env".to_string(), ec));

        let c = ColumnType::Tag(Column::from(
            &["Alpha", "Alpha", "Bravo", "Bravo", "Alpha", "Alpha"][..],
        ));
        columns.push(("letters".to_string(), c));

        let c = ColumnType::Tag(Column::from(
            &["one", "two", "two", "two", "one", "three"][..],
        ));
        columns.push(("numbers".to_string(), c));

        let fc = ColumnType::Field(Column::from(&[100_u64, 101, 200, 203, 203, 10][..]));
        columns.push(("counter".to_string(), fc));

        let row_group = RowGroup::new(6, columns);

        // test queries with no predicates and grouping on low cardinality
        // columns
        read_aggregate_all_rows_all_rle(&row_group);

        // test row group queries that group on fewer than five columns.
        read_aggregate_hash_u128_key(&row_group);

        // test row group queries that use a vector-based group key.
        read_aggregate_hash_vec_key(&row_group);

        // test row group queries that only group on one column.
        read_aggregate_single_groupby_column(&row_group);
    }

    // the read_group path where grouping is on fewer than five columns.
    fn read_aggregate_hash_u128_key(row_group: &RowGroup) {
        let cases = vec![
            (
                Predicate::with_time_range(&[], 0, 7), // all time but without explicit pred
                vec!["region", "method"],
                vec![("counter", AggregateType::Sum)],
                "region,method,counter_sum
east,POST,200
north,GET,10
south,PUT,203
west,GET,100
west,POST,304
",
            ),
            (
                Predicate::with_time_range(&[], 2, 6), // all time but without explicit pred
                vec!["env", "region"],
                vec![
                    ("counter", AggregateType::Sum),
                    ("counter", AggregateType::Count),
                ],
                "env,region,counter_sum,counter_count
NULL,south,203,1
prod,west,304,2
stag,east,200,1
",
            ),
            (
                Predicate::with_time_range(&[], -1, 10),
                vec!["region", "env"],
                vec![("method", AggregateType::Min)], // Yep, you can aggregate any column.
                "region,env,method_min
east,stag,POST
north,NULL,GET
south,NULL,PUT
west,prod,GET
",
            ),
            // This case is identical to above but has an explicit `region >
            // "north"` predicate.
            (
                Predicate::with_time_range(&[BinaryExpr::from(("region", ">", "north"))], -1, 10),
                vec!["region", "env"],
                vec![("method", AggregateType::Min)], // Yep, you can aggregate any column.
                "region,env,method_min
south,NULL,PUT
west,prod,GET
",
            ),
            (
                Predicate::with_time_range(&[], -1, 10),
                vec!["region", "env", "method"],
                vec![("time", AggregateType::Max)], // Yep, you can aggregate any column.
                "region,env,method,time_max
east,stag,POST,3
north,NULL,GET,6
south,NULL,PUT,5
west,prod,GET,1
west,prod,POST,4
",
            ),
        ];

        for (predicate, group_cols, aggs, expected) in cases {
            let mut results = row_group.read_aggregate(&predicate, &group_cols, &aggs);
            results.sort();
            assert_eq!(format!("{:?}", &results), expected);
        }
    }

    // the read_group path where grouping is on five or more columns. This will
    // ensure that the `read_group_hash_with_vec_key` path is exercised.
    fn read_aggregate_hash_vec_key(row_group: &RowGroup) {
        let cases = vec![(
            Predicate::with_time_range(&[], 0, 7), // all time but with explicit pred
            vec!["region", "method", "env", "letters", "numbers"],
            vec![("counter", AggregateType::Sum)],
            "region,method,env,letters,numbers,counter_sum
east,POST,stag,Bravo,two,200
north,GET,NULL,Alpha,three,10
south,PUT,NULL,Alpha,one,203
west,GET,prod,Alpha,one,100
west,POST,prod,Alpha,two,101
west,POST,prod,Bravo,two,203
",
        )];

        for (predicate, group_cols, aggs, expected) in cases {
            let mut results = row_group.read_aggregate(&predicate, &group_cols, &aggs);
            results.sort();
            assert_eq!(format!("{:?}", &results), expected);
        }
    }

    // the read_group path where grouping is on a single column.
    fn read_aggregate_single_groupby_column(row_group: &RowGroup) {
        let cases = vec![(
            Predicate::with_time_range(&[], 0, 7), // all time but with explicit pred
            vec!["method"],
            vec![("counter", AggregateType::Sum)],
            "method,counter_sum
GET,110
POST,504
PUT,203
",
        )];

        for (predicate, group_cols, aggs, expected) in cases {
            let mut results = row_group.read_aggregate(&predicate, &group_cols, &aggs);
            results.sort();
            assert_eq!(format!("{:?}", &results), expected);
        }
    }

    fn read_aggregate_all_rows_all_rle(row_group: &RowGroup) {
        let cases = vec![
            (
                Predicate::default(),
                vec!["region", "method"],
                vec![("counter", AggregateType::Sum)],
                "region,method,counter_sum
east,POST,200
north,GET,10
south,PUT,203
west,GET,100
west,POST,304
",
            ),
            (
                Predicate::default(),
                vec!["region", "method", "env"],
                vec![("counter", AggregateType::Sum)],
                "region,method,env,counter_sum
east,POST,stag,200
north,GET,NULL,10
south,PUT,NULL,203
west,GET,prod,100
west,POST,prod,304
",
            ),
            (
                Predicate::default(),
                vec!["env"],
                vec![("counter", AggregateType::Count)],
                "env,counter_count
NULL,2
prod,3
stag,1
",
            ),
            (
                Predicate::default(),
                vec!["region", "method"],
                vec![
                    ("counter", AggregateType::Sum),
                    ("counter", AggregateType::Min),
                    ("counter", AggregateType::Max),
                ],
                "region,method,counter_sum,counter_min,counter_max
east,POST,200,200,200
north,GET,10,10,10
south,PUT,203,203,203
west,GET,100,100,100
west,POST,304,101,203
",
            ),
        ];

        for (predicate, group_cols, aggs, expected) in cases {
            let results = row_group.read_aggregate(&predicate, &group_cols, &aggs);
            assert_eq!(format!("{:?}", &results), expected);
        }
    }

    #[test]
    fn row_aggregate_could_satisfy_predicate() {
        let mut columns = vec![];
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        columns.push(("time".to_string(), tc));

        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        columns.push(("region".to_string(), rc));

        let mc = ColumnType::Tag(Column::from(
            &["GET", "GET", "GET", "GET", "GET", "GET"][..],
        ));
        columns.push(("method".to_string(), mc));

        let row_group = RowGroup::new(6, columns);

        let cases = vec![
            (("az", "=", "west"), false),      // no az column
            (("region", "=", "west"), true),   // region column does contain "west"
            (("region", "=", "over"), true),   // region column might contain "over"
            (("region", "=", "abc"), false),   // region column can't contain "abc"
            (("region", "=", "zoo"), false),   // region column can't contain "zoo"
            (("region", "!=", "hello"), true), // region column might not contain "hello"
            (("method", "!=", "GET"), false),  // method must only contain "GET"
            (("region", ">", "abc"), true),    // region column might contain something > "abc"
            (("region", ">", "north"), true),  // region column might contain something > "north"
            (("region", ">", "west"), false),  // region column can't contain something > "west"
            (("region", ">=", "abc"), true),   // region column might contain something ≥ "abc"
            (("region", ">=", "east"), true),  // region column might contain something ≥ "east"
            (("region", ">=", "west"), true),  // region column might contain something ≥ "west"
            (("region", ">=", "zoo"), false),  // region column can't contain something ≥ "zoo"
            (("region", "<", "foo"), true),    // region column might contain something < "foo"
            (("region", "<", "north"), true),  // region column might contain something < "north"
            (("region", "<", "south"), true),  // region column might contain something < "south"
            (("region", "<", "east"), false),  // region column can't contain something < "east"
            (("region", "<", "abc"), false),   // region column can't contain something < "abc"
            (("region", "<=", "east"), true),  // region column might contain something ≤ "east"
            (("region", "<=", "north"), true), // region column might contain something ≤ "north"
            (("region", "<=", "south"), true), // region column might contain something ≤ "south"
            (("region", "<=", "abc"), false),  // region column can't contain something ≤ "abc"
        ];

        for ((col, op, value), exp) in cases {
            let predicate = Predicate::new(vec![BinaryExpr::from((col, op, value))]);

            assert_eq!(
                row_group.could_satisfy_conjunctive_binary_expressions(predicate.iter()),
                exp,
                "{:?} failed",
                predicate
            );
        }
    }

    #[test]
    fn row_aggregate_satisfies_predicate() {
        let mut columns = vec![];
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3, 4, 5, 6][..]));
        columns.push(("time".to_string(), tc));

        let rc = ColumnType::Tag(Column::from(
            &["west", "west", "east", "west", "south", "north"][..],
        ));
        columns.push(("region".to_string(), rc));

        let mc = ColumnType::Tag(Column::from(
            &["GET", "GET", "GET", "GET", "GET", "GET"][..],
        ));
        columns.push(("method".to_string(), mc));

        let row_group = RowGroup::new(6, columns);

        let mut predicate = Predicate::default();
        assert!(row_group.satisfies_predicate(&predicate));

        predicate = Predicate::new(vec![BinaryExpr::from(("region", "=", "east"))]);
        assert!(row_group.satisfies_predicate(&predicate));

        // all expressions satisfied in data
        predicate = Predicate::new(vec![
            BinaryExpr::from(("region", "=", "east")),
            BinaryExpr::from(("method", "!=", "POST")),
        ]);
        assert!(row_group.satisfies_predicate(&predicate));

        // all expressions satisfied in data by all rows
        predicate = Predicate::new(vec![BinaryExpr::from(("method", "=", "GET"))]);
        assert!(row_group.satisfies_predicate(&predicate));

        // one expression satisfied in data but other ruled out via column pruning.
        predicate = Predicate::new(vec![
            BinaryExpr::from(("region", "=", "east")),
            BinaryExpr::from(("method", ">", "GET")),
        ]);
        assert!(!row_group.satisfies_predicate(&predicate));

        // all expressions rules out via column pruning.
        predicate = Predicate::new(vec![
            BinaryExpr::from(("region", ">", "west")),
            BinaryExpr::from(("method", ">", "GET")),
        ]);
        assert!(!row_group.satisfies_predicate(&predicate));

        // column does not exist
        predicate = Predicate::new(vec![BinaryExpr::from(("track", "=", "Jeanette"))]);
        assert!(!row_group.satisfies_predicate(&predicate));

        // one column satisfies expression but other column does not exist
        predicate = Predicate::new(vec![
            BinaryExpr::from(("region", "=", "south")),
            BinaryExpr::from(("track", "=", "Jeanette")),
        ]);
        assert!(!row_group.satisfies_predicate(&predicate));
    }

    #[test]
    fn pack_unpack_group_keys() {
        let cases = vec![
            vec![0, 0, 0, 0],
            vec![1, 2, 3, 4],
            vec![1, 3, 4, 2],
            vec![0],
            vec![0, 1],
            vec![u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            vec![u32::MAX, u16::MAX as u32, u32::MAX, u16::MAX as u32],
            vec![0, u16::MAX as u32, 0],
            vec![0, u16::MAX as u32, 0, 0],
            vec![0, 0, u32::MAX, 0],
        ];

        for case in cases {
            let mut packed_value = 0_u128;
            for (i, &encoded_id) in case.iter().enumerate() {
                packed_value = pack_u32_in_u128(packed_value, encoded_id, i);
            }

            assert_eq!(
                unpack_u128_group_key(packed_value, case.len(), vec![]),
                case
            );
        }
    }

    #[test]
    fn read_aggregate_result_display() {
        let mut result = ReadAggregateResult {
            schema: ResultSchema {
                select_columns: vec![],
                group_columns: vec![
                    (
                        schema::ColumnType::Tag("region".to_owned()),
                        LogicalDataType::String,
                    ),
                    (
                        schema::ColumnType::Tag("host".to_owned()),
                        LogicalDataType::String,
                    ),
                ],
                aggregate_columns: vec![
                    (
                        schema::ColumnType::Field("temp".to_owned()),
                        AggregateType::Sum,
                        LogicalDataType::Integer,
                    ),
                    (
                        schema::ColumnType::Field("voltage".to_owned()),
                        AggregateType::Count,
                        LogicalDataType::Unsigned,
                    ),
                ],
            },
            group_key_cols: vec![
                vec![
                    Some("east"),
                    Some("east"),
                    Some("west"),
                    Some("west"),
                    Some("west"),
                ],
                vec![
                    Some("host-a"),
                    Some("host-b"),
                    Some("host-a"),
                    Some("host-c"),
                    Some("host-d"),
                ],
            ],
            aggregate_cols: vec![
                AggregateVec::SumI64(vec![Some(10), Some(20), Some(25), Some(21), Some(11)]),
                AggregateVec::Count(vec![Some(3), Some(4), Some(3), Some(1), Some(9)]),
            ],
            group_keys_sorted: false,
        };

        // Debug implementation
        assert_eq!(
            format!("{:?}", &result),
            "region,host,temp_sum,voltage_count
east,host-a,10,3
east,host-b,20,4
west,host-a,25,3
west,host-c,21,1
west,host-d,11,9
"
        );

        // Display implementation
        assert_eq!(
            format!("{}", &result),
            "east,host-a,10,3
east,host-b,20,4
west,host-a,25,3
west,host-c,21,1
west,host-d,11,9
"
        );

        // results don't have to have group keys.
        result.schema.group_columns = vec![];
        result.group_key_cols = vec![];

        // Debug implementation
        assert_eq!(
            format!("{:?}", &result),
            "temp_sum,voltage_count
10,3
20,4
25,3
21,1
11,9
"
        );

        // Display implementation
        assert_eq!(
            format!("{}", &result),
            "10,3
20,4
25,3
21,1
11,9
"
        );
    }

    #[test]
    fn read_aggregate_result_sort() {
        let mut result = ReadAggregateResult {
            schema: ResultSchema::default(), // schema not needed for sorting.
            group_key_cols: vec![
                vec![
                    Some("east"),
                    Some("west"),
                    Some("west"),
                    Some("east"),
                    Some("west"),
                ],
                vec![
                    Some("host-a"),
                    Some("host-c"),
                    Some("host-a"),
                    Some("host-d"),
                    Some("host-b"),
                ],
            ],
            aggregate_cols: vec![
                AggregateVec::SumI64(vec![Some(10), Some(20), Some(25), Some(21), Some(11)]),
                AggregateVec::Count(vec![Some(3), Some(4), Some(3), Some(1), Some(9)]),
            ],
            group_keys_sorted: false,
        };

        result.sort();

        // Debug implementation
        assert_eq!(
            format!("{}", &result),
            "east,host-a,10,3
east,host-d,21,1
west,host-a,25,3
west,host-b,11,9
west,host-c,20,4
"
        );

        let mut result = ReadAggregateResult {
            schema: ResultSchema::default(),
            group_key_cols: vec![
                vec![Some("west"), Some("east"), Some("north")],
                vec![Some("host-c"), Some("host-c"), Some("host-c")],
                vec![Some("pro"), Some("stag"), Some("dev")],
            ],
            aggregate_cols: vec![
                AggregateVec::SumI64(vec![Some(10), Some(20), Some(-5)]),
                AggregateVec::Count(vec![Some(6), Some(8), Some(2)]),
            ],
            ..Default::default()
        };
        result.sort();

        assert_eq!(
            format!("{}", &result),
            "east,host-c,stag,20,8
north,host-c,dev,-5,2
west,host-c,pro,10,6
"
        );
    }

    #[test]
    fn read_aggregate_result_merge() {
        let schema = ResultSchema {
            group_columns: vec![
                (
                    schema::ColumnType::Tag("region".to_owned()),
                    LogicalDataType::String,
                ),
                (
                    schema::ColumnType::Tag("host".to_owned()),
                    LogicalDataType::String,
                ),
            ],
            aggregate_columns: vec![
                (
                    schema::ColumnType::Field("temp".to_owned()),
                    AggregateType::Sum,
                    LogicalDataType::Integer,
                ),
                (
                    schema::ColumnType::Field("voltage".to_owned()),
                    AggregateType::Count,
                    LogicalDataType::Unsigned,
                ),
            ],
            ..ResultSchema::default()
        };

        let mut result = ReadAggregateResult {
            schema: schema.clone(),
            ..Default::default()
        };

        let other_result = ReadAggregateResult {
            schema: schema.clone(),
            group_key_cols: vec![
                vec![Some("east"), Some("east")],
                vec![Some("host-a"), Some("host-b")],
            ],
            aggregate_cols: vec![
                AggregateVec::SumI64(vec![Some(10), Some(20)]),
                AggregateVec::Count(vec![Some(3), Some(4)]),
            ],
            ..Default::default()
        };

        // merging something into nothing results in having a copy of something.
        result = result.merge(other_result.clone());
        assert_eq!(result, other_result.clone());

        // merging the something into the result again results in all the
        // aggregates doubling.
        result = result.merge(other_result.clone());
        assert_eq!(
            result,
            ReadAggregateResult {
                schema: schema.clone(),
                group_key_cols: vec![
                    vec![Some("east"), Some("east")],
                    vec![Some("host-a"), Some("host-b")],
                ],
                aggregate_cols: vec![
                    AggregateVec::SumI64(vec![Some(20), Some(40)]),
                    AggregateVec::Count(vec![Some(6), Some(8)]),
                ],
                ..Default::default()
            }
        );

        // merging a result in with different group keys merges those group
        // keys in.
        let other_result = ReadAggregateResult {
            schema: schema.clone(),
            group_key_cols: vec![vec![Some("north")], vec![Some("host-a")]],
            aggregate_cols: vec![
                AggregateVec::SumI64(vec![Some(-5)]),
                AggregateVec::Count(vec![Some(2)]),
            ],
            ..Default::default()
        };
        result = result.merge(other_result.clone());

        assert_eq!(
            result,
            ReadAggregateResult {
                schema: schema.clone(),
                group_key_cols: vec![
                    vec![Some("east"), Some("east"), Some("north")],
                    vec![Some("host-a"), Some("host-b"), Some("host-a")],
                ],
                aggregate_cols: vec![
                    AggregateVec::SumI64(vec![Some(20), Some(40), Some(-5)]),
                    AggregateVec::Count(vec![Some(6), Some(8), Some(2)]),
                ],
                ..Default::default()
            }
        );

        // merging nothing in doesn't change the result.
        let other_result = ReadAggregateResult {
            schema: schema.clone(),
            ..Default::default()
        };
        result = result.merge(other_result.clone());

        assert_eq!(
            result,
            ReadAggregateResult {
                schema,
                group_key_cols: vec![
                    vec![Some("east"), Some("east"), Some("north")],
                    vec![Some("host-a"), Some("host-b"), Some("host-a")],
                ],
                aggregate_cols: vec![
                    AggregateVec::SumI64(vec![Some(20), Some(40), Some(-5)]),
                    AggregateVec::Count(vec![Some(6), Some(8), Some(2)]),
                ],
                ..Default::default()
            }
        );
    }

    #[test]
    fn column_meta_equal() {
        let col1 = ColumnMeta {
            typ: schema::ColumnType::Tag("region".to_owned()),
            logical_data_type: schema::LogicalDataType::String,
            range: (
                OwnedValue::String("east".to_owned()),
                OwnedValue::String("west".to_owned()),
            ),
            distinct_count: Some(NonZeroU64::new(233).unwrap()),
            null_count: 0,
        };

        let col2 = ColumnMeta {
            typ: schema::ColumnType::Tag("region".to_owned()),
            logical_data_type: schema::LogicalDataType::String,
            range: (
                OwnedValue::String("north".to_owned()),
                OwnedValue::String("west".to_owned()),
            ),
            distinct_count: Some(NonZeroU64::new(233).unwrap()),
            null_count: 0,
        };

        let col3 = ColumnMeta {
            typ: schema::ColumnType::Tag("host".to_owned()),
            logical_data_type: schema::LogicalDataType::String,
            range: (
                OwnedValue::String("east".to_owned()),
                OwnedValue::String("west".to_owned()),
            ),
            distinct_count: None,
            null_count: 1,
        };

        assert_eq!(col1, col2);
        assert_ne!(col1, col3);
        assert_ne!(col2, col3);
    }

    #[test]
    fn column_names() {
        let mut columns = vec![];
        let rc = ColumnType::Tag(Column::from(&[Some("west"), Some("west"), None, None][..]));
        columns.push(("region".to_string(), rc));
        let track = ColumnType::Tag(Column::from(
            &[Some("Thinking"), Some("of"), Some("a"), Some("place")][..],
        ));
        columns.push(("track".to_string(), track));
        let temp = ColumnType::Field(Column::from(
            &[Some("hot"), Some("cold"), Some("cold"), Some("warm")][..],
        ));
        columns.push(("temp".to_string(), temp));

        let tc = ColumnType::Time(Column::from(&[100_i64, 200, 500, 600][..]));
        columns.push(("time".to_string(), tc));
        let row_group = RowGroup::new(4, columns);

        // No predicate - just find a value in each column that matches.
        let mut dst = BTreeSet::new();
        row_group.column_names(&Predicate::default(), Selection::All, &mut dst);
        assert_eq!(
            dst,
            vec!["region", "temp", "time", "track"]
                .into_iter()
                .map(|s| s.to_owned())
                .collect()
        );

        // A predicate, but no rows match. No columns should be returned.
        let mut dst = BTreeSet::new();
        row_group.column_names(
            &Predicate::new(vec![BinaryExpr::from(("region", "=", "east"))]),
            Selection::All,
            &mut dst,
        );
        assert!(dst.is_empty());

        // A predicate, that matches some rows. Columns with non-null values at
        // those rows should be returned.
        let mut dst = BTreeSet::new();
        row_group.column_names(
            &Predicate::new(vec![BinaryExpr::from(("track", "=", "place"))]),
            Selection::All,
            &mut dst,
        );
        // query matches one row.
        //
        // region, temp, track, time
        // NULL  , warm, place, 600
        //
        assert_eq!(
            dst,
            vec!["temp", "time", "track",]
                .into_iter()
                .map(|s| s.to_owned())
                .collect()
        );

        // Reusing the same buffer keeps existing results even if they're not
        // part of the result-set from the row group.
        let mut columns = vec![];
        let rc = ColumnType::Tag(Column::from(&[Some("prod")][..]));
        columns.push(("env".to_string(), rc));
        let tc = ColumnType::Time(Column::from(&[100_i64][..]));
        let temp = ColumnType::Field(Column::from(&[Some("hot")][..]));
        columns.push(("temp".to_string(), temp));

        columns.push(("time".to_string(), tc));
        let row_group = RowGroup::new(1, columns);

        row_group.column_names(&Predicate::default(), Selection::All, &mut dst);
        assert_eq!(
            dst,
            vec!["env", "temp", "time", "track"]
                .into_iter()
                .map(|s| s.to_owned())
                .collect()
        );

        // just tag keys
        dst.clear();
        row_group.column_names(&Predicate::default(), Selection::Some(&["env"]), &mut dst);
        assert_eq!(
            dst.iter().cloned().collect::<Vec<_>>(),
            vec!["env".to_owned()],
        );

        // just field keys
        dst.clear();
        row_group.column_names(&Predicate::default(), Selection::Some(&["temp"]), &mut dst);
        assert_eq!(
            dst.iter().cloned().collect::<Vec<_>>(),
            vec!["temp".to_owned()],
        );
    }

    fn to_map(arr: Vec<(&str, &[&str])>) -> BTreeMap<String, BTreeSet<String>> {
        arr.iter()
            .map(|(k, values)| {
                (
                    k.to_string(),
                    values
                        .iter()
                        .map(|s| s.to_string())
                        .collect::<BTreeSet<_>>(),
                )
            })
            .collect::<BTreeMap<_, _>>()
    }

    #[test]
    fn column_values() {
        // Build a row group.
        let mut columns = vec![];
        let tc = ColumnType::Time(Column::from(&[1_i64, 2, 3][..]));
        columns.push(("time".to_string(), tc));

        let rc = ColumnType::Tag(Column::from(&["west", "south", "north"][..]));
        columns.push(("region".to_string(), rc));

        let ec = ColumnType::Tag(Column::from(&["prod", "stag", "stag"][..]));
        columns.push(("env".to_string(), ec));

        let rg = RowGroup::new(3, columns);

        let result = rg.column_values(&Predicate::default(), &["region"], BTreeMap::new());
        assert_eq!(
            result,
            to_map(vec![("region", &["north", "west", "south"])])
        );

        let result = rg.column_values(&Predicate::default(), &["env", "region"], BTreeMap::new());
        assert_eq!(
            result,
            to_map(vec![
                ("env", &["prod", "stag"]),
                ("region", &["north", "west", "south"])
            ])
        );

        let result = rg.column_values(
            &Predicate::new(vec![BinaryExpr::from(("time", ">", 1_i64))]),
            &["env", "region"],
            BTreeMap::new(),
        );
        assert_eq!(
            result,
            to_map(vec![("env", &["stag"]), ("region", &["north", "south"])])
        );

        let mut dst = BTreeMap::new();
        dst.insert(
            "env".to_owned(),
            vec!["stag".to_owned()].into_iter().collect::<BTreeSet<_>>(),
        );
        let result = rg.column_values(
            &Predicate::new(vec![BinaryExpr::from(("time", ">", 1_i64))]),
            &["env", "region"],
            dst,
        );
        assert_eq!(
            result,
            to_map(vec![("env", &["stag"]), ("region", &["north", "south"])])
        );

        let result = rg.column_values(
            &Predicate::new(vec![BinaryExpr::from(("time", ">", 4_i64))]),
            &["env", "region"],
            BTreeMap::new(),
        );
        assert_eq!(result, to_map(vec![]));
    }

    use datafusion::{
        logical_plan::{col, Expr},
        scalar::ScalarValue,
    };
    use std::convert::TryFrom;

    #[test]
    fn to_binary_expr() {
        let cases = vec![
            (
                // a = 22
                col("a").eq(Expr::Literal(ScalarValue::Int64(Some(22)))),
                BinaryExpr::from(("a", "=", 22_i64)),
            ),
            (
                // a > 10
                col("a").gt(Expr::Literal(ScalarValue::Int64(Some(10)))),
                BinaryExpr::from(("a", ">", 10_i64)),
            ),
            (
                // 100 = c
                Expr::Literal(ScalarValue::Int64(Some(100))).eq(col("c")),
                BinaryExpr::from(("c", "=", 100_i64)),
            ),
            (
                // 100 != c
                Expr::Literal(ScalarValue::Int64(Some(100))).not_eq(col("c")),
                BinaryExpr::from(("c", "!=", 100_i64)),
            ),
            (
                // 100 < c
                Expr::Literal(ScalarValue::Int64(Some(100))).lt(col("c")),
                BinaryExpr::from(("c", ">", 100_i64)),
            ),
            (
                // 100 <= c
                Expr::Literal(ScalarValue::Int64(Some(100))).lt_eq(col("c")),
                BinaryExpr::from(("c", ">=", 100_i64)),
            ),
            (
                // 100 >= c
                Expr::Literal(ScalarValue::Int64(Some(100))).gt_eq(col("c")),
                BinaryExpr::from(("c", "<=", 100_i64)),
            ),
            (
                // 100 > c
                Expr::Literal(ScalarValue::Int64(Some(100))).gt(col("c")),
                BinaryExpr::from(("c", "<", 100_i64)),
            ),
            (
                // a = timestamp(100000)
                col("a").eq(Expr::Literal(ScalarValue::TimestampNanosecond(Some(
                    1000000,
                )))),
                BinaryExpr::from(("a", "=", 1000000_i64)),
            ),
        ];

        for (input, exp) in cases {
            assert_eq!(BinaryExpr::try_from(&input).unwrap(), exp);
        }

        // Error cases
        let cases = vec![
            // 33 = 33
            Expr::Literal(ScalarValue::Int64(Some(33)))
                .eq(Expr::Literal(ScalarValue::Int64(Some(33)))),
            // a > b
            col("a").gt(col("b")),
        ];

        for input in cases {
            assert!(BinaryExpr::try_from(&input).is_err());
        }
    }
}
