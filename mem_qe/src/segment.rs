use std::collections::{BTreeMap, BTreeSet};

use observability_deps::tracing::{debug, error, info};

use super::column;
use super::column::{AggregateType, Column};
use arrow::datatypes::SchemaRef;

// Only used in a couple of specific places for experimentation.
const THREADS: usize = 16;

/// ColumnType describes the logical type a column can have.
pub enum ColumnType {
    Tag(column::Column),
    Field(column::Column),
    Time(column::Column),
}

impl ColumnType {
    fn num_rows(&self) -> usize {
        match &self {
            ColumnType::Tag(c) => c.num_rows(),
            ColumnType::Field(c) => c.num_rows(),
            ColumnType::Time(c) => c.num_rows(),
        }
    }
}

#[derive(Debug)]
pub struct Schema {
    _ref: SchemaRef,
    col_sort_order: Vec<String>,
}

impl Schema {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            _ref: schema,
            col_sort_order: vec![],
        }
    }

    pub fn with_sort_order(schema: SchemaRef, sort_order: Vec<String>) -> Self {
        let set = sort_order.iter().collect::<BTreeSet<_>>();
        assert_eq!(set.len(), sort_order.len());
        assert!(sort_order.len() <= schema.fields().len());

        Self {
            _ref: schema,
            col_sort_order: sort_order,
        }
    }

    pub fn sort_order(&self) -> &[String] {
        self.col_sort_order.as_slice()
    }

    pub fn schema_ref(&self) -> SchemaRef {
        self._ref.clone()
    }

    pub fn cols(&self) -> usize {
        let len = &self._ref.fields().len();
        *len
    }
}

#[derive(Debug)]
pub struct Segment {
    meta: SegmentMetaData,

    // Columns within a segment
    columns: Vec<Column>,

    tag_column_idxs: Vec<usize>, // todo(edd): add vectors to each type
    time_column_idx: usize,
}

impl Segment {
    pub fn new(rows: usize, schema: Schema) -> Self {
        let cols = schema.cols();
        Self {
            meta: SegmentMetaData::new(rows, schema),
            columns: Vec::with_capacity(cols),
            tag_column_idxs: vec![],
            time_column_idx: 0,
        }
    }

    pub fn add_column(&mut self, name: &str, ct: ColumnType) {
        assert_eq!(
            self.meta.rows,
            ct.num_rows(),
            "Column {:?} has {:?} rows but wanted {:?}",
            name,
            ct.num_rows(),
            self.meta.rows
        );

        // validate column doesn't already exist in segment
        assert!(!self.meta.column_names.contains(&name.to_owned()));
        self.meta.column_names.push(name.to_owned());

        match ct {
            ColumnType::Time(c) => {
                assert_eq!(name, "time");

                if let Column::Integer(ts) = &c {
                    // Right now assumption is ts column has some non-null values
                    self.meta.time_range = ts.column_range().unwrap();
                } else {
                    panic!("incorrect column type for time");
                }
                self.time_column_idx = self.columns.len();
                self.columns.push(c);
            }
            ColumnType::Tag(c) => {
                self.tag_column_idxs.push(self.columns.len());
                self.columns.push(c);
            }
            ColumnType::Field(c) => {
                self.columns.push(c);
            }
        }
    }

    pub fn num_rows(&self) -> usize {
        self.meta.rows
    }

    pub fn column_names(&self) -> &[String] {
        &self.meta.column_names
    }

    /// Determines if the segment contains a column with the provided name.
    pub fn has_column(&self, name: &str) -> bool {
        self.meta.column_names.contains(&name.to_string())
    }

    /// column returns the column with name
    pub fn column(&self, name: &str) -> Option<&Column> {
        if let Some(id) = &self.meta.column_names.iter().position(|c| c == name) {
            return self.columns.get(*id);
        }
        None
    }

    pub fn time_range(&self) -> (i64, i64) {
        self.meta.time_range
    }

    pub fn schema(&self) -> SchemaRef {
        self.meta.schema()
    }

    // TODO - iterator....
    /// Returns the size of the segment in bytes.
    pub fn size(&self) -> usize {
        let mut size = 0;
        for c in &self.columns {
            size += c.size();
        }
        size
    }

    // Returns the size of each of the segment's columns in bytes.
    pub fn column_sizes(&self) -> BTreeMap<String, usize> {
        let mut column_sizes = BTreeMap::new();
        let names = self.column_names();
        for (i, column) in self.columns.iter().enumerate() {
            match column {
                Column::String(c) => {
                    column_sizes.insert(names[i].to_owned(), c.size());
                }
                Column::Float(c) => {
                    column_sizes.insert(names[i].to_owned(), c.size());
                }
                Column::Integer(c) => {
                    column_sizes.insert(names[i].to_owned(), c.size());
                }
            }
        }
        column_sizes
    }

    // pub fn scan_column_from(&self, column_name: &str, row_id: usize) ->
    // Option<column::Vector> {     if let Some(i) =
    // self.column_names().iter().position(|c| c == column_name) {
    //         return self.columns[i].scan_from(row_id);
    //     }
    //     None
    // }

    // Materialise all rows for each desired column.
    //
    // `columns` determines which column values are returned. An empty `columns`
    // value will result in rows for all columns being returned.
    pub fn rows(
        &self,
        row_ids: &croaring::Bitmap,
        columns: &[String],
    ) -> BTreeMap<String, column::Values<'_>> {
        let mut rows = BTreeMap::new();
        if row_ids.is_empty() {
            // nothing to return
            return rows;
        }

        let cols_to_process = if columns.is_empty() {
            &self.meta.column_names
        } else {
            columns
        };

        for col_name in cols_to_process {
            let column = self.column(col_name.as_str());
            if let Some(column) = column {
                rows.insert(col_name.clone(), column.rows(row_ids));
            };
        }

        rows
    }

    pub fn group_by_column_ids(
        &self,
        name: &str,
    ) -> Option<&std::collections::BTreeMap<u32, croaring::Bitmap>> {
        if let Some(c) = self.column(name) {
            return Some(c.group_by_ids());
        }
        None
    }

    // Determines if a segment is already sorted by a group key. Only supports
    // ascending ordering at the moment. If this function returns true then
    // the columns being grouped on are naturally sorted and for basic
    // aggregations should not need to be sorted or hashed.
    fn group_key_sorted(&self, group_cols: &[String]) -> bool {
        let sorted_by_cols = self.meta.schema.sort_order();
        if group_cols.len() > sorted_by_cols.len() {
            // grouping by more columns than there are defined sorts.
            return false;
        }

        let mut covered = 0;
        'outer: for sc in sorted_by_cols {
            // find col in group key - doesn't matter what location in group key
            for gc in group_cols {
                if sc == gc {
                    covered += 1;
                    continue 'outer;
                }
            }

            // didn't find this sorted column in group key. That's okay if there
            // are no more columns being grouped
            return covered == group_cols.len();
        }
        true
    }

    pub fn aggregate_by_group_with_hash(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        group_columns: &[String],
        aggregates: &[(String, AggregateType)],
        window: i64,
    ) -> BTreeMap<Vec<i64>, Vec<(&String, column::Aggregate)>> {
        // Build a hash table - essentially, scan columns for matching row ids,
        // emitting the encoded value for each column and track those value
        // combinations in a hashmap with running aggregates.

        debug!("aggregate_by_group_with_hash called");

        if window > 0 {
            // last column on group key should be time.
            assert_eq!(group_columns[group_columns.len() - 1], "time");
        } else {
            assert_ne!(group_columns[group_columns.len() - 1], "time");
        }

        // TODO(edd): Perf - if there is no predicate and we want entire segment
        // then it will be a lot faster to not build filtered_row_ids and just
        // get all encoded values for each grouping column...

        // filter on predicates and time
        let filtered_row_ids: croaring::Bitmap;
        if let Some(row_ids) = self.filter_by_predicates_eq(time_range, predicates) {
            filtered_row_ids = row_ids;
        } else {
            return BTreeMap::new();
        }
        let total_rows = &filtered_row_ids.cardinality();

        // materialise the row ids we need to filter on as a vec.
        let filtered_row_ids_vec = filtered_row_ids
            .to_vec()
            .iter()
            .map(|v| *v as usize)
            .collect::<Vec<_>>();

        // materialise all encoded values for the matching rows in the columns
        // we are grouping on and store each group as an iterator.
        let mut group_column_encoded_values = Vec::with_capacity(group_columns.len());
        for group_column in group_columns {
            if let Some(column) = self.column(group_column) {
                let encoded_values = if filtered_row_ids_vec.len() == self.meta.rows {
                    column.all_encoded_values()
                } else {
                    column.encoded_values(&filtered_row_ids_vec)
                };

                assert_eq!(
                    filtered_row_ids.cardinality() as usize,
                    encoded_values.len()
                );
                group_column_encoded_values.push(encoded_values);
            } else {
                panic!("need to handle no results for filtering/grouping...");
            }
        }

        // TODO(edd): we could do this with an iterator I expect.
        //
        // materialise all decoded values for the rows in the columns we are
        // aggregating on.
        let mut aggregate_column_decoded_values = Vec::with_capacity(aggregates.len());
        for (column_name, _) in aggregates {
            // let column_name: &String = column_name;

            if let Some(column) = self.column(column_name) {
                let decoded_values = column.values(&filtered_row_ids_vec);
                assert_eq!(
                    filtered_row_ids.cardinality() as usize,
                    decoded_values.len()
                );
                aggregate_column_decoded_values.push((column_name, Some(decoded_values)));
            } else {
                aggregate_column_decoded_values.push((column_name, None));
            }
        }

        // now we have all the matching rows for each grouping column and each
        // aggregation column. Materialised values for grouping are in encoded
        // form.
        //
        // Next we iterate all rows in all columns and create a hash entry with
        // running aggregates.

        // First we will build a collection of iterators over the columns we
        // are grouping on. For columns that have no matching rows from the
        // filtering stage we will just emit None.
        let mut group_itrs = group_column_encoded_values
            .into_iter()
            .map(|vector| match vector {
                column::Vector::Unsigned32(_) => column::VectorIterator::new(vector), // encoded tag columns
                column::Vector::Integer(_) => column::VectorIterator::new(vector), // encoded (but actually just raw) timestamp column
                _ => panic!("don't support grouping on non-encoded values or timestamps"),
            })
            .collect::<Vec<_>>();

        // Next we will build a collection of iterators over the columns we
        // are aggregating on. For columns that have no matching rows from the
        // filtering stage we will just emit None.
        let mut aggregate_itrs = aggregate_column_decoded_values
            .into_iter()
            .map(|(col_name, values)| match values {
                Some(values) => (col_name.as_str(), Some(column::VectorIterator::new(values))),
                None => (col_name.as_str(), None),
            })
            .collect::<Vec<_>>();

        // hashMap is about 20% faster than BTreeMap in this case
        let mut hash_table: BTreeMap<Vec<i64>, Vec<(&String, column::Aggregate)>> = BTreeMap::new();

        let mut aggregate_row: Vec<(&str, Option<column::Scalar>)> =
            std::iter::repeat_with(|| ("", None))
                .take(aggregate_itrs.len())
                .collect();

        let group_itrs_len = &group_itrs.len();
        let mut group_key: Vec<i64> = vec![0; *group_itrs_len];

        let mut processed_rows = 0;
        while processed_rows < *total_rows {
            group_itrs.iter_mut().enumerate().for_each(|(i, itr)| {
                if i == group_itrs_len - 1 && window > 0 {
                    // time column - apply window function
                    //
                    // TODO(edd): this is assuming non-null timestamps
                    if let Some(Some(column::Scalar::Integer(v))) = itr.next() {
                        group_key[i] = v / window * window;
                    } else {
                        unreachable!(
                            "something broken with grouping! Either processed None or wrong type"
                        );
                    }
                // The double Some is ok because encoded values are always
                // non-null
                } else if let Some(Some(column::Scalar::Unsigned32(v))) = itr.next() {
                    group_key[i] = v as i64
                } else {
                    unreachable!(
                        "something broken with grouping! Either processed None or wrong type"
                    );
                }
            });

            // re-use aggregate_row vector.
            for (i, &mut (col_name, ref mut itr)) in aggregate_itrs.iter_mut().enumerate() {
                match itr {
                    Some(itr) => {
                        // This is clunky. We don't need to check for the sentinel None value
                        // to indicate the end of the iterator because we use the guard in
                        // the while loop to do so.
                        aggregate_row[i] = (col_name, itr.next().unwrap_or(None));
                    }
                    None => aggregate_row[i] = (col_name, None),
                }
            }

            // This is cheaper than allocating a key and using the entry API
            if !hash_table.contains_key(&group_key) {
                let mut agg_results: Vec<(&String, column::Aggregate)> =
                    Vec::with_capacity(aggregates.len());
                for (col_name, agg_type) in aggregates {
                    agg_results.push((
                        col_name,
                        match agg_type {
                            AggregateType::Count => column::Aggregate::Count(0),
                            AggregateType::Sum => column::Aggregate::Sum(None),
                        },
                    ));
                }
                hash_table.insert(group_key.clone(), agg_results);
            }
            let group_key_entry = hash_table.get_mut(&group_key).unwrap();

            // Update aggregates - we process each row value and for each one
            // check which aggregates apply to it.
            //
            // TODO(edd): this is probably a bit of a perf suck.
            for (col_name, row_value) in &aggregate_row {
                for &mut (cum_col_name, ref mut cum_agg_value) in group_key_entry.iter_mut() {
                    if col_name != cum_col_name {
                        continue;
                    }

                    match cum_agg_value {
                        column::Aggregate::Count(x) => {
                            *x += 1;
                        }
                        column::Aggregate::Sum(v) => {
                            if let Some(row_value) = row_value {
                                match v {
                                    Some(x) => {
                                        *x += row_value;
                                    }
                                    None => *v = Some(row_value.clone()),
                                }
                            }
                        }
                    }
                }
            }
            processed_rows += 1;
        }

        debug!("({:?} rows processed) {:?}", processed_rows, hash_table);
        BTreeMap::new()
        // hash_table
    }

    pub fn aggregate_by_group_using_sort(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        group_columns: &[String],
        aggregates: &[(String, AggregateType)],
        window: i64,
    ) -> Vec<GroupedAggregates> {
        debug!("aggregate_by_group_with_sort_unsorted called");

        if window > 0 {
            // last column on group key should be time.
            assert_eq!(group_columns[group_columns.len() - 1], "time");
        } else {
            assert_ne!(group_columns[group_columns.len() - 1], "time");
        }

        // TODO(edd): Perf - if there is no predicate and we want entire segment
        // then it will be a lot faster to not build filtered_row_ids and just
        // get all encoded values for each grouping column...

        // filter on predicates and time
        let filtered_row_ids: croaring::Bitmap;
        if let Some(row_ids) = self.filter_by_predicates_eq(time_range, predicates) {
            filtered_row_ids = row_ids;
        } else {
            return vec![];
        }
        let total_rows = &filtered_row_ids.cardinality();

        let filtered_row_ids_vec = filtered_row_ids
            .to_vec()
            .iter()
            .map(|v| *v as usize)
            .collect::<Vec<_>>();
        debug!("filtered to {:?} rows.", filtered_row_ids_vec.len());

        // materialise all encoded values for the matching rows in the columns
        // we are grouping on and store each group as an iterator.
        let mut group_column_encoded_values = Vec::with_capacity(group_columns.len());
        for group_column in group_columns {
            if let Some(column) = self.column(group_column) {
                let encoded_values = if filtered_row_ids_vec.len() == self.meta.rows {
                    column.all_encoded_values()
                } else {
                    column.encoded_values(&filtered_row_ids_vec)
                };
                assert_eq!(
                    filtered_row_ids.cardinality() as usize,
                    encoded_values.len()
                );
                group_column_encoded_values.push(Some(encoded_values));
            } else {
                group_column_encoded_values.push(None);
            }
        }

        // TODO(edd): we could do this with an iterator I expect.
        //
        // materialise all decoded values for the rows in the columns we are
        // aggregating on.
        let mut aggregate_column_decoded_values = Vec::with_capacity(aggregates.len());
        for (column_name, _) in aggregates {
            if let Some(column) = self.column(column_name) {
                let decoded_values = column.values(&filtered_row_ids_vec);
                assert_eq!(
                    filtered_row_ids.cardinality() as usize,
                    decoded_values.len()
                );
                aggregate_column_decoded_values.push((column_name, Some(decoded_values)));
            } else {
                aggregate_column_decoded_values.push((column_name, None));
            }
        }

        let mut all_columns = Vec::with_capacity(
            group_column_encoded_values.len() + aggregate_column_decoded_values.len(),
        );

        for gc in group_column_encoded_values {
            if let Some(p) = gc {
                all_columns.push(p);
            } else {
                panic!("need to handle no results for filtering/grouping...");
            }
        }

        for ac in aggregate_column_decoded_values {
            if let (_, Some(p)) = ac {
                all_columns.push(p);
            } else {
                panic!("need to handle no results for filtering/grouping...");
            }
        }

        let now = std::time::Instant::now();
        assert!(!self.group_key_sorted(group_columns)); // should always need a sort if in this method
        debug!("time checking sort {:?}", now.elapsed());

        // now sort on the first grouping columns. Right now the order doesn't matter...
        let group_col_sort_order = &(0..group_columns.len()).collect::<Vec<_>>();
        super::sorter::sort(&mut all_columns, group_col_sort_order).unwrap();

        let mut group_vecs = Vec::with_capacity(group_columns.len());
        let mut agg_vecs = Vec::with_capacity(aggregates.len());
        for (i, vec) in all_columns.into_iter().enumerate() {
            if i < group_columns.len() {
                group_vecs.push(vec);
            } else {
                agg_vecs.push(vec);
            }
        }

        let mut aggregate_cols = Vec::with_capacity(aggregates.len());
        for (sorted_vector, agg) in agg_vecs
            .into_iter()
            .skip(group_columns.len())
            .zip(aggregates.iter())
        {
            aggregate_cols.push((agg.0.clone(), agg.1.clone(), sorted_vector));
        }

        let group_itrs = group_vecs
            .into_iter()
            .take(group_columns.len())
            .map(|vector| match vector {
                column::Vector::Unsigned32(_) => {
                    column::VectorIterator::new(vector) // encoded tag columns
                }
                column::Vector::Integer(_) => column::VectorIterator::new(vector), // encoded (but actually just raw) timestamp column
                _ => panic!("don't support grouping on non-encoded values or timestamps"),
            })
            .collect::<Vec<_>>();

        Self::stream_grouped_aggregates(group_itrs, aggregate_cols, *total_rows as usize, window)
    }

    // Executes aggregates grouping by group_columns. If window is positive then
    // a windowed aggregate result set is produced.
    //
    // `aggregate_by_group_using_stream` assumes that all columns being grouped
    // on are part of the overall segment sort, therefore it does no sorting or
    // hashing, and just streams aggregates out in order.
    pub fn aggregate_by_group_using_stream(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        group_columns: &[String],
        aggregates: &[(String, AggregateType)],
        window: i64,
    ) -> Vec<GroupedAggregates> {
        debug!("aggregate_by_group_using_stream called");

        if window > 0 {
            // last column on group key should be time.
            assert_eq!(group_columns[group_columns.len() - 1], "time");
        } else {
            assert_ne!(group_columns[group_columns.len() - 1], "time");
        }

        // TODO(edd): Perf - if there is no predicate and we want entire segment
        // then it will be a lot faster to not build filtered_row_ids and just
        // get all encoded values for each grouping column...

        // filter on predicates and time
        let filtered_row_ids: croaring::Bitmap;
        if let Some(row_ids) = self.filter_by_predicates_eq(time_range, predicates) {
            filtered_row_ids = row_ids;
        } else {
            return vec![];
        }
        let total_rows = &filtered_row_ids.cardinality();

        let filtered_row_ids_vec = filtered_row_ids
            .to_vec()
            .iter()
            .map(|v| *v as usize)
            .collect::<Vec<_>>();
        debug!("filtered to {:?} rows.", filtered_row_ids_vec.len());

        // materialise all encoded values for the matching rows in the columns
        // we are grouping on and store each group as an iterator.
        let mut group_column_encoded_values = Vec::with_capacity(group_columns.len());
        for group_column in group_columns {
            if let Some(column) = self.column(group_column) {
                let encoded_values = if filtered_row_ids_vec.len() == self.meta.rows {
                    column.all_encoded_values()
                } else {
                    column.encoded_values(&filtered_row_ids_vec)
                };
                assert_eq!(
                    filtered_row_ids.cardinality() as usize,
                    encoded_values.len()
                );

                group_column_encoded_values.push(encoded_values);
            } else {
                panic!("need to handle no results for filtering/grouping...");
            }
        }

        let group_itrs = group_column_encoded_values
            .into_iter()
            .map(|vector| match vector {
                column::Vector::Unsigned32(_) => column::VectorIterator::new(vector), // encoded tag columns
                column::Vector::Integer(_) => column::VectorIterator::new(vector), // encoded (but actually just raw) timestamp column
                _ => panic!("don't support grouping on non-encoded values or timestamps"),
            })
            .collect::<Vec<_>>();

        let mut aggregate_cols = Vec::with_capacity(aggregates.len());
        for (column_name, agg_type) in aggregates {
            aggregate_cols.push((
                column_name.clone(),
                agg_type.clone(),
                self.column(column_name).unwrap(),
            ));
        }

        Self::stream_grouped_aggregates(group_itrs, aggregate_cols, *total_rows as usize, window)
    }

    // Once the rows necessary for doing a (windowed) grouped aggregate are
    // available and appropriately sorted this method will build a result set of
    // aggregates in a streaming way.
    pub fn stream_grouped_aggregates(
        mut group_itrs: Vec<column::VectorIterator>,
        aggregate_cols: Vec<(String, AggregateType, impl column::AggregatableByRange)>,
        total_rows: usize,
        window: i64,
    ) -> Vec<GroupedAggregates> {
        // this tracks the last seen group key row. When it changes we can emit
        // the grouped aggregates.
        let group_itrs_len = &group_itrs.len();
        let mut last_group_row = group_itrs
            .iter_mut()
            .enumerate()
            .map(|(i, itr)| {
                if i == group_itrs_len - 1 && window > 0 {
                    // time column - apply window function
                    //
                    // TODO(edd): this is assuming non-null time column
                    if let Some(Some(column::Scalar::Integer(v))) = itr.next() {
                        v / window * window
                    } else {
                        unreachable!(
                            "something broken with grouping! Either processed None or wrong type"
                        );
                    }
                // the double some should be ok as encoded values can never be
                // None
                } else if let Some(Some(column::Scalar::Unsigned32(v))) = itr.next() {
                    v as i64
                } else {
                    unreachable!(
                        "something broken with grouping! Either processed None or wrong type"
                    );
                }
            })
            .collect::<Vec<_>>();

        let mut curr_group_row = last_group_row.clone();

        let mut results = vec![];

        let mut processed_rows = 1;

        let mut group_key_start_row_id = 0;
        let mut group_size = 0;

        while processed_rows < total_rows {
            // update next group key.
            let mut group_key_changed = false;
            for (i, (curr_v, itr)) in curr_group_row
                .iter_mut()
                .zip(group_itrs.iter_mut())
                .enumerate()
            {
                let next_v = if i == group_itrs_len - 1 && window > 0 {
                    // time column - apply window function
                    if let Some(Some(column::Scalar::Integer(v))) = itr.next() {
                        v / window * window
                    } else {
                        unreachable!(
                            "something broken with grouping! Either processed None or wrong type"
                        );
                    }
                } else if let Some(Some(column::Scalar::Unsigned32(v))) = itr.next() {
                    v as i64
                } else {
                    unreachable!(
                        "something broken with grouping! Either processed None or wrong type"
                    );
                };

                if curr_v != &next_v {
                    group_key_changed = true;
                }
                *curr_v = next_v;
            }

            // group key changed - emit group row and aggregates.
            if group_key_changed {
                let mut group_key_aggregates = Vec::with_capacity(aggregate_cols.len());
                for (name, agg_type, vector) in &aggregate_cols {
                    let agg_result = vector.aggregate_by_id_range(
                        agg_type,
                        group_key_start_row_id,
                        group_key_start_row_id + group_size,
                    );

                    group_key_aggregates.push((name.clone(), agg_result));
                }

                results.push(GroupedAggregates {
                    group_key: last_group_row,
                    aggregates: group_key_aggregates,
                });

                // update group key
                last_group_row = curr_group_row.clone();

                // reset counters tracking group key row range
                group_key_start_row_id = processed_rows as usize; // TODO(edd) - could be an off-by-one?
                group_size = 0;
            }

            group_size += 1;
            processed_rows += 1;
        }

        // Emit final row
        let mut group_key_aggregates = Vec::with_capacity(aggregate_cols.len());
        for (name, agg_type, vector) in &aggregate_cols {
            let agg_result = vector.aggregate_by_id_range(
                agg_type,
                group_key_start_row_id,
                group_key_start_row_id + group_size,
            );

            // TODO(edd): fix weirdness
            group_key_aggregates.push((name.clone(), agg_result));
        }

        results.push(GroupedAggregates {
            group_key: last_group_row,
            aggregates: group_key_aggregates,
        });

        debug!("({:?} rows processed) {:?}", processed_rows, results);
        // vec![]
        results
    }

    pub fn sum_column(&self, name: &str, row_ids: &mut croaring::Bitmap) -> Option<column::Scalar> {
        if let Some(c) = self.column(name) {
            return c.sum_by_ids(row_ids);
        }
        None
    }

    // Returns the count aggregate for a given column name.
    pub fn count_column(&self, name: &str, row_ids: &mut croaring::Bitmap) -> Option<u64> {
        // call count on column
        if self.column(name).is_some() {
            return Some(row_ids.cardinality() as u64);
        }
        None
    }

    pub fn filter_by_predicates_eq(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
    ) -> Option<croaring::Bitmap> {
        if !self.meta.overlaps_time_range(time_range.0, time_range.1) {
            return None; // segment doesn't have time range
        }

        let (seg_min, seg_max) = self.meta.time_range;
        if time_range.0 <= seg_min && time_range.1 > seg_max {
            // the segment is completely overlapped by the time range of query,
            // so don't  need to intersect predicate results with time column.
            return self.filter_by_predicates_eq_no_time(predicates);
        }
        self.filter_by_predicates_eq_time(time_range, predicates)
    }

    fn filter_by_predicates_eq_time(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
    ) -> Option<croaring::Bitmap> {
        // Get all row_ids matching the time range:
        //
        //  time > time_range.0 AND time < time_range.1
        let mut bm = self.columns[self.time_column_idx].row_ids_gte_lt(
            &column::Value::Scalar(column::Scalar::Integer(time_range.0)),
            &column::Value::Scalar(column::Scalar::Integer(time_range.1)),
        )?;
        debug!("time col bitmap contains {:?} values out of {:?} rows. requested range was {:?}, meta range is {:?}",bm.cardinality(),self.num_rows(), time_range, self.meta.time_range);

        // now intersect matching rows for each column
        for (col_pred_name, col_pred_value) in predicates {
            if let Some(c) = self.column(col_pred_name) {
                match c.row_ids_eq(&column::Value::String(col_pred_value)) {
                    Some(row_ids) => {
                        if row_ids.is_empty() {
                            return None;
                        }

                        bm.and_inplace(&row_ids);
                        if bm.is_empty() {
                            return None;
                        }
                    }
                    None => return None, // if this predicate doesn't match then no rows match
                }
            }
        }
        Some(bm)
    }

    // in this case the complete time range of segment covered so no need to
    // intersect on time.
    //
    // We return an &Option here because we don't want to move the read-only
    // meta row_ids bitmap.
    fn filter_by_predicates_eq_no_time(
        &self,
        predicates: &[(&str, &str)],
    ) -> Option<croaring::Bitmap> {
        if predicates.is_empty() {
            // In this case there are no predicates provided and we have no time
            // range restrictions - we need to return a bitset for all row ids.
            let mut bm = croaring::Bitmap::create_with_capacity(self.num_rows() as u32);
            bm.add_range(0..self.num_rows() as u64);
            return Some(bm);
        }

        let mut bm: Option<croaring::Bitmap> = None;
        // now intersect matching rows for each column
        for (col_pred_name, col_pred_value) in predicates {
            if let Some(c) = self.column(col_pred_name) {
                match c.row_ids_eq(&column::Value::String(col_pred_value)) {
                    Some(row_ids) => {
                        if row_ids.is_empty() {
                            return None;
                        }

                        if let Some(bm) = &mut bm {
                            bm.and_inplace(&row_ids);
                            if bm.is_empty() {
                                return None;
                            }
                        } else {
                            bm = Some(row_ids);
                        }
                    }
                    None => {
                        return None;
                    } // if this predicate doesn't match then no rows match
                }
            } else {
                return None; // column doesn't exist - no matching rows
            }
        }
        bm
    }

    pub fn group_single_agg_by_predicate_eq(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        group_column: &str,
        aggregates: &[(String, column::AggregateType)],
    ) -> BTreeMap<u32, Vec<((String, AggregateType), column::Aggregate)>> {
        let mut grouped_results = BTreeMap::new();

        let filter_row_ids: croaring::Bitmap;
        match self.filter_by_predicates_eq(time_range, predicates) {
            Some(row_ids) => filter_row_ids = row_ids,
            None => {
                return grouped_results;
            }
        }

        if let Some(grouped_row_ids) = self.group_by_column_ids(group_column) {
            for (group_key_value, row_ids) in grouped_row_ids.iter() {
                let mut filtered_row_ids = row_ids.and(&filter_row_ids);
                if !filtered_row_ids.is_empty() {
                    // First calculate all of the aggregates for this grouped value
                    let mut aggs: Vec<((String, AggregateType), column::Aggregate)> =
                        Vec::with_capacity(aggregates.len());

                    for (col_name, agg) in aggregates {
                        match &agg {
                            AggregateType::Sum => {
                                aggs.push((
                                    (col_name.to_string(), agg.clone()),
                                    column::Aggregate::Sum(
                                        self.sum_column(col_name, &mut filtered_row_ids),
                                    ),
                                ));
                            }
                            AggregateType::Count => {
                                aggs.push((
                                    (col_name.to_string(), agg.clone()),
                                    column::Aggregate::Count(
                                        self.count_column(col_name, &mut filtered_row_ids).unwrap(),
                                    ),
                                ));
                            }
                        }
                    }

                    // Next add these aggregates to the result set, keyed
                    // by the grouped value.
                    assert_eq!(aggs.len(), aggregates.len());
                    grouped_results.insert(*group_key_value, aggs);
                } else {
                    // In this case there are grouped values in the column with no
                    // rows falling into time-range/predicate set.
                    error!(
                        "grouped value {:?} has no rows in time-range/predicate set",
                        group_key_value
                    );
                }
            }
        } else {
            // segment doesn't have the column so can't group on it.
            error!("don't have column - can't group");
        }
        grouped_results
    }

    pub fn tag_keys(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        exclude_columns: &BTreeSet<String>,
    ) -> Option<BTreeSet<String>> {
        // first check if we have any columns not in the exclusion set.
        let mut all_excluded = true;
        for &i in &self.tag_column_idxs {
            let col_name = self.column_names().get(i).unwrap();
            if !exclude_columns.contains(col_name) {
                all_excluded = false;
                break;
            }
        }

        if all_excluded {
            debug!("skipping segment as all tag columns excluded");
            return None; // we don't have any tag columns to offer.
        }

        let (seg_min, seg_max) = self.meta.time_range;
        if predicates.is_empty() && time_range.0 <= seg_min && time_range.1 > seg_max {
            // the segment is completely overlapped by the time range of query,
            // and there are no predicates
            todo!("fast path")
        }

        let filtered_row_ids: croaring::Bitmap;
        if let Some(row_ids) = self.filter_by_predicates_eq(time_range, predicates) {
            filtered_row_ids = row_ids;
        } else {
            return None; // no matching rows for predicate + time range
        }

        let filtered_row_ids_vec = filtered_row_ids
            .to_vec()
            .iter()
            .map(|v| *v as usize)
            .collect::<Vec<_>>();
        debug!("filtered to {:?} rows.", filtered_row_ids_vec.len());
        let mut results = BTreeSet::new();

        // any columns that are in predicate set using equality predicates should
        // be automatically included in results.
        //
        // TODO(edd): when predicates get more complicated it's likely this
        // assumption will be a hard one to make.
        for (col, _) in predicates {
            results.insert(String::from(*col));
        }

        // now check if any of the other tag columns have a non-null value for
        // any of the filtered ids.
        for &i in &self.tag_column_idxs {
            let col = &self.columns[i];
            let col_name = self.column_names().get(i).unwrap();

            if exclude_columns.contains(col_name) {
                continue;
            }

            if col.has_non_null_value_in_row_ids(&filtered_row_ids_vec) {
                results.insert(col_name.clone());
            }
        }

        Some(results)
    }

    pub fn tag_values(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        tag_keys: &[String],
        excluded_tag_values: &BTreeMap<String, BTreeSet<&String>>,
    ) -> Option<BTreeMap<&String, BTreeSet<&String>>> {
        // first check if we have any columns that should be processed.
        let mut have_some_cols = false;
        for &i in &self.tag_column_idxs {
            let col_name = self.column_names().get(i).unwrap();
            if tag_keys.contains(col_name) {
                have_some_cols = true;
                break;
            }
        }

        if !have_some_cols {
            debug!("skipping segment because no columns for tag keys present");
            return None; // we don't have any tag columns to offer.
        }

        let (seg_min, seg_max) = self.meta.time_range;
        if predicates.is_empty() && time_range.0 <= seg_min && time_range.1 > seg_max {
            // the segment is completely overlapped by the time range of query,
            // and there are no predicates
            todo!("fast path")
        }

        let filtered_row_ids: croaring::Bitmap;
        if let Some(row_ids) = self.filter_by_predicates_eq(time_range, predicates) {
            filtered_row_ids = row_ids;
        } else {
            return None; // no matching rows for predicate + time range
        }

        let mut results = BTreeMap::new();

        let filtered_row_ids_vec = filtered_row_ids
            .to_vec()
            .iter()
            .map(|v| *v as usize)
            .collect::<Vec<_>>();
        debug!("filtered to {:?} rows.", filtered_row_ids_vec.len());

        for &i in &self.tag_column_idxs {
            let col = &self.columns[i];
            let col_name = self.column_names().get(i).unwrap();

            if !tag_keys.contains(col_name) {
                continue;
            }

            // if !col.contains_other_values(&column::Set::String(
            //     *excluded_tag_values.get(col_name).unwrap(),
            // )) {
            //     debug!("skipping!!");
            //     continue;
            // }

            if let Some(exclude_tag_values) = excluded_tag_values.get(col_name) {
                if !col.contains_other_values(exclude_tag_values) {
                    debug!("skipping!!");
                    continue;
                }
            }

            if let column::Set::String(values) = col.distinct_values(&filtered_row_ids_vec) {
                debug!("distinct values: {:?}", values);
                results.insert(col_name, values);
            } else {
                unreachable!("only works on tag columns");
            }
        }

        Some(results)
    }
}

impl std::fmt::Display for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Rows: {}\nSize: {} Columns: ",
            self.num_rows(),
            self.size()
        )?;
        for (c, name) in self.columns.iter().zip(self.column_names().iter()) {
            writeln!(f, "{} {}", name, c)?;
        }
        Ok(())
    }
}

/// Meta data for a segment. This data is mainly used to determine if a segment
/// may contain a value that can answer a query.
#[derive(Debug)]
pub struct SegmentMetaData {
    size: usize, // TODO
    rows: usize,
    schema: Schema,

    column_names: Vec<String>,
    time_range: (i64, i64),
    /* row_ids is a bitmap containing all row ids.
     * row_ids: croaring::Bitmap,
     * TODO column sort order */
}

impl SegmentMetaData {
    pub fn new(rows: usize, schema: Schema) -> Self {
        Self {
            size: 0,
            rows,
            schema,
            column_names: vec![],
            time_range: (0, 0),
            // row_ids: croaring::Bitmap::create_with_capacity(rows as u32),
        }
        // meta.row_ids.add_range(0..rows as u64);
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.schema_ref()
    }

    pub fn overlaps_time_range(&self, from: i64, to: i64) -> bool {
        self.time_range.0 <= to && from <= self.time_range.1
    }
}

pub struct Segments<'a> {
    segments: Vec<&'a Segment>,
}

impl<'a> Segments<'a> {
    pub fn new(segments: Vec<&'a Segment>) -> Self {
        Self { segments }
    }

    pub fn segments(&self) -> &Vec<&'a Segment> {
        &self.segments
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn filter_by_time(&self, min: i64, max: i64) -> Segments<'a> {
        let mut segments: Vec<&Segment> = vec![];
        for segment in &self.segments {
            if segment.meta.overlaps_time_range(min, max) {
                segments.push(segment);
            }
        }
        Self::new(segments)
    }

    // read_filter_eq returns rows of data for the desired columns. Results may
    // be filtered by (currently) equality predicates and ranged by time.
    pub fn read_filter_eq(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        select_columns: Vec<String>,
    ) -> BTreeMap<String, column::Values<'_>> {
        let (min, max) = time_range;
        if max <= min {
            panic!("max <= min");
        }

        let mut columns: BTreeMap<String, column::Values<'_>> = BTreeMap::new();
        for segment in &self.segments {
            if !segment.meta.overlaps_time_range(min, max) {
                continue; // segment doesn't have time range
            }
            if let Some(bm) = segment.filter_by_predicates_eq(time_range, predicates) {
                let rows = segment.rows(&bm, &select_columns);
                for (k, v) in rows {
                    let segment_values = columns.get_mut(&k);
                    match segment_values {
                        Some(values) => values.extend(v),
                        None => {
                            columns.insert(k.to_owned(), v);
                        }
                    }
                }
            };
        }

        columns
    }

    // read_group_eq returns grouped aggregates of for the specified columns.
    // Results may be filtered by (currently) equality predicates and ranged
    // by time.
    pub fn read_group_eq(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        group_columns: Vec<String>,
        aggregates: Vec<(String, AggregateType)>,
        window: i64,
        strategy: &GroupingStrategy,
    ) -> BTreeMap<Vec<String>, Vec<((String, column::AggregateType), column::Aggregate)>> {
        let (min, max) = time_range;
        if max <= min {
            panic!("max <= min");
        }

        match strategy {
            GroupingStrategy::HashGroup => self.read_group_eq_hash(
                time_range,
                predicates,
                group_columns,
                aggregates,
                window,
                false,
            ),
            GroupingStrategy::HashGroupConcurrent => self.read_group_eq_hash(
                time_range,
                predicates,
                group_columns,
                aggregates,
                window,
                true,
            ),
            GroupingStrategy::SortGroup => self.read_group_eq_sort(
                time_range,
                predicates,
                group_columns,
                aggregates,
                window,
                false,
            ),
            GroupingStrategy::SortGroupConcurrent => self.read_group_eq_sort(
                time_range,
                predicates,
                group_columns,
                aggregates,
                window,
                true,
            ),
        }
    }

    fn read_group_eq_hash(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        mut group_columns: Vec<String>,
        aggregates: Vec<(String, AggregateType)>,
        window: i64,
        concurrent: bool,
    ) -> BTreeMap<Vec<String>, Vec<((String, column::AggregateType), column::Aggregate)>> {
        if window > 0 {
            // add time column to the group key
            group_columns.push("time".to_string());
        }

        if concurrent {
            // let group_columns_arc = std::sync::Arc::new(group_columns);
            // let aggregates_arc = std::sync::Arc::new(aggregates);

            for chunked_segments in self.segments.chunks(THREADS) {
                crossbeam::scope(|scope| {
                    for segment in chunked_segments {
                        let group_columns = &group_columns;
                        let aggregates = &aggregates;

                        scope.spawn(move |_| {
                            let now = std::time::Instant::now();
                            segment.aggregate_by_group_with_hash(
                                time_range,
                                predicates,
                                group_columns,
                                aggregates,
                                window,
                            );
                            info!(
                                "processed segment {:?} using multi-threaded hash-grouping in {:?}",
                                segment.time_range(),
                                now.elapsed()
                            )
                        });
                    }
                })
                .unwrap();
            }

            let rem = self.segments.len() % THREADS;
            for segment in &self.segments[self.segments.len() - rem..] {
                let now = std::time::Instant::now();
                segment.aggregate_by_group_with_hash(
                    time_range,
                    predicates,
                    &group_columns,
                    &aggregates,
                    window,
                );
                info!(
                    "processed segment {:?} using multi-threaded hash-grouping in {:?}",
                    segment.time_range(),
                    now.elapsed()
                )
            }

            // TODO(edd): aggregate the aggregates. not expensive
            return BTreeMap::new();
        }

        // Single threaded

        for segment in &self.segments {
            let now = std::time::Instant::now();
            segment.aggregate_by_group_with_hash(
                time_range,
                predicates,
                &group_columns,
                &aggregates,
                window,
            );
            info!(
                "processed segment {:?} using single-threaded hash-grouping in {:?}",
                segment.time_range(),
                now.elapsed()
            )
        }

        BTreeMap::new()
    }

    fn read_group_eq_sort(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        mut group_columns: Vec<String>,
        aggregates: Vec<(String, AggregateType)>,
        window: i64,
        concurrent: bool,
    ) -> BTreeMap<Vec<String>, Vec<((String, column::AggregateType), column::Aggregate)>> {
        if window > 0 {
            // add time column to the group key
            group_columns.push("time".to_string());
        }

        if concurrent {
            let group_columns_arc = std::sync::Arc::new(group_columns);
            let aggregates_arc = std::sync::Arc::new(aggregates);

            for chunked_segments in self.segments.chunks(THREADS) {
                crossbeam::scope(|scope| {
                    for segment in chunked_segments {
                        let group_columns = group_columns_arc.clone();
                        let aggregates = aggregates_arc.clone();

                        scope.spawn(move |_| {
                            let sorted = segment.group_key_sorted(&group_columns);

                            let now = std::time::Instant::now();
                            if sorted {
                                segment.aggregate_by_group_using_stream(
                                    time_range,
                                    predicates,
                                    &group_columns,
                                    &aggregates,
                                    window,
                                );
                                info!(
                                    "processed segment {:?} using multi-threaded STREAM in {:?}",
                                    segment.time_range(),
                                    now.elapsed()
                                )
                            } else {
                                segment.aggregate_by_group_using_sort(
                                    time_range,
                                    predicates,
                                    &group_columns,
                                    &aggregates,
                                    window,
                                );
                                info!(
                                    "processed segment {:?} using multi-threaded SORT in {:?}",
                                    segment.time_range(),
                                    now.elapsed()
                                )
                            }
                        });
                    }
                })
                .unwrap();
            }

            let rem = self.segments.len() % THREADS;
            for segment in &self.segments[self.segments.len() - rem..] {
                let group_columns = group_columns_arc.clone();
                let aggregates = aggregates_arc.clone();
                let sorted = segment.group_key_sorted(&group_columns);

                let now = std::time::Instant::now();
                if sorted {
                    segment.aggregate_by_group_using_stream(
                        time_range,
                        predicates,
                        &group_columns,
                        &aggregates,
                        window,
                    );
                    info!(
                        "processed segment {:?} using multi-threaded STREAM in {:?}",
                        segment.time_range(),
                        now.elapsed()
                    )
                } else {
                    segment.aggregate_by_group_using_sort(
                        time_range,
                        predicates,
                        &group_columns,
                        &aggregates,
                        window,
                    );
                    info!(
                        "processed segment {:?} using multi-threaded SORT in {:?}",
                        segment.time_range(),
                        now.elapsed()
                    )
                }
            }

            // TODO(edd): aggregate the aggregates. not expensive
            return BTreeMap::new();
        }

        // Single threaded

        for segment in &self.segments {
            let sorted = segment.group_key_sorted(&group_columns);

            let now = std::time::Instant::now();
            if sorted {
                segment.aggregate_by_group_using_stream(
                    time_range,
                    predicates,
                    &group_columns,
                    &aggregates,
                    window,
                );
                info!(
                    "processed segment {:?} using single-threaded STREAM in {:?}",
                    segment.time_range(),
                    now.elapsed()
                )
            } else {
                segment.aggregate_by_group_using_sort(
                    time_range,
                    predicates,
                    &group_columns,
                    &aggregates,
                    window,
                );
                info!(
                    "processed segment {:?} using single-threaded SORT in {:?}",
                    segment.time_range(),
                    now.elapsed()
                )
            }
        }

        BTreeMap::new()
    }

    /// Returns the minimum value for a column in a set of segments.
    pub fn column_min(&self, column_name: &str) -> column::Value<'_> {
        if self.segments.is_empty() {
            return column::Value::Null;
        }

        let mut min_min = column::Value::Null;
        for segment in &self.segments {
            if let Some(i) = segment.column_names().iter().position(|c| c == column_name) {
                let min = segment.columns[i].min();
                if let column::Value::Null = min_min {
                    min_min = min
                } else if min_min > min {
                    min_min = min;
                }
            }
        }

        min_min
    }

    /// Returns the maximum value for a column in a set of segments.
    pub fn column_max(&self, column_name: &str) -> column::Value<'_> {
        if self.segments.is_empty() {
            return column::Value::Null;
        }

        let mut max_max = column::Value::Null;
        for segment in &self.segments {
            if let Some(i) = segment.column_names().iter().position(|c| c == column_name) {
                let max = segment.columns[i].max();
                if let column::Value::Null = max_max {
                    max_max = max;
                } else if max_max < max {
                    max_max = max;
                }
            }
        }

        max_max
    }

    /// Returns the first value for a column in a set of segments.
    ///
    /// The first value is based on the time column, therefore the returned
    /// value may not be at the end of the column.
    ///
    /// If the time column has multiple max time values then the result is
    /// abitrary.
    ///
    /// TODO(edd): could return NULL value..
    pub fn first(&self, column_name: &str) -> Option<(i64, column::Value<'_>, usize)> {
        // First let's find the segment with the earliest time range.
        // notice we order  a < b on max time range.
        let segment = self
            .segments
            .iter()
            .min_by(|a, b| a.meta.time_range.0.cmp(&b.meta.time_range.0))?;

        // first find the logical row id of the minimum timestamp value
        if let Column::Integer(ts_col) = &segment.columns[segment.time_column_idx] {
            // TODO(edd): clean up unwrap
            let min_ts = ts_col.column_range().unwrap().0;
            assert_eq!(min_ts, segment.meta.time_range.0);

            let min_ts_id = ts_col.row_id_eq_value(min_ts).unwrap();

            // now we have row id we can get value for that row id
            let value = segment.column(column_name).unwrap().value(min_ts_id);
            Some((min_ts, value, min_ts_id))
        } else {
            panic!("time column wrong type!");
        }
    }

    /// Returns the last value for a column in a set of segments.
    ///
    /// The last value is based on the time column, therefore the returned value
    /// may not be at the end of the column.
    ///
    /// If the time column has multiple max time values then the result is
    /// undefined.
    ///
    /// TODO(edd): could return NULL value..
    pub fn last(&self, column_name: &str) -> Option<(i64, column::Value<'_>, usize)> {
        // First let's find the segment with the latest time range.
        // notice we order a > b on max time range.
        let segment = self
            .segments
            .iter()
            .max_by(|a, b| a.meta.time_range.1.cmp(&b.meta.time_range.1))?;

        // first find the logical row id of the minimum timestamp value
        if let Column::Integer(ts_col) = &segment.columns[segment.time_column_idx] {
            // TODO(edd): clean up unwrap
            let max_ts = ts_col.column_range().unwrap().1;
            assert_eq!(max_ts, segment.meta.time_range.1);

            let max_ts_id = ts_col.row_id_eq_value(max_ts).unwrap();

            // now we have row id we can get value for that row id
            let value = segment.column(column_name).unwrap().value(max_ts_id);
            Some((max_ts, value, max_ts_id))
        } else {
            panic!("time column wrong type!");
        }
    }

    /// Returns the distinct set of tag keys (column names) matching the
    /// provided predicates and time range.
    pub fn tag_keys(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
    ) -> BTreeSet<String> {
        let (min, max) = time_range;
        if max <= min {
            panic!("max <= min");
        }

        let mut columns = BTreeSet::new();

        for segment in &self.segments {
            if !segment.meta.overlaps_time_range(min, max) {
                continue; // segment doesn't have time range
            }
            let segment_columns = segment.tag_keys(time_range, predicates, &columns);
            if let Some(mut result) = segment_columns {
                columns.append(&mut result);
            }
        }

        columns
    }

    /// Returns the distinct set of tag values (column values) for each provided
    /// tag key, where each returned value lives in a row matching the provided
    /// predicates and time range.
    ///
    /// As a special case, if no values are provided for `tag_keys` then all
    /// tag key-values are returned for the segments.
    pub fn tag_values(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, &str)],
        tag_keys: &[String],
    ) -> BTreeMap<String, BTreeSet<&String>> {
        let (min, max) = time_range;
        if max <= min {
            panic!("max <= min");
        }

        let mut results: BTreeMap<String, BTreeSet<&String>> = BTreeMap::new();

        for segment in &self.segments {
            if !segment.meta.overlaps_time_range(min, max) {
                continue; // segment doesn't have time range
            }

            let col_names = if tag_keys.is_empty() {
                segment.column_names()
            } else {
                tag_keys
            };

            let segment_values = segment.tag_values(time_range, predicates, col_names, &results);
            match segment_values {
                Some(values) => {
                    for (tag_key, mut tag_values) in values {
                        if !results.contains_key(tag_key) {
                            results.insert(tag_key.clone(), tag_values);
                        } else {
                            let all_values = results.get_mut(tag_key).unwrap();
                            all_values.append(&mut tag_values);
                        }
                    }
                }
                None => continue,
            }
        }

        results
    }
}

#[derive(Debug)]
pub enum GroupingStrategy {
    HashGroup,
    HashGroupConcurrent,
    SortGroup,
    SortGroupConcurrent,
}

#[derive(Debug)]
pub struct GroupedAggregates {
    pub group_key: Vec<i64>,
    pub aggregates: Vec<(String, column::Aggregate)>,
}

#[cfg(test)]
mod test {

    use arrow::datatypes::*;

    #[test]
    fn segment_group_key_sorted() {
        let schema = super::Schema::with_sort_order(
            arrow::datatypes::SchemaRef::new(Schema::new(vec![
                Field::new("env", DataType::Utf8, false),
                Field::new("role", DataType::Utf8, false),
                Field::new("path", DataType::Utf8, false),
                Field::new("time", DataType::Int64, false),
            ])),
            vec![
                "env".to_string(),
                "role".to_string(),
                "path".to_string(),
                "time".to_string(),
            ],
        );
        let s = super::Segment::new(0, schema);

        let cases = vec![
            (vec!["env"], true),
            (vec!["role"], false),
            (vec!["foo"], false),
            (vec![], true),
            (vec!["env", "role"], true),
            (vec!["env", "role", "foo"], false), // group key contains non-sorted col
            (vec!["env", "role", "time"], false), // time may be out of order due to path column
            (vec!["env", "role", "path", "time"], true),
            (vec!["env", "role", "path", "time", "foo"], false), /* group key contains
                                                                  * non-sorted col */
            (vec!["env", "path", "role"], true), // order of columns in group key does not matter
        ];

        for (group_key, expected) in cases {
            assert_eq!(
                s.group_key_sorted(&group_key.iter().map(|x| x.to_string()).collect::<Vec<_>>()),
                expected
            );
        }
    }
}
