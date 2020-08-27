use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::column;
use super::column::{AggregateType, Column};
use arrow::datatypes::SchemaRef;

// Only used in a couple of specific places for experimentation.
const THREADS: usize = 16;

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
    columns: Vec<column::Column>,
    time_column_idx: usize,
}

impl Segment {
    pub fn new(rows: usize, schema: Schema) -> Self {
        let cols = schema.cols();
        Self {
            meta: SegmentMetaData::new(rows, schema),
            columns: Vec::with_capacity(cols),
            time_column_idx: 0,
        }
    }

    pub fn add_column(&mut self, name: &str, c: column::Column) {
        assert_eq!(
            self.meta.rows,
            c.num_rows(),
            "Column {:?} has {:?} rows but wanted {:?}",
            name,
            c.num_rows(),
            self.meta.rows
        );

        // TODO(edd) yuk
        if name == "time" {
            if let column::Column::Integer(ts) = &c {
                self.meta.time_range = ts.column_range();
            } else {
                panic!("incorrect column type for time");
            }
            self.time_column_idx = self.columns.len();
        }

        // validate column doesn't already exist in segment
        assert!(!self.meta.column_names.contains(&name.to_owned()));
        self.meta.column_names.push(name.to_owned());
        self.columns.push(c);
    }

    pub fn num_rows(&self) -> usize {
        self.meta.rows
    }

    pub fn column_names(&self) -> &[String] {
        &self.meta.column_names
    }

    /// column returns the column with name
    pub fn column(&self, name: &str) -> Option<&column::Column> {
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

    pub fn scan_column_from(&self, column_name: &str, row_id: usize) -> Option<column::Vector> {
        if let Some(i) = self.column_names().iter().position(|c| c == column_name) {
            return self.columns[i].scan_from(row_id);
        }
        None
    }

    // Materialise all rows for each desired column.
    //
    // `columns` determines which column values are returned. An empty `columns`
    // value will result in rows for all columns being returned.
    pub fn rows(
        &self,
        row_ids: &croaring::Bitmap,
        columns: &[String],
    ) -> BTreeMap<String, column::Vector> {
        let mut rows: BTreeMap<String, column::Vector> = BTreeMap::new();
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
        predicates: &[(&str, Option<&column::Scalar>)],
        group_columns: &[String],
        aggregates: &[(String, AggregateType)],
    ) -> BTreeMap<Vec<String>, Vec<(String, Option<column::Aggregate>)>> {
        // println!("working segment {:?}", time_range);
        // Build a hash table - essentially, scan columns for matching row ids,
        // emitting the encoded value for each column and track those value
        // combinations in a hashmap with running aggregates.

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
        // println!("TOTAL FILTERED ROWS {:?}", total_rows);

        // materialise all encoded values for the matching rows in the columns
        // we are grouping on and store each group as an iterator.
        let mut group_column_encoded_values = Vec::with_capacity(group_columns.len());
        for group_column in group_columns {
            if let Some(column) = self.column(&group_column) {
                let encoded_values: Vec<i64>;
                if let column::Vector::Integer(vector) =
                    column.encoded_values(&filtered_row_ids_vec)
                {
                    encoded_values = vector;
                } else {
                    unimplemented!("currently you can only group on encoded string columns");
                }

                assert_eq!(
                    filtered_row_ids.cardinality() as usize,
                    encoded_values.len()
                );
                group_column_encoded_values.push(Some(encoded_values));
            } else {
                group_column_encoded_values.push(None);
            }
        }
        // println!("grouped columns {:?}", group_column_encoded_values);

        // TODO(edd): we could do this with an iterator I expect.
        //
        // materialise all decoded values for the rows in the columns we are
        // aggregating on.
        let mut aggregate_column_decoded_values = Vec::with_capacity(aggregates.len());
        for (column_name, _) in aggregates {
            if let Some(column) = self.column(&column_name) {
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

        // now we have all the matching rows for each grouping column and each aggregation
        // column. Materialised values for grouping are in encoded form.
        //
        // Next we iterate all rows in all columns and create a hash entry with
        // running aggregates.

        // First we will build a collection of iterators over the columns we
        // are grouping on. For columns that have no matching rows from the
        // filtering stage we will just emit None.
        let mut group_itrs = group_column_encoded_values
            .iter()
            .map(|x| match x {
                Some(values) => Some(values.iter()),
                None => None,
            })
            .collect::<Vec<_>>();

        // Next we will build a collection of iterators over the columns we
        // are aggregating on. For columns that have no matching rows from the
        // filtering stage we will just emit None.
        let mut aggregate_itrs = aggregate_column_decoded_values
            .iter()
            .map(|(col_name, values)| match values {
                Some(values) => (col_name.as_str(), Some(column::VectorIterator::new(values))),
                None => (col_name.as_str(), None),
            })
            .collect::<Vec<_>>();

        let mut hash_table: HashMap<
            Vec<Option<&i64>>,
            Vec<(&String, &AggregateType, Option<column::Aggregate>)>,
        > = HashMap::with_capacity(30000);

        let mut aggregate_row: Vec<(&str, Option<column::Scalar>)> =
            std::iter::repeat_with(|| ("", None))
                .take(aggregate_itrs.len())
                .collect();

        let mut processed_rows = 0;
        while processed_rows < *total_rows {
            let group_row: Vec<Option<&i64>> = group_itrs
                .iter_mut()
                .map(|x| match x {
                    Some(itr) => itr.next(),
                    None => None,
                })
                .collect();

            // let aggregate_row: Vec<(&str, Option<column::Scalar>)> = aggregate_itrs
            //     .iter_mut()
            //     .map(|&mut (col_name, ref mut itr)| match itr {
            //         Some(itr) => (col_name, itr.next()),
            //         None => (col_name, None),
            //     })
            //     .collect();

            // re-use aggregate_row vector.
            for (i, &mut (col_name, ref mut itr)) in aggregate_itrs.iter_mut().enumerate() {
                match itr {
                    Some(itr) => aggregate_row[i] = (col_name, itr.next()),
                    None => aggregate_row[i] = (col_name, None),
                }
            }

            // Lookup the group key in the hash map - if it's empty then insert
            // a place-holder for each aggregate being executed.
            let group_key_entry = hash_table.entry(group_row).or_insert_with(|| {
                // TODO COULD BE MAP/COLLECT
                let mut agg_results: Vec<(&String, &AggregateType, Option<column::Aggregate>)> =
                    Vec::with_capacity(aggregates.len());
                for (col_name, agg_type) in aggregates {
                    agg_results.push((col_name, agg_type, None)); // switch out Aggregate for Option<column::Aggregate>
                }
                agg_results
            });

            // Update aggregates - we process each row value and for each one
            // check which aggregates apply to it.
            //
            // TODO(edd): this is probably a bit of a perf suck.
            for (col_name, row_value) in &aggregate_row {
                for &mut (cum_col_name, agg_type, ref mut cum_agg_value) in
                    group_key_entry.iter_mut()
                {
                    if col_name != cum_col_name {
                        continue;
                    }

                    // TODO(edd): remove unwrap - it should work because we are
                    // tracking iteration count in loop.
                    let row_value = row_value.as_ref().unwrap();

                    match cum_agg_value {
                        Some(agg) => match agg {
                            column::Aggregate::Count(cum_count) => {
                                *cum_count += 1;
                            }
                            column::Aggregate::Sum(cum_sum) => {
                                *cum_sum += row_value;
                            }
                        },
                        None => {
                            *cum_agg_value = match agg_type {
                                AggregateType::Count => Some(column::Aggregate::Count(0)),
                                AggregateType::Sum => {
                                    Some(column::Aggregate::Sum(row_value.clone()))
                                }
                            }
                        }
                    }
                }
            }
            processed_rows += 1;
        }
        log::debug!("{:?}", hash_table);
        BTreeMap::new()
    }

    pub fn aggregate_by_group_with_sort(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, Option<&column::Scalar>)],
        group_columns: &[String],
        aggregates: &[(String, AggregateType)],
        window: i64,
    ) -> BTreeMap<Vec<&i64>, Vec<(String, column::Aggregate)>> {
        if self.group_key_sorted(group_columns) {
            log::info!("group key is already sorted {:?}", group_columns);
            self.aggregate_by_group_with_sort_sorted(
                time_range,
                predicates,
                group_columns,
                aggregates,
                window,
            )
        } else {
            log::info!("group key needs sorting {:?}", group_columns);
            self.aggregate_by_group_with_sort_unsorted(
                time_range,
                predicates,
                group_columns,
                aggregates,
                window,
            )
        }
    }

    fn aggregate_by_group_with_sort_unsorted(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, Option<&column::Scalar>)],
        group_columns: &[String],
        aggregates: &[(String, AggregateType)],
        window: i64,
    ) -> BTreeMap<Vec<&i64>, Vec<(String, column::Aggregate)>> {
        log::debug!("aggregate_by_group_with_sort_unsorted called");

        if window > 0 {
            // last column on group key should be time.
            assert_eq!(group_columns[group_columns.len() - 1], "time");
        } else {
            assert_ne!(group_columns[group_columns.len() - 1], "time");
        }

        // filter on predicates and time
        let filtered_row_ids: croaring::Bitmap;
        if let Some(row_ids) = self.filter_by_predicates_eq(time_range, predicates) {
            filtered_row_ids = row_ids;
        } else {
            return BTreeMap::new();
        }
        let total_rows = &filtered_row_ids.cardinality();
        // println!("TOTAL FILTERED ROWS {:?}", total_rows);

        let filtered_row_ids_vec = filtered_row_ids
            .to_vec()
            .iter()
            .map(|v| *v as usize)
            .collect::<Vec<_>>();

        // materialise all encoded values for the matching rows in the columns
        // we are grouping on and store each group as an iterator.
        let mut group_column_encoded_values = Vec::with_capacity(group_columns.len());
        for group_column in group_columns {
            if let Some(column) = self.column(&group_column) {
                let encoded_values = column.encoded_values(&filtered_row_ids_vec);
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
            if let Some(column) = self.column(&column_name) {
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
        if self.group_key_sorted(group_columns) {
            panic!("This shouldn't be called!!!");
        } else {
            // now sort on the first grouping columns. Right now the order doesn't matter...
            let group_col_sort_order = &(0..group_columns.len()).collect::<Vec<_>>();
            super::sorter::sort(&mut all_columns, group_col_sort_order).unwrap();
        }
        log::debug!("time checking sort {:?}", now.elapsed());

        let mut group_itrs = all_columns
            .iter()
            .take(group_columns.len()) // only use grouping columns
            .map(|vector| {
                if let column::Vector::Integer(v) = vector {
                    v.iter()
                } else {
                    panic!("don't support grouping on non-encoded values");
                }
            })
            .collect::<Vec<_>>();

        let mut aggregate_itrs = all_columns
            .iter()
            .skip(group_columns.len()) // only use grouping columns
            .map(|v| column::VectorIterator::new(v))
            .collect::<Vec<_>>();

        // this tracks the last seen group key row. When it changes we can emit
        // the grouped aggregates.
        let group_itrs_len = &group_itrs.len();
        let mut last_group_row = group_itrs
            .iter_mut()
            .enumerate()
            .map(|(i, itr)| {
                if i == group_itrs_len - 1 && window > 0 {
                    // time column - apply window function
                    return itr.next().unwrap() / window * window;
                }
                *itr.next().unwrap()
            })
            .collect::<Vec<_>>();

        let mut curr_group_row = last_group_row.clone();

        // this tracks the last row for each column we are aggregating.
        let last_agg_row: Vec<column::Scalar> = aggregate_itrs
            .iter_mut()
            .map(|itr| itr.next().unwrap())
            .collect();

        // this keeps the current cumulative aggregates for the columns we
        // are aggregating.
        let mut cum_aggregates: Vec<(String, column::Aggregate)> = aggregates
            .iter()
            .zip(last_agg_row.iter())
            .map(|((col_name, agg_type), curr_agg)| {
                let agg = match agg_type {
                    AggregateType::Count => column::Aggregate::Count(1),
                    AggregateType::Sum => column::Aggregate::Sum(curr_agg.clone()),
                };
                (col_name.clone(), agg)
            })
            .collect();

        let mut results = BTreeMap::new();
        let mut processed_rows = 1;
        while processed_rows < *total_rows {
            // update next group key.
            let mut group_key_changed = false;
            for (i, (curr_v, itr)) in curr_group_row
                .iter_mut()
                .zip(group_itrs.iter_mut())
                .enumerate()
            {
                let next_v = if i == group_itrs_len - 1 && window > 0 {
                    // time column - apply window function
                    itr.next().unwrap() / window * window
                } else {
                    *itr.next().unwrap()
                };
                if curr_v != &next_v {
                    group_key_changed = true;
                }
                *curr_v = next_v;
            }

            // group key changed - emit group row and aggregates.
            if group_key_changed {
                let key = last_group_row.clone();
                results.insert(key, cum_aggregates.clone());

                // update group key
                last_group_row = curr_group_row.clone();

                // reset cumulative aggregates
                for (_, agg) in cum_aggregates.iter_mut() {
                    match agg {
                        column::Aggregate::Count(c) => {
                            *c = 0;
                        }
                        column::Aggregate::Sum(s) => s.reset(),
                    }
                }
            }

            // update aggregates
            for bind in cum_aggregates.iter_mut().zip(&mut aggregate_itrs) {
                let (_, curr_agg) = bind.0;
                let next_value = bind.1.next().unwrap();
                curr_agg.update_with(next_value);
            }

            processed_rows += 1;
        }

        // Emit final row
        results.insert(last_group_row, cum_aggregates);

        log::info!("({:?} rows processed) {:?}", processed_rows, results);
        // results
        BTreeMap::new()
    }

    // this method assumes that the segment's columns are sorted such that a
    // sort of columns is not required.
    fn aggregate_by_group_with_sort_sorted(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, Option<&column::Scalar>)],
        group_columns: &[String],
        aggregates: &[(String, AggregateType)],
        window: i64,
    ) -> BTreeMap<Vec<&i64>, Vec<(String, column::Aggregate)>> {
        log::debug!("aggregate_by_group_with_sort_sorted called");

        if window > 0 {
            // last column on group key should be time.
            assert_eq!(group_columns[group_columns.len() - 1], "time");
        } else {
            assert_ne!(group_columns[group_columns.len() - 1], "time");
        }

        // filter on predicates and time
        let filtered_row_ids: croaring::Bitmap;
        if let Some(row_ids) = self.filter_by_predicates_eq(time_range, predicates) {
            filtered_row_ids = row_ids;
        } else {
            return BTreeMap::new();
        }
        let total_rows = &filtered_row_ids.cardinality();

        let filtered_row_ids_vec = filtered_row_ids
            .to_vec()
            .iter()
            .map(|v| *v as usize)
            .collect::<Vec<_>>();

        // materialise all encoded values for the matching rows in the columns
        // we are grouping on and store each group as an iterator.
        let mut group_column_encoded_values = Vec::with_capacity(group_columns.len());
        for group_column in group_columns {
            if let Some(column) = self.column(&group_column) {
                let encoded_values = column.encoded_values(&filtered_row_ids_vec);
                assert_eq!(
                    filtered_row_ids.cardinality() as usize,
                    encoded_values.len()
                );

                group_column_encoded_values.push(encoded_values);
            } else {
                panic!("need to handle no results for filtering/grouping...");
            }
        }

        let mut new_agg_cols = Vec::with_capacity(aggregates.len());
        for (column_name, agg_type) in aggregates {
            new_agg_cols.push((column_name, agg_type, self.column(&column_name)));
        }

        let mut group_itrs = group_column_encoded_values
            .iter()
            .map(|vector| {
                if let column::Vector::Integer(v) = vector {
                    v.iter()
                } else {
                    panic!("don't support grouping on non-encoded values");
                }
            })
            .collect::<Vec<_>>();

        // this tracks the last seen group key row. When it changes we can emit
        // the grouped aggregates.
        let group_itrs_len = &group_itrs.len();
        let mut last_group_row = group_itrs
            .iter_mut()
            .enumerate()
            .map(|(i, itr)| {
                if i == group_itrs_len - 1 && window > 0 {
                    // time column - apply window function
                    return itr.next().unwrap() / window * window;
                }
                *itr.next().unwrap()
            })
            .collect::<Vec<_>>();

        let mut curr_group_row = last_group_row.clone();

        let mut results = BTreeMap::new();
        let mut processed_rows = 1;

        let mut group_key_start_row_id = 0;
        let mut group_size = 0;

        while processed_rows < *total_rows {
            // update next group key.
            let mut group_key_changed = false;
            for (i, (curr_v, itr)) in curr_group_row
                .iter_mut()
                .zip(group_itrs.iter_mut())
                .enumerate()
            {
                let next_v = if i == group_itrs_len - 1 && window > 0 {
                    // time column - apply window function
                    itr.next().unwrap() / window * window
                } else {
                    *itr.next().unwrap()
                };
                if curr_v != &next_v {
                    group_key_changed = true;
                }
                *curr_v = next_v;
            }

            // group key changed - emit group row and aggregates.
            if group_key_changed {
                let mut group_key_aggregates = Vec::with_capacity(aggregates.len());
                for (name, agg_type, col) in &new_agg_cols {
                    if let Some(c) = col {
                        let agg_result = c.aggregate_by_id_range(
                            agg_type,
                            group_key_start_row_id,
                            group_key_start_row_id + group_size,
                        );
                        group_key_aggregates.push((name, agg_result));
                    } else {
                        panic!("figure this out");
                    }
                }

                let key = last_group_row.clone();
                results.insert(key, group_key_aggregates);

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
        let mut group_key_aggregates = Vec::with_capacity(aggregates.len());
        for (name, agg_type, col) in &new_agg_cols {
            if let Some(c) = col {
                let agg_result = c.aggregate_by_id_range(
                    agg_type,
                    group_key_start_row_id,
                    group_key_start_row_id + group_size,
                );
                group_key_aggregates.push((name, agg_result));
            } else {
                panic!("figure this out");
            }
        }

        let key = last_group_row;
        results.insert(key, group_key_aggregates);

        log::info!("({:?} rows processed) {:?}", processed_rows, results);
        // results
        BTreeMap::new()
    }

    pub fn window_aggregate_with_sort(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, Option<&column::Scalar>)],
        group_columns: &[String],
        aggregates: &[(String, AggregateType)],
        window: i64,
    ) -> BTreeMap<Vec<&i64>, Vec<(String, column::Aggregate)>> {
        if self.group_key_sorted(group_columns) {
            log::info!("group key is already sorted {:?}", group_columns);
            self.window_aggregate_with_sort_sorted(
                time_range,
                predicates,
                group_columns,
                aggregates,
                window,
            )
        } else {
            log::info!("group key needs sorting {:?}", group_columns);
            self.window_aggregate_with_sort_unsorted(
                time_range,
                predicates,
                group_columns,
                aggregates,
                window,
            )
        }
    }

    // this method assumes that the segment's columns are sorted such that a
    // sort of columns is not required.
    fn window_aggregate_with_sort_sorted(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, Option<&column::Scalar>)],
        group_columns: &[String],
        aggregates: &[(String, AggregateType)],
        window: i64,
    ) -> BTreeMap<Vec<&i64>, Vec<(String, column::Aggregate)>> {
        // filter on predicates and time
        let filtered_row_ids: croaring::Bitmap;
        if let Some(row_ids) = self.filter_by_predicates_eq(time_range, predicates) {
            filtered_row_ids = row_ids;
        } else {
            return BTreeMap::new();
        }
        let total_rows = &filtered_row_ids.cardinality();

        let filtered_row_ids_vec = filtered_row_ids
            .to_vec()
            .iter()
            .map(|v| *v as usize)
            .collect::<Vec<_>>();

        // materialise all encoded values for the matching rows in the columns
        // we are grouping on and store each group as an iterator.
        let mut group_column_encoded_values = Vec::with_capacity(group_columns.len());
        for group_column in group_columns {
            if let Some(column) = self.column(&group_column) {
                let encoded_values = column.encoded_values(&filtered_row_ids_vec);
                assert_eq!(
                    filtered_row_ids.cardinality() as usize,
                    encoded_values.len()
                );

                group_column_encoded_values.push(encoded_values);
            } else {
                panic!("need to handle no results for filtering/grouping...");
            }
        }

        let mut new_agg_cols = Vec::with_capacity(aggregates.len());
        for (column_name, agg_type) in aggregates {
            new_agg_cols.push((column_name, agg_type, self.column(&column_name)));
        }

        let mut group_itrs = group_column_encoded_values
            .iter()
            .map(|vector| {
                if let column::Vector::Integer(v) = vector {
                    v.iter()
                } else {
                    panic!("don't support grouping on non-encoded values or time");
                }
            })
            .collect::<Vec<_>>();

        // this tracks the last seen group key row. When it changes we can emit
        // the grouped aggregates.
        let group_itrs_len = &group_itrs.len();
        let mut last_group_row = group_itrs
            .iter_mut()
            .enumerate()
            .map(|(i, itr)| {
                if i == group_itrs_len - 1 {
                    // time column - apply window function
                    return itr.next().unwrap() / window * window;
                }
                *itr.next().unwrap()
            })
            .collect::<Vec<_>>();

        let mut curr_group_row = last_group_row.clone();

        let mut results = BTreeMap::new();
        let mut processed_rows = 1;

        let mut group_key_start_row_id = 0;
        let mut group_size = 0;

        while processed_rows < *total_rows {
            // update next group key.
            let mut group_key_changed = false;
            for (i, (curr_v, itr)) in curr_group_row
                .iter_mut()
                .zip(group_itrs.iter_mut())
                .enumerate()
            {
                let next_v = if i == group_itrs_len - 1 {
                    // time column - apply window function
                    itr.next().unwrap() / window * window
                } else {
                    *itr.next().unwrap()
                };
                if *curr_v != next_v {
                    group_key_changed = true;
                }
                *curr_v = next_v;
            }

            // group key changed - emit group row and aggregates.
            if group_key_changed {
                let mut group_key_aggregates = Vec::with_capacity(aggregates.len());
                for (name, agg_type, col) in &new_agg_cols {
                    if let Some(c) = col {
                        let agg_result = c.aggregate_by_id_range(
                            agg_type,
                            group_key_start_row_id,
                            group_key_start_row_id + group_size,
                        );
                        group_key_aggregates.push((name, agg_result));
                    } else {
                        panic!("figure this out");
                    }
                }

                let key = last_group_row.clone();
                results.insert(key, group_key_aggregates);

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
        let mut group_key_aggregates = Vec::with_capacity(aggregates.len());
        for (name, agg_type, col) in &new_agg_cols {
            if let Some(c) = col {
                let agg_result = c.aggregate_by_id_range(
                    agg_type,
                    group_key_start_row_id,
                    group_key_start_row_id + group_size,
                );
                group_key_aggregates.push((name, agg_result));
            } else {
                panic!("figure this out");
            }
        }

        let key = last_group_row;
        results.insert(key, group_key_aggregates);

        log::info!("({:?} rows processed) {:?}", processed_rows, results);
        // results
        BTreeMap::new()
    }

    fn window_aggregate_with_sort_unsorted(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, Option<&column::Scalar>)],
        group_columns: &[String],
        aggregates: &[(String, AggregateType)],
        window: i64,
    ) -> BTreeMap<Vec<&i64>, Vec<(String, column::Aggregate)>> {
        // filter on predicates and time
        let filtered_row_ids: croaring::Bitmap;
        if let Some(row_ids) = self.filter_by_predicates_eq(time_range, predicates) {
            filtered_row_ids = row_ids;
        } else {
            return BTreeMap::new();
        }
        let total_rows = &filtered_row_ids.cardinality();

        let filtered_row_ids_vec = filtered_row_ids
            .to_vec()
            .iter()
            .map(|v| *v as usize)
            .collect::<Vec<_>>();

        // materialise all encoded values for the matching rows in the columns
        // we are grouping on and store each group as an iterator.
        let mut group_column_encoded_values = Vec::with_capacity(group_columns.len());
        for group_column in group_columns {
            if let Some(column) = self.column(&group_column) {
                let encoded_values = column.encoded_values(&filtered_row_ids_vec);
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
            if let Some(column) = self.column(&column_name) {
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
        if self.group_key_sorted(&group_columns) {
            panic!("This shouldn't be called!!!");
        } else {
            // now sort on the first grouping columns. Right now the order doesn't matter...
            let group_col_sort_order = &(0..group_columns.len()).collect::<Vec<_>>();
            super::sorter::sort(&mut all_columns, group_col_sort_order).unwrap();
        }
        log::debug!("time checking sort {:?}", now.elapsed());

        let mut group_itrs = all_columns
            .iter()
            .take(group_columns.len()) // only use grouping columns
            .map(|vector| {
                if let column::Vector::Integer(v) = vector {
                    v.iter()
                } else {
                    panic!("don't support grouping on non-encoded values");
                }
            })
            .collect::<Vec<_>>();

        let mut aggregate_itrs = all_columns
            .iter()
            .skip(group_columns.len()) // only use grouping columns
            .map(|v| column::VectorIterator::new(v))
            .collect::<Vec<_>>();

        // this tracks the last seen group key row. When it changes we can emit
        // the grouped aggregates.
        let mut last_group_row = group_itrs
            .iter_mut()
            .enumerate()
            .map(|(i, itr)| {
                if i == group_columns.len() - 1 {
                    // time column - apply window function
                    return itr.next().unwrap() / window * window;
                }
                *itr.next().unwrap()
            })
            .collect::<Vec<_>>();

        let mut curr_group_row = last_group_row.clone();

        // this tracks the last row for each column we are aggregating.
        let last_agg_row: Vec<column::Scalar> = aggregate_itrs
            .iter_mut()
            .map(|itr| itr.next().unwrap())
            .collect();

        // this keeps the current cumulative aggregates for the columns we
        // are aggregating.
        let mut cum_aggregates: Vec<(String, column::Aggregate)> = aggregates
            .iter()
            .zip(last_agg_row.iter())
            .map(|((col_name, agg_type), curr_agg)| {
                let agg = match agg_type {
                    AggregateType::Count => column::Aggregate::Count(1),
                    AggregateType::Sum => column::Aggregate::Sum(curr_agg.clone()),
                };
                (col_name.clone(), agg)
            })
            .collect();

        let mut results = BTreeMap::new();
        let mut processed_rows = 1;
        while processed_rows < *total_rows {
            // update next group key.
            let mut group_key_changed = false;
            for (i, (curr_v, itr)) in curr_group_row
                .iter_mut()
                .zip(group_itrs.iter_mut())
                .enumerate()
            {
                let next_v = if i == group_columns.len() - 1 {
                    // time column - apply window function
                    itr.next().unwrap() / window * window
                } else {
                    *itr.next().unwrap()
                };
                if curr_v != &next_v {
                    group_key_changed = true;
                }
                *curr_v = next_v;
            }

            // group key changed - emit group row and aggregates.
            if group_key_changed {
                let key = last_group_row.clone();
                results.insert(key, cum_aggregates.clone());

                // update group key
                last_group_row = curr_group_row.clone();

                // reset cumulative aggregates
                for (_, agg) in cum_aggregates.iter_mut() {
                    match agg {
                        column::Aggregate::Count(c) => {
                            *c = 0;
                        }
                        column::Aggregate::Sum(s) => s.reset(),
                    }
                }
            }

            // update aggregates
            for bind in cum_aggregates.iter_mut().zip(&mut aggregate_itrs) {
                let (_, curr_agg) = bind.0;
                let next_value = bind.1.next().unwrap();
                curr_agg.update_with(next_value);
            }

            processed_rows += 1;
        }

        // Emit final row
        results.insert(last_group_row, cum_aggregates);

        log::info!("({:?} rows processed) {:?}", processed_rows, results);
        // results
        BTreeMap::new()
    }

    pub fn sum_column(&self, name: &str, row_ids: &mut croaring::Bitmap) -> Option<column::Scalar> {
        if let Some(c) = self.column(name) {
            return c.sum_by_ids(row_ids);
        }
        None
    }

    // Returns the count aggregate for a given column name.
    //
    // Since we guarantee to provide row ids for the segment, and all columns
    // have the same number of logical rows, the count is just the number of
    // requested logical rows.
    pub fn count_column(&self, name: &str, row_ids: &mut croaring::Bitmap) -> Option<u64> {
        if self.column(name).is_some() {
            return Some(row_ids.cardinality() as u64);
        }
        None
    }

    pub fn filter_by_predicates_eq(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, Option<&column::Scalar>)],
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
        self.filter_by_predicates_eq_time(time_range, predicates.to_vec())
    }

    fn filter_by_predicates_eq_time(
        &self,
        time_range: (i64, i64),
        predicates: Vec<(&str, Option<&column::Scalar>)>,
    ) -> Option<croaring::Bitmap> {
        // Get all row_ids matching the time range:
        //
        //  time > time_range.0 AND time < time_range.1
        let mut bm = self.columns[self.time_column_idx].row_ids_gte_lt(
            &column::Scalar::Integer(time_range.0),
            &column::Scalar::Integer(time_range.1),
        )?;

        // now intersect matching rows for each column
        for (col_pred_name, col_pred_value) in predicates {
            if let Some(c) = self.column(col_pred_name) {
                match c.row_ids_eq(col_pred_value) {
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

    // in this case the complete time range of segment covered so no need to intersect
    // on time.
    //
    // We return an &Option here because we don't want to move the read-only
    // meta row_ids bitmap.
    fn filter_by_predicates_eq_no_time(
        &self,
        predicates: &[(&str, Option<&column::Scalar>)],
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
                // TODO(edd): rework this clone
                match c.row_ids_eq(*col_pred_value) {
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
        predicates: &[(&str, Option<&column::Scalar>)],
        group_column: &String,
        aggregates: &Vec<(String, column::AggregateType)>,
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
                                        self.sum_column(col_name, &mut filtered_row_ids).unwrap(),
                                    ), // assuming no non-null group keys
                                ));
                            }
                            AggregateType::Count => {
                                aggs.push((
                                    (col_name.to_string(), agg.clone()),
                                    column::Aggregate::Count(
                                        self.count_column(col_name, &mut filtered_row_ids).unwrap(),
                                    ), // assuming no non-null group keys
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
                    log::error!(
                        "grouped value {:?} has no rows in time-range/predicate set",
                        group_key_value
                    );
                }
            }
        } else {
            // segment doesn't have the column so can't group on it.
            log::error!("don't have column - can't group");
        }
        grouped_results
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

    // row_ids is a bitmap containing all row ids.
    row_ids: croaring::Bitmap,
    // TODO column sort order
}

impl SegmentMetaData {
    pub fn new(rows: usize, schema: Schema) -> Self {
        let mut meta = Self {
            size: 0,
            rows,
            schema,
            column_names: vec![],
            time_range: (0, 0),
            row_ids: croaring::Bitmap::create_with_capacity(rows as u32),
        };
        meta.row_ids.add_range(0..rows as u64);
        meta
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.schema_ref()
    }

    pub fn overlaps_time_range(&self, from: i64, to: i64) -> bool {
        self.time_range.0 <= to && from <= self.time_range.1
    }
}

#[derive(Debug, Clone)]
pub enum Aggregate {
    Count,
    Sum,
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
        predicates: &[(&str, Option<&column::Scalar>)],
        select_columns: Vec<String>,
    ) -> BTreeMap<String, column::Vector> {
        let (min, max) = time_range;
        if max <= min {
            panic!("max <= min");
        }

        let mut columns: BTreeMap<String, column::Vector> = BTreeMap::new();
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
        predicates: &[(&str, Option<&column::Scalar>)],
        group_columns: Vec<String>,
        aggregates: Vec<(String, AggregateType)>,
        window: i64,
        strategy: &GroupingStrategy,
    ) -> BTreeMap<Vec<String>, Vec<((String, Aggregate), column::Aggregate)>> {
        let (min, max) = time_range;
        if max <= min {
            panic!("max <= min");
        }

        match strategy {
            GroupingStrategy::HashGroup => {
                self.read_group_eq_hash(time_range, predicates, group_columns, aggregates, false)
            }
            GroupingStrategy::HashGroupConcurrent => {
                self.read_group_eq_hash(time_range, predicates, group_columns, aggregates, true)
            }
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
        predicates: &[(&str, Option<&column::Scalar>)],
        group_columns: Vec<String>,
        aggregates: Vec<(String, AggregateType)>,
        concurrent: bool,
    ) -> BTreeMap<Vec<String>, Vec<((String, Aggregate), column::Aggregate)>> {
        if concurrent {
            let group_columns_arc = std::sync::Arc::new(group_columns);
            let aggregates_arc = std::sync::Arc::new(aggregates);

            for chunked_segments in self.segments.chunks(THREADS) {
                crossbeam::scope(|scope| {
                    for segment in chunked_segments {
                        let group_columns = group_columns_arc.clone();
                        let aggregates = aggregates_arc.clone();

                        scope.spawn(move |_| {
                            let now = std::time::Instant::now();
                            segment.aggregate_by_group_with_hash(
                                time_range,
                                predicates,
                                &group_columns,
                                &aggregates,
                            );
                            log::info!(
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
                    &group_columns_arc.clone(),
                    &aggregates_arc.clone(),
                );
                log::info!(
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
            );
            log::info!(
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
        predicates: &[(&str, Option<&column::Scalar>)],
        mut group_columns: Vec<String>,
        aggregates: Vec<(String, AggregateType)>,
        window: i64,
        concurrent: bool,
    ) -> BTreeMap<Vec<String>, Vec<((String, Aggregate), column::Aggregate)>> {
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
                            let now = std::time::Instant::now();
                            segment.aggregate_by_group_with_sort(
                                time_range,
                                predicates,
                                &group_columns,
                                &aggregates,
                                window,
                            );
                            log::info!(
                                "processed segment {:?} using multi-threaded sort in {:?}",
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
                segment.aggregate_by_group_with_sort(
                    time_range,
                    predicates,
                    &group_columns_arc.clone(),
                    &aggregates_arc.clone(),
                    window,
                );
                log::info!(
                    "processed segment {:?} using multi-threaded sort in {:?}",
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
            segment.aggregate_by_group_with_sort(
                time_range,
                predicates,
                &group_columns,
                &aggregates,
                window,
            );
            log::info!(
                "processed segment {:?} using single-threaded sort in {:?}",
                segment.time_range(),
                now.elapsed()
            )
        }

        BTreeMap::new()
    }

    /// Returns the minimum value for a column in a set of segments.
    pub fn column_min(&self, column_name: &str) -> Option<column::Scalar> {
        if self.segments.is_empty() {
            return None;
        }

        let mut min_min: Option<column::Scalar> = None;
        for segment in &self.segments {
            if let Some(i) = segment.column_names().iter().position(|c| c == column_name) {
                let min = segment.columns[i].min();
                if min_min.is_none() {
                    min_min = min
                } else if min_min > min {
                    min_min = min;
                }
            }
        }

        min_min
    }

    pub fn window_agg_eq(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, Option<&column::Scalar>)],
        group_columns: Vec<String>,
        aggregates: Vec<(String, AggregateType)>,
        window: i64,
    ) -> BTreeMap<Vec<String>, Vec<((String, Aggregate), column::Aggregate)>> {
        let (min, max) = time_range;
        if max <= min {
            panic!("max <= min");
        }

        // add time column to the group key
        let mut group_columns = group_columns.clone();
        group_columns.push("time".to_string());

        for segment in &self.segments {
            let now = std::time::Instant::now();
            segment.window_aggregate_with_sort(
                time_range,
                predicates,
                &group_columns,
                &aggregates,
                window,
            );
            log::info!(
                "processed segment {:?} using windowed single-threaded sort in {:?}",
                segment.time_range(),
                now.elapsed()
            )
        }
        BTreeMap::new()
    }

    /// Returns the maximum value for a column in a set of segments.
    pub fn column_max(&self, column_name: &str) -> Option<column::Scalar> {
        if self.segments.is_empty() {
            return None;
        }

        let mut max_max: Option<column::Scalar> = None;
        for segment in &self.segments {
            if let Some(i) = segment.column_names().iter().position(|c| c == column_name) {
                let max = segment.columns[i].max();
                if max_max.is_none() {
                    max_max = max
                } else if max_max < max {
                    max_max = max;
                }
            }
        }

        max_max
    }

    /// Returns the first value for a column in a set of segments.
    ///
    /// The first value is based on the time column, therefore the returned value
    /// may not be at the end of the column.
    ///
    /// If the time column has multiple max time values then the result is abitrary.
    ///
    /// TODO(edd): could return NULL value..
    pub fn first(&self, column_name: &str) -> Option<(i64, Option<column::Scalar>, usize)> {
        // First let's find the segment with the earliest time range.
        // notice we order  a < b on max time range.
        let segment = self
            .segments
            .iter()
            .min_by(|a, b| a.meta.time_range.0.cmp(&b.meta.time_range.0))?;

        // first find the logical row id of the minimum timestamp value
        if let Column::Integer(ts_col) = &segment.columns[segment.time_column_idx] {
            // TODO(edd): clean up unwrap
            let min_ts = ts_col.column_range().0;
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
    /// If the time column has multiple max time values then the result is abitrary.
    ///
    /// TODO(edd): could return NULL value..
    pub fn last(&self, column_name: &str) -> Option<(i64, Option<column::Scalar>, usize)> {
        // First let's find the segment with the latest time range.
        // notice we order a > b on max time range.
        let segment = self
            .segments
            .iter()
            .max_by(|a, b| a.meta.time_range.1.cmp(&b.meta.time_range.1))?;

        // first find the logical row id of the minimum timestamp value
        if let Column::Integer(ts_col) = &segment.columns[segment.time_column_idx] {
            // TODO(edd): clean up unwrap
            let max_ts = ts_col.column_range().1;
            assert_eq!(max_ts, segment.meta.time_range.1);

            let max_ts_id = ts_col.row_id_eq_value(max_ts).unwrap();

            // now we have row id we can get value for that row id
            let value = segment.column(column_name).unwrap().value(max_ts_id);
            Some((max_ts, value, max_ts_id))
        } else {
            panic!("time column wrong type!");
        }
    }
}

#[derive(Debug)]
pub enum GroupingStrategy {
    HashGroup,
    HashGroupConcurrent,
    SortGroup,
    SortGroupConcurrent,
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
            (vec!["env", "role", "path", "time", "foo"], false), // group key contains non-sorted col
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
