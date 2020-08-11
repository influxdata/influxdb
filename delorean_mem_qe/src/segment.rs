use std::collections::BTreeMap;

use super::column;
use super::column::Column;

#[derive(Debug)]
pub struct Segment {
    meta: SegmentMetaData,

    // Columns within a segment
    columns: Vec<column::Column>,
    time_column_idx: usize,
}

impl Segment {
    pub fn new(rows: usize) -> Self {
        Self {
            meta: SegmentMetaData::new(rows),
            columns: vec![],
            time_column_idx: 0,
        }
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

    // TODO - iterator....
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

    // Materialise all rows for each desired column. `rows` expects `row_ids` to
    // be ordered in ascending order.
    //
    // `columns` determines which column values are returned. An empty `columns`
    // value will result in rows for all columns being returned.
    pub fn rows(&self, row_ids: &[usize], columns: &[String]) -> BTreeMap<String, column::Vector> {
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
    ) -> Option<&std::collections::BTreeMap<Option<std::string::String>, croaring::Bitmap>> {
        if let Some(c) = self.column(name) {
            return Some(c.group_by_ids());
        }
        None
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

    pub fn group_agg_by_predicate_eq(
        &self,
        time_range: (i64, i64),
        predicates: &[(&str, Option<&column::Scalar>)],
        group_columns: &Vec<String>,
        aggregates: &Vec<(String, Aggregate)>,
    ) -> BTreeMap<Vec<String>, Vec<((String, Aggregate), column::Aggregate)>> {
        let mut grouped_results = BTreeMap::new();

        let filter_row_ids: croaring::Bitmap;
        match self.filter_by_predicates_eq(time_range, predicates) {
            Some(row_ids) => filter_row_ids = row_ids,
            None => {
                return grouped_results;
            }
        }

        if let Some(grouped_row_ids) = self.group_by_column_ids(&group_columns[0]) {
            for (group_key_value, row_ids) in grouped_row_ids.iter() {
                let mut filtered_row_ids = row_ids.and(&filter_row_ids);
                if !filtered_row_ids.is_empty() {
                    // First calculate all of the aggregates for this grouped value
                    let mut aggs: Vec<((String, Aggregate), column::Aggregate)> =
                        Vec::with_capacity(aggregates.len());

                    for (col_name, agg) in aggregates {
                        match &agg {
                            Aggregate::Sum => {
                                aggs.push((
                                    (col_name.to_string(), agg.clone()),
                                    column::Aggregate::Sum(
                                        self.sum_column(col_name, &mut filtered_row_ids).unwrap(),
                                    ), // assuming no non-null group keys
                                ));
                            }
                            Aggregate::Count => {
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
                    grouped_results.insert(vec![group_key_value.clone().unwrap()], aggs);
                } else {
                    // In this case there are grouped values in the column with no
                    // rows falling into time-range/predicate set.
                    println!(
                        "grouped value {:?} has no rows in time-range/predicate set",
                        group_key_value
                    );
                }
            }
        } else {
            // segment doesn't have the column so can't group on it.
            println!("don't have column - can't group");
        }
        grouped_results
    }
}

/// Meta data for a segment. This data is mainly used to determine if a segment
/// may contain value for answering a query.
#[derive(Debug)]
pub struct SegmentMetaData {
    size: usize, // TODO
    rows: usize,

    column_names: Vec<String>,
    time_range: (i64, i64),

    // row_ids is a bitmap containing all row ids.
    row_ids: croaring::Bitmap,
    // TODO column sort order
}

impl SegmentMetaData {
    pub fn new(rows: usize) -> Self {
        let mut meta = Self {
            size: 0,
            rows,
            column_names: vec![],
            time_range: (0, 0),
            row_ids: croaring::Bitmap::create_with_capacity(rows as u32),
        };
        meta.row_ids.add_range(0..rows as u64);
        meta
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
                let bm_vec = bm.to_vec();
                let row_ids = bm_vec.iter().map(|v| *v as usize).collect::<Vec<usize>>();

                let rows = segment.rows(&row_ids, &select_columns);
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
        aggregates: Vec<(String, Aggregate)>,
    ) -> BTreeMap<Vec<String>, Vec<((String, Aggregate), column::Aggregate)>> {
        // TODO(edd): support multi column groups
        assert_eq!(group_columns.len(), 1);

        let (min, max) = time_range;
        if max <= min {
            panic!("max <= min");
        }

        let mut cum_results: BTreeMap<Vec<String>, Vec<((String, Aggregate), column::Aggregate)>> =
            BTreeMap::new();

        for segment in &self.segments {
            let segment_results = segment.group_agg_by_predicate_eq(
                time_range,
                predicates,
                &group_columns,
                &aggregates,
            );

            for (k, segment_aggs) in segment_results {
                // assert_eq!(v.len(), aggregates.len());
                let cum_result = cum_results.get_mut(&k);
                match cum_result {
                    Some(cum) => {
                        assert_eq!(cum.len(), segment_aggs.len());
                        // In this case we need to aggregate the aggregates from
                        // each segment.
                        for i in 0..cum.len() {
                            // TODO(edd): this is more expensive than necessary
                            cum[i] = (cum[i].0.clone(), cum[i].1.clone() + &segment_aggs[i].1);
                        }
                    }
                    None => {
                        cum_results.insert(k, segment_aggs);
                    }
                }
            }
        }

        // columns
        cum_results
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
        // First let's find the segment with the latest time range.
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

#[cfg(test)]
mod test {}
