use std::collections::BTreeMap;

use super::column;
use super::column::Column;

#[derive(Debug, Default)]
pub struct Segment {
    meta: SegmentMetaData,

    // Columns within a segment
    columns: Vec<column::Column>,
    time_column_idx: usize,
}

impl Segment {
    pub fn new(rows: usize) -> Self {
        let mut segment = Self::default();
        segment.meta.rows = rows;
        segment
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
        // TODO(edd) yuk
        if name == "time" {
            if let column::Column::Integer(ts) = &c {
                self.meta.time_range = ts.column_range();
            } else {
                panic!("incorrect column type for time");
            }
            self.time_column_idx = self.columns.len();
        }
        self.meta.rows = c.num_rows();

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
}

pub struct Segments<'a> {
    segments: &'a [Segment],
}

impl<'a> Segments<'a> {
    pub fn new(segments: &'a [Segment]) -> Self {
        Self { segments }
    }

    /// Returns the minimum value for a column in a set of segments.
    pub fn column_min(&self, column_name: &str) -> Option<column::Scalar> {
        if self.segments.is_empty() {
            return None;
        }

        let mut min_min: Option<column::Scalar> = None;
        for segment in self.segments {
            if let Some(i) = segment.column_names().iter().position(|c| c == column_name) {
                let min = Some(segment.columns[i].min());
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
        for segment in self.segments {
            if let Some(i) = segment.column_names().iter().position(|c| c == column_name) {
                let max = Some(segment.columns[i].max());
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
    /// TODO(edd): could return NULL value..
    pub fn first(&self, column_name: &str) -> Option<(i64, Option<column::Scalar>)> {
        if self.segments.is_empty() {
            return None;
        }

        let mut first_first: Option<(i64, Option<column::Scalar>)> = None;
        for segment in self.segments {
            // first find the logical row id of the minimum timestamp value
            if let Column::Integer(ts_col) = &segment.columns[segment.time_column_idx] {
                // TODO(edd): clean up unwr
                let min_ts = ts_col.column_range().0;
                let min_ts_id = ts_col.row_id_for_value(min_ts).unwrap();

                // now we have row id we can get value for that row id
                let value = segment.column(column_name).unwrap().value(min_ts_id);

                match &first_first {
                    Some(prev) => {
                        if prev.0 > min_ts {
                            first_first = Some((min_ts, value));
                        }
                    }
                    None => first_first = Some((min_ts, value)),
                }
            }
        }

        first_first
    }
}

/// Meta data for a segment. This data is mainly used to determine if a segment
/// may contain value for answering a query.
#[derive(Debug, Default)]
pub struct SegmentMetaData {
    size: usize, // TODO
    rows: usize,

    column_names: Vec<String>,
    time_range: (i64, i64),
    // TODO column sort order
}

#[cfg(test)]
mod test {}
