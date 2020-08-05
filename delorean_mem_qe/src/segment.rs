use std::collections::BTreeMap;

use super::column;
use super::column::Column;

#[derive(Debug, Default)]
pub struct Segment {
    meta: SegmentMetaData,

    // Columns within a segment
    columns: Vec<column::Column>,
    // string_columns: Vec<column::String>,
    // f64_columns: Vec<column::F64>,
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

    pub fn column_names(&self) -> Vec<String> {
        self.meta.column_names.clone()
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
                    column_sizes.insert(names[i].clone(), c.size());
                }
                Column::Float(c) => {
                    column_sizes.insert(names[i].clone(), c.size());
                }
                Column::Integer(c) => {
                    column_sizes.insert(names[i].clone(), c.size());
                }
            }
        }
        column_sizes
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
