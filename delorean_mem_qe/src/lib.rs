pub mod column;
pub mod encoding;
pub mod segment;
pub mod sorter;

use segment::{Segment, Segments};

#[derive(Debug, Default)]
pub struct Store {
    segments: Vec<Segment>,

    /// Total size of the store, in bytes
    store_size: usize,
}

impl Store {
    pub fn add_segment(&mut self, segment: Segment) {
        self.store_size += segment.size();
        self.segments.push(segment);
    }

    /// The total size of all segments in the store, in bytes.
    pub fn size(&self) -> usize {
        self.store_size
    }

    pub fn segment_total(&self) -> usize {
        self.segments.len()
    }

    pub fn segments(&self) -> Segments {
        Segments::new(self.segments.iter().collect::<Vec<&Segment>>())
    }
}
