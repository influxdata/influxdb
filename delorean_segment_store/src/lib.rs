#![deny(rust_2018_idioms)]
pub mod partition;
pub mod segment;

use std::collections::BTreeMap;

use partition::Partition;
use segment::Segment;

#[derive(Default)]
/// The Segment Store is responsible for providing read access to partition data.
///
///
pub struct Store {
    segments: BTreeMap<String, Partition>,
}

impl Store {
    pub fn add_segment(&mut self, name: String, segment: Segment) {
        self.segments
            .entry(name)
            .or_insert_with(Partition::new)
            .add_segment(segment);
    }
}
