#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![allow(clippy::type_complexity)]
pub mod adapter;
pub mod column;
pub mod encoding;
pub mod segment;
pub mod sorter;

use arrow::datatypes::SchemaRef;
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

    pub fn segments(&self) -> Segments<'_> {
        // let iter: std::slice::Iter<'a, Segment> = self.segments.iter();
        // let segments = iter.collect::<Vec<&'a Segment>>();
        Segments::new(self.segments.iter().collect::<Vec<&Segment>>())
    }

    pub fn schema(&self) -> SchemaRef {
        assert!(
            !self.segments.is_empty(),
            "Need to have at least one segment in a store"
        );
        // assume all segments have the same schema
        self.segments[0].schema()
    }
}
