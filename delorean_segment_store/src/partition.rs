use crate::segment::Segment;

pub struct Partition {
    segments: Vec<Segment>,
}

impl Partition {
    pub fn new() -> Self {
        Self { segments: vec![] }
    }

    pub fn add_segment(&mut self, segment: Segment) {
        todo!();
    }
}
