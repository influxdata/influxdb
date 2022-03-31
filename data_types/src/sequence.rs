#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Sequence {
    pub sequencer_id: u32,
    pub sequence_number: u64,
}

impl Sequence {
    pub fn new(sequencer_id: u32, sequence_number: u64) -> Self {
        Self {
            sequencer_id,
            sequence_number,
        }
    }
}
