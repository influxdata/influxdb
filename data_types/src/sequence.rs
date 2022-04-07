#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Sequence {
    /// The sequencer id (kafka partition id)
    pub sequencer_id: u32,
    /// The sequence number (kafka offset)
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
