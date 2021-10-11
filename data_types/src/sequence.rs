#[derive(Debug, Copy, Clone)]
pub struct Sequence {
    pub id: u32,
    pub number: u64,
}

impl Sequence {
    pub fn new(sequencer_id: u32, sequence_number: u64) -> Self {
        Self {
            id: sequencer_id,
            number: sequence_number,
        }
    }
}
