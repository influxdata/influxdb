use data_types2::SequenceNumber;

/// Information on how much data a particular sequencer has been processed
///
/// ```text
/// Write Lifecycle (compaction not shown):
///
/// Durable --------------> Readable -------------> Persisted
///
///  in sequencer,          in memory, not yet      in parquet
///  not readable.          in parquet
/// ```
///
/// Note: min_readable_sequence_number <= min_totally_persisted_sequence_number
#[derive(Clone, Debug, PartialEq, Default)]
pub struct SequencerProgress {
    /// Smallest sequence number of data that is buffered in memory
    min_buffered: Option<SequenceNumber>,

    /// Largest sequence number of data that is buffered in memory
    max_buffered: Option<SequenceNumber>,

    /// Largest sequence number of data that has been written to parquet
    max_persisted: Option<SequenceNumber>,
}

impl SequencerProgress {
    pub fn new() -> Self {
        Default::default()
    }

    /// Note that `sequence_number` is buffered
    pub fn with_buffered(mut self, sequence_number: SequenceNumber) -> Self {
        self.min_buffered = Some(
            self.min_buffered
                .take()
                .map(|cur| cur.min(sequence_number))
                .unwrap_or(sequence_number),
        );
        self.max_buffered = Some(
            self.max_buffered
                .take()
                .map(|cur| cur.max(sequence_number))
                .unwrap_or(sequence_number),
        );
        self
    }

    /// Note that data with `sequence_number` was persisted; Note this does not
    /// mean that all sequence numbers less than `sequence_number`
    /// have been persisted.
    pub fn with_persisted(mut self, sequence_number: SequenceNumber) -> Self {
        self.max_persisted = Some(
            self.max_persisted
                .take()
                .map(|cur| cur.max(sequence_number))
                .unwrap_or(sequence_number),
        );
        self
    }

    // return true if this sequence number is readable
    pub fn readable(&self, sequence_number: SequenceNumber) -> bool {
        match (&self.max_buffered, &self.max_persisted) {
            (Some(max_buffered), Some(max_persisted)) => {
                &sequence_number <= max_buffered || &sequence_number <= max_persisted
            }
            (None, Some(max_persisted)) => &sequence_number <= max_persisted,
            (Some(max_buffered), _) => &sequence_number <= max_buffered,
            (None, None) => {
                false // data not yet ingested
            }
        }
    }

    // return true if this sequence number is persisted
    pub fn persisted(&self, sequence_number: SequenceNumber) -> bool {
        // with both buffered and persisted data, need to
        // ensure that no data is buffered to know that all is
        // persisted
        match (&self.min_buffered, &self.max_persisted) {
            (Some(min_buffered), Some(max_persisted)) => {
                // with both buffered and persisted data, need to
                // ensure that no data is buffered to know that all is
                // persisted
                &sequence_number < min_buffered && &sequence_number <= max_persisted
            }
            (None, Some(max_persisted)) => &sequence_number <= max_persisted,
            (_, None) => {
                false // data not yet persisted
            }
        }
    }

    /// Combine the values from other
    pub fn combine(self, other: Self) -> Self {
        let updated = if let Some(min_buffered) = other.min_buffered {
            self.with_buffered(min_buffered)
        } else {
            self
        };

        let updated = if let Some(max_buffered) = other.max_buffered {
            updated.with_buffered(max_buffered)
        } else {
            updated
        };

        if let Some(max_persisted) = other.max_persisted {
            updated.with_persisted(max_persisted)
        } else {
            updated
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let progress = SequencerProgress::new();
        let sequence_number = SequenceNumber::new(0);
        assert!(!progress.readable(sequence_number));
        assert!(!progress.persisted(sequence_number));
    }

    #[test]
    fn persisted() {
        let lt = SequenceNumber::new(0);
        let eq = SequenceNumber::new(1);
        let gt = SequenceNumber::new(2);

        let progress = SequencerProgress::new().with_persisted(eq);

        assert!(progress.readable(lt));
        assert!(progress.persisted(lt));

        // persisted implies it is also readable
        assert!(progress.readable(eq));
        assert!(progress.persisted(eq));

        assert!(!progress.readable(gt));
        assert!(!progress.persisted(gt));
    }

    #[test]
    fn buffered() {
        let lt = SequenceNumber::new(0);
        let eq = SequenceNumber::new(1);
        let gt = SequenceNumber::new(2);

        let progress = SequencerProgress::new().with_buffered(eq);

        assert!(progress.readable(lt));
        assert!(!progress.persisted(lt));

        assert!(progress.readable(eq));
        assert!(!progress.persisted(eq));

        assert!(!progress.readable(gt));
        assert!(!progress.persisted(gt));
    }

    #[test]
    fn buffered_greater_than_persisted() {
        let lt = SequenceNumber::new(0);
        let eq = SequenceNumber::new(1);
        let gt = SequenceNumber::new(2);

        let progress = SequencerProgress::new()
            .with_buffered(eq)
            .with_persisted(lt);

        assert!(progress.readable(lt));
        assert!(progress.persisted(lt));

        assert!(progress.readable(eq));
        assert!(!progress.persisted(eq));

        assert!(!progress.readable(gt));
        assert!(!progress.persisted(gt));
    }

    #[test]
    fn buffered_and_persisted() {
        let lt = SequenceNumber::new(0);
        let eq = SequenceNumber::new(1);
        let gt = SequenceNumber::new(2);

        let progress = SequencerProgress::new()
            .with_buffered(eq)
            .with_persisted(eq);

        assert!(progress.readable(lt));
        assert!(progress.persisted(lt));

        assert!(progress.readable(eq));
        assert!(!progress.persisted(eq)); // have buffered data, so can't be persisted here

        assert!(!progress.readable(gt));
        assert!(!progress.persisted(gt));
    }

    #[test]
    fn buffered_less_than_persisted() {
        let lt = SequenceNumber::new(0);
        let eq = SequenceNumber::new(1);
        let gt = SequenceNumber::new(2);

        // data buffered between lt and eq
        let progress = SequencerProgress::new()
            .with_buffered(lt)
            .with_buffered(eq)
            .with_persisted(eq);

        assert!(progress.readable(lt));
        assert!(!progress.persisted(lt)); // have buffered data at lt, can't be persisted

        assert!(progress.readable(eq));
        assert!(!progress.persisted(eq)); // have buffered data, so can't be persisted

        assert!(!progress.readable(gt));
        assert!(!progress.persisted(gt));
    }

    #[test]
    fn combine() {
        let lt = SequenceNumber::new(0);
        let eq = SequenceNumber::new(1);
        let gt = SequenceNumber::new(2);

        let progress1 = SequencerProgress::new().with_buffered(gt);

        let progress2 = SequencerProgress::new()
            .with_buffered(lt)
            .with_persisted(eq);

        let expected = SequencerProgress::new()
            .with_buffered(lt)
            .with_buffered(gt)
            .with_persisted(eq);

        assert_eq!(progress1.combine(progress2), expected);
    }
}
