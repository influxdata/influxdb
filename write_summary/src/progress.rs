use data_types::SequenceNumber;

/// Information on how much data a particular shard has processed
///
/// ```text
/// Write Lifecycle (compaction not shown):
///
/// Durable --------------> Readable -------------> Persisted
///
///  in shard,          in memory, not yet      in parquet
///  not readable.          in parquet
/// ```
///
/// Note: min_readable_sequence_number <= min_totally_persisted_sequence_number
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ShardProgress {
    /// Smallest sequence number of data that is buffered in memory
    min_buffered: Option<SequenceNumber>,

    /// Largest sequence number of data that is buffered in memory
    max_buffered: Option<SequenceNumber>,

    /// Largest sequence number of data that has been written to parquet
    max_persisted: Option<SequenceNumber>,

    /// The sequence number that is actively buffering, if any. The
    /// actively buffering sequence number is not yet completely
    /// buffered to all partitions, and thus is excluded from the
    /// min/max buffered calculation.
    actively_buffering: Option<SequenceNumber>,
}

impl ShardProgress {
    pub fn new() -> Self {
        Default::default()
    }

    /// Note that `sequence_number` is buffered
    pub fn with_buffered(mut self, sequence_number: SequenceNumber) -> Self {
        // clamp if there is any actively buffering operation
        let sequence_number = if let Some(v) = clamp(sequence_number, self.actively_buffering) {
            v
        } else {
            return self;
        };

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

    /// Note that the specified sequence number is still actively
    /// buffering, and adjusting all subsequent sequence numbers
    /// accordingly.
    pub fn actively_buffering(mut self, sequence_number: Option<SequenceNumber>) -> Self {
        self.actively_buffering = sequence_number;
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

    /// Return true if this shard progress has no information on
    /// shard progress, false otherwise
    pub fn is_empty(&self) -> bool {
        self.min_buffered.is_none() && self.max_buffered.is_none() && self.max_persisted.is_none()
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

// Ensures that `buffered` is less than `actively_buffering`,
// returning the new buffered value
fn clamp(
    buffered: SequenceNumber,
    actively_buffering: Option<SequenceNumber>,
) -> Option<SequenceNumber> {
    let completed_buffering = if let Some(val) = actively_buffering {
        // returns `None` if no sequence number has completed buffering yet
        let min_sequence = (val.get() as u64).checked_sub(1)?;
        SequenceNumber::new(min_sequence as i64)
    } else {
        return Some(buffered);
    };

    if buffered > completed_buffering {
        Some(completed_buffering)
    } else {
        // no adjustment needed
        Some(buffered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let progress = ShardProgress::new();
        let sequence_number = SequenceNumber::new(0);
        assert!(!progress.readable(sequence_number));
        assert!(!progress.persisted(sequence_number));
    }

    #[test]
    fn persisted() {
        let lt = SequenceNumber::new(0);
        let eq = SequenceNumber::new(1);
        let gt = SequenceNumber::new(2);

        let progress = ShardProgress::new().with_persisted(eq);

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

        let progress = ShardProgress::new().with_buffered(eq);

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

        let progress = ShardProgress::new().with_buffered(eq).with_persisted(lt);

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

        let progress = ShardProgress::new().with_buffered(eq).with_persisted(eq);

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
        let progress = ShardProgress::new()
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

        let progress1 = ShardProgress::new().with_buffered(gt);

        let progress2 = ShardProgress::new().with_buffered(lt).with_persisted(eq);

        let expected = ShardProgress::new()
            .with_buffered(lt)
            .with_buffered(gt)
            .with_persisted(eq);

        assert_eq!(progress1.combine(progress2), expected);
    }

    #[test]
    fn actively_buffering() {
        let num0 = SequenceNumber::new(0);
        let num1 = SequenceNumber::new(1);
        let num2 = SequenceNumber::new(2);

        #[derive(Debug)]
        struct Expected {
            min_buffered: Option<SequenceNumber>,
            max_buffered: Option<SequenceNumber>,
        }

        let cases = vec![
            // No buffering
            (
                ShardProgress::new()
                    .actively_buffering(None)
                    .with_buffered(num1)
                    .with_buffered(num2),
                Expected {
                    min_buffered: Some(num1),
                    max_buffered: Some(num2),
                },
            ),
            // actively buffering num2
            (
                ShardProgress::new()
                    .actively_buffering(Some(num2))
                    .with_buffered(num1)
                    .with_buffered(num2),
                Expected {
                    min_buffered: Some(num1),
                    max_buffered: Some(num1),
                },
            ),
            // actively buffering only one
            (
                ShardProgress::new()
                    .actively_buffering(Some(num1))
                    .with_buffered(num1),
                Expected {
                    min_buffered: Some(num0),
                    max_buffered: Some(num0),
                },
            ),
            // actively buffering, haven't buffed any yet
            (
                ShardProgress::new()
                    .actively_buffering(Some(num1))
                    .with_buffered(num0),
                Expected {
                    min_buffered: Some(num0),
                    max_buffered: Some(num0),
                },
            ),
            // actively buffering, haven't buffered any
            (
                ShardProgress::new().actively_buffering(Some(num0)),
                Expected {
                    min_buffered: None,
                    max_buffered: None,
                },
            ),
            // actively buffering partially buffered
            (
                ShardProgress::new()
                    .actively_buffering(Some(num0))
                    .with_buffered(num0),
                Expected {
                    min_buffered: None,
                    max_buffered: None,
                },
            ),
        ];

        for (progress, expected) in cases {
            println!("Comparing {progress:?} to {expected:?}");
            assert_eq!(
                progress.min_buffered, expected.min_buffered,
                "min buffered mismatch"
            );
            assert_eq!(
                progress.max_buffered, expected.max_buffered,
                "max buffered mismatch"
            );
            assert_eq!(progress.max_persisted, None, "unexpected persisted");
        }
    }

    #[test]
    fn test_clamp() {
        let num0 = SequenceNumber::new(0);
        let num1 = SequenceNumber::new(1);
        let num2 = SequenceNumber::new(2);

        assert_eq!(clamp(num0, None), Some(num0));

        // num0 hasn't completed buffering yet
        assert_eq!(clamp(num0, Some(num0)), None);

        assert_eq!(clamp(num1, Some(num0)), None);
        assert_eq!(clamp(num1, Some(num1)), Some(num0));
        assert_eq!(clamp(num1, Some(num2)), Some(num1));

        assert_eq!(clamp(num2, Some(num0)), None);
        assert_eq!(clamp(num2, Some(num1)), Some(num0));
        assert_eq!(clamp(num2, Some(num2)), Some(num1));
    }
}
