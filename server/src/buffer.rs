//! This module contains code for managing the WAL buffer

use data_types::{
    data::ReplicatedWrite,
    database_rules::{WalBufferRollover, WriterId},
};

use std::{collections::BTreeMap, convert::TryFrom, mem, sync::Arc};

use chrono::{DateTime, Utc};
use snafu::Snafu;
use tokio::sync::Mutex;

use tracing::warn;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Max size limit hit {}", size))]
    MaxSizeLimit { size: u64 },

    #[snafu(display(
        "Unable to drop segment to reduce size below max. Current size {}, segment count {}",
        size,
        segment_count
    ))]
    UnableToDropSegment { size: u64, segment_count: usize },

    #[snafu(display(
        "Sequence from writer {} out of order. Current: {}, incomming {}",
        writer,
        current_sequence,
        incoming_sequence,
    ))]
    SequenceOutOfOrder {
        writer: WriterId,
        current_sequence: u64,
        incoming_sequence: u64,
    },
}

#[allow(dead_code)]
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An in-memory buffer of a write ahead log. It is split up into segments,
/// which can be persisted to object storage.
#[derive(Debug)]
pub struct Buffer {
    max_size: u64,
    current_size: u64,
    segment_size: u64,
    open_segment: Segment,
    closed_segments: Vec<Arc<Segment>>,
    rollover_behavior: WalBufferRollover,
}

impl Buffer {
    #[allow(dead_code)]
    pub fn new(max_size: u64, segment_size: u64, rollover_behavior: WalBufferRollover) -> Self {
        Self {
            max_size,
            segment_size,
            rollover_behavior,
            open_segment: Segment::new(1),
            current_size: 0,
            closed_segments: vec![],
        }
    }

    /// Appends a replicated write onto the buffer, returning the segment if it
    /// has been closed out. If the max size of the buffer would be exceeded
    /// by accepting the write, the oldest (first) of the closed segments
    /// will be dropped, if it is persisted. Otherwise, an error is returned.
    #[allow(dead_code)]
    pub async fn append(&mut self, write: ReplicatedWrite) -> Result<Option<Arc<Segment>>> {
        let write_size = u64::try_from(write.data.len())
            .expect("appended data must be less than a u64 in length");

        while self.current_size + write_size > self.max_size {
            let oldest_is_persisted = match self.closed_segments.get(0) {
                Some(s) => s.persisted_at().await.is_some(),
                None => false,
            };

            if oldest_is_persisted {
                self.remove_oldest_segment();
                continue;
            }

            match self.rollover_behavior {
                WalBufferRollover::DropIncoming => {
                    warn!(
                        "WAL is full, dropping incoming write for current segment (segment id: {:?})",
                        self.open_segment.id,
                    );
                    return Ok(None);
                }
                WalBufferRollover::DropOldSegment => {
                    let oldest_segment_id = self.remove_oldest_segment();
                    warn!(
                        "WAL is full, dropping oldest segment (segment id: {:?})",
                        oldest_segment_id
                    );
                }
                WalBufferRollover::ReturnError => {
                    return UnableToDropSegment {
                        size: self.current_size,
                        segment_count: self.closed_segments.len(),
                    }
                    .fail()
                }
            }
        }

        let mut closed_segment = None;

        self.current_size += write_size;
        self.open_segment.append(write)?;
        if self.open_segment.size > self.segment_size {
            let next_id = self.open_segment.id + 1;
            let segment = mem::replace(&mut self.open_segment, Segment::new(next_id));
            let segment = Arc::new(segment);

            self.closed_segments.push(segment.clone());
            closed_segment = Some(segment);
        }

        Ok(closed_segment)
    }

    /// Returns the current size of the buffer.
    #[allow(dead_code)]
    pub fn size(&self) -> u64 {
        self.current_size
    }

    /// Returns any replicated writes from the given writer ID and sequence
    /// number onward. This will include writes from other writers. The
    /// given writer ID and sequence are to identify from what point to
    /// replay writes. If no write matches the given writer ID and sequence
    /// number, all replicated writes within the buffer will be returned.
    #[allow(dead_code)]
    pub fn all_writes_since(&self, since: WriterSequence) -> Vec<Arc<ReplicatedWrite>> {
        let mut writes = Vec::new();

        // start with the newest writes and go back. Hopefully they're asking for
        // something recent.
        for w in self.open_segment.writes.iter().rev() {
            if w.equal_to_writer_and_sequence(since.id, since.sequence) {
                writes.reverse();
                return writes;
            }
            writes.push(w.clone());
        }

        for s in self.closed_segments.iter().rev() {
            for w in s.writes.iter().rev() {
                if w.equal_to_writer_and_sequence(since.id, since.sequence) {
                    writes.reverse();
                    return writes;
                }
                writes.push(w.clone());
            }
        }

        writes.reverse();
        writes
    }

    /// Returns replicated writes from the given writer ID and sequence number
    /// onward. This returns only writes from the passed in writer ID. If no
    /// write matches the given writer ID and sequence number, all
    /// replicated writes within the buffer for that writer will be returned.
    #[allow(dead_code)]
    pub fn writes_since(&self, since: WriterSequence) -> Vec<Arc<ReplicatedWrite>> {
        let mut writes = Vec::new();

        // start with the newest writes and go back. Hopefully they're asking for
        // something recent.
        for w in self.open_segment.writes.iter().rev() {
            let (writer, sequence) = w.writer_and_sequence();
            if writer == since.id {
                if sequence == since.sequence {
                    writes.reverse();
                    return writes;
                }
                writes.push(w.clone());
            }
        }

        for s in self.closed_segments.iter().rev() {
            for w in s.writes.iter().rev() {
                let (writer, sequence) = w.writer_and_sequence();
                if writer == since.id {
                    if sequence == since.sequence {
                        writes.reverse();
                        return writes;
                    }
                    writes.push(w.clone());
                }
            }
        }

        writes.reverse();
        writes
    }

    // Removes the oldest segment present in the buffer, returning its id
    #[allow(dead_code)]
    fn remove_oldest_segment(&mut self) -> u64 {
        let removed_segment = self.closed_segments.remove(0);
        self.current_size -= removed_segment.size;
        removed_segment.id
    }
}

#[derive(Debug)]
pub struct Segment {
    id: u64,
    size: u64,
    writes: Vec<Arc<ReplicatedWrite>>,
    writers: BTreeMap<WriterId, WriterSummary>,
    // If set, this is the time at which this segment was persisted
    persisted: Mutex<Option<DateTime<Utc>>>,
}

impl Segment {
    #[allow(dead_code)]
    fn new(id: u64) -> Self {
        Self {
            id,
            size: 0,
            writes: vec![],
            writers: BTreeMap::new(),
            persisted: Mutex::new(None),
        }
    }

    // appends the write to the segment, keeping track of the summary information
    // about the writer
    #[allow(dead_code)]
    fn append(&mut self, write: ReplicatedWrite) -> Result<()> {
        let (writer_id, sequence_number) = write.writer_and_sequence();
        self.validate_and_update_sequence_summary(writer_id, sequence_number)?;

        let size = write.data.len();
        let size = u64::try_from(size).expect("appended data must be less than a u64 in length");
        self.size += size;

        self.writes.push(Arc::new(write));
        Ok(())
    }

    // checks that the sequence numbers in this segment are monotonically
    // increasing. Also keeps track of the starting and ending sequence numbers
    // and if any were missing.
    fn validate_and_update_sequence_summary(
        &mut self,
        writer_id: WriterId,
        sequence_number: u64,
    ) -> Result<()> {
        match self.writers.get_mut(&writer_id) {
            Some(summary) => {
                if summary.end_sequence >= sequence_number {
                    return SequenceOutOfOrder {
                        writer: writer_id,
                        current_sequence: summary.end_sequence,
                        incoming_sequence: sequence_number,
                    }
                    .fail();
                } else if summary.end_sequence + 1 != sequence_number {
                    summary.missing_sequence = true;
                }

                summary.end_sequence = sequence_number;
            }
            None => {
                let summary = WriterSummary {
                    start_sequence: sequence_number,
                    end_sequence: sequence_number,
                    missing_sequence: false,
                };

                self.writers.insert(writer_id, summary);
            }
        }

        Ok(())
    }

    /// sets the time this segment was persisted at
    #[allow(dead_code)]
    pub async fn set_persisted_at(&self, time: DateTime<Utc>) {
        let mut persisted = self.persisted.lock().await;
        *persisted = Some(time);
    }

    /// returns the time this segment was persisted at or none if not set
    pub async fn persisted_at(&self) -> Option<DateTime<Utc>> {
        let persisted = self.persisted.lock().await;
        *persisted
    }
}

/// The summary information for a writer that has data in a segment
#[derive(Debug, Eq, PartialEq)]
pub struct WriterSummary {
    start_sequence: u64,
    end_sequence: u64,
    missing_sequence: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct WriterSequence {
    pub id: WriterId,
    pub sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{data::lines_to_replicated_write, database_rules::DatabaseRules};
    use influxdb_line_protocol::parse_lines;

    #[tokio::test]
    async fn append_increments_current_size_and_uses_existing_segment() {
        let max = 1 << 32;
        let segment = 1 << 16;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::ReturnError);
        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");

        let size = write.data.len() as u64;
        assert_eq!(0, buf.size());
        let segment = buf.append(write).await.unwrap();
        assert_eq!(size, buf.size());
        assert!(segment.is_none());

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        assert_eq!(size * 2, buf.size());
        assert!(segment.is_none());
    }

    #[tokio::test]
    async fn append_rolls_over_segment() {
        let max = 1 << 16;
        let segment = 1;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::ReturnError);
        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");

        let segment = buf.append(write).await.unwrap();
        let segment = segment.unwrap();
        assert_eq!(segment.id, 1);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");

        let segment = buf.append(write).await.unwrap();
        let segment = segment.unwrap();
        assert_eq!(segment.id, 2);
    }

    #[tokio::test]
    async fn drops_persisted_segment_when_over_size() {
        let max = 600;
        let segment = 1;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::ReturnError);

        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap().unwrap();
        assert_eq!(1, segment.id);
        assert!(segment.persisted_at().await.is_none());
        segment.set_persisted_at(Utc::now()).await;

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        let segment = segment.unwrap();
        assert_eq!(3, segment.id);
        assert!(segment.persisted_at().await.is_none());

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(2, buf.closed_segments[0].id);
        assert_eq!(3, buf.closed_segments[1].id);
    }

    #[tokio::test]
    async fn drops_old_segment_even_if_not_persisted() {
        let max = 600;
        let segment = 1;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::DropOldSegment);

        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap().unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        let segment = segment.unwrap();
        assert_eq!(3, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(2, buf.closed_segments[0].id);
        assert_eq!(3, buf.closed_segments[1].id);
    }

    #[tokio::test]
    async fn drops_incoming_write_if_oldest_segment_not_persisted() {
        let max = 600;
        let segment = 1;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::DropIncoming);

        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap().unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        assert!(segment.is_none());

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);
    }

    #[tokio::test]
    async fn returns_error_if_oldest_segment_not_persisted() {
        let max = 600;
        let segment = 1;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::ReturnError);

        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap().unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        assert!(buf.append(write).await.is_err());

        assert_eq!(2, buf.closed_segments.len());
        assert_eq!(1, buf.closed_segments[0].id);
        assert_eq!(2, buf.closed_segments[1].id);
    }

    #[tokio::test]
    async fn all_writes_since() {
        let max = 1 << 63;
        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = (write.data.len() + 1) as u64;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::ReturnError);

        let segment = buf.append(write).await.unwrap();
        assert!(segment.is_none());

        let write = lp_to_replicated_write(2, 1, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        let segment = segment.unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        assert!(segment.is_none());

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        let write = lp_to_replicated_write(2, 2, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        assert!(segment.is_none());

        let writes = buf.all_writes_since(WriterSequence { id: 0, sequence: 1 });
        assert_eq!(5, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(1, 1));
        assert!(writes[1].equal_to_writer_and_sequence(2, 1));
        assert!(writes[2].equal_to_writer_and_sequence(1, 2));
        assert!(writes[3].equal_to_writer_and_sequence(1, 3));
        assert!(writes[4].equal_to_writer_and_sequence(2, 2));

        let writes = buf.all_writes_since(WriterSequence { id: 1, sequence: 1 });
        assert_eq!(4, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(2, 1));
        assert!(writes[1].equal_to_writer_and_sequence(1, 2));
        assert!(writes[2].equal_to_writer_and_sequence(1, 3));
        assert!(writes[3].equal_to_writer_and_sequence(2, 2));

        let writes = buf.all_writes_since(WriterSequence { id: 2, sequence: 1 });
        assert_eq!(3, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(1, 2));
        assert!(writes[1].equal_to_writer_and_sequence(1, 3));
        assert!(writes[2].equal_to_writer_and_sequence(2, 2));

        let writes = buf.all_writes_since(WriterSequence { id: 1, sequence: 3 });
        assert_eq!(1, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(2, 2));

        let writes = buf.all_writes_since(WriterSequence { id: 2, sequence: 2 });
        assert_eq!(0, writes.len());
    }

    #[tokio::test]
    async fn writes_since() {
        let max = 1 << 63;
        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        let segment = (write.data.len() + 1) as u64;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::ReturnError);

        let segment = buf.append(write).await.unwrap();
        assert!(segment.is_none());

        let write = lp_to_replicated_write(2, 1, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        let segment = segment.unwrap();
        assert_eq!(1, segment.id);

        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        assert!(segment.is_none());

        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        let segment = segment.unwrap();
        assert_eq!(2, segment.id);

        let write = lp_to_replicated_write(2, 2, "cpu val=1 10");
        let segment = buf.append(write).await.unwrap();
        assert!(segment.is_none());

        let writes = buf.writes_since(WriterSequence { id: 0, sequence: 1 });
        assert_eq!(0, writes.len());

        let writes = buf.writes_since(WriterSequence { id: 1, sequence: 0 });
        assert_eq!(3, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(1, 1));
        assert!(writes[1].equal_to_writer_and_sequence(1, 2));
        assert!(writes[2].equal_to_writer_and_sequence(1, 3));

        let writes = buf.writes_since(WriterSequence { id: 1, sequence: 1 });
        assert_eq!(2, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(1, 2));
        assert!(writes[1].equal_to_writer_and_sequence(1, 3));

        let writes = buf.writes_since(WriterSequence { id: 2, sequence: 1 });
        assert_eq!(1, writes.len());
        assert!(writes[0].equal_to_writer_and_sequence(2, 2));
    }

    #[tokio::test]
    async fn returns_error_if_sequence_decreases() {
        let max = 1 << 63;
        let write = lp_to_replicated_write(1, 3, "cpu val=1 10");
        let segment = (write.data.len() + 1) as u64;
        let mut buf = Buffer::new(max, segment, WalBufferRollover::ReturnError);

        let segment = buf.append(write).await.unwrap();
        assert!(segment.is_none());

        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        assert!(buf.append(write).await.is_err());
    }

    #[test]
    fn segment_keeps_writer_summaries() {
        let mut segment = Segment::new(1);
        let write = lp_to_replicated_write(1, 1, "cpu val=1 10");
        segment.append(write).unwrap();
        let write = lp_to_replicated_write(2, 1, "cpu val=1 10");
        segment.append(write).unwrap();
        let write = lp_to_replicated_write(1, 2, "cpu val=1 10");
        segment.append(write).unwrap();
        let write = lp_to_replicated_write(2, 4, "cpu val=1 10");
        segment.append(write).unwrap();

        let summary = segment.writers.get(&1).unwrap();
        assert_eq!(
            &WriterSummary {
                start_sequence: 1,
                end_sequence: 2,
                missing_sequence: false
            },
            summary
        );

        let summary = segment.writers.get(&2).unwrap();
        assert_eq!(
            &WriterSummary {
                start_sequence: 1,
                end_sequence: 4,
                missing_sequence: true
            },
            summary
        );
    }

    fn lp_to_replicated_write(writer_id: u32, sequence_number: u64, lp: &str) -> ReplicatedWrite {
        let lines: Vec<_> = parse_lines(lp).map(|l| l.unwrap()).collect();
        let rules = DatabaseRules::default();
        lines_to_replicated_write(writer_id, sequence_number, &lines, &rules)
    }
}
