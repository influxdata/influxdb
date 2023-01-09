use base64::{prelude::BASE64_STANDARD, Engine};
use data_types::{SequenceNumber, ShardIndex, ShardWriteStatus};
use dml::DmlMeta;
/// Protobuf to/from conversion
use generated_types::influxdata::iox::write_summary::v1 as proto;
use observability_deps::tracing::debug;
use snafu::{OptionExt, Snafu};
use std::collections::BTreeMap;

mod progress;
pub use progress::ShardProgress;

#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum Error {
    #[snafu(display("Unknown shard index: {}", shard_index))]
    UnknownShard { shard_index: ShardIndex },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Contains information about a single write.
///
/// A single write consisting of multiple lines of line protocol
/// formatted data are shared and partitioned across potentially
/// several shards which are then processed by the ingester to
/// become readable at potentially different times.
///
/// This struct contains sufficient information to determine the
/// current state of the write as a whole
#[derive(Debug, Default, Clone, PartialEq, Eq)]
/// Summary of a `Vec<Vec<DmlMeta>>`
pub struct WriteSummary {
    /// Key is the shard index from the `DmlMeta` structure (aka kafka
    /// partition id), value is the sequence numbers from that
    /// shard.
    ///
    /// Note: `BTreeMap` to ensure the output is in a consistent order
    shards: BTreeMap<ShardIndex, Vec<SequenceNumber>>,
}

impl WriteSummary {
    pub fn new(metas: Vec<Vec<DmlMeta>>) -> Self {
        debug!(?metas, "Creating write summary");
        let sequences = metas
            .iter()
            .flat_map(|v| v.iter())
            .filter_map(|meta| meta.sequence());

        let mut shards = BTreeMap::new();
        for s in sequences {
            let shard_index = s.shard_index;
            let sequence_number = s.sequence_number;

            shards
                .entry(shard_index)
                .or_insert_with(Vec::new)
                .push(sequence_number)
        }

        Self { shards }
    }

    /// Return an opaque summary "token" of this summary
    pub fn to_token(self) -> String {
        let proto_write_summary: proto::WriteSummary = self.into();
        BASE64_STANDARD.encode(
            serde_json::to_string(&proto_write_summary)
                .expect("unexpected error serializing token to json"),
        )
    }

    /// Return a WriteSummary from the "token" (created with [Self::to_token]), or error if not possible
    pub fn try_from_token(token: &str) -> Result<Self, String> {
        let data = BASE64_STANDARD
            .decode(token)
            .map_err(|e| format!("Invalid write token, invalid base64: {}", e))?;

        let json = String::from_utf8(data)
            .map_err(|e| format!("Invalid write token, non utf8 data in write token: {}", e))?;

        let proto = serde_json::from_str::<proto::WriteSummary>(&json)
            .map_err(|e| format!("Invalid write token, protobuf decode error: {}", e))?;

        proto
            .try_into()
            .map_err(|e| format!("Invalid write token, invalid content: {}", e))
    }

    /// return what shard indexes from the write buffer were present in this write summary
    pub fn shard_indexes(&self) -> Vec<ShardIndex> {
        self.shards.keys().cloned().collect()
    }

    /// Given the write described by this summary, and the shard's progress for a particular
    /// shard index, returns the status of that write in this write summary
    pub fn write_status(
        &self,
        shard_index: ShardIndex,
        progress: &ShardProgress,
    ) -> Result<ShardWriteStatus> {
        let sequence_numbers = self
            .shards
            .get(&shard_index)
            .context(UnknownShardSnafu { shard_index })?;

        debug!(?shard_index, ?progress, ?sequence_numbers, "write_status");

        if progress.is_empty() {
            return Ok(ShardWriteStatus::ShardUnknown);
        }

        let is_persisted = sequence_numbers
            .iter()
            .all(|sequence_number| progress.persisted(*sequence_number));

        if is_persisted {
            return Ok(ShardWriteStatus::Persisted);
        }

        let is_readable = sequence_numbers
            .iter()
            .all(|sequence_number| progress.readable(*sequence_number));

        if is_readable {
            return Ok(ShardWriteStatus::Readable);
        }

        Ok(ShardWriteStatus::Durable)
    }
}

impl From<WriteSummary> for proto::WriteSummary {
    fn from(summary: WriteSummary) -> Self {
        let shards = summary
            .shards
            .into_iter()
            .map(|(shard_index, sequence_numbers)| proto::ShardWrite {
                shard_index: shard_index.get(),
                sequence_numbers: sequence_numbers.into_iter().map(|v| v.get()).collect(),
            })
            .collect();

        Self { shards }
    }
}

impl TryFrom<proto::WriteSummary> for WriteSummary {
    type Error = String;

    fn try_from(summary: proto::WriteSummary) -> Result<Self, Self::Error> {
        let shards = summary
            .shards
            .into_iter()
            .map(
                |proto::ShardWrite {
                     shard_index,
                     sequence_numbers,
                 }| {
                    let sequence_numbers = sequence_numbers
                        .into_iter()
                        .map(SequenceNumber::new)
                        .collect::<Vec<_>>();

                    Ok((ShardIndex::new(shard_index), sequence_numbers))
                },
            )
            .collect::<Result<BTreeMap<_, _>, String>>()?;

        Ok(Self { shards })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::Sequence;

    #[test]
    fn empty() {
        let metas = vec![];
        let summary: proto::WriteSummary = WriteSummary::new(metas).into();

        let expected = proto::WriteSummary { shards: vec![] };

        assert_eq!(summary, expected);
    }

    #[test]
    fn one() {
        let metas = vec![vec![make_meta(Sequence::new(
            ShardIndex::new(1),
            SequenceNumber::new(2),
        ))]];
        let summary: proto::WriteSummary = WriteSummary::new(metas).into();

        let expected = proto::WriteSummary {
            shards: vec![proto::ShardWrite {
                shard_index: 1,
                sequence_numbers: vec![2],
            }],
        };

        assert_eq!(summary, expected);
    }

    #[test]
    fn many() {
        let metas = vec![
            vec![
                make_meta(Sequence::new(ShardIndex::new(1), SequenceNumber::new(2))),
                make_meta(Sequence::new(ShardIndex::new(10), SequenceNumber::new(20))),
            ],
            vec![make_meta(Sequence::new(
                ShardIndex::new(1),
                SequenceNumber::new(3),
            ))],
        ];
        let summary: proto::WriteSummary = WriteSummary::new(metas).into();

        let expected = proto::WriteSummary {
            shards: vec![
                proto::ShardWrite {
                    shard_index: 1,
                    sequence_numbers: vec![2, 3],
                },
                proto::ShardWrite {
                    shard_index: 10,
                    sequence_numbers: vec![20],
                },
            ],
        };

        assert_eq!(summary, expected);
    }

    #[test]
    fn different_order() {
        // order in sequences shouldn't matter
        let metas1 = vec![vec![
            make_meta(Sequence::new(ShardIndex::new(1), SequenceNumber::new(2))),
            make_meta(Sequence::new(ShardIndex::new(2), SequenceNumber::new(3))),
        ]];

        // order in sequences shouldn't matter
        let metas2 = vec![vec![
            make_meta(Sequence::new(ShardIndex::new(2), SequenceNumber::new(3))),
            make_meta(Sequence::new(ShardIndex::new(1), SequenceNumber::new(2))),
        ]];

        let summary1: proto::WriteSummary = WriteSummary::new(metas1).into();
        let summary2: proto::WriteSummary = WriteSummary::new(metas2).into();

        let expected = proto::WriteSummary {
            shards: vec![
                proto::ShardWrite {
                    shard_index: 1,
                    sequence_numbers: vec![2],
                },
                proto::ShardWrite {
                    shard_index: 2,
                    sequence_numbers: vec![3],
                },
            ],
        };

        assert_eq!(summary1, expected);
        assert_eq!(summary2, expected);
    }

    #[test]
    fn token_creation() {
        let metas = vec![vec![make_meta(Sequence::new(
            ShardIndex::new(1),
            SequenceNumber::new(2),
        ))]];
        let summary = WriteSummary::new(metas.clone());
        let summary_copy = WriteSummary::new(metas);

        let metas2 = vec![vec![make_meta(Sequence::new(
            ShardIndex::new(2),
            SequenceNumber::new(3),
        ))]];
        let summary2 = WriteSummary::new(metas2);

        let token = summary.to_token();

        // non empty
        assert!(!token.is_empty());

        // same when created with same metas
        assert_eq!(token, summary_copy.to_token());

        // different when created with different metas
        assert_ne!(token, summary2.to_token());

        assert!(
            !token.contains("sequenceNumbers"),
            "token not obscured: {}",
            token
        );
        assert!(!token.contains("shards"), "token not obscured: {}", token);
    }

    #[test]
    fn token_parsing() {
        let metas = vec![vec![make_meta(Sequence::new(
            ShardIndex::new(1),
            SequenceNumber::new(2),
        ))]];
        let summary = WriteSummary::new(metas);

        let token = summary.clone().to_token();

        // round trip should parse to the same summary
        let new_summary = WriteSummary::try_from_token(&token).expect("parsing successful");
        assert_eq!(summary, new_summary);
    }

    #[test]
    #[should_panic(expected = "Invalid write token, invalid base64")]
    fn token_parsing_bad_base64() {
        let token = "foo%%";
        WriteSummary::try_from_token(token).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid write token, non utf8 data in write token")]
    fn token_parsing_bad_utf8() {
        let token = BASE64_STANDARD.encode(vec![0xa0, 0xa1]);
        WriteSummary::try_from_token(&token).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid write token, protobuf decode error: key must be a string")]
    fn token_parsing_bad_proto() {
        let token = BASE64_STANDARD.encode("{not_valid_json}");
        WriteSummary::try_from_token(&token).unwrap();
    }

    #[test]
    fn no_progress() {
        let summary = test_summary();

        // if we have no info about this shard in the progress
        let shard_index = ShardIndex::new(1);
        let progress = ShardProgress::new();
        assert_eq!(
            summary.write_status(shard_index, &progress),
            Ok(ShardWriteStatus::ShardUnknown)
        );
    }

    #[test]
    fn unknown_shard() {
        let summary = test_summary();
        // No information on shard index 3
        let shard_index = ShardIndex::new(3);
        let progress = ShardProgress::new().with_buffered(SequenceNumber::new(2));
        let err = summary.write_status(shard_index, &progress).unwrap_err();
        assert_eq!(err.to_string(), "Unknown shard index: 3");
    }

    #[test]
    fn readable() {
        let summary = test_summary();

        // shard index 1 made it to sequence number 3
        let shard_index = ShardIndex::new(1);
        let progress = ShardProgress::new().with_buffered(SequenceNumber::new(3));
        assert_eq!(
            summary.write_status(shard_index, &progress),
            Ok(ShardWriteStatus::Readable)
        );

        // if shard index 1 only made it to sequence number 2, but write includes sequence number 3
        let shard_index = ShardIndex::new(1);
        let progress = ShardProgress::new().with_buffered(SequenceNumber::new(2));
        assert_eq!(
            summary.write_status(shard_index, &progress),
            Ok(ShardWriteStatus::Durable)
        );

        // shard index 2 made it to sequence number 2
        let shard_index = ShardIndex::new(2);
        let progress = ShardProgress::new().with_buffered(SequenceNumber::new(2));

        assert_eq!(
            summary.write_status(shard_index, &progress),
            Ok(ShardWriteStatus::Readable)
        );
    }

    #[test]
    fn persisted() {
        let summary = test_summary();

        // shard index 1 has persisted up to sequence number 3
        let shard_index = ShardIndex::new(1);
        let progress = ShardProgress::new().with_persisted(SequenceNumber::new(3));
        assert_eq!(
            summary.write_status(shard_index, &progress),
            Ok(ShardWriteStatus::Persisted)
        );

        // shard index 2 has persisted up to sequence number 2
        let shard_index = ShardIndex::new(2);
        let progress = ShardProgress::new().with_persisted(SequenceNumber::new(2));
        assert_eq!(
            summary.write_status(shard_index, &progress),
            Ok(ShardWriteStatus::Persisted)
        );

        // shard index 1 only persisted up to sequence number 2, have buffered data at sequence
        // number 3
        let shard_index = ShardIndex::new(1);
        let progress = ShardProgress::new()
            .with_buffered(SequenceNumber::new(3))
            .with_persisted(SequenceNumber::new(2));

        assert_eq!(
            summary.write_status(shard_index, &progress),
            Ok(ShardWriteStatus::Readable)
        );
    }

    /// Return a write summary that describes a write with:
    /// shard 1 --> sequence 3
    /// shard 2 --> sequence 1
    fn test_summary() -> WriteSummary {
        let metas = vec![vec![
            make_meta(Sequence::new(ShardIndex::new(1), SequenceNumber::new(2))),
            make_meta(Sequence::new(ShardIndex::new(1), SequenceNumber::new(3))),
            make_meta(Sequence::new(ShardIndex::new(2), SequenceNumber::new(1))),
        ]];
        WriteSummary::new(metas)
    }

    fn make_meta(s: Sequence) -> DmlMeta {
        use iox_time::TimeProvider;
        let time_provider = iox_time::SystemProvider::new();

        let span_context = None;
        let bytes_read = 132;
        DmlMeta::sequenced(s, time_provider.now(), span_context, bytes_read)
    }
}
