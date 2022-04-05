use std::collections::BTreeMap;

use data_types2::{SequenceNumber, SequencerId};
/// Protobuf to/from conversion
use generated_types::influxdata::iox::write_summary::v1 as proto;

use dml::DmlMeta;

/// Contains information about a single write.
///
/// A single write consisting of multiple lines of line protocol
/// formatted data are shared and partitioned across potentially
/// several sequencers which are then processed by the ingester to
/// become readable at potentially different times.
///
/// This struct contains sufficient information to determine the
/// current state of the write as a whole
#[derive(Debug, Default, Clone, PartialEq)]
/// Summary of a Vec<Vec<DmlMeta>>
pub struct WriteSummary {
    /// Key is the sequencer, value is the sequence numbers from that sequencer.
    /// Note: BTreeMap to ensure the output is in a consistent order
    sequencers: BTreeMap<SequencerId, Vec<SequenceNumber>>,
}

impl WriteSummary {
    pub fn new(metas: Vec<Vec<DmlMeta>>) -> Self {
        let sequences = metas
            .iter()
            .flat_map(|v| v.iter())
            .filter_map(|meta| meta.sequence());

        let mut sequencers = BTreeMap::new();
        for s in sequences {
            let sequencer_id: i16 = s.sequencer_id.try_into().expect("Invalid sequencer id");

            let sequence_number: i64 = s
                .sequence_number
                .try_into()
                .expect("Invalid sequencer number");

            sequencers
                .entry(SequencerId::new(sequencer_id))
                .or_insert_with(Vec::new)
                .push(SequenceNumber::new(sequence_number))
        }

        Self { sequencers }
    }

    /// Return an opaque summary "token" of this summary
    pub fn to_token(self) -> String {
        let proto_write_summary: proto::WriteSummary = self.into();
        base64::encode(
            serde_json::to_string(&proto_write_summary)
                .expect("unexpected error serializing token to json"),
        )
    }

    /// Return a WriteSummary from the "token" (created with [to_token]), or error if not possible
    pub fn try_from_token(token: &str) -> Result<Self, String> {
        let data = base64::decode(token)
            .map_err(|e| format!("Invalid write token, invalid base64: {}", e))?;

        let json = String::from_utf8(data)
            .map_err(|e| format!("Invalid write token, non utf8 data in write token: {}", e))?;

        let proto = serde_json::from_str::<proto::WriteSummary>(&json)
            .map_err(|e| format!("Invalid write token, protobuf decode error: {}", e))?;

        proto
            .try_into()
            .map_err(|e| format!("Invalid write token, invalid content: {}", e))
    }

    /// return what sequencer ids were present in this write summary
    pub fn sequencer_ids(&self) -> Vec<SequencerId> {
        self.sequencers.keys().cloned().collect()
    }
}

impl From<WriteSummary> for proto::WriteSummary {
    fn from(summary: WriteSummary) -> Self {
        let sequencers = summary
            .sequencers
            .into_iter()
            .map(|(sequencer_id, sequence_numbers)| proto::SequencerWrite {
                sequencer_id: sequencer_id.get() as i32,
                sequence_numbers: sequence_numbers.into_iter().map(|v| v.get()).collect(),
            })
            .collect();

        Self { sequencers }
    }
}

impl TryFrom<proto::WriteSummary> for WriteSummary {
    type Error = String;

    fn try_from(summary: proto::WriteSummary) -> Result<Self, Self::Error> {
        let sequencers = summary
            .sequencers
            .into_iter()
            .map(
                |proto::SequencerWrite {
                     sequencer_id,
                     sequence_numbers,
                 }| {
                    let sequencer_id = sequencer_id.try_into().map_err(|e| {
                        format!("Invalid sequencer id {} in proto: {}", sequencer_id, e)
                    })?;

                    let sequence_numbers = sequence_numbers
                        .into_iter()
                        .map(SequenceNumber::new)
                        .collect::<Vec<_>>();

                    Ok((SequencerId::new(sequencer_id), sequence_numbers))
                },
            )
            .collect::<Result<BTreeMap<_, _>, String>>()?;

        Ok(Self { sequencers })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::sequence::Sequence;

    #[test]
    fn empty() {
        let metas = vec![];
        let summary: proto::WriteSummary = WriteSummary::new(metas).into();

        let expected = proto::WriteSummary { sequencers: vec![] };

        assert_eq!(summary, expected);
    }

    #[test]
    fn one() {
        let metas = vec![vec![make_meta(Sequence::new(1, 2))]];
        let summary: proto::WriteSummary = WriteSummary::new(metas).into();

        let expected = proto::WriteSummary {
            sequencers: vec![proto::SequencerWrite {
                sequencer_id: 1,
                sequence_numbers: vec![2],
            }],
        };

        assert_eq!(summary, expected);
    }

    #[test]
    fn many() {
        let metas = vec![
            vec![
                make_meta(Sequence::new(1, 2)),
                make_meta(Sequence::new(10, 20)),
            ],
            vec![make_meta(Sequence::new(1, 3))],
        ];
        let summary: proto::WriteSummary = WriteSummary::new(metas).into();

        let expected = proto::WriteSummary {
            sequencers: vec![
                proto::SequencerWrite {
                    sequencer_id: 1,
                    sequence_numbers: vec![2, 3],
                },
                proto::SequencerWrite {
                    sequencer_id: 10,
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
            make_meta(Sequence::new(1, 2)),
            make_meta(Sequence::new(2, 3)),
        ]];

        // order in sequences shouldn't matter
        let metas2 = vec![vec![
            make_meta(Sequence::new(2, 3)),
            make_meta(Sequence::new(1, 2)),
        ]];

        let summary1: proto::WriteSummary = WriteSummary::new(metas1).into();
        let summary2: proto::WriteSummary = WriteSummary::new(metas2).into();

        let expected = proto::WriteSummary {
            sequencers: vec![
                proto::SequencerWrite {
                    sequencer_id: 1,
                    sequence_numbers: vec![2],
                },
                proto::SequencerWrite {
                    sequencer_id: 2,
                    sequence_numbers: vec![3],
                },
            ],
        };

        assert_eq!(summary1, expected);
        assert_eq!(summary2, expected);
    }

    #[test]
    fn token_creation() {
        let metas = vec![vec![make_meta(Sequence::new(1, 2))]];
        let summary = WriteSummary::new(metas.clone());
        let summary_copy = WriteSummary::new(metas);

        let metas2 = vec![vec![make_meta(Sequence::new(2, 3))]];
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
        assert!(
            !token.contains("sequencers"),
            "token not obscured: {}",
            token
        );
    }

    #[test]
    fn token_parsing() {
        let metas = vec![vec![make_meta(Sequence::new(1, 2))]];
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
        let token = base64::encode(vec![0xa0, 0xa1]);
        WriteSummary::try_from_token(&token).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid write token, protobuf decode error: key must be a string")]
    fn token_parsing_bad_proto() {
        let token = base64::encode("{not_valid_json}");
        WriteSummary::try_from_token(&token).unwrap();
    }

    #[test]
    #[should_panic(
        expected = "Invalid write token, invalid content: Invalid sequencer id 2147483647 in proto"
    )]
    fn token_parsing_bad_sequencer() {
        // construct a message with sequencer id that can not be converted into i16
        let bad_proto = proto::WriteSummary {
            sequencers: vec![proto::SequencerWrite {
                sequencer_id: i32::MAX,
                sequence_numbers: vec![2],
            }],
        };

        let token = base64::encode(serde_json::to_string(&bad_proto).unwrap());

        WriteSummary::try_from_token(&token).unwrap();
    }

    fn make_meta(s: Sequence) -> DmlMeta {
        use time::TimeProvider;
        let time_provider = time::SystemProvider::new();

        let span_context = None;
        let bytes_read = 132;
        DmlMeta::sequenced(s, time_provider.now(), span_context, bytes_read)
    }
}
