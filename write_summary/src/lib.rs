use std::collections::BTreeMap;

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
#[derive(Debug, Default)]
/// Summary of a Vec<Vec<DmlMeta>>
pub struct WriteSummary {
    metas: Vec<Vec<DmlMeta>>,
}

impl WriteSummary {
    pub fn new(metas: Vec<Vec<DmlMeta>>) -> Self {
        Self { metas }
    }

    /// Return an opaque summary "token" of this summary
    pub fn to_token(self) -> String {
        let proto_write_summary: proto::WriteSummary = self.into();
        base64::encode(
            serde_json::to_string(&proto_write_summary)
                .expect("unexpected error serializing token to json"),
        )
    }
}

impl From<WriteSummary> for proto::WriteSummary {
    fn from(summary: WriteSummary) -> Self {
        // create a map from sequencer_id to sequences
        let sequences = summary
            .metas
            .iter()
            .flat_map(|v| v.iter())
            .filter_map(|meta| meta.sequence());

        // Use BTreeMap to ensure consistent output
        let mut sequencers = BTreeMap::new();
        for s in sequences {
            sequencers
                .entry(s.sequencer_id)
                .or_insert_with(Vec::new)
                .push(s.sequence_number)
        }

        let sequencers = sequencers
            .into_iter()
            .map(|(sequencer_id, sequence_numbers)| proto::SequencerWrite {
                sequencer_id,
                sequence_numbers,
            })
            .collect();

        Self { sequencers }
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

    fn make_meta(s: Sequence) -> DmlMeta {
        use time::TimeProvider;
        let time_provider = time::SystemProvider::new();

        let span_context = None;
        let bytes_read = 132;
        DmlMeta::sequenced(s, time_provider.now(), span_context, bytes_read)
    }
}
