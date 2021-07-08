use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use regex::Regex;

use data_types::database_rules::{
    HashRing, Matcher, MatcherToShard, NodeGroup, Shard, ShardConfig,
};
use data_types::server_id::ServerId;

use crate::google::{FieldViolation, FieldViolationExt, FromField};
use crate::influxdata::iox::management::v1 as management;

impl From<ShardConfig> for management::ShardConfig {
    fn from(shard_config: ShardConfig) -> Self {
        Self {
            specific_targets: shard_config
                .specific_targets
                .into_iter()
                .map(|i| i.into())
                .collect(),
            hash_ring: shard_config.hash_ring.map(|i| i.into()),
            ignore_errors: shard_config.ignore_errors,
            shards: shard_config
                .shards
                .iter()
                .map(|(k, v)| (*k, v.clone().into()))
                .collect(),
        }
    }
}

impl TryFrom<management::ShardConfig> for ShardConfig {
    type Error = FieldViolation;

    fn try_from(proto: management::ShardConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            specific_targets: proto
                .specific_targets
                .into_iter()
                .map(|i| i.try_into())
                .collect::<Result<_, FieldViolation>>()
                .field("specific_targets")?,
            hash_ring: proto
                .hash_ring
                .map(|i| i.try_into())
                .map_or(Ok(None), |r| r.map(Some))
                .field("hash_ring")?,
            ignore_errors: proto.ignore_errors,
            shards: Arc::new(
                proto
                    .shards
                    .into_iter()
                    .map(|(k, v)| Ok((k, v.try_into()?)))
                    .collect::<Result<_, FieldViolation>>()
                    .field("shards")?,
            ),
        })
    }
}

/// Returns none if v matches its default value.
fn none_if_default<T: Default + PartialEq>(v: T) -> Option<T> {
    if v == Default::default() {
        None
    } else {
        Some(v)
    }
}

impl From<MatcherToShard> for management::MatcherToShard {
    fn from(matcher_to_shard: MatcherToShard) -> Self {
        Self {
            matcher: none_if_default(matcher_to_shard.matcher.into()),
            shard: matcher_to_shard.shard,
        }
    }
}

impl TryFrom<management::MatcherToShard> for MatcherToShard {
    type Error = FieldViolation;

    fn try_from(proto: management::MatcherToShard) -> Result<Self, Self::Error> {
        Ok(Self {
            matcher: proto.matcher.unwrap_or_default().scope("matcher")?,
            shard: proto.shard,
        })
    }
}

impl From<HashRing> for management::HashRing {
    fn from(hash_ring: HashRing) -> Self {
        Self {
            table_name: hash_ring.table_name,
            columns: hash_ring.columns,
            shards: hash_ring.shards.into(),
        }
    }
}

impl TryFrom<management::HashRing> for HashRing {
    type Error = FieldViolation;

    fn try_from(proto: management::HashRing) -> Result<Self, Self::Error> {
        Ok(Self {
            table_name: proto.table_name,
            columns: proto.columns,
            shards: proto.shards.into(),
        })
    }
}

impl From<Shard> for management::Shard {
    fn from(shard: Shard) -> Self {
        let sink = match shard {
            Shard::Iox(node_group) => management::shard::Sink::Iox(node_group.into()),
        };
        management::Shard { sink: Some(sink) }
    }
}

impl TryFrom<management::Shard> for Shard {
    type Error = FieldViolation;

    fn try_from(proto: management::Shard) -> Result<Self, Self::Error> {
        let sink = proto.sink.ok_or_else(|| FieldViolation::required(""))?;
        Ok(match sink {
            management::shard::Sink::Iox(node_group) => Shard::Iox(node_group.scope("node_group")?),
        })
    }
}

impl From<NodeGroup> for management::NodeGroup {
    fn from(node_group: NodeGroup) -> Self {
        Self {
            nodes: node_group
                .into_iter()
                .map(|id| management::node_group::Node { id: id.get_u32() })
                .collect(),
        }
    }
}

impl TryFrom<management::NodeGroup> for NodeGroup {
    type Error = FieldViolation;

    fn try_from(proto: management::NodeGroup) -> Result<Self, Self::Error> {
        proto
            .nodes
            .into_iter()
            .map(|i| {
                ServerId::try_from(i.id).map_err(|_| FieldViolation {
                    field: "id".to_string(),
                    description: "Node ID must be nonzero".to_string(),
                })
            })
            .collect()
    }
}

impl From<Matcher> for management::Matcher {
    fn from(matcher: Matcher) -> Self {
        Self {
            table_name_regex: matcher
                .table_name_regex
                .map(|r| r.to_string())
                .unwrap_or_default(),
            predicate: matcher.predicate.unwrap_or_default(),
        }
    }
}

impl TryFrom<management::Matcher> for Matcher {
    type Error = FieldViolation;

    fn try_from(proto: management::Matcher) -> Result<Self, Self::Error> {
        let table_name_regex = match &proto.table_name_regex as &str {
            "" => None,
            re => Some(Regex::new(re).map_err(|e| FieldViolation {
                field: "table_name_regex".to_string(),
                description: e.to_string(),
            })?),
        };
        let predicate = match proto.predicate {
            p if p.is_empty() => None,
            p => Some(p),
        };

        Ok(Self {
            table_name_regex,
            predicate,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::consistent_hasher::ConsistentHasher;
    use data_types::database_rules::DatabaseRules;

    #[test]
    fn test_matcher_default() {
        let protobuf = management::Matcher {
            ..Default::default()
        };

        let matcher: Matcher = protobuf.clone().try_into().unwrap();
        let back: management::Matcher = matcher.clone().into();

        assert!(matcher.table_name_regex.is_none());
        assert_eq!(protobuf.table_name_regex, back.table_name_regex);

        assert_eq!(matcher.predicate, None);
        assert_eq!(protobuf.predicate, back.predicate);
    }

    #[test]
    fn test_matcher_regexp() {
        let protobuf = management::Matcher {
            table_name_regex: "^foo$".into(),
            ..Default::default()
        };

        let matcher: Matcher = protobuf.clone().try_into().unwrap();
        let back: management::Matcher = matcher.clone().into();

        assert_eq!(matcher.table_name_regex.unwrap().to_string(), "^foo$");
        assert_eq!(protobuf.table_name_regex, back.table_name_regex);
    }

    #[test]
    fn test_matcher_bad_regexp() {
        let protobuf = management::Matcher {
            table_name_regex: "*".into(),
            ..Default::default()
        };

        let matcher: Result<Matcher, FieldViolation> = protobuf.try_into();
        assert!(matcher.is_err());
        assert_eq!(matcher.err().unwrap().field, "table_name_regex");
    }

    #[test]
    fn test_hash_ring_default() {
        let protobuf = management::HashRing {
            ..Default::default()
        };

        let hash_ring: HashRing = protobuf.clone().try_into().unwrap();
        let back: management::HashRing = hash_ring.clone().into();

        assert!(!hash_ring.table_name);
        assert_eq!(protobuf.table_name, back.table_name);
        assert!(hash_ring.columns.is_empty());
        assert_eq!(protobuf.columns, back.columns);
        assert!(hash_ring.shards.is_empty());
        assert_eq!(protobuf.shards, back.shards);
    }

    #[test]
    fn test_hash_ring_nodes() {
        let protobuf = management::HashRing {
            shards: vec![1, 2],
            ..Default::default()
        };

        let hash_ring: HashRing = protobuf.try_into().unwrap();

        assert_eq!(hash_ring.shards.len(), 2);
        assert_eq!(hash_ring.shards.find(1), Some(2));
        assert_eq!(hash_ring.shards.find(2), Some(1));
    }

    #[test]
    fn test_matcher_to_shard_default() {
        let protobuf = management::MatcherToShard {
            ..Default::default()
        };

        let matcher_to_shard: MatcherToShard = protobuf.clone().try_into().unwrap();
        let back: management::MatcherToShard = matcher_to_shard.clone().into();

        assert_eq!(
            matcher_to_shard.matcher,
            Matcher {
                ..Default::default()
            }
        );
        assert_eq!(protobuf.matcher, back.matcher);

        assert_eq!(matcher_to_shard.shard, 0);
        assert_eq!(protobuf.shard, back.shard);
    }

    #[test]
    fn test_shard_config_default() {
        let protobuf = management::ShardConfig {
            ..Default::default()
        };

        let shard_config: ShardConfig = protobuf.clone().try_into().unwrap();
        let back: management::ShardConfig = shard_config.clone().into();

        assert!(shard_config.specific_targets.is_empty());
        assert_eq!(protobuf.specific_targets, back.specific_targets);

        assert!(shard_config.hash_ring.is_none());
        assert_eq!(protobuf.hash_ring, back.hash_ring);

        assert!(!shard_config.ignore_errors);
        assert_eq!(protobuf.ignore_errors, back.ignore_errors);

        assert!(shard_config.shards.is_empty());
        assert_eq!(protobuf.shards, back.shards);
    }

    #[test]
    fn test_database_rules_shard_config() {
        let protobuf = management::DatabaseRules {
            name: "database".to_string(),
            routing_rules: Some(management::database_rules::RoutingRules::ShardConfig(
                management::ShardConfig {
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        let rules: DatabaseRules = protobuf.try_into().unwrap();
        let back: management::DatabaseRules = rules.into();

        assert!(back.routing_rules.is_some());
        assert!(matches!(
            back.routing_rules,
            Some(management::database_rules::RoutingRules::ShardConfig(_))
        ));
    }

    #[test]
    fn test_shard_config_shards() {
        let protobuf = management::ShardConfig {
            shards: vec![
                (
                    1,
                    management::Shard {
                        sink: Some(management::shard::Sink::Iox(management::NodeGroup {
                            nodes: vec![
                                management::node_group::Node { id: 10 },
                                management::node_group::Node { id: 11 },
                                management::node_group::Node { id: 12 },
                            ],
                        })),
                    },
                ),
                (
                    2,
                    management::Shard {
                        sink: Some(management::shard::Sink::Iox(management::NodeGroup {
                            nodes: vec![management::node_group::Node { id: 20 }],
                        })),
                    },
                ),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        let shard_config: ShardConfig = protobuf.try_into().unwrap();

        assert_eq!(shard_config.shards.len(), 2);
        assert!(
            matches!(&shard_config.shards[&1], Shard::Iox(node_group) if node_group.len() == 3)
        );
        assert!(
            matches!(&shard_config.shards[&2], Shard::Iox(node_group) if node_group.len() == 1)
        );
    }

    #[test]
    fn test_sharder() {
        let protobuf = management::ShardConfig {
            specific_targets: vec![management::MatcherToShard {
                matcher: Some(management::Matcher {
                    table_name_regex: "pu\\d.$".to_string(),
                    ..Default::default()
                }),
                shard: 1,
            }],
            hash_ring: Some(management::HashRing {
                table_name: true,
                columns: vec!["t1".to_string(), "t2".to_string()],
                shards: vec![1, 2, 3, 4],
            }),
            ..Default::default()
        };

        let shard_config: ShardConfig = protobuf.try_into().unwrap();

        assert_eq!(
            shard_config,
            ShardConfig {
                specific_targets: vec![MatcherToShard {
                    matcher: data_types::database_rules::Matcher {
                        table_name_regex: Some(Regex::new("pu\\d.$").unwrap()),
                        predicate: None
                    },
                    shard: 1
                }],
                hash_ring: Some(HashRing {
                    table_name: true,
                    columns: vec!["t1".to_string(), "t2".to_string(),],
                    shards: ConsistentHasher::new(&[1, 2, 3, 4])
                }),
                ..Default::default()
            }
        );
    }
}
