use data_types::router::{
    HashRing, Matcher, MatcherToShard, QuerySinks, Router, ShardConfig, ShardId, WriteSink,
    WriteSinkSet, WriteSinkVariant,
};
use regex::Regex;

use crate::google::{FieldViolation, FieldViolationExt, FromField, FromOptionalField};
use crate::influxdata::iox::router::v1 as router;

impl From<ShardConfig> for router::ShardConfig {
    fn from(shard_config: ShardConfig) -> Self {
        Self {
            specific_targets: shard_config
                .specific_targets
                .into_iter()
                .map(|i| i.into())
                .collect(),
            hash_ring: shard_config.hash_ring.map(|i| i.into()),
        }
    }
}

impl TryFrom<router::ShardConfig> for ShardConfig {
    type Error = FieldViolation;

    fn try_from(proto: router::ShardConfig) -> Result<Self, Self::Error> {
        Ok(Self {
            specific_targets: proto
                .specific_targets
                .into_iter()
                .map(|i| i.try_into())
                .collect::<Result<_, FieldViolation>>()
                .scope("specific_targets")?,
            hash_ring: proto
                .hash_ring
                .map(|i| i.try_into())
                .map_or(Ok(None), |r| r.map(Some))
                .scope("hash_ring")?,
        })
    }
}

impl From<MatcherToShard> for router::MatcherToShard {
    fn from(matcher_to_shard: MatcherToShard) -> Self {
        Self {
            matcher: none_if_default(matcher_to_shard.matcher.into()),
            shard: matcher_to_shard.shard.get(),
        }
    }
}

impl TryFrom<router::MatcherToShard> for MatcherToShard {
    type Error = FieldViolation;

    fn try_from(proto: router::MatcherToShard) -> Result<Self, Self::Error> {
        Ok(Self {
            matcher: proto.matcher.unwrap_or_default().field("matcher")?,
            shard: ShardId::new(proto.shard),
        })
    }
}

impl From<HashRing> for router::HashRing {
    fn from(hash_ring: HashRing) -> Self {
        let shards: Vec<ShardId> = hash_ring.shards.into();
        Self {
            shards: shards.into_iter().map(|id| id.get()).collect(),
        }
    }
}

impl TryFrom<router::HashRing> for HashRing {
    type Error = FieldViolation;

    fn try_from(proto: router::HashRing) -> Result<Self, Self::Error> {
        Ok(Self {
            shards: proto
                .shards
                .into_iter()
                .map(ShardId::new)
                .collect::<Vec<ShardId>>()
                .into(),
        })
    }
}

impl From<Matcher> for router::Matcher {
    fn from(matcher: Matcher) -> Self {
        Self {
            table_name_regex: matcher
                .table_name_regex
                .map(|r| r.to_string())
                .unwrap_or_default(),
        }
    }
}

impl TryFrom<router::Matcher> for Matcher {
    type Error = FieldViolation;

    fn try_from(proto: router::Matcher) -> Result<Self, Self::Error> {
        let table_name_regex = match &proto.table_name_regex as &str {
            "" => None,
            re => Some(Regex::new(re).map_err(|e| FieldViolation {
                field: "table_name_regex".to_string(),
                description: e.to_string(),
            })?),
        };

        Ok(Self { table_name_regex })
    }
}

impl From<QuerySinks> for router::QuerySinks {
    fn from(query_sinks: QuerySinks) -> Self {
        Self {
            grpc_remotes: query_sinks
                .grpc_remotes
                .into_iter()
                .map(|id| id.get_u32())
                .collect(),
        }
    }
}

impl TryFrom<router::QuerySinks> for QuerySinks {
    type Error = FieldViolation;

    fn try_from(proto: router::QuerySinks) -> Result<Self, Self::Error> {
        Ok(Self {
            grpc_remotes: proto
                .grpc_remotes
                .into_iter()
                .map(|i| i.try_into())
                .collect::<Result<_, data_types::server_id::Error>>()
                .scope("grpc_remotes")?,
        })
    }
}

impl From<WriteSinkVariant> for router::write_sink::Sink {
    fn from(write_sink_variant: WriteSinkVariant) -> Self {
        match write_sink_variant {
            WriteSinkVariant::GrpcRemote(server_id) => {
                router::write_sink::Sink::GrpcRemote(server_id.get_u32())
            }
            WriteSinkVariant::WriteBuffer(write_buffer_conn) => {
                router::write_sink::Sink::WriteBuffer(write_buffer_conn.into())
            }
        }
    }
}

impl TryFrom<router::write_sink::Sink> for WriteSinkVariant {
    type Error = FieldViolation;

    fn try_from(proto: router::write_sink::Sink) -> Result<Self, Self::Error> {
        match proto {
            router::write_sink::Sink::GrpcRemote(server_id) => Ok(WriteSinkVariant::GrpcRemote(
                server_id.try_into().scope("server_id")?,
            )),
            router::write_sink::Sink::WriteBuffer(write_buffer_conn) => {
                Ok(WriteSinkVariant::WriteBuffer(
                    write_buffer_conn
                        .try_into()
                        .scope("write_buffer_connection")?,
                ))
            }
        }
    }
}

impl From<WriteSink> for router::WriteSink {
    fn from(write_sink: WriteSink) -> Self {
        Self {
            sink: Some(write_sink.sink.into()),
            ignore_errors: write_sink.ignore_errors,
        }
    }
}

impl TryFrom<router::WriteSink> for WriteSink {
    type Error = FieldViolation;

    fn try_from(proto: router::WriteSink) -> Result<Self, Self::Error> {
        Ok(Self {
            sink: proto.sink.required("sink")?,
            ignore_errors: proto.ignore_errors,
        })
    }
}

impl From<WriteSinkSet> for router::WriteSinkSet {
    fn from(write_sink_set: WriteSinkSet) -> Self {
        Self {
            sinks: write_sink_set
                .sinks
                .into_iter()
                .map(|sink| sink.into())
                .collect(),
        }
    }
}

impl TryFrom<router::WriteSinkSet> for WriteSinkSet {
    type Error = FieldViolation;

    fn try_from(proto: router::WriteSinkSet) -> Result<Self, Self::Error> {
        Ok(Self {
            sinks: proto
                .sinks
                .into_iter()
                .map(|sink| sink.try_into())
                .collect::<Result<_, FieldViolation>>()?,
        })
    }
}

impl From<Router> for router::Router {
    fn from(router: Router) -> Self {
        Self {
            name: router.name,
            write_sharder: none_if_default(router.write_sharder.into()),
            write_sinks: router
                .write_sinks
                .into_iter()
                .map(|(id, sink_set)| (id.get(), sink_set.into()))
                .collect(),
            query_sinks: none_if_default(router.query_sinks.into()),
        }
    }
}

impl TryFrom<router::Router> for Router {
    type Error = FieldViolation;

    fn try_from(proto: router::Router) -> Result<Self, Self::Error> {
        Ok(Self {
            name: proto.name,
            write_sharder: proto
                .write_sharder
                .optional("write_sharder")?
                .unwrap_or_default(),
            write_sinks: proto
                .write_sinks
                .into_iter()
                .map(|(id, sink_set)| Ok((ShardId::new(id), sink_set.try_into()?)))
                .collect::<Result<_, FieldViolation>>()?,
            query_sinks: proto
                .query_sinks
                .optional("query_sinks")?
                .unwrap_or_default(),
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use data_types::{consistent_hasher::ConsistentHasher, server_id::ServerId};

    use super::*;

    #[test]
    fn test_matcher_default() {
        let protobuf = router::Matcher {
            ..Default::default()
        };

        let matcher: Matcher = protobuf.clone().try_into().unwrap();
        let back: router::Matcher = matcher.clone().into();

        assert!(matcher.table_name_regex.is_none());
        assert_eq!(protobuf.table_name_regex, back.table_name_regex);
    }

    #[test]
    fn test_matcher_regexp() {
        let protobuf = router::Matcher {
            table_name_regex: "^foo$".into(),
        };

        let matcher: Matcher = protobuf.clone().try_into().unwrap();
        let back: router::Matcher = matcher.clone().into();

        assert_eq!(matcher.table_name_regex.unwrap().to_string(), "^foo$");
        assert_eq!(protobuf.table_name_regex, back.table_name_regex);
    }

    #[test]
    fn test_matcher_bad_regexp() {
        let protobuf = router::Matcher {
            table_name_regex: "*".into(),
        };

        let matcher: Result<Matcher, FieldViolation> = protobuf.try_into();
        assert!(matcher.is_err());
        assert_eq!(matcher.err().unwrap().field, "table_name_regex");
    }

    #[test]
    fn test_hash_ring_default() {
        let protobuf = router::HashRing {
            ..Default::default()
        };

        let hash_ring: HashRing = protobuf.clone().try_into().unwrap();
        let back: router::HashRing = hash_ring.clone().into();

        assert!(hash_ring.shards.is_empty());
        assert_eq!(protobuf.shards, back.shards);
    }

    #[test]
    fn test_hash_ring_nodes() {
        let protobuf = router::HashRing { shards: vec![1, 2] };

        let hash_ring: HashRing = protobuf.try_into().unwrap();

        assert_eq!(hash_ring.shards.len(), 2);
        assert_eq!(hash_ring.shards.find(1), Some(ShardId::new(2)));
        assert_eq!(hash_ring.shards.find(2), Some(ShardId::new(1)));
    }

    #[test]
    fn test_matcher_to_shard_default() {
        let protobuf = router::MatcherToShard {
            ..Default::default()
        };

        let matcher_to_shard: MatcherToShard = protobuf.clone().try_into().unwrap();
        let back: router::MatcherToShard = matcher_to_shard.clone().into();

        assert_eq!(
            matcher_to_shard.matcher,
            Matcher {
                ..Default::default()
            }
        );
        assert_eq!(protobuf.matcher, back.matcher);

        assert_eq!(matcher_to_shard.shard, ShardId::new(0));
        assert_eq!(protobuf.shard, back.shard);
    }

    #[test]
    fn test_shard_config_default() {
        let protobuf = router::ShardConfig {
            ..Default::default()
        };

        let shard_config: ShardConfig = protobuf.clone().try_into().unwrap();
        let back: router::ShardConfig = shard_config.clone().into();

        assert!(shard_config.specific_targets.is_empty());
        assert_eq!(protobuf.specific_targets, back.specific_targets);

        assert!(shard_config.hash_ring.is_none());
        assert_eq!(protobuf.hash_ring, back.hash_ring);
    }

    #[test]
    fn test_sharder() {
        let protobuf = router::ShardConfig {
            specific_targets: vec![router::MatcherToShard {
                matcher: Some(router::Matcher {
                    table_name_regex: "pu\\d.$".to_string(),
                }),
                shard: 1,
            }],
            hash_ring: Some(router::HashRing {
                shards: vec![1, 2, 3, 4],
            }),
        };

        let shard_config: ShardConfig = protobuf.try_into().unwrap();

        assert_eq!(
            shard_config,
            ShardConfig {
                specific_targets: vec![MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: Some(Regex::new("pu\\d.$").unwrap()),
                    },
                    shard: ShardId::new(1),
                }],
                hash_ring: Some(HashRing {
                    shards: ConsistentHasher::new(&[
                        ShardId::new(1),
                        ShardId::new(2),
                        ShardId::new(3),
                        ShardId::new(4)
                    ])
                }),
            }
        );
    }

    #[test]
    fn test_router() {
        let protobuf = router::Router {
            name: String::from("my_router"),
            write_sharder: None,
            write_sinks: HashMap::from([(
                13,
                router::WriteSinkSet {
                    sinks: vec![router::WriteSink {
                        sink: Some(router::write_sink::Sink::GrpcRemote(1)),
                        ignore_errors: false,
                    }],
                },
            )]),
            query_sinks: Some(router::QuerySinks {
                grpc_remotes: vec![1, 3],
            }),
        };

        let router: Router = protobuf.try_into().unwrap();

        assert_eq!(
            router,
            Router {
                name: String::from("my_router"),
                write_sharder: ShardConfig::default(),
                write_sinks: BTreeMap::from([(
                    ShardId::new(13),
                    WriteSinkSet {
                        sinks: vec![WriteSink {
                            sink: WriteSinkVariant::GrpcRemote(ServerId::try_from(1).unwrap()),
                            ignore_errors: false,
                        },],
                    },
                ),]),
                query_sinks: QuerySinks {
                    grpc_remotes: vec![
                        ServerId::try_from(1).unwrap(),
                        ServerId::try_from(3).unwrap()
                    ]
                }
            },
        );
    }
}
