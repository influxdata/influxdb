use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write,
    sync::Arc,
};

use data_types::router::{Router as RouterConfig, ShardId};
use dml::DmlOperation;
use snafu::{ResultExt, Snafu};

use crate::{connection_pool::ConnectionPool, resolver::Resolver, write_sink::WriteSinkSet};

#[derive(Debug, Snafu)]
pub enum WriteErrorShard {
    #[snafu(display("Did not find sink set for shard ID {}", shard_id.get()))]
    NoSinkSetFound { shard_id: ShardId },

    #[snafu(display("Write to sink set failed: {}", source))]
    SinkSetFailure { source: crate::write_sink::Error },
}

#[derive(Debug, Snafu)]
pub enum WriteError {
    #[snafu(display("One or more writes failed: {}", fmt_write_errors(errors)))]
    MultiWriteFailure {
        errors: BTreeMap<ShardId, WriteErrorShard>,
    },
}

fn fmt_write_errors(errors: &BTreeMap<ShardId, WriteErrorShard>) -> String {
    const MAX_ERRORS: usize = 2;

    let mut out = String::new();

    for (shard_id, error) in errors.iter().take(MAX_ERRORS) {
        if !out.is_empty() {
            write!(&mut out, ", ").expect("write to string failed?!");
        }
        write!(&mut out, "{} => \"{}\"", shard_id, error).expect("write to string failed?!");
    }

    if errors.len() > MAX_ERRORS {
        write!(&mut out, "...").expect("write to string failed?!");
    }

    out
}

/// Router for a single database.
#[derive(Debug)]
pub struct Router {
    /// Router config.
    config: RouterConfig,

    /// We use a [`HashMap`] here for `O(1)` lookups. Do not rely on the iteration order.
    write_sink_sets: HashMap<ShardId, WriteSinkSet>,
}

impl Router {
    /// Create new router from config.
    pub fn new(
        config: RouterConfig,
        resolver: Arc<Resolver>,
        connection_pool: Arc<ConnectionPool>,
    ) -> Self {
        let write_sink_sets = config
            .write_sinks
            .iter()
            .map(|(shard_id, set_config)| {
                (
                    *shard_id,
                    WriteSinkSet::new(
                        &config.name,
                        set_config.clone(),
                        Arc::clone(&resolver),
                        Arc::clone(&connection_pool),
                    ),
                )
            })
            .collect();

        Self {
            config,
            write_sink_sets,
        }
    }

    /// Router config.
    pub fn config(&self) -> &RouterConfig {
        &self.config
    }

    /// Router name.
    ///
    /// This is the same as the database that this router acts for.
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Shard and write data.
    pub async fn write(&self, operation: DmlOperation) -> Result<(), WriteError> {
        let mut errors: BTreeMap<ShardId, WriteErrorShard> = Default::default();

        // The iteration order is stable here, so we ensure deterministic behavior and error order.
        for (shard_id, operation) in operation.shard(&self.config.write_sharder) {
            if let Err(e) = self.write_shard(shard_id, &operation).await {
                errors.insert(shard_id, e);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(WriteError::MultiWriteFailure { errors })
        }
    }

    /// Write operation to the specified shard.
    async fn write_shard(
        &self,
        shard_id: ShardId,
        operation: &DmlOperation,
    ) -> Result<(), WriteErrorShard> {
        match self.write_sink_sets.get(&shard_id) {
            Some(sink_set) => sink_set.write(operation).await.context(SinkSetFailureSnafu),
            None => Err(WriteErrorShard::NoSinkSetFound { shard_id }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{grpc_client::MockClient, resolver::RemoteTemplate};

    use super::*;

    use data_types::{
        delete_predicate::DeletePredicate,
        non_empty::NonEmptyString,
        router::{
            Matcher, MatcherToShard, ShardConfig, WriteSink as WriteSinkConfig,
            WriteSinkSet as WriteSinkSetConfig, WriteSinkVariant as WriteSinkVariantConfig,
        },
        sequence::Sequence,
        server_id::ServerId,
        timestamp::TimestampRange,
    };
    use dml::{DmlDelete, DmlMeta, DmlWrite};
    use mutable_batch_lp::lines_to_batches;
    use regex::Regex;
    use time::Time;

    #[tokio::test]
    async fn test_getters() {
        let resolver = Arc::new(Resolver::new(None));
        let connection_pool = Arc::new(ConnectionPool::new_testing().await);

        let cfg = RouterConfig {
            name: String::from("my_router"),
            write_sharder: Default::default(),
            write_sinks: Default::default(),
            query_sinks: Default::default(),
        };
        let router = Router::new(cfg.clone(), resolver, connection_pool);

        assert_eq!(router.config(), &cfg);
        assert_eq!(router.name(), "my_router");
    }

    #[tokio::test]
    async fn test_write() {
        let server_id_1 = ServerId::try_from(1).unwrap();
        let server_id_2 = ServerId::try_from(2).unwrap();
        let server_id_3 = ServerId::try_from(3).unwrap();

        let resolver = Arc::new(Resolver::new(Some(RemoteTemplate::new("{id}"))));
        let connection_pool = Arc::new(ConnectionPool::new_testing().await);

        let client_1 = connection_pool.grpc_client("1").await.unwrap();
        let client_2 = connection_pool.grpc_client("2").await.unwrap();
        let client_3 = connection_pool.grpc_client("3").await.unwrap();
        let client_1 = client_1.as_any().downcast_ref::<MockClient>().unwrap();
        let client_2 = client_2.as_any().downcast_ref::<MockClient>().unwrap();
        let client_3 = client_3.as_any().downcast_ref::<MockClient>().unwrap();

        let cfg = RouterConfig {
            name: String::from("my_router"),
            write_sharder: ShardConfig {
                specific_targets: vec![
                    MatcherToShard {
                        matcher: Matcher {
                            table_name_regex: Some(Regex::new("foo_bar").unwrap()),
                        },
                        shard: ShardId::new(10),
                    },
                    MatcherToShard {
                        matcher: Matcher {
                            table_name_regex: Some(Regex::new("foo_three").unwrap()),
                        },
                        shard: ShardId::new(30),
                    },
                    MatcherToShard {
                        matcher: Matcher {
                            table_name_regex: Some(Regex::new("foo_.*").unwrap()),
                        },
                        shard: ShardId::new(20),
                    },
                    MatcherToShard {
                        matcher: Matcher {
                            table_name_regex: Some(Regex::new("doom").unwrap()),
                        },
                        shard: ShardId::new(40),
                    },
                    MatcherToShard {
                        matcher: Matcher {
                            table_name_regex: Some(Regex::new("nooo").unwrap()),
                        },
                        shard: ShardId::new(50),
                    },
                    MatcherToShard {
                        matcher: Matcher {
                            table_name_regex: Some(Regex::new(".*").unwrap()),
                        },
                        shard: ShardId::new(20),
                    },
                ],
                hash_ring: None,
            },
            write_sinks: BTreeMap::from([
                (
                    ShardId::new(10),
                    WriteSinkSetConfig {
                        sinks: vec![WriteSinkConfig {
                            sink: WriteSinkVariantConfig::GrpcRemote(server_id_1),
                            ignore_errors: false,
                        }],
                    },
                ),
                (
                    ShardId::new(20),
                    WriteSinkSetConfig {
                        sinks: vec![WriteSinkConfig {
                            sink: WriteSinkVariantConfig::GrpcRemote(server_id_2),
                            ignore_errors: false,
                        }],
                    },
                ),
                (
                    ShardId::new(30),
                    WriteSinkSetConfig {
                        sinks: vec![WriteSinkConfig {
                            sink: WriteSinkVariantConfig::GrpcRemote(server_id_3),
                            ignore_errors: false,
                        }],
                    },
                ),
            ]),
            query_sinks: Default::default(),
        };
        let router = Router::new(cfg.clone(), resolver, connection_pool);

        // clean write
        let meta_1 = DmlMeta::sequenced(
            Sequence::new(1, 2),
            Time::from_timestamp_nanos(1337),
            None,
            10,
        );
        let write_1 = db_write(
            &["foo_x x=2 2", "foo_bar x=1 1", "foo_y x=3 3", "www x=4 4"],
            &meta_1,
        );
        router.write(write_1).await.unwrap();
        client_1.assert_writes(&[(
            String::from("my_router"),
            db_write(&["foo_bar x=1 1"], &meta_1),
        )]);
        client_2.assert_writes(&[(
            String::from("my_router"),
            db_write(&["foo_x x=2 2", "foo_y x=3 3", "www x=4 4"], &meta_1),
        )]);

        // write w/ errors
        client_2.poison();
        let meta_2 = DmlMeta::sequenced(
            Sequence::new(3, 4),
            Time::from_timestamp_nanos(42),
            None,
            20,
        );
        let write_2 = db_write(
            &[
                "foo_bar x=5 5",
                "doom x=6 6",
                "foo_bar x=7 7",
                "www x=8 8",
                "foo_bar x=9 9",
                "nooo x=10 10",
                "foo_bar x=11 11",
                "foo_three x=12 12",
                "doom x=13 13",
                "foo_three x=14 14",
                "www x=15 15",
                "foo_three x=16 16",
                "nooo x=17 17",
                "foo_three x=18 18",
            ],
            &meta_2,
        );
        let err = router.write(write_2).await.unwrap_err();
        assert_eq!(err.to_string(), "One or more writes failed: ShardId(20) => \"Write to sink set failed: Cannot write: poisened\", ShardId(40) => \"Did not find sink set for shard ID 40\"...");
        client_1.assert_writes(&[
            (
                String::from("my_router"),
                db_write(&["foo_bar x=1 1"], &meta_1),
            ),
            (
                String::from("my_router"),
                db_write(
                    &[
                        "foo_bar x=5 5",
                        "foo_bar x=7 7",
                        "foo_bar x=9 9",
                        "foo_bar x=11 11",
                    ],
                    &meta_2,
                ),
            ),
        ]);
        client_2.assert_writes(&[(
            String::from("my_router"),
            db_write(&["foo_x x=2 2", "foo_y x=3 3", "www x=4 4"], &meta_1),
        )]);
        client_3.assert_writes(&[(
            String::from("my_router"),
            db_write(
                &[
                    "foo_three x=12 12",
                    "foo_three x=14 14",
                    "foo_three x=16 16",
                    "foo_three x=18 18",
                ],
                &meta_2,
            ),
        )]);
    }

    #[tokio::test]
    async fn test_delete() {
        let server_id_1 = ServerId::try_from(1).unwrap();
        let server_id_2 = ServerId::try_from(2).unwrap();

        let resolver = Arc::new(Resolver::new(Some(RemoteTemplate::new("{id}"))));
        let connection_pool = Arc::new(ConnectionPool::new_testing().await);

        let client_1 = connection_pool.grpc_client("1").await.unwrap();
        let client_2 = connection_pool.grpc_client("2").await.unwrap();
        let client_1 = client_1.as_any().downcast_ref::<MockClient>().unwrap();
        let client_2 = client_2.as_any().downcast_ref::<MockClient>().unwrap();

        let cfg = RouterConfig {
            name: String::from("my_router"),
            write_sharder: ShardConfig {
                specific_targets: vec![
                    MatcherToShard {
                        matcher: Matcher {
                            table_name_regex: Some(Regex::new("foo_bar").unwrap()),
                        },
                        shard: ShardId::new(10),
                    },
                    MatcherToShard {
                        matcher: Matcher {
                            table_name_regex: Some(Regex::new("foo_.*").unwrap()),
                        },
                        shard: ShardId::new(20),
                    },
                ],
                hash_ring: None,
            },
            write_sinks: BTreeMap::from([
                (
                    ShardId::new(10),
                    WriteSinkSetConfig {
                        sinks: vec![WriteSinkConfig {
                            sink: WriteSinkVariantConfig::GrpcRemote(server_id_1),
                            ignore_errors: false,
                        }],
                    },
                ),
                (
                    ShardId::new(20),
                    WriteSinkSetConfig {
                        sinks: vec![WriteSinkConfig {
                            sink: WriteSinkVariantConfig::GrpcRemote(server_id_2),
                            ignore_errors: false,
                        }],
                    },
                ),
            ]),
            query_sinks: Default::default(),
        };
        let router = Router::new(cfg.clone(), resolver, connection_pool);

        // clean write
        let meta = DmlMeta::sequenced(
            Sequence::new(1, 2),
            Time::from_timestamp_nanos(1337),
            None,
            10,
        );
        let delete = DmlOperation::Delete(DmlDelete::new(
            DeletePredicate {
                range: TimestampRange::new(1, 2),
                exprs: vec![],
            },
            Some(NonEmptyString::new("foo_foo").unwrap()),
            meta,
        ));
        router.write(delete.clone()).await.unwrap();
        client_1.assert_writes(&[]);
        client_2.assert_writes(&[(String::from("my_router"), delete)]);
    }

    fn db_write(lines: &[&str], meta: &DmlMeta) -> DmlOperation {
        DmlOperation::Write(DmlWrite::new(
            lines_to_batches(&lines.join("\n"), 0).unwrap(),
            meta.clone(),
        ))
    }
}
