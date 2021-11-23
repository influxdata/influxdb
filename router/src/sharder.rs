use std::collections::{BTreeMap, HashSet};

use data_types::router::{ShardConfig, ShardId};
use dml::{DmlDelete, DmlOperation, DmlWrite};
use hashbrown::HashMap;
use mutable_batch::MutableBatch;

/// Shard a given operation, returns an iterator sorted by ShardId
pub fn shard_operation(
    operation: DmlOperation,
    config: &ShardConfig,
) -> BTreeMap<ShardId, DmlOperation> {
    match operation {
        DmlOperation::Write(write) => shard_write(&write, config)
            .into_iter()
            .map(|(shard, write)| (shard, DmlOperation::Write(write)))
            .collect(),
        DmlOperation::Delete(delete) => shard_delete(&delete, config)
            .into_iter()
            .map(|(shard, delete)| (shard, DmlOperation::Delete(delete)))
            .collect(),
    }
}

/// Shard given write according to config.
fn shard_write(write: &DmlWrite, config: &ShardConfig) -> BTreeMap<ShardId, DmlWrite> {
    let mut batches: HashMap<ShardId, HashMap<String, MutableBatch>> = HashMap::new();

    for (table, batch) in write.tables() {
        if let Some(shard_id) = shard_table(table, config) {
            assert!(batches
                .entry(shard_id)
                .or_default()
                .insert(table.to_string(), batch.clone())
                .is_none());
        }
    }

    batches
        .into_iter()
        .map(|(shard_id, tables)| (shard_id, DmlWrite::new(tables, write.meta().clone())))
        .collect()
}

/// Shard given delete according to config.
fn shard_delete(delete: &DmlDelete, config: &ShardConfig) -> BTreeMap<ShardId, DmlDelete> {
    if let Some(table) = delete.table_name() {
        if let Some(shard_id) = shard_table(table, config) {
            BTreeMap::from([(shard_id, delete.clone())])
        } else {
            BTreeMap::default()
        }
    } else {
        let shards: HashSet<ShardId> = config
            .specific_targets
            .iter()
            .map(|matcher2shard| matcher2shard.shard)
            .chain(
                config
                    .hash_ring
                    .iter()
                    .map(|hashring| Vec::<ShardId>::from(hashring.shards.clone()).into_iter())
                    .flatten(),
            )
            .collect();

        shards
            .into_iter()
            .map(|shard| (shard, delete.clone()))
            .collect()
    }
}

/// Shard only based on table name
fn shard_table(table: &str, config: &ShardConfig) -> Option<ShardId> {
    let mut shard_id = None;

    for matcher2shard in &config.specific_targets {
        if let Some(regex) = &matcher2shard.matcher.table_name_regex {
            if regex.is_match(table) {
                shard_id = Some(matcher2shard.shard);
                break;
            }
        }
    }

    if shard_id.is_none() {
        if let Some(hash_ring) = &config.hash_ring {
            if let Some(id) = hash_ring.shards.find(table) {
                shard_id = Some(id);
            }
        }
    }

    shard_id
}

#[cfg(test)]
mod tests {
    use data_types::{
        consistent_hasher::ConsistentHasher,
        delete_predicate::DeletePredicate,
        non_empty::NonEmptyString,
        router::{HashRing, Matcher, MatcherToShard},
        timestamp::TimestampRange,
    };
    use dml::{
        test_util::{assert_deletes_eq, assert_writes_eq},
        DmlMeta,
    };
    use mutable_batch_lp::lines_to_batches;
    use regex::Regex;

    use super::*;

    #[test]
    fn test_write_sharding() {
        let config = ShardConfig {
            specific_targets: vec![
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: None,
                    },
                    shard: ShardId::new(1),
                },
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: Some(Regex::new("some_foo").unwrap()),
                    },
                    shard: ShardId::new(2),
                },
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: Some(Regex::new("other").unwrap()),
                    },
                    shard: ShardId::new(3),
                },
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: Some(Regex::new("some_.*").unwrap()),
                    },
                    shard: ShardId::new(4),
                },
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: Some(Regex::new("baz").unwrap()),
                    },
                    shard: ShardId::new(2),
                },
            ],
            hash_ring: Some(HashRing {
                shards: ConsistentHasher::new(&[
                    ShardId::new(11),
                    ShardId::new(12),
                    ShardId::new(13),
                ]),
            }),
        };

        let meta = DmlMeta::unsequenced(None);
        let write = db_write(
            &[
                "some_foo x=1 10",
                "some_foo x=2 20",
                "some_bar y=3 30",
                "other z=4 40",
                "rnd1 r=5 50",
                "rnd2 r=6 60",
                "rnd3 r=7 70",
                "baz b=8 80",
            ],
            &meta,
        );

        let actual = shard_write(&write, &config);
        let expected = BTreeMap::from([
            (
                ShardId::new(2),
                db_write(&["some_foo x=1 10", "some_foo x=2 20", "baz b=8 80"], &meta),
            ),
            (ShardId::new(3), db_write(&["other z=4 40"], &meta)),
            (ShardId::new(4), db_write(&["some_bar y=3 30"], &meta)),
            (ShardId::new(11), db_write(&["rnd1 r=5 50"], &meta)),
            (ShardId::new(12), db_write(&["rnd3 r=7 70"], &meta)),
            (ShardId::new(13), db_write(&["rnd2 r=6 60"], &meta)),
        ]);

        let actual_shard_ids: Vec<_> = actual.keys().cloned().collect();
        let expected_shard_ids: Vec<_> = expected.keys().cloned().collect();
        assert_eq!(actual_shard_ids, expected_shard_ids);

        for (actual_write, expected_write) in actual.values().zip(expected.values()) {
            assert_writes_eq(actual_write, expected_write);
        }
    }

    #[test]
    fn test_write_no_mach() {
        let config = ShardConfig::default();

        let meta = DmlMeta::default();
        let write = db_write(&["foo x=1 10"], &meta);

        let actual = shard_write(&write, &config);
        assert!(actual.is_empty());
    }

    #[test]
    fn test_delete_sharding() {
        let config = ShardConfig {
            specific_targets: vec![
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: None,
                    },
                    shard: ShardId::new(1),
                },
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: Some(Regex::new("some_foo").unwrap()),
                    },
                    shard: ShardId::new(2),
                },
                MatcherToShard {
                    matcher: Matcher {
                        table_name_regex: Some(Regex::new("some_.*").unwrap()),
                    },
                    shard: ShardId::new(3),
                },
            ],
            hash_ring: Some(HashRing {
                shards: ConsistentHasher::new(&[
                    ShardId::new(11),
                    ShardId::new(12),
                    ShardId::new(13),
                ]),
            }),
        };

        // Deletes w/o table name go to all shards
        let meta = DmlMeta::unsequenced(None);
        let delete = DmlDelete::new(
            DeletePredicate {
                range: TimestampRange { start: 1, end: 2 },
                exprs: vec![],
            },
            None,
            meta,
        );

        let actual = shard_delete(&delete, &config);
        let expected = BTreeMap::from([
            (ShardId::new(1), delete.clone()),
            (ShardId::new(2), delete.clone()),
            (ShardId::new(3), delete.clone()),
            (ShardId::new(11), delete.clone()),
            (ShardId::new(12), delete.clone()),
            (ShardId::new(13), delete),
        ]);
        assert_sharded_deletes_eq(&actual, &expected);

        // Deletes are matched by table name regex
        let meta = DmlMeta::unsequenced(None);
        let delete = DmlDelete::new(
            DeletePredicate {
                range: TimestampRange { start: 3, end: 4 },
                exprs: vec![],
            },
            Some(NonEmptyString::new("some_foo").unwrap()),
            meta,
        );

        let actual = shard_delete(&delete, &config);
        let expected = BTreeMap::from([(ShardId::new(2), delete)]);
        assert_sharded_deletes_eq(&actual, &expected);

        // Deletes can be matched by hash-ring
        let meta = DmlMeta::unsequenced(None);
        let delete = DmlDelete::new(
            DeletePredicate {
                range: TimestampRange { start: 5, end: 6 },
                exprs: vec![],
            },
            Some(NonEmptyString::new("bar").unwrap()),
            meta,
        );

        let actual = shard_delete(&delete, &config);
        let expected = BTreeMap::from([(ShardId::new(13), delete)]);
        assert_sharded_deletes_eq(&actual, &expected);
    }

    fn db_write(lines: &[&str], meta: &DmlMeta) -> DmlWrite {
        DmlWrite::new(
            lines_to_batches(&lines.join("\n"), 0).unwrap(),
            meta.clone(),
        )
    }

    fn assert_sharded_deletes_eq(
        actual: &BTreeMap<ShardId, DmlDelete>,
        expected: &BTreeMap<ShardId, DmlDelete>,
    ) {
        let actual_shard_ids: Vec<_> = actual.keys().cloned().collect();
        let expected_shard_ids: Vec<_> = expected.keys().cloned().collect();
        assert_eq!(actual_shard_ids, expected_shard_ids);

        for (actual_delete, expected_delete) in actual.values().zip(expected.values()) {
            assert_deletes_eq(actual_delete, expected_delete);
        }
    }
}
