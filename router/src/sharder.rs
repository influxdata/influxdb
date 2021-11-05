use std::collections::BTreeMap;

use data_types::router::{ShardConfig, ShardId};
use hashbrown::HashMap;
use mutable_batch::{DbWrite, MutableBatch};

/// Shard given write according to config.
pub fn shard_write(write: &DbWrite, config: &ShardConfig) -> BTreeMap<ShardId, DbWrite> {
    let mut batches: HashMap<ShardId, HashMap<String, MutableBatch>> = HashMap::new();

    for (table, batch) in write.tables() {
        let mut shard_id = None;

        'match_loop: for matcher2shard in &config.specific_targets {
            if let Some(regex) = &matcher2shard.matcher.table_name_regex {
                if regex.is_match(table) {
                    shard_id = Some(matcher2shard.shard);
                    break 'match_loop;
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

        if let Some(shard_id) = shard_id {
            assert!(batches
                .entry(shard_id)
                .or_default()
                .insert(table.to_string(), batch.clone())
                .is_none());
        }
    }

    batches
        .into_iter()
        .map(|(shard_id, tables)| (shard_id, DbWrite::new(tables, write.meta().clone())))
        .collect()
}

#[cfg(test)]
mod tests {
    use data_types::{
        consistent_hasher::ConsistentHasher,
        router::{HashRing, Matcher, MatcherToShard},
    };
    use mutable_batch::{test_util::assert_writes_eq, WriteMeta};
    use mutable_batch_lp::lines_to_batches;
    use regex::Regex;
    use time::Time;

    use super::*;

    #[test]
    fn test_sharding() {
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

        let meta = WriteMeta::new(None, Some(Time::from_timestamp_millis(1337)), None, None);
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
    fn test_no_mach() {
        let config = ShardConfig::default();

        let meta = WriteMeta::default();
        let write = db_write(&["foo x=1 10"], &meta);

        let actual = shard_write(&write, &config);
        assert!(actual.is_empty());
    }

    fn db_write(lines: &[&str], meta: &WriteMeta) -> DbWrite {
        DbWrite::new(
            lines_to_batches(&lines.join("\n"), 0).unwrap(),
            meta.clone(),
        )
    }
}
