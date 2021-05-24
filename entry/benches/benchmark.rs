use criterion::{criterion_group, criterion_main, Criterion};
use data_types::{database_rules::ShardConfig, server_id::ServerId};
use entry::{
    lines_to_sharded_entries, test_helpers::partitioner, ClockValue, OwnedSequencedEntry,
    SequencedEntry,
};
use std::convert::TryFrom;

static LINES: &str = include_str!("../../tests/fixtures/lineproto/prometheus.lp");

fn sequenced_entry(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequenced_entry_generator");

    let lines = influxdb_line_protocol::parse_lines(LINES)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let shard_config: Option<&ShardConfig> = None;
    let default_time = 456;
    let sharded_entries =
        lines_to_sharded_entries(&lines, default_time, shard_config, &partitioner(1)).unwrap();
    let entry = &sharded_entries.first().unwrap().entry;
    let data = entry.data();
    assert_eq!(
        entry
            .partition_writes()
            .unwrap()
            .first()
            .unwrap()
            .table_batches()
            .first()
            .unwrap()
            .row_count(),
        554
    );

    let clock_value = ClockValue::try_from(23).unwrap();
    let server_id = ServerId::try_from(2).unwrap();

    group.bench_function("new_from_entry_bytes", |b| {
        b.iter(|| {
            let sequenced_entry =
                OwnedSequencedEntry::new_from_entry_bytes(clock_value, server_id, data).unwrap();
            assert_eq!(sequenced_entry.clock_value(), clock_value);
            assert_eq!(sequenced_entry.server_id(), server_id);
        })
    });

    group.finish();
}

criterion_group!(benches, sequenced_entry);

criterion_main!(benches);
