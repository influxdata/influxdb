use chrono::{DateTime, Utc};
use criterion::{criterion_group, criterion_main, Criterion};
use data_types::database_rules::{Error as DataError, Partitioner, Sharder};
use influxdb_line_protocol::ParsedLine;
use internal_types::entry::{lines_to_sharded_entries, SequencedEntry};

static LINES: &str = include_str!("../../tests/fixtures/lineproto/prometheus.lp");

fn sequenced_entry(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequenced_entry_generator");

    let lines = influxdb_line_protocol::parse_lines(LINES)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let sharded_entries = lines_to_sharded_entries(&lines, &sharder(1), &partitioner(1)).unwrap();
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

    group.bench_function("new_from_entry_bytes", |b| {
        b.iter(|| {
            let sequenced_entry = SequencedEntry::new_from_entry_bytes(23, 2, data).unwrap();
            assert_eq!(sequenced_entry.clock_value(), 23);
            assert_eq!(sequenced_entry.writer_id(), 2);
        })
    });

    group.finish();
}

criterion_group!(benches, sequenced_entry);

criterion_main!(benches);

fn sharder(count: u16) -> TestSharder {
    TestSharder {
        count,
        n: std::cell::RefCell::new(0),
    }
}

// For each line passed to shard returns a shard id from [0, count) in order
struct TestSharder {
    count: u16,
    n: std::cell::RefCell<u16>,
}

impl Sharder for TestSharder {
    fn shard(&self, _line: &ParsedLine<'_>) -> Result<u16, DataError> {
        let n = *self.n.borrow();
        self.n.replace(n + 1);
        Ok(n % self.count)
    }
}

fn partitioner(count: u8) -> TestPartitioner {
    TestPartitioner {
        count,
        n: std::cell::RefCell::new(0),
    }
}

// For each line passed to partition_key returns a key with a number from [0,
// count)
struct TestPartitioner {
    count: u8,
    n: std::cell::RefCell<u8>,
}

impl Partitioner for TestPartitioner {
    fn partition_key(
        &self,
        _line: &ParsedLine<'_>,
        _default_time: &DateTime<Utc>,
    ) -> data_types::database_rules::Result<String> {
        let n = *self.n.borrow();
        self.n.replace(n + 1);
        Ok(format!("key_{}", n % self.count))
    }
}
