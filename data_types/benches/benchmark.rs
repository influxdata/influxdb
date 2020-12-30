use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, Bencher, BenchmarkId, Criterion, Throughput};
use data_types::data::{lines_to_replicated_write as lines_to_rw, ReplicatedWrite};
use data_types::database_rules::{DatabaseRules, PartitionTemplate, TemplatePart};
use generated_types::wal as wb;
use influxdb_line_protocol::{parse_lines, ParsedLine};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::Duration;

const NEXT_ENTRY_NS: i64 = 1_000_000_000;
const STARTING_TIMESTAMP_NS: i64 = 0;

#[derive(Debug)]
struct Config {
    // total number of rows written in, regardless of the number of partitions
    line_count: usize,
    // this will be the number of write buffer entries per replicated write
    partition_count: usize,
    // the number of tables (measurements) in each replicated write
    table_count: usize,
    // the number of unique tag values for the tag in each replicated write
    tag_cardinality: usize,
}

impl Config {
    fn lines_per_partition(&self) -> usize {
        self.line_count / self.partition_count
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "lines: {} ({} per partition) | partitions: {} | tables per: {} | unique tag values per: {}",
            self.line_count,
            self.lines_per_partition(),
            self.partition_count,
            self.table_count,
            self.tag_cardinality
        )
    }
}

fn lines_to_replicated_write(c: &mut Criterion) {
    run_group("lines_to_replicated_write", c, |lines, rules, config, b| {
        b.iter(|| {
            let write = lines_to_rw(0, 0, &lines, &rules);
            assert_eq!(write.entry_count(), config.partition_count);
        });
    });
}

fn replicated_write_into_bytes(c: &mut Criterion) {
    run_group(
        "replicated_write_into_bytes",
        c,
        |lines, rules, config, b| {
            let write = lines_to_rw(0, 0, &lines, &rules);
            assert_eq!(write.entry_count(), config.partition_count);

            b.iter(|| {
                let _ = write.bytes().len();
            });
        },
    );
}

// simulates the speed of marshalling the bytes into something like the mutable
// buffer or read buffer, which won't use the replicated write structure anyway
fn bytes_into_struct(c: &mut Criterion) {
    run_group("bytes_into_struct", c, |lines, rules, config, b| {
        let write = lines_to_rw(0, 0, &lines, &rules);
        assert_eq!(write.entry_count(), config.partition_count);
        let data = write.bytes();

        b.iter(|| {
            let mut db = Db::default();
            db.deserialize_write(data);
            assert_eq!(db.partition_count(), config.partition_count);
            assert_eq!(db.row_count(), config.line_count);
            assert_eq!(db.measurement_count(), config.table_count);
            assert_eq!(db.tag_cardinality(), config.tag_cardinality);
        });
    });
}

fn run_group(
    group_name: &str,
    c: &mut Criterion,
    bench: impl Fn(&[ParsedLine], &DatabaseRules, &Config, &mut Bencher<WallTime>),
) {
    let mut group = c.benchmark_group(group_name);
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    let rules = rules_with_time_partition();

    for partition_count in [1, 100].iter() {
        let config = Config {
            line_count: 1_000,
            partition_count: *partition_count,
            table_count: 1,
            tag_cardinality: 1,
        };
        let id = BenchmarkId::new("partition count", config.partition_count);
        group.throughput(Throughput::Elements(config.line_count as u64));

        let lp = create_lp(&config);
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        group.bench_with_input(id, &config, |b, config| {
            bench(&lines, &rules, &config, b);
        });
    }

    for table_count in [1, 100].iter() {
        let config = Config {
            line_count: 1_000,
            partition_count: 1,
            table_count: *table_count,
            tag_cardinality: 1,
        };
        let id = BenchmarkId::new("table count", config.table_count);
        group.throughput(Throughput::Elements(config.line_count as u64));

        let lp = create_lp(&config);
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        group.bench_with_input(id, &config, |b, config| {
            bench(&lines, &rules, &config, b);
        });
    }

    for tag_cardinality in [1, 100].iter() {
        let config = Config {
            line_count: 1_000,
            partition_count: 1,
            table_count: 1,
            tag_cardinality: *tag_cardinality,
        };
        let id = BenchmarkId::new("tag cardinality", config.tag_cardinality);
        group.throughput(Throughput::Elements(config.line_count as u64));

        let lp = create_lp(&config);
        let lines: Vec<_> = parse_lines(&lp).map(|l| l.unwrap()).collect();

        group.bench_with_input(id, &config, |b, config| {
            bench(&lines, &rules, &config, b);
        });
    }

    group.finish();
}

#[derive(Default)]
struct Db {
    partitions: BTreeMap<String, Partition>,
}

impl Db {
    fn deserialize_write(&mut self, data: &[u8]) {
        let write = ReplicatedWrite::from(data);

        if let Some(batch) = write.write_buffer_batch() {
            if let Some(entries) = batch.entries() {
                for entry in entries {
                    let key = entry.partition_key().unwrap();

                    if self.partitions.get(key).is_none() {
                        self.partitions
                            .insert(key.to_string(), Partition::default());
                    }
                    let partition = self.partitions.get_mut(key).unwrap();

                    if let Some(tables) = entry.table_batches() {
                        for table_fb in tables {
                            let table_name = table_fb.name().unwrap();

                            if partition.tables.get(table_name).is_none() {
                                let table = Table {
                                    name: table_name.to_string(),
                                    ..Default::default()
                                };
                                partition.tables.insert(table_name.to_string(), table);
                            }
                            let table = partition.tables.get_mut(table_name).unwrap();

                            if let Some(rows) = table_fb.rows() {
                                for row_fb in rows {
                                    if let Some(values) = row_fb.values() {
                                        let mut row = Row {
                                            values: Vec::with_capacity(values.len()),
                                        };

                                        for value in values {
                                            let column_name = value.column().unwrap();

                                            if partition.dict.get(column_name).is_none() {
                                                partition.dict.insert(
                                                    column_name.to_string(),
                                                    partition.dict.len(),
                                                );
                                            }
                                            let column_index =
                                                *partition.dict.get(column_name).unwrap();

                                            let val = match value.value_type() {
                                                wb::ColumnValue::TagValue => {
                                                    let tag = value
                                                        .value_as_tag_value()
                                                        .unwrap()
                                                        .value()
                                                        .unwrap();

                                                    if partition.dict.get(tag).is_none() {
                                                        partition.dict.insert(
                                                            tag.to_string(),
                                                            partition.dict.len(),
                                                        );
                                                    }
                                                    let tag_index =
                                                        *partition.dict.get(tag).unwrap();

                                                    Value::Tag(tag_index)
                                                }
                                                wb::ColumnValue::F64Value => {
                                                    let val =
                                                        value.value_as_f64value().unwrap().value();

                                                    Value::F64(val)
                                                }
                                                wb::ColumnValue::I64Value => {
                                                    let val =
                                                        value.value_as_i64value().unwrap().value();

                                                    Value::I64(val)
                                                }
                                                _ => panic!("not supported!"),
                                            };

                                            let column_value = ColumnValue {
                                                column_name_index: column_index,
                                                value: val,
                                            };

                                            row.values.push(column_value);
                                        }

                                        table.rows.push(row);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    fn row_count(&self) -> usize {
        let mut count = 0;

        for p in self.partitions.values() {
            for t in p.tables.values() {
                count += t.rows.len();
            }
        }

        count
    }

    fn tag_cardinality(&self) -> usize {
        let mut tags = BTreeMap::new();

        for p in self.partitions.values() {
            for t in p.tables.values() {
                for r in &t.rows {
                    for v in &r.values {
                        if let Value::Tag(idx) = &v.value {
                            tags.insert(*idx, ());
                        }
                    }
                }
            }
        }

        tags.len()
    }

    fn measurement_count(&self) -> usize {
        let mut measurements = BTreeSet::new();

        for p in self.partitions.values() {
            for t in p.tables.values() {
                measurements.insert(t.name.to_string());
            }
        }

        measurements.len()
    }
}

#[derive(Default)]
struct Partition {
    dict: BTreeMap<String, usize>,
    tables: BTreeMap<String, Table>,
}

#[derive(Default)]
struct Table {
    name: String,
    rows: Vec<Row>,
}

struct Row {
    values: Vec<ColumnValue>,
}

#[derive(Debug)]
struct ColumnValue {
    column_name_index: usize,
    value: Value,
}

#[derive(Debug)]
enum Value {
    I64(i64),
    F64(f64),
    Tag(usize),
}

fn create_lp(config: &Config) -> String {
    use std::fmt::Write;

    let mut s = String::new();

    let lines_per_partition = config.lines_per_partition();
    assert!(
        lines_per_partition >= config.table_count,
        format!(
            "can't fit {} unique tables (measurements) into partition with {} rows",
            config.table_count, lines_per_partition
        )
    );
    assert!(
        lines_per_partition >= config.tag_cardinality,
        format!(
            "can't fit {} unique tags into partition with {} rows",
            config.tag_cardinality, lines_per_partition
        )
    );

    for line in 0..config.line_count {
        let partition_number = line / lines_per_partition % config.partition_count;
        let timestamp = STARTING_TIMESTAMP_NS + (partition_number as i64 * NEXT_ENTRY_NS);
        let mn = line % config.table_count;
        let tn = line % config.tag_cardinality;
        writeln!(
            s,
            "processes-{mn},host=my.awesome.computer.example.com-{tn} blocked=0i,zombies=0i,stopped=0i,running=42i,sleeping=999i,total=1041i,unknown=0i,idle=0i {timestamp}",
            mn = mn,
            tn = tn,
            timestamp = timestamp,
        ).unwrap();
    }

    s
}

fn rules_with_time_partition() -> DatabaseRules {
    let partition_template = PartitionTemplate {
        parts: vec![TemplatePart::TimeFormat("%Y-%m-%d %H:%M:%S".to_string())],
    };

    DatabaseRules {
        partition_template,
        ..Default::default()
    }
}

criterion_group!(
    benches,
    lines_to_replicated_write,
    replicated_write_into_bytes,
    bytes_into_struct,
);

criterion_main!(benches);
