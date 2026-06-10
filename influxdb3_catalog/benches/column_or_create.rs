use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use influxdb3_catalog::catalog::versions::v2::{
    ColumnDefinition, ColumnSet, FieldFamilyMode, TableDefinition,
};
use influxdb3_id::{ColumnId, FieldFamilyId, FieldId, FieldIdentifier, TableId, TagId};
use rand::prelude::*;
use schema::{InfluxColumnType, InfluxFieldType};
use std::sync::Arc;

/// Generate realistic column names similar to what would be seen in InfluxDB
fn generate_column_names(count: usize, seed: u64) -> Vec<(String, InfluxColumnType)> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut names = Vec::with_capacity(count);

    // Common field name patterns in InfluxDB
    let field_prefixes = [
        "cpu_",
        "memory_",
        "disk_",
        "network_",
        "temperature_",
        "pressure_",
        "humidity_",
        "voltage_",
        "current_",
        "power_",
        "energy_",
        "speed_",
        "rate_",
        "count_",
        "total_",
        "avg_",
        "min_",
        "max_",
        "value_",
    ];

    let field_suffixes = [
        "_percent", "_bytes", "_ms", "_ns", "_total", "_free", "_used", "_in", "_out", "_rx",
        "_tx", "_read", "_write", "_errors",
    ];

    // Common tag patterns
    let tag_names = [
        "host",
        "region",
        "zone",
        "cluster",
        "service",
        "instance",
        "device",
        "sensor_id",
        "location",
        "environment",
        "datacenter",
        "rack",
    ];

    // Generate ~20% tags, 80% fields (typical ratio)
    let num_tags = count / 5;

    // Add tags
    for tag_name in tag_names.iter().take(num_tags) {
        names.push((tag_name.to_string(), InfluxColumnType::Tag));
    }

    // Add numbered tags if we need more
    for i in tag_names.len()..num_tags {
        names.push((format!("tag_{}", i), InfluxColumnType::Tag));
    }

    // Add fields
    let remaining = count - names.len();
    for i in 0..remaining {
        let field_type = match rng.gen_range(0..4) {
            0 => InfluxFieldType::Integer,
            1 => InfluxFieldType::UInteger,
            2 => InfluxFieldType::Float,
            _ => InfluxFieldType::String,
        };

        let name = match rng.gen_range(0..3) {
            0 => {
                // prefix + suffix pattern
                let prefix = field_prefixes.choose(&mut rng).unwrap();
                let suffix = field_suffixes.choose(&mut rng).unwrap();
                format!("{}{}{}", prefix, i % 100, suffix)
            }
            1 => {
                // prefix + number pattern
                let prefix = field_prefixes.choose(&mut rng).unwrap();
                format!("{}{}", prefix, i)
            }
            _ => {
                // custom metric pattern
                format!("metric_{}_{:04}", i % 1000, rng.gen_range(0..9999))
            }
        };

        names.push((name, InfluxColumnType::Field(field_type)));
    }

    names
}

/// Create a table definition with pre-populated columns
fn create_table_with_columns(columns: &[(String, InfluxColumnType)]) -> TableDefinition {
    let table_id = TableId::from(1u32);
    let table_name = Arc::from("measurements");
    let mut table = TableDefinition::new_empty(table_id, table_name, FieldFamilyMode::Auto);

    let mut column_id_counter = 0u16;
    let mut tag_id_counter = 0u16;
    let field_family_id = FieldFamilyId::from(0u16);
    let mut field_id_counter = 0u16;

    // Add time column first
    let _ = table
        .columns
        .insert(ColumnDefinition::timestamp(ColumnId::from(
            column_id_counter,
        )));
    column_id_counter += 1;

    // Add columns to the table
    for (name, col_type) in columns.iter() {
        let column_name = Arc::from(name.as_str());

        let col_def = match col_type {
            InfluxColumnType::Tag => {
                let tag_id = TagId::from(tag_id_counter);
                tag_id_counter += 1;
                let col_id = ColumnId::from(column_id_counter);
                column_id_counter += 1;
                ColumnDefinition::tag(tag_id, col_id, column_name)
            }
            InfluxColumnType::Field(field_type) => {
                let field_id = FieldId::from(field_id_counter);
                field_id_counter += 1;
                let field_identifier = FieldIdentifier(field_family_id, field_id);
                let col_id = ColumnId::from(column_id_counter);
                column_id_counter += 1;
                ColumnDefinition::field(field_identifier, col_id, column_name, *field_type)
            }
            InfluxColumnType::Timestamp => {
                continue; // Already added
            }
        };

        // Add to table's column set
        let _ = table.columns.insert(col_def);
    }

    table
}

/// Benchmark column lookups that already exist (hits)
fn bench_column_lookup_hits(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_definition_hits");

    for size in [100, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(100)); // We'll do 100 lookups per iteration

        let column_names = generate_column_names(*size, 42);

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let table = create_table_with_columns(&column_names);
            let lookup_names: Vec<&str> = column_names
                .iter()
                .take(100.min(column_names.len()))
                .map(|(name, _)| name.as_str())
                .collect();

            b.iter(|| {
                let mut sum = 0;
                for name in &lookup_names {
                    if let Some(def) = table.column_definition(*name) {
                        // Use the result to prevent optimization
                        sum += def.name().len();
                    }
                }
                black_box(sum);
            });
        });
    }

    group.finish();
}

/// Benchmark column lookups that miss (not found)
fn bench_column_lookup_misses(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_definition_misses");

    for size in [100, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(100));

        let column_names = generate_column_names(*size, 42);
        let miss_names: Vec<String> = (0..100)
            .map(|i| format!("nonexistent_column_{}", i))
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let table = create_table_with_columns(&column_names);

            b.iter(|| {
                let mut count = 0;
                for name in &miss_names {
                    if table.column_definition(name).is_none() {
                        count += 1;
                    }
                }
                black_box(count);
            });
        });
    }

    group.finish();
}

/// Benchmark mixed workload (90% hits, 10% misses)
fn bench_column_lookup_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_definition_mixed");

    for size in [100, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(1000));

        let column_names = generate_column_names(*size, 42);

        // Generate mixed lookup pattern
        let mut rng = StdRng::seed_from_u64(123);
        let lookups: Vec<String> = (0..1000)
            .map(|i| {
                if rng.gen_bool(0.9) && !column_names.is_empty() {
                    // Hit - existing column
                    column_names[rng.gen_range(0..column_names.len())].0.clone()
                } else {
                    // Miss - non-existing column
                    format!("miss_column_{}", i)
                }
            })
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            let table = create_table_with_columns(&column_names);

            b.iter(|| {
                let mut hit_count = 0;
                for name in &lookups {
                    if table.column_definition(name).is_some() {
                        hit_count += 1;
                    }
                }
                black_box(hit_count);
            });
        });
    }

    group.finish();
}

/// Benchmark ColumnSet insertion performance
fn bench_column_set_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_set_insert");

    for size in [100, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        let column_names = generate_column_names(*size, 42);

        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter(|| {
                let mut column_set = ColumnSet::new();
                let mut column_id_counter = 0u16;
                let mut tag_id_counter = 0u16;
                let field_family_id = FieldFamilyId::from(0u16);
                let mut field_id_counter = 0u16;

                // Add time column
                let _ = column_set.insert(ColumnDefinition::timestamp(ColumnId::from(
                    column_id_counter,
                )));
                column_id_counter += 1;

                for (name, col_type) in column_names.iter() {
                    let column_name = Arc::from(name.as_str());

                    let col_def = match col_type {
                        InfluxColumnType::Tag => {
                            let tag_id = TagId::from(tag_id_counter);
                            tag_id_counter += 1;
                            let col_id = ColumnId::from(column_id_counter);
                            column_id_counter += 1;
                            ColumnDefinition::tag(tag_id, col_id, column_name)
                        }
                        InfluxColumnType::Field(field_type) => {
                            let field_id = FieldId::from(field_id_counter);
                            field_id_counter += 1;
                            let field_identifier = FieldIdentifier(field_family_id, field_id);
                            let col_id = ColumnId::from(column_id_counter);
                            column_id_counter += 1;
                            ColumnDefinition::field(
                                field_identifier,
                                col_id,
                                column_name,
                                *field_type,
                            )
                        }
                        InfluxColumnType::Timestamp => {
                            continue; // Already added
                        }
                    };

                    let _ = column_set.insert(col_def);
                }

                black_box(column_set);
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_column_lookup_hits,
    bench_column_lookup_misses,
    bench_column_lookup_mixed,
    bench_column_set_insert
);
criterion_main!(benches);
