//! Benchmark of the encoder used by the catalog cache's LIST operation.
#![expect(unused_crate_dependencies)]

use std::str::FromStr;

use bytes::Bytes;
use catalog_cache::{
    CacheKey, CacheValue,
    api::list::{ListEntry, v2},
};
use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
    measurement::WallTime,
};
use data_types::{
    ColumnSet, CompactionLevel, NamespaceId, ObjectStoreId, ParquetFile, ParquetFileId, Partition,
    PartitionHashId, PartitionId, PartitionKey, TableId, Timestamp,
    snapshot::partition::PartitionSnapshot,
};
use generated_types::{influxdata::iox::catalog_cache::v1 as proto, prost::Message};

fn bench_encode_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_encode");
    for size in [100, 1000, 10_000, 100_000, 1_000_000] {
        run(&mut group, size);
    }

    group.finish();
}

fn run(group: &mut BenchmarkGroup<'_, WallTime>, size: usize) {
    group.throughput(Throughput::Elements(size as u64));

    group.bench_with_input(BenchmarkId::new("v2/real", size), &size, |b, i| {
        b.iter_batched(
            || get_list_entries_real(*i),
            |entries: Vec<ListEntry>| {
                let _encoded: Vec<_> = v2::ListEncoder::new(entries).collect();
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_with_input(BenchmarkId::new("v2/short", size), &size, |b, i| {
        b.iter_batched(
            || get_list_entries_short(*i),
            |entries: Vec<ListEntry>| {
                let _encoded: Vec<_> = v2::ListEncoder::new(entries).collect();
            },
            BatchSize::SmallInput,
        );
    });
}

fn get_list_entries_real(size: usize) -> Vec<ListEntry> {
    let mut entries = Vec::with_capacity(size);

    for i in 0..size {
        entries.push(ListEntry::new(
            CacheKey::Partition(i as i64),
            CacheValue::new(encode_partition_snapshot(i), 0),
        ));
    }
    entries
}

fn get_list_entries_short(size: usize) -> Vec<ListEntry> {
    let mut entries = Vec::with_capacity(size);

    for i in 0..size {
        entries.push(ListEntry::new(
            CacheKey::Partition(i as i64),
            CacheValue::new(Bytes::from(format!("data_{i}")), 0),
        ));
    }
    entries
}

fn encode_partition_snapshot(i: usize) -> Bytes {
    let namespace_id = NamespaceId::new(3);
    let table_id = TableId::new(4);
    let partition_id = PartitionId::new(i as i64);
    let partition_key = PartitionKey::from(format!("arbitrary_{i}"));
    let expected_partition_hash_id = PartitionHashId::new(table_id, &partition_key);
    let generation = 6;

    let partition = Partition::new_catalog_only(
        partition_id,
        table_id,
        partition_key.clone(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        None, // max_time
        Default::default(),
    );
    // Create associated Parquet file
    let parquet_files = vec![ParquetFile {
        id: ParquetFileId::new(7 + i as i64),
        namespace_id,
        table_id,
        partition_id,
        partition_hash_id: expected_partition_hash_id.clone(),
        object_store_id: ObjectStoreId::from_str("00000000-0000-0001-0000-000000000000").unwrap(),
        min_time: Timestamp::new(2),
        max_time: Timestamp::new(3),
        to_delete: None,
        file_size_bytes: 4,
        row_count: 5,
        compaction_level: CompactionLevel::Initial,
        created_at: Timestamp::new(6),
        column_set: ColumnSet::empty(),
        max_l0_created_at: Timestamp::new(6),
        source: None,
    }];

    // Encode the partition and its Parquet file
    let snapshot = PartitionSnapshot::encode(
        namespace_id,
        partition,
        parquet_files.clone(),
        None,
        generation,
    )
    .unwrap();
    let proto: proto::Partition = snapshot.into();
    Message::encode_to_vec(&proto).into()
}

criterion_group!(benches, bench_encode_list);
criterion_main!(benches);
