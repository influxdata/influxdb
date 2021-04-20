use criterion::{criterion_group, criterion_main, Criterion};
use influxdb_tsm::mapper::*;
use influxdb_tsm::reader::*;
use influxdb_tsm::*;
use std::collections::BTreeMap;

fn map_field_columns(c: &mut Criterion) {
    let mut group = c.benchmark_group("mapper");

    let mut measurement_table = mapper::MeasurementTable::new("cpu".to_string(), 0);

    measurement_table
        .add_series_data(
            vec![],
            "temp".to_string(),
            Block {
                min_time: 0,
                max_time: 0,
                offset: 0,
                size: 0,
                typ: influxdb_tsm::BlockType::Float,
                reader_idx: 0,
            },
        )
        .unwrap();

    measurement_table
        .add_series_data(
            vec![],
            "temp".to_string(),
            Block {
                min_time: 1,
                max_time: 0,
                offset: 0,
                size: 0,
                typ: influxdb_tsm::BlockType::Float,
                reader_idx: 0,
            },
        )
        .unwrap();

    measurement_table
        .add_series_data(
            vec![],
            "voltage".to_string(),
            Block {
                min_time: 2,
                max_time: 0,
                offset: 0,
                size: 0,
                typ: influxdb_tsm::BlockType::Integer,
                reader_idx: 0,
            },
        )
        .unwrap();

    // setup mock block decoder
    let block0 = BlockData::Float {
        i: 0,
        values: vec![100.0; 1000],
        ts: (0..1000).collect(),
    };

    let block1 = BlockData::Float {
        i: 0,
        values: vec![200.0; 500],
        ts: (1000..1500).collect(),
    };

    let block2 = BlockData::Integer {
        i: 0,
        values: vec![22; 800],
        ts: (1000..1800).collect(),
    };

    let mut block_map = BTreeMap::new();
    block_map.insert(0, block0);
    block_map.insert(1, block1);
    block_map.insert(2, block2);
    let decoder = reader::MockBlockDecoder::new(block_map);

    group.bench_function("map_field_columns", move |b| {
        b.iter_batched(
            || (decoder.clone(), measurement_table.clone()),
            |(mut data, mut measurement_table)| {
                measurement_table
                    .process(&mut data, |section: TableSection| -> Result<(), TsmError> {
                        assert_eq!(section.len(), 1800);
                        Ok(())
                    })
                    .unwrap();
            },
            criterion::BatchSize::LargeInput,
        )
    });

    group.finish();
}

criterion_group!(benches, map_field_columns);
criterion_main!(benches);
