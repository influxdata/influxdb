use criterion::{criterion_group, criterion_main, Criterion};
use delorean_tsm::*;
use std::collections::BTreeMap;

fn map_field_columns(c: &mut Criterion) {
    let mut group = c.benchmark_group("mapper");
    let mut field_blocks: BTreeMap<String, Vec<Block>> = BTreeMap::new();

    field_blocks.insert(
        "temp".to_string(),
        vec![
            Block {
                min_time: 0,
                max_time: 0,
                offset: 0,
                size: 0,
                typ: delorean_tsm::BlockType::Float,
            },
            Block {
                min_time: 1,
                max_time: 0,
                offset: 0,
                size: 0,
                typ: delorean_tsm::BlockType::Float,
            },
        ],
    );

    field_blocks.insert(
        "voltage".to_string(),
        vec![Block {
            min_time: 2,
            max_time: 0,
            offset: 0,
            size: 0,
            typ: delorean_tsm::BlockType::Integer,
        }],
    );

    let block0 = BlockData::Float {
        values: vec![100.0; 1000],
        ts: (0..1000).collect(),
    };

    let block1 = BlockData::Float {
        values: vec![200.0; 500],
        ts: (1000..1500).collect(),
    };

    let block2 = BlockData::Integer {
        values: vec![22; 800],
        ts: (1000..1800).collect(),
    };

    let mut block_map = BTreeMap::new();
    block_map.insert(0, block0);
    block_map.insert(1, block1);
    block_map.insert(2, block2);
    let decoder = reader::MockBlockDecoder::new(block_map);

    group.bench_function("map field columns", move |b| {
        b.iter_batched(
            || (decoder.clone(), field_blocks.clone()),
            |(mut data, mut field_blocks)| {
                let res = mapper::map_field_columns(&mut data, &mut field_blocks).unwrap();
                assert_eq!(res.0.len(), 1800);
            },
            criterion::BatchSize::LargeInput,
        )
    });

    group.finish();
}

criterion_group!(benches, map_field_columns);
criterion_main!(benches);
