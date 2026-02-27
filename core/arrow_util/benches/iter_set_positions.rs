#![expect(unused_crate_dependencies)]

use core::hint::black_box;

use arrow_util::bitset::iter_set_positions_with_offset;
use criterion::{Criterion, criterion_group, criterion_main};

use workspace_hack as _;

fn run_tests(bytes: &[u8], offset: usize, name: &'static str, c: &mut Criterion) {
    c.bench_function(name, |b| {
        b.iter(|| {
            for i in iter_set_positions_with_offset(bytes, offset) {
                black_box(i);
            }
        })
    });
}

fn short(c: &mut Criterion) {
    let bytes = &[9];
    run_tests(bytes, 0, "short", c);
}

fn short_medium(c: &mut Criterion) {
    let bytes = &[28, 187, 254, 19];
    run_tests(bytes, 3, "short_medium", c);
}

fn medium(c: &mut Criterion) {
    let bytes = &[8, 118, 1, 0, 29, 89, 43, 143];
    run_tests(bytes, 30, "medium", c);
}

fn medium_long(c: &mut Criterion) {
    let bytes = &[28, 0, 197, 36, 62, 191, 84, 71, 76, 34, 117, 29, 66, 8, 80];
    run_tests(bytes, 41, "medium_long", c);
}

fn long(c: &mut Criterion) {
    let bytes = &[
        54, 1, 0, 73, 5, 218, 14, 97, 82, 8, 99, 25, 8, 1, 30, 0, 74, 96, 85, 3, 86, 8, 1, 1, 86,
        25, 78, 32, 11,
    ];
    run_tests(bytes, 120, "long", c);
}

fn all_full(c: &mut Criterion) {
    let bytes = &[0xffu8; 16];
    run_tests(bytes, 13, "all_full", c);
}

fn all_empty(c: &mut Criterion) {
    let bytes = &[0; 16];
    run_tests(bytes, 1, "all_empty", c);
}

criterion_group!(
    benches,
    short,
    short_medium,
    medium,
    medium_long,
    long,
    all_full,
    all_empty
);
criterion_main!(benches);
