mod tag_values;

use criterion::{criterion_group, criterion_main};

use tag_values::benchmark_tag_values;

criterion_group!(benches, benchmark_tag_values);
criterion_main!(benches);
