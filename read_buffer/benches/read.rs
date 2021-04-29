mod read_group;

use criterion::{criterion_group, criterion_main};

use read_group::read_group;

criterion_group!(benches, read_group);
criterion_main!(benches);
