use criterion::{criterion_group, criterion_main};

mod read_filter;
mod read_group;
use read_filter::read_filter;
use read_group::read_group;

criterion_group!(benches, read_filter, read_group);
criterion_main!(benches);
