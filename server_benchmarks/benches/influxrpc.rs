mod read_filter;
mod read_group;
mod tag_values;

use criterion::{criterion_group, criterion_main};

use read_filter::benchmark_read_filter;
use read_group::benchmark_read_group;
use tag_values::benchmark_tag_values;

// criterion_group!(benches, benchmark_tag_values, benchmark_read_filter);
criterion_group!(
    benches,
    benchmark_tag_values,
    benchmark_read_filter,
    benchmark_read_group,
);
criterion_main!(benches);
