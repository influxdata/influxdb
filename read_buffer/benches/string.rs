use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use rand::Rng;

use read_buffer::benchmarks::{string, Operator, RowIDs};

const ROWS: [usize; 3] = [100_000, 1_000_000, 10_000_000];
const LOCATIONS: [Location; 3] = [Location::Start, Location::Middle, Location::End];
const ROWS_MATCHING_VALUE: [usize; 3] = [10, 100, 1000];

#[derive(Debug)]
enum Location {
    Start,
    Middle,
    End,
}

enum EncType {
    Rle,
    Dictionary,
}

fn select(c: &mut Criterion) {
    let mut rng = rand::thread_rng();
    benchmark_select(
        c,
        "select",
        EncType::Rle,
        &ROWS,
        &LOCATIONS,
        &ROWS_MATCHING_VALUE,
        &mut rng,
    );

    benchmark_select(
        c,
        "_select",
        EncType::Dictionary,
        &ROWS,
        &LOCATIONS,
        &ROWS_MATCHING_VALUE,
        &mut rng,
    );
}

fn benchmark_select(
    c: &mut Criterion,
    benchmark_group_name: &str,
    enc_type: EncType,
    row_size: &[usize],
    locations: &[Location],
    rows_selected: &[usize],
    rng: &mut ThreadRng,
) {
    let mut group = c.benchmark_group(benchmark_group_name);
    for &num_rows in row_size {
        for location in locations {
            for &rows_select in rows_selected.iter().rev() {
                let col_data = generate_column(num_rows, rows_select, rng);
                let cardinality = num_rows / rows_select;

                let mut col_dict = std::collections::BTreeSet::new();
                for v in &col_data {
                    col_dict.insert(v.clone());
                }

                let value = match location {
                    Location::Start => {
                        // find a value in the column close to the beginning.
                        &col_data[rng.gen_range(0..col_data.len() / 20)] // something in first 5%
                    }
                    Location::Middle => {
                        // find a value in the column somewhere in the middle
                        let fifth = col_data.len() / 5;
                        &col_data[rng.gen_range(2 * fifth..3 * fifth)] // something in middle fifth
                    }
                    Location::End => {
                        &col_data
                            [rng.gen_range(col_data.len() - (col_data.len() / 9)..col_data.len())]
                    } // something in the last ~10%
                };

                group.throughput(Throughput::Elements(num_rows as u64));
                let encoding: string::Encoding = match enc_type {
                    EncType::Rle => {
                        let mut encoding = string::RLE::with_dictionary(col_dict);
                        // Could be faster but it's just the bench setup...
                        for v in &col_data {
                            encoding.push(v.to_owned());
                        }
                        string::Encoding::RLE(encoding)
                    }
                    EncType::Dictionary => {
                        let mut encoding = string::Dictionary::with_dictionary(col_dict);
                        // Could be faster but it's just the bench setup...
                        for v in &col_data {
                            encoding.push(v.to_owned());
                        }
                        string::Encoding::Plain(encoding)
                    }
                };

                let input = (RowIDs::new_bitmap(), value);

                group.bench_with_input(
                    BenchmarkId::from_parameter(format!(
                        "enc_{:?}/rows_{:?}/loc_{:?}/card_{:?}",
                        encoding.debug_name(),
                        num_rows,
                        location,
                        cardinality
                    )),
                    &input,
                    |b, _| {
                        b.iter(|| {
                            // do work
                            let row_ids = encoding.row_ids_filter(
                                value.as_str(),
                                &Operator::Equal,
                                RowIDs::new_bitmap(),
                            );

                            let as_vec = row_ids.to_vec();
                            let values = encoding.values(as_vec.as_slice(), vec![]);
                            assert_eq!(values.len(), rows_select);
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

fn generate_column(rows: usize, rows_per_value: usize, rng: &mut ThreadRng) -> Vec<String> {
    let mut col = Vec::with_capacity(rows);
    let distinct_values = rows / rows_per_value;
    for _ in 0..distinct_values {
        let value = format!(
            "value-{}",
            rng.sample_iter(&Alphanumeric)
                .map(char::from)
                .take(8)
                .collect::<String>()
        );
        col.extend(std::iter::repeat(value).take(rows_per_value));
    }

    assert_eq!(col.len(), rows);
    col
}

criterion_group!(benches, select,);
criterion_main!(benches);
