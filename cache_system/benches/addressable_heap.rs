use std::mem::size_of;

use cache_system::addressable_heap::AddressableHeap;
use criterion::{
    criterion_group, criterion_main, measurement::WallTime, AxisScale, BatchSize, BenchmarkGroup,
    BenchmarkId, Criterion, PlotConfiguration, SamplingMode,
};
use rand::{prelude::SliceRandom, thread_rng, Rng};

/// Payload (`V`) for testing.
///
/// This is a 64bit-wide object which is enough to store a [`Box`] or a [`usize`].
#[derive(Debug, Clone, Default)]
struct Payload([u8; 8]);

const _: () = assert!(size_of::<Payload>() == 8);
const _: () = assert!(size_of::<Payload>() >= size_of::<Box<Vec<u32>>>());
const _: () = assert!(size_of::<Payload>() >= size_of::<usize>());

type TestHeap = AddressableHeap<u64, Payload, u64>;

const TEST_SIZES: &[usize] = &[0, 1, 10, 100, 1_000, 10_000];

#[derive(Debug, Clone)]
struct Entry {
    k: u64,
    o: u64,
}

impl Entry {
    fn new_random<R>(rng: &mut R) -> Self
    where
        R: Rng,
    {
        Self {
            // leave some room at the top and bottom
            k: (rng.gen::<u64>() << 1) + (u64::MAX << 2),
            // leave some room at the top and bottom
            o: (rng.gen::<u64>() << 1) + (u64::MAX << 2),
        }
    }

    fn new_random_n<R>(rng: &mut R, n: usize) -> Vec<Self>
    where
        R: Rng,
    {
        (0..n).map(|_| Self::new_random(rng)).collect()
    }
}

fn create_filled_heap<R>(rng: &mut R, n: usize) -> (TestHeap, Vec<Entry>)
where
    R: Rng,
{
    let mut heap = TestHeap::default();
    let mut entries = Vec::with_capacity(n);

    for _ in 0..n {
        let entry = Entry::new_random(rng);
        heap.insert(entry.k, Payload::default(), entry.o);
        entries.push(entry);
    }

    (heap, entries)
}

fn setup_group(g: &mut BenchmarkGroup<'_, WallTime>) {
    g.plot_config(PlotConfiguration::default().summary_scale(AxisScale::Logarithmic));
    g.sampling_mode(SamplingMode::Flat);
}

fn bench_insert_n_elements(c: &mut Criterion) {
    let mut g = c.benchmark_group("insert_n_elements");
    setup_group(&mut g);

    let mut rng = thread_rng();

    for n in TEST_SIZES {
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &_n| {
            b.iter_batched(
                || (TestHeap::default(), Entry::new_random_n(&mut rng, *n)),
                |(mut heap, entries)| {
                    for entry in &entries {
                        heap.insert(entry.k, Payload::default(), entry.o);
                    }

                    // let criterion handle the drop
                    (heap, entries)
                },
                BatchSize::LargeInput,
            );
        });
    }

    g.finish();
}

fn bench_peek_after_n_elements(c: &mut Criterion) {
    let mut g = c.benchmark_group("peek_after_n_elements");
    setup_group(&mut g);

    let mut rng = thread_rng();

    for n in TEST_SIZES {
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &_n| {
            b.iter_batched(
                || create_filled_heap(&mut rng, *n).0,
                |heap| {
                    heap.peek();

                    // let criterion handle the drop
                    heap
                },
                BatchSize::LargeInput,
            );
        });
    }

    g.finish();
}

fn bench_get_existing_after_n_elements(c: &mut Criterion) {
    let mut g = c.benchmark_group("get_existing_after_n_elements");
    setup_group(&mut g);

    let mut rng = thread_rng();

    for n in TEST_SIZES {
        if *n == 0 {
            continue;
        }

        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &_n| {
            b.iter_batched(
                || {
                    let (heap, entries) = create_filled_heap(&mut rng, *n);
                    let entry = entries.choose(&mut rng).unwrap().clone();
                    (heap, entry)
                },
                |(heap, entry)| {
                    heap.get(&entry.k);

                    // let criterion handle the drop
                    heap
                },
                BatchSize::LargeInput,
            );
        });
    }

    g.finish();
}

fn bench_get_new_after_n_elements(c: &mut Criterion) {
    let mut g = c.benchmark_group("get_new_after_n_elements");
    setup_group(&mut g);

    let mut rng = thread_rng();

    for n in TEST_SIZES {
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &_n| {
            b.iter_batched(
                || {
                    let (heap, _entries) = create_filled_heap(&mut rng, *n);
                    let entry = Entry::new_random(&mut rng);
                    (heap, entry)
                },
                |(heap, entry)| {
                    heap.get(&entry.k);

                    // let criterion handle the drop
                    heap
                },
                BatchSize::LargeInput,
            );
        });
    }

    g.finish();
}

fn bench_pop_n_elements(c: &mut Criterion) {
    let mut g = c.benchmark_group("pop_n_elements");
    setup_group(&mut g);

    let mut rng = thread_rng();

    for n in TEST_SIZES {
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &_n| {
            b.iter_batched(
                || create_filled_heap(&mut rng, *n).0,
                |mut heap| {
                    for _ in 0..*n {
                        heap.pop();
                    }

                    // let criterion handle the drop
                    heap
                },
                BatchSize::LargeInput,
            );
        });
    }

    g.finish();
}

fn bench_remove_existing_after_n_elements(c: &mut Criterion) {
    let mut g = c.benchmark_group("remove_existing_after_n_elements");
    setup_group(&mut g);

    let mut rng = thread_rng();

    for n in TEST_SIZES {
        if *n == 0 {
            continue;
        }

        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &_n| {
            b.iter_batched(
                || {
                    let (heap, entries) = create_filled_heap(&mut rng, *n);
                    let entry = entries.choose(&mut rng).unwrap().clone();
                    (heap, entry)
                },
                |(mut heap, entry)| {
                    heap.remove(&entry.k);

                    // let criterion handle the drop
                    heap
                },
                BatchSize::LargeInput,
            );
        });
    }

    g.finish();
}

fn bench_remove_new_after_n_elements(c: &mut Criterion) {
    let mut g = c.benchmark_group("remove_new_after_n_elements");
    setup_group(&mut g);

    let mut rng = thread_rng();

    for n in TEST_SIZES {
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &_n| {
            b.iter_batched(
                || {
                    let (heap, _entries) = create_filled_heap(&mut rng, *n);
                    let entry = Entry::new_random(&mut rng);
                    (heap, entry)
                },
                |(mut heap, entry)| {
                    heap.remove(&entry.k);

                    // let criterion handle the drop
                    heap
                },
                BatchSize::LargeInput,
            );
        });
    }

    g.finish();
}

fn bench_replace_after_n_elements(c: &mut Criterion) {
    let mut g = c.benchmark_group("replace_after_n_elements");
    setup_group(&mut g);

    let mut rng = thread_rng();

    for n in TEST_SIZES {
        if *n == 0 {
            continue;
        }

        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &_n| {
            b.iter_batched(
                || {
                    let (heap, entries) = create_filled_heap(&mut rng, *n);
                    let entry = entries.choose(&mut rng).unwrap().clone();
                    let entry = Entry {
                        k: entry.k,
                        o: Entry::new_random(&mut rng).o,
                    };
                    (heap, entry)
                },
                |(mut heap, entry)| {
                    heap.insert(entry.k, Payload::default(), entry.o);

                    // let criterion handle the drop
                    heap
                },
                BatchSize::LargeInput,
            );
        });
    }

    g.finish();
}

fn bench_update_order_existing_to_random_after_n_elements(c: &mut Criterion) {
    let mut g = c.benchmark_group("update_order_existing_to_random_after_n_elements");
    setup_group(&mut g);

    let mut rng = thread_rng();

    for n in TEST_SIZES {
        if *n == 0 {
            continue;
        }

        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &_n| {
            b.iter_batched(
                || {
                    let (heap, entries) = create_filled_heap(&mut rng, *n);
                    let entry = entries.choose(&mut rng).unwrap().clone();
                    let entry = Entry {
                        k: entry.k,
                        o: Entry::new_random(&mut rng).o,
                    };
                    (heap, entry)
                },
                |(mut heap, entry)| {
                    heap.update_order(&entry.k, entry.o);

                    // let criterion handle the drop
                    heap
                },
                BatchSize::LargeInput,
            );
        });
    }

    g.finish();
}

fn bench_update_order_existing_to_last_after_n_elements(c: &mut Criterion) {
    let mut g = c.benchmark_group("update_order_existing_to_first_after_n_elements");
    setup_group(&mut g);

    let mut rng = thread_rng();

    for n in TEST_SIZES {
        if *n == 0 {
            continue;
        }

        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &_n| {
            b.iter_batched(
                || {
                    let (heap, entries) = create_filled_heap(&mut rng, *n);
                    let entry = entries.choose(&mut rng).unwrap().clone();
                    let entry = Entry {
                        k: entry.k,
                        o: u64::MAX - (u64::MAX << 2),
                    };
                    (heap, entry)
                },
                |(mut heap, entry)| {
                    heap.update_order(&entry.k, entry.o);

                    // let criterion handle the drop
                    heap
                },
                BatchSize::LargeInput,
            );
        });
    }

    g.finish();
}

fn bench_update_order_new_after_n_elements(c: &mut Criterion) {
    let mut g = c.benchmark_group("update_order_new_after_n_elements");
    setup_group(&mut g);

    let mut rng = thread_rng();

    for n in TEST_SIZES {
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &_n| {
            b.iter_batched(
                || {
                    let (heap, _entries) = create_filled_heap(&mut rng, *n);
                    let entry = Entry::new_random(&mut rng);
                    (heap, entry)
                },
                |(mut heap, entry)| {
                    heap.update_order(&entry.k, entry.o);

                    // let criterion handle the drop
                    heap
                },
                BatchSize::LargeInput,
            );
        });
    }

    g.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets =
        bench_insert_n_elements,
        bench_peek_after_n_elements,
        bench_get_existing_after_n_elements,
        bench_get_new_after_n_elements,
        bench_pop_n_elements,
        bench_remove_existing_after_n_elements,
        bench_remove_new_after_n_elements,
        bench_replace_after_n_elements,
        bench_update_order_existing_to_random_after_n_elements,
        bench_update_order_existing_to_last_after_n_elements,
        bench_update_order_new_after_n_elements,
}
criterion_main!(benches);
