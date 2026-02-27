// Tests and benchmarks don't use all the crate dependencies and that's all right.
#![expect(unused_crate_dependencies)]

use std::{
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};

use bytes::Bytes;
use clap::Parser;
use object_store::path::Path;
use object_store_mem_cache::cache_system::{
    hook::test_utils::NoOpHook,
    s3_fifo_cache::{S3Config, S3Fifo},
};
use rand::{Rng, SeedableRng, rngs::StdRng};

const WORST_N: usize = 3;

#[derive(Debug, Clone, Parser)]
struct BenchmarkParams {
    /// Run actual benchmark.
    ///
    /// If not passed, this is treated as a smoke-test.
    #[clap(long)]
    bench: bool,

    /// Number of threads used.
    #[clap(
        long,
        default_value="dummy",
        default_value_ifs=[("bench", "true", "5"), ("bench", "false", "1")],
    )]
    threads: u64,

    /// Number of keys that a thread can choose from.
    #[clap(
        long,
        default_value="dummy",
        default_value_ifs=[("bench", "true", "10000"), ("bench", "false", "10")],
    )]
    n_keys: u64,

    /// The payload size of each value in bytes.
    ///
    /// This is identical for all keys.
    #[clap(
        long,
        default_value="dummy",
        default_value_ifs=[("bench", "true", "1000000"), ("bench", "false", "1000")],
    )]
    key_size: u64,

    /// The fraction of the working set size ([`n_keys`](Self::n_keys) \* [`key_size`](Self::key_size)) that the cache
    /// can hold.
    #[clap(long, default_value_t = 0.5)]
    size_fraction: f64,

    /// Number of [`get`](ObjectStore::get) calls for each thread.
    #[clap(
        long,
        default_value = "dummy",
        default_value_ifs=[("bench", "true", "10000"), ("bench", "false", "5")],
    )]
    requests: u64,

    /// Number of times the whole benchmark is executed.
    #[clap(
        long,
        default_value = "dummy",
        default_value_ifs=[("bench", "true", "2"), ("bench", "false", "1")],
    )]
    iters: u64,
}

struct Measurement {
    when: Duration,
    duration: Duration,
    thread: u64,
}

impl std::fmt::Display for Measurement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            when,
            duration,
            thread,
        } = self;
        write!(f, "duration={duration:?} when={when:?} thread={thread}")
    }
}

fn run_bench(
    params: BenchmarkParams,
    mut rng: StdRng,
    t_program_start: Instant,
    iteration: u64,
) -> Vec<Measurement> {
    let BenchmarkParams {
        threads,
        n_keys,
        key_size,
        size_fraction,
        requests,
        iters: _,
        bench: _,
    } = params;

    let metrics = metric::Registry::new();
    let max_memory_size = ((key_size * n_keys) as f64 * size_fraction).ceil() as usize;
    let store = Arc::new(S3Fifo::<Path, Arc<Bytes>>::new(
        S3Config {
            cache_name: "bench",
            max_memory_size,
            max_ghost_memory_size: 10_000_000,
            hook: Arc::new(NoOpHook::default()),
            move_to_main_threshold: 0.25,
            inflight_bytes: max_memory_size.div_ceil(4), // 25%
        },
        &metrics,
    ));

    // Pre-calculate the result but do NOT convert it into bytes yet. Bytes are ref-counted internally and we want to
    // include the cost of the drop.
    let data = Arc::new(vec![0u8; key_size as usize]);

    let barrier = Arc::new(Barrier::new(threads as usize));
    // create N futures that wait for the last thread to spawn
    std::thread::scope(|s| {
        let mut handles = vec![];

        for thread in 0..threads {
            let barrier = Arc::clone(&barrier);
            let store = Arc::clone(&store);
            let data = Arc::clone(&data);
            let mut rng = StdRng::from_rng(&mut rng);

            handles.push(
                std::thread::Builder::new()
                    .name(format!("i{iteration}-t{thread}"))
                    .spawn_scoped(s, move || {
                        let mut measurements = Vec::with_capacity(requests as usize);

                        barrier.wait();

                        for _ in 0..requests {
                            // generate keys with linearly decreasing probability
                            let idx = rng.random::<u64>() % (n_keys * n_keys);
                            let key_idx = n_keys - 1 - (idx as f64).sqrt().floor() as u64;
                            assert!(key_idx < n_keys, "{key_idx}");

                            let data = Arc::new(Bytes::from(data.as_ref().clone()));

                            let t_begin = Instant::now();
                            let (_, evicted) = store.get_or_put(Arc::new(path(key_idx)), data, 0);
                            drop(evicted);
                            let t_end = Instant::now();

                            measurements.push(Measurement {
                                when: t_begin - t_program_start,
                                duration: t_end - t_begin,
                                thread,
                            });
                        }

                        measurements
                    })
                    .unwrap(),
            );
        }

        let mut measurements = vec![];
        for handle in handles {
            measurements.extend(handle.join().unwrap());
        }

        measurements
    })
}

fn path(key_idx: u64) -> Path {
    Path::parse(format!("1/{key_idx}/ebd1041daa7c644c99967b817ae607bdcb754c663f2c415f270d6df720280f7a/00000000-0000-0000-0000-000000000000.parquet")).unwrap()
}

fn main() {
    println!("concurrency benchmark");

    let params = BenchmarkParams::parse();

    // make it easy to correlate the output with a profiler
    // Sadly, many profilers don't support wall-clock times.
    // Refs:
    // - https://github.com/KDAB/hotspot/issues/537
    if params.bench {
        std::thread::sleep(Duration::from_secs(1));
    }
    let t_program_start = Instant::now();

    let mut measurements = vec![];
    let mut rng = StdRng::seed_from_u64(0);
    for iteration in 0..params.iters {
        println!("Running iteration {}/{}", iteration + 1, params.iters);
        let params = params.clone();
        let rng = StdRng::from_rng(&mut rng);
        let m = run_bench(params, rng, t_program_start, iteration);
        measurements.extend(m);
    }

    measurements.sort_by_key(|m| std::cmp::Reverse(m.duration));
    println!();
    println!("Results:");
    println!(
        "avg: {:?}",
        measurements.iter().map(|m| m.duration).sum::<Duration>() / (measurements.len() as u32)
    );
    print_quantile(&measurements, 100);
    print_quantile(&measurements, 99);
    print_quantile(&measurements, 95);
    print_quantile(&measurements, 50);
    print_quantile(&measurements, 0);
    println!("worst {WORST_N}:");
    for m in measurements.iter().take(WORST_N) {
        println!("- {m}");
    }
}

fn print_quantile(measurements: &[Measurement], q: u8) {
    assert!(q <= 100);
    println!(
        "q{q:03}: {}",
        measurements[(measurements.len() * (100 - q as usize) / 100).min(measurements.len() - 1)],
    );
}
