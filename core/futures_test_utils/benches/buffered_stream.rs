use std::sync::Arc;

use clap::Parser;
use futures::{FutureExt, StreamExt, future::BoxFuture};
use futures_concurrency::prelude::*;
use futures_test_utils::{AssertFutureExt, FutureObserver};
use rand::Rng;
use tokio::sync::Barrier;

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

#[derive(Debug, Clone, Parser)]
struct BenchmarkParams {
    /// Run actual benchmark.
    ///
    /// If not passed, this is treated as a smoke-test.
    #[clap(long)]
    bench: bool,
}

#[tokio::main]
async fn main() {
    let params = BenchmarkParams::parse();

    print_header();
    CaseIoSeps {
        name: "buffered",
        f_collect: collect_via_futures_buffered_steam,
    }
    .run(params.bench)
    .await;
    CaseIoSeps {
        name: "co_stream",
        f_collect: collect_via_futures_concurrency_co_steam,
    }
    .run(params.bench)
    .await;
}

#[derive(Debug)]
struct CaseIoStepsConfig {
    n_io_steps: usize,
    n_chunks: usize,
}

impl CaseIoStepsConfig {
    fn draw(bench: bool) -> Self {
        let mut rng = rand::rng();
        let upper_bound = if bench { 20 } else { 4 };
        Self {
            n_io_steps: rng.random_range(1..upper_bound),
            n_chunks: rng.random_range(1..upper_bound),
        }
    }
}

struct CaseIoSeps<Collect>
where
    Collect: AsyncFn(Vec<BoxFuture<'static, ()>>) + Send + Sync,
{
    name: &'static str,
    f_collect: Collect,
}

impl<Collect> CaseIoSeps<Collect>
where
    Collect: AsyncFn(Vec<BoxFuture<'static, ()>>) + Send + Sync,
{
    async fn run(self, bench: bool) {
        let n_runs = if bench { 100 } else { 1 };

        for _ in 0..n_runs {
            self.run_config(CaseIoStepsConfig::draw(bench)).await;
        }
    }

    async fn run_config(&self, config: CaseIoStepsConfig) {
        let Self { name, f_collect } = self;
        println!("========== RUN: name='{name}' config={config:?} ==========");
        let CaseIoStepsConfig {
            n_io_steps,
            n_chunks,
        } = &config;

        let barriers = Arc::new(
            (0..*n_io_steps)
                .map(|_| Barrier::new(n_chunks + 1))
                .collect::<Vec<_>>(),
        );
        let mut f_chunks = Vec::new();
        for _ in 0..*n_chunks {
            let barriers_captured = Arc::clone(&barriers);
            let mut fut = async move {
                for barrier in barriers_captured.iter() {
                    barrier.wait().await;
                }
            }
            .boxed();

            fut.assert_pending().await;
            f_chunks.push(fut);
        }

        let fut_io = async {
            for barrier in barriers.iter() {
                barrier.wait().await;
            }
        }
        .boxed();

        let fut = f_collect(f_chunks);
        let fut = FutureObserver::new(fut, "collect");
        let stats = fut.stats();

        // Don't use `tokio::select!` or `tokio::join!` because they poll too often. What the H?!
        // So we use this lovely crate instead: https://crates.io/crates/futures-concurrency
        fut.join(fut_io).await;

        eprintln!(
            "{name},{n_io_steps},{n_chunks},{},{}",
            stats.polled(),
            stats.woken()
        );
    }
}

async fn collect_via_futures_buffered_steam(chunks: Vec<BoxFuture<'static, ()>>) {
    let n_chunks = chunks.len();
    futures::stream::iter(chunks)
        .buffered(n_chunks)
        .collect::<Vec<_>>()
        .await;
}

async fn collect_via_futures_concurrency_co_steam(chunks: Vec<BoxFuture<'static, ()>>) {
    chunks
        .into_co_stream()
        .map(move |f| async move {
            f.await;
        })
        .collect::<Vec<_>>()
        .await;
}

fn print_header() {
    eprintln!("name,n_io_steps,n_chunks,collector_polled,collector_woken");
}
