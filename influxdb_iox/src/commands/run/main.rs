use std::sync::Arc;

use ioxd_common::Service;
use ioxd_common::{grpc_listener, http_listener, serve, server_type::CommonServerState};
use observability_deps::tracing::{debug, error, info};
use panic_logging::SendPanicsToTracing;
use snafu::{ResultExt, Snafu};
use tokio_util::sync::CancellationToken;

#[cfg(all(not(feature = "heappy"), feature = "jemalloc_replacing_malloc"))]
mod jemalloc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{}", source))]
    Wrapper { source: ioxd_common::Error },

    #[snafu(display("Error joining server task: {}", source))]
    Joining { source: tokio::task::JoinError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<ioxd_common::Error> for Error {
    fn from(source: ioxd_common::Error) -> Self {
        Self::Wrapper { source }
    }
}

#[cfg(all(not(feature = "heappy"), not(feature = "jemalloc_replacing_malloc")))]
fn build_malloc_conf() -> String {
    "system".to_string()
}

#[cfg(all(feature = "heappy", not(feature = "jemalloc_replacing_malloc")))]
fn build_malloc_conf() -> String {
    "heappy".to_string()
}

#[cfg(all(not(feature = "heappy"), feature = "jemalloc_replacing_malloc"))]
fn build_malloc_conf() -> String {
    tikv_jemalloc_ctl::config::malloc_conf::mib()
        .unwrap()
        .read()
        .unwrap()
        .to_string()
}

#[cfg(all(
    feature = "heappy",
    feature = "jemalloc_replacing_malloc",
    not(feature = "clippy")
))]
fn build_malloc_conf() -> String {
    compile_error!("must use exactly one memory allocator")
}

#[cfg(feature = "clippy")]
fn build_malloc_conf() -> String {
    "clippy".to_string()
}

/// This is the entry point for the IOx server.
///
/// This entry point ensures that the given set of Services are
/// started using best practice, e.g. that we print the GIT-hash and
/// malloc-configs, that a panic handler is installed, etc.
///
/// Due to its invasive nature (install global panic handling,
/// logging, etc) this function should not be used during unit tests.
pub async fn main(
    common_state: CommonServerState,
    services: Vec<Service>,
    metrics: Arc<metric::Registry>,
) -> Result<()> {
    let git_hash = env!("GIT_HASH", "starting influxdb_iox server");
    let num_cpus = num_cpus::get();
    let build_malloc_conf = build_malloc_conf();
    info!(
        git_hash,
        num_cpus,
        %build_malloc_conf,
        "InfluxDB IOx server starting",
    );

    for service in &services {
        if let Some(http_bind_address) = &service.http_bind_address {
            if (&service.grpc_bind_address == http_bind_address)
                && (service.grpc_bind_address.port() != 0)
            {
                error!(
                    grpc_bind_address=%service.grpc_bind_address,
                    http_bind_address=%http_bind_address,
                    "grpc and http bind addresses must differ",
                );
                std::process::exit(1);
            }
        }
    }

    // Install custom panic handler and forget about it.
    //
    // This leaks the handler and prevents it from ever being dropped during the
    // lifetime of the program - this is actually a good thing, as it prevents
    // the panic handler from being removed while unwinding a panic (which in
    // turn, causes a panic - see #548)
    let f = SendPanicsToTracing::new().with_metrics(&metrics);
    std::mem::forget(f);

    // Register jemalloc metrics
    #[cfg(all(not(feature = "heappy"), feature = "jemalloc_replacing_malloc"))]
    for service in &services {
        service
            .server_type
            .metric_registry()
            .register_instrument("jemalloc_metrics", jemalloc::JemallocMetrics::new);
    }

    // Construct a token to trigger clean shutdown
    let frontend_shutdown = CancellationToken::new();

    let mut serving_futures = Vec::new();
    for service in services {
        let common_state = common_state.clone();
        // start them all in their own tasks so the servers run at the same time
        let frontend_shutdown = frontend_shutdown.clone();
        let Service {
            http_bind_address,
            grpc_bind_address,
            server_type,
        } = service;
        let server_type_name = format!("{server_type:?}");
        let handle = tokio::spawn(async move {
            let trace_exporter = common_state.trace_exporter();
            info!(?grpc_bind_address, ?server_type, "Binding gRPC services");
            let grpc_listener = grpc_listener(grpc_bind_address.into()).await?;

            let http_listener = match http_bind_address {
                Some(http_bind_address) => {
                    info!(
                        ?http_bind_address,
                        ?server_type,
                        "Completed bind of gRPC, binding http"
                    );
                    Some(http_listener(http_bind_address.into()).await?)
                }
                None => {
                    info!(?server_type, "No http server specified");
                    None
                }
            };

            let r = serve(
                common_state,
                frontend_shutdown,
                grpc_listener,
                http_listener,
                Arc::clone(&server_type),
            )
            .await;

            info!(
                ?grpc_bind_address,
                ?http_bind_address,
                ?server_type,
                "done serving, draining futures"
            );
            if let Some(trace_exporter) = trace_exporter {
                if let Err(e) = trace_exporter.drain().await {
                    error!(%e, "error draining trace exporter");
                }
            }
            r
        });
        serving_futures.push((server_type_name, handle));
    }

    for (name, f) in serving_futures {
        debug!(
            server_type=%name,
            "wait for handle"
        );
        // Use ?? to unwrap Result<Result<..>>
        // "I heard you like errors, so I put an error in your error...."
        f.await.context(JoiningSnafu)??;
        debug!(
            server_type=%name,
            "handle returned"
        );
    }

    Ok(())
}
