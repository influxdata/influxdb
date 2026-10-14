//! Operator control socket.
//!
//! A Unix domain socket that serves a small HTTP allowlist without authentication.
//! Authorization is filesystem access to the socket, which is created with 0660
//! permissions, so whoever runs the process decides who can reach it. Each edition
//! supplies its own router; this crate owns the listener.

use std::convert::Infallible;
use std::future::Future;
use std::path::PathBuf;

use hyper::body::Incoming;
use iox_http_util::Response;
use observability_deps::tracing::info;
use tokio_util::sync::CancellationToken;
use trace_http::tower::TraceLayer;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("socket has permissions {actual:o}, expected {expected:o}")]
    Permissions { actual: u32, expected: u32 },

    #[error("operator socket is only supported on unix")]
    Unsupported,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Mode the socket is created with: owner and group read/write.
pub const SOCKET_MODE: u32 = 0o660;

/// Serve `router` over a Unix domain socket at `socket_path` until `shutdown` is cancelled.
///
/// A stale socket file from a previous run is removed before binding, and the file is
/// removed again on shutdown. Errors come only from setting up the socket; accept errors
/// are logged and the listener keeps serving. With no path the future pends forever, so
/// callers can `select!` on it whether or not the socket is configured.
pub async fn serve<F, Fut>(
    socket_path: Option<PathBuf>,
    trace_layer: TraceLayer,
    shutdown: CancellationToken,
    router: F,
) -> Result<()>
where
    F: Fn(hyper::Request<Incoming>) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = std::result::Result<Response, Infallible>> + Send + 'static,
{
    let Some(socket_path) = socket_path else {
        return std::future::pending().await;
    };
    info!(path = %socket_path.display(), "operator socket enabled");

    #[cfg(unix)]
    {
        unix::serve(socket_path, trace_layer, shutdown, router).await
    }
    #[cfg(not(unix))]
    {
        let _ = (trace_layer, shutdown, router);
        Err(Error::Unsupported)
    }
}

#[cfg(unix)]
mod unix {
    use super::*;

    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder as ConnectionBuilder;
    use hyper_util::service::TowerToHyperService;
    use observability_deps::tracing::{debug, warn};
    use tokio::net::UnixListener;

    /// Pause after a failed accept, so a persistent error such as `EMFILE` does not spin.
    const ACCEPT_ERROR_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);

    /// Bind the socket so that it is created with [`SOCKET_MODE`] and never briefly wider.
    ///
    /// Binding under a restrictive umask avoids a window where the socket exists with
    /// looser permissions before a chmod. The previous umask is restored afterwards.
    fn bind_with_permissions(socket_path: &Path) -> Result<UnixListener> {
        const SOCKET_UMASK: libc::mode_t = 0o117; // 0o777 - 0o117 = 0o660

        // SAFETY: umask only changes this process's file mode creation mask and always
        // succeeds. The previous mask is restored immediately after the bind.
        let old_umask = unsafe { libc::umask(SOCKET_UMASK) };
        let listener = UnixListener::bind(socket_path);
        unsafe { libc::umask(old_umask) };
        let listener = listener?;

        let actual = std::fs::metadata(socket_path)?.permissions().mode() & 0o777;
        if actual != SOCKET_MODE {
            return Err(Error::Permissions {
                actual,
                expected: SOCKET_MODE,
            });
        }

        Ok(listener)
    }

    pub(super) async fn serve<F, Fut>(
        socket_path: PathBuf,
        trace_layer: TraceLayer,
        shutdown: CancellationToken,
        router: F,
    ) -> Result<()>
    where
        F: Fn(hyper::Request<Incoming>) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = std::result::Result<Response, Infallible>> + Send + 'static,
    {
        // Bind fails if the file exists, so clear a socket left by a previous run.
        if socket_path.exists() {
            std::fs::remove_file(&socket_path)?;
        }

        let listener = bind_with_permissions(&socket_path)?;

        info!(path = %socket_path.display(), "operator control socket listening");

        loop {
            tokio::select! {
                res = listener.accept() => {
                    let stream = match res {
                        Ok((stream, _)) => stream,
                        Err(error) => {
                            warn!(%error, "operator socket failed to accept a connection");
                            tokio::select! {
                                _ = tokio::time::sleep(ACCEPT_ERROR_BACKOFF) => continue,
                                _ = shutdown.cancelled() => break,
                            }
                        }
                    };
                    let router = router.clone();
                    let trace_layer = trace_layer.clone();

                    tokio::spawn(async move {
                        let io = TokioIo::new(stream);
                        let service = tower::ServiceBuilder::new()
                            .layer(trace_layer)
                            .service(tower::service_fn(router));
                        let service = TowerToHyperService::new(service);

                        if let Err(err) = ConnectionBuilder::new(TokioExecutor::new())
                            .serve_connection(io, service)
                            .await
                        {
                            // Clients such as curl and Prometheus close the connection
                            // once they have the full response, which hyper reports as
                            // an error on every request. Keep it at debug.
                            debug!("operator socket connection ended with error: {:?}", err);
                        }
                    });
                }
                _ = shutdown.cancelled() => break,
            }
        }

        if let Err(e) = std::fs::remove_file(&socket_path) {
            warn!(error = %e, path = %socket_path.display(), "failed to remove operator socket file");
        }

        info!("operator control socket shut down");

        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use http_body_util::{BodyExt, Full};
    use hyper::StatusCode;
    use hyper_util::rt::TokioIo;
    use iox_http_util::{ResponseBuilder, bytes_to_response_body};
    use tokio::net::UnixStream;
    use trace_http::ctx::TraceHeaderParser;
    use trace_http::metrics::{MetricFamily, RequestMetrics};
    use trace_http::tower::ServiceProtocol;

    fn trace_layer() -> TraceLayer {
        TraceLayer::new(
            TraceHeaderParser::new(),
            Arc::new(RequestMetrics::new(
                Arc::new(metric::Registry::default()),
                MetricFamily::HttpServer,
            )),
            None,
            "test",
            ServiceProtocol::Http,
        )
    }

    async fn echo_path(req: hyper::Request<Incoming>) -> std::result::Result<Response, Infallible> {
        Ok(ResponseBuilder::new()
            .status(StatusCode::OK)
            .body(bytes_to_response_body(req.uri().path().to_string()))
            .unwrap())
    }

    async fn connect(path: &Path) -> UnixStream {
        let mut attempts = 0;
        loop {
            match UnixStream::connect(path).await {
                Ok(s) => break s,
                Err(e) if attempts >= 40 => panic!("connect failed after {attempts} tries: {e}"),
                Err(_) => {
                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            }
        }
    }

    async fn get(path: &Path, uri: &str) -> (StatusCode, String) {
        let io = TokioIo::new(connect(path).await);
        let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let request = hyper::Request::builder()
            .uri(uri)
            .body(Full::<hyper::body::Bytes>::default())
            .unwrap();
        let response = sender.send_request(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn serves_router_with_restricted_permissions_and_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator.sock");
        let shutdown = CancellationToken::new();

        let server = tokio::spawn(serve(
            Some(path.clone()),
            trace_layer(),
            shutdown.clone(),
            echo_path,
        ));

        assert_eq!(
            get(&path, "/metrics").await,
            (StatusCode::OK, "/metrics".to_string())
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            SOCKET_MODE
        );

        shutdown.cancel();
        server.await.unwrap().unwrap();
        assert!(!path.exists(), "socket file should be removed on shutdown");
    }

    #[tokio::test]
    async fn replaces_stale_socket_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("operator.sock");
        std::fs::write(&path, b"stale").unwrap();
        let shutdown = CancellationToken::new();

        let server = tokio::spawn(serve(
            Some(path.clone()),
            trace_layer(),
            shutdown.clone(),
            echo_path,
        ));

        assert_eq!(
            get(&path, "/health").await,
            (StatusCode::OK, "/health".to_string())
        );

        shutdown.cancel();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn pends_without_a_path() {
        let shutdown = CancellationToken::new();
        let fut = serve(None, trace_layer(), shutdown.clone(), echo_path);
        let outcome = tokio::time::timeout(Duration::from_millis(50), fut).await;
        assert!(outcome.is_err(), "serve(None) must never complete");

        // Cancelling shutdown does not wake it either: the caller's select! owns exit.
        shutdown.cancel();
        let fut = serve(None, trace_layer(), shutdown, echo_path);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), fut)
                .await
                .is_err()
        );
    }
}
