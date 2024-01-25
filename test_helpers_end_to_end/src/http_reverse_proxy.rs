//! Poor-mans simulation of an HTTP/2 service that randomizes incoming requests to a number of backend services.

use std::{
    net::{SocketAddr, TcpListener},
    sync::{Arc, Weak},
    thread::JoinHandle,
};

use http::{Request, Response};
use hyper::{
    client::HttpConnector,
    service::{make_service_fn, service_fn},
    Body, Client, Server,
};
use rand::seq::SliceRandom;
use tokio_util::sync::CancellationToken;

use crate::service_link::{LinkableService, LinkableServiceImpl};

/// A basic HTTP reverse proxy for use by end-to-end tests
///
/// Intended to approximate a Kubernetes Service.
///
/// # Implementation
/// This runs in a dedicated thread in its own tokio runtime. The reason is that we potentially share a single proxy
/// between multiple tests, but every test sets up its own tokio runtime and moving IO tasks between runtimes can cause blocking.
#[derive(Debug)]
pub struct HttpReverseProxy {
    addr: SocketAddr,
    shutdown: CancellationToken,
    task: Option<JoinHandle<()>>,
    links: LinkableServiceImpl,
}

impl HttpReverseProxy {
    pub fn new<I, S>(backends: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: ToString,
    {
        let client = Client::builder().http2_only(true).build_http();
        let inner = Arc::new(Inner {
            backends: backends.into_iter().map(|s| s.to_string()).collect(),
            client,
        });
        assert!(!inner.backends.is_empty(), "need at least 1 backend");

        let addr = SocketAddr::from(([127, 0, 0, 1], 0));

        let make_service = make_service_fn(move |_conn| {
            let inner = Arc::clone(&inner);

            async move {
                Ok::<_, hyper::Error>(service_fn(move |req| {
                    let inner = Arc::clone(&inner);

                    async move { inner.handle(req).await }
                }))
            }
        });

        let listener = TcpListener::bind(addr).unwrap();
        let addr = listener.local_addr().unwrap();

        let shutdown = CancellationToken::new();
        let shutdown_captured = shutdown.clone();
        let task = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let server = Server::from_tcp(listener)
                    .unwrap()
                    .http2_only(true)
                    .serve(make_service);

                tokio::select! {
                    _ = shutdown_captured.cancelled() => {}
                    res = server => {
                        if let Err(e) = res {
                            eprintln!("server error: {}", e);
                        }
                    }
                }
            })
        });

        Self {
            addr,
            shutdown,
            task: Some(task),
            links: Default::default(),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for HttpReverseProxy {
    fn drop(&mut self) {
        self.shutdown.cancel();

        if self.task.take().expect("not joined yet").join().is_err() {
            eprintln!("server task error, check logs");
        }
    }
}

impl LinkableService for HttpReverseProxy {
    fn add_link_client(&self, client: Weak<dyn LinkableService>) {
        self.links.add_link_client(client)
    }

    fn remove_link_clients(&self) -> Vec<Arc<dyn LinkableService>> {
        self.links.remove_link_clients()
    }

    fn add_link_server(&self, server: Arc<dyn LinkableService>) {
        self.links.add_link_server(server)
    }

    fn remove_link_server(&self, server: Arc<dyn LinkableService>) {
        self.links.remove_link_server(server)
    }
}

#[derive(Debug)]
struct Inner {
    backends: Vec<String>,
    client: Client<HttpConnector>,
}

impl Inner {
    async fn handle(&self, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
        let uri = self.pick_backend();

        let (mut parts, body) = req.into_parts();

        // build URI
        let mut uri = uri.to_owned();
        uri.push_str(parts.uri.path());
        if let Some(q) = parts.uri.query() {
            uri.push('?');
            uri.push_str(q);
        }
        parts.uri = uri.parse().unwrap();

        let req = Request::from_parts(parts, body);
        self.client.request(req).await
    }

    fn pick_backend(&self) -> &str {
        let mut rng = rand::thread_rng();
        self.backends.choose(&mut rng).expect("not empty")
    }
}
