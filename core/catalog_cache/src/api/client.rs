//! Client for the cache HTTP API

use crate::api::list::{ListEntry, MAX_VALUE_SIZE, v2};
use crate::api::{GENERATION, GENERATION_NOT_MATCH, LIST_PROTOCOL_V2, NO_VALUE, RequestPath};
use crate::{CacheKey, CacheValue};
use futures::prelude::*;
use futures::stream::BoxStream;
use hyper::header::{ACCEPT, CONTENT_TYPE, ETAG, IF_NONE_MATCH, ToStrError};
use hyper::service::Service;
use hyper_util::{client::legacy::connect::dns::GaiResolver, service::TowerToHyperService};
use metric::DurationHistogram;
use reqwest::dns::{Name, Resolve, Resolving};
use reqwest::{Client, StatusCode, Url};
use snafu::{OptionExt, ResultExt, Snafu, ensure};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Snafu)]
#[expect(missing_docs)]
pub enum Error {
    #[snafu(display("Creating client: {source}"))]
    Client { source: reqwest::Error },

    #[snafu(display("Invalid endpoint: {url}"))]
    InvalidEndpoint { url: Url },

    #[snafu(display("Put Reqwest error: {source}"))]
    Put { source: reqwest::Error },

    #[snafu(display("Get Reqwest error: {source}"))]
    Get { source: reqwest::Error },

    #[snafu(display("List Reqwest error: {source}"))]
    List { source: reqwest::Error },

    #[snafu(display("Health Reqwest error: {source}"))]
    Health { source: reqwest::Error },

    #[snafu(display("Missing generation header"))]
    MissingGeneration,

    #[snafu(display("Invalid generation value"))]
    InvalidGeneration,

    #[snafu(display("Invalid etag: {source}"))]
    InvalidEtag { source: ToStrError },

    #[snafu(display("Not modified"))]
    NotModified,
}

/// Result type for [`CatalogCacheClient`]
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The type returned by [`CatalogCacheClient::list`]
pub type ListStream = BoxStream<'static, Result<ListEntry, crate::api::list::Error>>;

/// Builder for [`CatalogCacheClient`].
#[derive(Debug)]
pub struct CatalogCacheClientBuilder {
    connect_timeout: Duration,
    get_request_timeout: Duration,
    put_request_timeout: Duration,
    list_request_timeout: Duration,
    endpoint: Url,
    registry: Arc<metric::Registry>,
}

impl CatalogCacheClientBuilder {
    /// Build client.
    pub fn build(self) -> Result<CatalogCacheClient> {
        let Self {
            connect_timeout,
            get_request_timeout,
            put_request_timeout,
            list_request_timeout,
            endpoint,
            registry,
        } = self;

        ensure!(
            endpoint.path().is_empty() || endpoint.path() == "/",
            InvalidEndpointSnafu {
                url: endpoint.clone()
            },
        );
        ensure!(
            endpoint.query().is_none(),
            InvalidEndpointSnafu {
                url: endpoint.clone()
            },
        );
        ensure!(
            endpoint.fragment().is_none(),
            InvalidEndpointSnafu {
                url: endpoint.clone()
            },
        );

        let dns_resolver = Arc::new(Resolver::new(&registry));
        let client_builder = Box::new(move || {
            // Note: do NOT set `.timeout` here because we set the timeout per request type.
            Client::builder()
                .connect_timeout(connect_timeout)
                .dns_resolver(Arc::clone(&dns_resolver))
                .http2_prior_knowledge()
                .http2_adaptive_window(true)
                .build()
        });
        let client = client_builder().context(ClientSnafu)?;

        Ok(CatalogCacheClient {
            endpoint,
            client,
            client_builder,
            get_request_timeout,
            put_request_timeout,
            list_request_timeout,
        })
    }

    /// Set a timeout for only the connect phase of a `Client`.
    pub fn connect_timeout(self, connect_timeout: Duration) -> Self {
        Self {
            connect_timeout,
            ..self
        }
    }

    /// Set timeout for `GET` requests.
    ///
    /// The timeout is applied from when the request starts connecting until the
    /// response body has finished.
    pub fn get_request_timeout(self, get_request_timeout: Duration) -> Self {
        Self {
            get_request_timeout,
            ..self
        }
    }

    /// Set timeout for `PUT` requests.
    ///
    /// The timeout is applied from when the request starts connecting until the
    /// response body has finished.
    pub fn put_request_timeout(self, put_request_timeout: Duration) -> Self {
        Self {
            put_request_timeout,
            ..self
        }
    }

    /// Set timeout for `LIST` requests.
    ///
    /// The timeout is applied from when the request starts connecting until the
    /// response body has finished.
    ///
    /// Given the non-trivial amount of data that this may transfer, this should be set higher than the
    /// [`get_request_timeout`](Self::get_request_timeout).
    pub fn list_request_timeout(self, list_request_timeout: Duration) -> Self {
        Self {
            list_request_timeout,
            ..self
        }
    }
}

/// A client for accessing a remote catalog cache
pub struct CatalogCacheClient {
    client: Client,
    client_builder: Box<dyn Fn() -> Result<Client, reqwest::Error> + Send + Sync>,
    endpoint: Url,
    get_request_timeout: Duration,
    put_request_timeout: Duration,
    list_request_timeout: Duration,
}

impl CatalogCacheClient {
    /// Set up builder with the given remote endpoint.
    pub fn builder(endpoint: Url, registry: Arc<metric::Registry>) -> CatalogCacheClientBuilder {
        CatalogCacheClientBuilder {
            connect_timeout: Duration::from_secs(2),
            get_request_timeout: Duration::from_secs(1),
            put_request_timeout: Duration::from_secs(1),
            // We use a longer timeout for list request as they may transfer a non-trivial amount of data
            list_request_timeout: Duration::from_secs(20),
            endpoint,
            registry,
        }
    }

    /// Get endpoint.
    pub fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// URL to given key.
    fn url(&self, path: RequestPath) -> Url {
        // try to construct URL rather cheaply, a true builder doesn't really exists yet, see https://github.com/servo/rust-url/issues/835
        let mut url = self.endpoint.clone();
        url.set_path(&path.to_string());
        url
    }

    /// Retrieve the given value from the remote cache, if present
    pub async fn get(&self, key: CacheKey) -> Result<Option<CacheValue>> {
        self.get_if_modified(key, None, None).await
    }

    /// Retrieve the given value from the remote cache, if present
    ///
    /// Returns [`Error::NotModified`] if value exists and matches `generation`
    pub async fn get_if_modified(
        &self,
        key: CacheKey,
        generation: Option<u64>,
        etag: Option<Arc<str>>,
    ) -> Result<Option<CacheValue>> {
        let url = self.url(RequestPath::Resource(key));
        let mut req = self.client.get(url).timeout(self.get_request_timeout);
        if let Some(generation) = generation {
            req = req.header(&GENERATION_NOT_MATCH, generation)
        }
        if let Some(etag) = etag {
            req = req.header(IF_NONE_MATCH, etag.as_ref())
        }

        let resp = req.send().await.context(GetSnafu)?;
        let resp = match resp.status() {
            StatusCode::NOT_FOUND => return Ok(None),
            StatusCode::NOT_MODIFIED => return Err(Error::NotModified),
            _ => resp.error_for_status().context(GetSnafu)?,
        };

        let generation = resp
            .headers()
            .get(&GENERATION)
            .context(MissingGenerationSnafu)?;

        let generation = generation
            .to_str()
            .ok()
            .and_then(|v| v.parse().ok())
            .context(InvalidGenerationSnafu)?;

        let etag = match resp.headers().get(ETAG) {
            Some(etag) => Some(etag.to_str().context(InvalidEtagSnafu)?.into()),
            None => None,
        };

        let no_value = resp
            .headers()
            .get(&NO_VALUE)
            .map(|v| v.to_str().unwrap_or("false") == "true")
            .unwrap_or(false);

        let value = if no_value {
            CacheValue::new_empty(generation)
        } else {
            let data = resp.bytes().await.context(GetSnafu)?;
            CacheValue::new(data, generation)
        };
        Ok(Some(value.with_etag_opt(etag)))
    }

    /// Upsert the given key-value pair to the remote cache
    ///
    /// Returns false if the value had a generation less than or equal to
    /// an existing value
    pub async fn put(&self, key: CacheKey, value: &CacheValue) -> Result<bool> {
        let url = self.url(RequestPath::Resource(key));

        let mut builder = self
            .client
            .put(url)
            .timeout(self.put_request_timeout)
            .header(&GENERATION, value.generation);

        if let Some(etag) = value.etag() {
            builder = builder.header(ETAG, etag.as_ref());
        }

        if let Some(data) = value.data().cloned() {
            builder = builder.body(data);
        } else {
            builder = builder.header(&NO_VALUE, "true");
            builder = builder.body(reqwest::Body::default());
        }

        let response = builder
            .send()
            .await
            .context(PutSnafu)?
            .error_for_status()
            .context(PutSnafu)?;

        Ok(matches!(response.status(), StatusCode::OK))
    }

    /// List the contents of the remote cache
    ///
    /// Values larger than `max_value_size` will not be returned inline, with only the key
    /// and generation returned instead. Defaults to [`MAX_VALUE_SIZE`]
    pub fn list(&self, max_value_size: Option<usize>) -> ListStream {
        let mut url = self.url(RequestPath::List);
        let size = max_value_size.unwrap_or(MAX_VALUE_SIZE);
        url.set_query(Some(&format!("size={size}")));

        // use dedicated client connection for LIST to bypass potential rate limits
        let client = match (self.client_builder)() {
            Ok(c) => c,
            Err(e) => {
                return futures::stream::once(async move {
                    Err(crate::api::list::Error::Reqwest { source: e })
                })
                .boxed();
            }
        };
        let fut = client
            .get(url)
            .header(ACCEPT, &LIST_PROTOCOL_V2)
            .timeout(self.list_request_timeout)
            .send();

        futures::stream::once(fut.map_err(Into::into))
            .and_then(move |response| {
                let stream = match response.headers().get(CONTENT_TYPE) {
                    Some(x) if x == LIST_PROTOCOL_V2 => {
                        v2::decode_response(response).map(|x| x.boxed())
                    }
                    other => Err(crate::api::list::Error::UnsupportedProtocol {
                        version: other
                            .map(|h| String::from_utf8_lossy(h.as_bytes()).into_owned())
                            .unwrap_or_else(|| "<none>".to_owned()),
                    }),
                };
                futures::future::ready(stream)
            })
            .try_flatten()
            .boxed()
    }
}

impl std::fmt::Debug for CatalogCacheClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            client,
            client_builder: _,
            endpoint,
            get_request_timeout,
            put_request_timeout,
            list_request_timeout,
        } = self;

        f.debug_struct("CatalogCacheClient")
            .field("client", client)
            .field("client_builder", &"<CLIENT_BUILDER>")
            .field("endpoint", endpoint)
            .field("get_request_timeout", get_request_timeout)
            .field("put_request_timeout", put_request_timeout)
            .field("list_request_timeout", list_request_timeout)
            .finish()
    }
}

/// A custom [`Resolve`] that collects [`ResolverMetrics`]
#[derive(Debug, Clone)]
struct Resolver {
    resolver: TowerToHyperService<GaiResolver>,
    metrics: Arc<ResolverMetrics>,
}

impl Resolver {
    fn new(registry: &metric::Registry) -> Self {
        Self {
            // the `GaiResolver` still uses the old `tower::Service`,
            // therefore, use the `TowerToHyperService` adapter
            // to make into a `hyper::Service`
            resolver: TowerToHyperService::new(GaiResolver::new()),
            metrics: Arc::new(ResolverMetrics::new(registry)),
        }
    }
}

impl Resolve for Resolver {
    fn resolve(&self, name: Name) -> Resolving {
        let s = self.clone();
        let metrics = Arc::clone(&self.metrics);
        let start = Instant::now();

        // `reqwest::dns::Name` is a wrapper around the `hyper` name.
        // need to translate to the `hyper_util` `Name` (which is a wrapper around the `hyper` name)
        use hyper_util::client::legacy::connect::dns::Name as LegacyName;
        let name: LegacyName = name.as_str().parse().expect("name is valid");

        Box::pin(async move {
            let r = s.resolver.call(name).await;
            metrics.record(start.elapsed());
            r.map(|addrs| Box::new(addrs) as _)
                .map_err(|e| Box::new(e) as _)
        })
    }
}

#[derive(Debug)]
struct ResolverMetrics {
    duration: DurationHistogram,
}

impl ResolverMetrics {
    fn new(registry: &metric::Registry) -> Self {
        let duration = registry.register_metric::<DurationHistogram>(
            "dns_request_duration",
            "Time to perform a DNS request",
        );

        Self {
            duration: duration.recorder(&[("client", "catalog")]),
        }
    }

    fn record(&self, duration: Duration) {
        self.duration.record(duration);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_checks_endpoint() {
        // OK
        construct_with_endpoint("http://example.com").unwrap();
        construct_with_endpoint("http://example.com/").unwrap();
        construct_with_endpoint("https://example.com").unwrap();
        construct_with_endpoint("https://example.com:11111").unwrap();

        // no OK
        construct_with_endpoint("http://example.com/x").unwrap_err();
        construct_with_endpoint("http://example.com?a").unwrap_err();
        construct_with_endpoint("http://example.com#a").unwrap_err();
    }

    fn construct_with_endpoint(endpoint: &'static str) -> Result<CatalogCacheClient> {
        CatalogCacheClient::builder(
            endpoint.try_into().unwrap(),
            Arc::new(metric::Registry::new()),
        )
        .build()
    }
}
