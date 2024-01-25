//! Client for the cache HTTP API

use crate::api::list::{ListDecoder, ListEntry, MAX_VALUE_SIZE};
use crate::api::{RequestPath, GENERATION};
use crate::{CacheKey, CacheValue};
use bytes::{Buf, Bytes};
use futures::prelude::*;
use futures::stream::BoxStream;
use reqwest::{Client, Response, StatusCode, Url};
use snafu::{OptionExt, ResultExt, Snafu};
use std::time::Duration;

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Creating client: {source}"))]
    Client { source: reqwest::Error },

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

    #[snafu(display("Error decoding list stream: {source}"), context(false))]
    ListStream { source: crate::api::list::Error },
}

/// Result type for [`CatalogCacheClient`]
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The type returned by [`CatalogCacheClient::list`]
pub type ListStream = BoxStream<'static, Result<ListEntry>>;

const RESOURCE_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

/// We use a longer timeout for list request as they may transfer a non-trivial amount of data
const LIST_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// A client for accessing a remote catalog cache
#[derive(Debug)]
pub struct CatalogCacheClient {
    client: Client,
    endpoint: Url,
}

impl CatalogCacheClient {
    /// Create a new [`CatalogCacheClient`] with the given remote endpoint
    pub fn try_new(endpoint: Url) -> Result<Self> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .build()
            .context(ClientSnafu)?;

        Ok(Self { endpoint, client })
    }

    /// Retrieve the given value from the remote cache, if present
    pub async fn get(&self, key: CacheKey) -> Result<Option<CacheValue>> {
        let url = format!("{}{}", self.endpoint, RequestPath::Resource(key));
        let timeout = RESOURCE_REQUEST_TIMEOUT;
        let req = self.client.get(url).timeout(timeout);
        let resp = req.send().await.context(GetSnafu)?;

        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let resp = resp.error_for_status().context(GetSnafu)?;

        let generation = resp
            .headers()
            .get(&GENERATION)
            .context(MissingGenerationSnafu)?;

        let generation = generation
            .to_str()
            .ok()
            .and_then(|v| v.parse().ok())
            .context(InvalidGenerationSnafu)?;

        let data = resp.bytes().await.context(GetSnafu)?;

        Ok(Some(CacheValue::new(data, generation)))
    }

    /// Upsert the given key-value pair to the remote cache
    ///
    /// Returns false if the value had a generation less than or equal to
    /// an existing value
    pub async fn put(&self, key: CacheKey, value: &CacheValue) -> Result<bool> {
        let url = format!("{}{}", self.endpoint, RequestPath::Resource(key));

        let response = self
            .client
            .put(url)
            .timeout(RESOURCE_REQUEST_TIMEOUT)
            .header(&GENERATION, value.generation)
            .body(value.data.clone())
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
        let size = max_value_size.unwrap_or(MAX_VALUE_SIZE);
        let url = format!("{}{}?size={size}", self.endpoint, RequestPath::List);
        let fut = self.client.get(url).timeout(LIST_REQUEST_TIMEOUT).send();

        futures::stream::once(fut.map_err(|source| Error::List { source }))
            .and_then(move |response| futures::future::ready(list_stream(response, size)))
            .try_flatten()
            .boxed()
    }
}

struct ListStreamState {
    response: Response,
    current: Bytes,
    decoder: ListDecoder,
}

impl ListStreamState {
    fn new(response: Response, max_value_size: usize) -> Self {
        Self {
            response,
            current: Default::default(),
            decoder: ListDecoder::new().with_max_value_size(max_value_size),
        }
    }
}

fn list_stream(
    response: Response,
    max_value_size: usize,
) -> Result<impl Stream<Item = Result<ListEntry>>> {
    let resp = response.error_for_status().context(ListSnafu)?;
    let state = ListStreamState::new(resp, max_value_size);
    Ok(stream::try_unfold(state, |mut state| async move {
        loop {
            if state.current.is_empty() {
                match state.response.chunk().await.context(ListSnafu)? {
                    Some(new) => state.current = new,
                    None => break,
                }
            }

            let to_read = state.current.len();
            let read = state.decoder.decode(&state.current)?;
            state.current.advance(read);
            if read != to_read {
                break;
            }
        }
        Ok(state.decoder.flush()?.map(|entry| (entry, state)))
    }))
}
