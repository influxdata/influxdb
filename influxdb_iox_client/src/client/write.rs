use std::{fmt::Debug, num::NonZeroUsize, sync::Arc};

use client_util::{connection::HttpConnection, namespace_translation::split_namespace};
use futures_util::{future::BoxFuture, FutureExt, Stream, StreamExt, TryStreamExt};

use crate::{
    connection::Connection,
    error::{translate_response, Error},
};
use reqwest::{Body, Method};

/// The default value for the maximum size of each request, in bytes
pub const DEFAULT_MAX_REQUEST_PAYLOAD_SIZE_BYTES: Option<usize> = Some(1024 * 1024);

/// An IOx Write API client.
///
/// ```no_run
/// #[tokio::main]
/// # async fn main() {
/// use influxdb_iox_client::{
///     write::Client,
///     connection::Builder,
/// };
///
/// let mut connection = Builder::default()
///     .build("http://127.0.0.1:8080")
///     .await
///     .unwrap();
///
/// let mut client = Client::new(connection);
///
/// // write a line of line procol data
/// client
///     .write_lp("bananas", "cpu,region=west user=23.2 100")
///     .await
///     .expect("failed to write to IOx");
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    /// The inner client used to actually make requests.
    ///
    /// Uses a trait for test mocking.
    ///
    /// Does not expose the trait in the `Client` type to avoid
    /// exposing an internal implementation detail (the trait) in the
    /// public interface.
    inner: Arc<dyn RequestMaker>,

    /// If `Some`, restricts the maximum amount of line protocol
    /// sent per request to this many bytes. If `None`, does not restrict
    /// the amount sent per request. Defaults to `Some(1MB)`
    ///
    /// Splitting the upload size consumes a non trivial amount of CPU
    /// to find line protocol boundaries. This can be disabled by
    /// setting `max_request_payload_size_bytes` to `None`.
    max_request_payload_size_bytes: Option<usize>,

    /// Makes this many concurrent requests at a time. Defaults to 1
    max_concurrent_uploads: NonZeroUsize,
}

impl Client {
    /// Creates a new client with the provided connection
    pub fn new(connection: Connection) -> Self {
        Self::new_with_maker(Arc::new(connection.into_http_connection()))
    }

    /// Creates a new client with the provided request maker
    fn new_with_maker(inner: Arc<dyn RequestMaker>) -> Self {
        Self {
            inner,
            max_request_payload_size_bytes: DEFAULT_MAX_REQUEST_PAYLOAD_SIZE_BYTES,
            max_concurrent_uploads: NonZeroUsize::new(1).unwrap(),
        }
    }

    /// Override the default of sending 1MB of line protocol per request.
    /// If `Some` is specified, restricts the maximum amount of line protocol
    /// sent per request to this many bytes. If `None`, does not restrict the amount of
    /// line protocol sent per request.
    pub fn with_max_request_payload_size_bytes(
        self,
        max_request_payload_size_bytes: Option<usize>,
    ) -> Self {
        Self {
            max_request_payload_size_bytes,
            ..self
        }
    }

    /// The client makes this many concurrent uploads at a
    /// time. Defaults to 1.
    pub fn with_max_concurrent_uploads(self, max_concurrent_uploads: NonZeroUsize) -> Self {
        Self {
            max_concurrent_uploads,
            ..self
        }
    }

    /// Write the [LineProtocol] formatted string in `lp_data` to
    /// namespace `namespace`.
    ///
    /// Returns the number of bytes which were written to the namespace.
    ///
    /// [LineProtocol]: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#data-types-and-format
    pub async fn write_lp(
        &mut self,
        namespace: impl AsRef<str> + Send,
        lp_data: impl Into<String> + Send,
    ) -> Result<usize, Error> {
        let sources = futures_util::stream::iter([lp_data.into()]);

        self.write_lp_stream(namespace, sources).await
    }

    /// Write the stream of [LineProtocol] formatted strings in
    /// `sources` to namespace `namespace`. It is assumed that
    /// individual lines (points) do not cross these strings
    ///
    /// Returns the number of bytes, in total, which were written to
    /// the namespace.
    ///
    /// [LineProtocol]: https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#data-types-and-format
    pub async fn write_lp_stream(
        &mut self,
        namespace: impl AsRef<str> + Send,
        sources: impl Stream<Item = String> + Send,
    ) -> Result<usize, Error> {
        let (org_id, bucket_id) = split_namespace(namespace.as_ref()).map_err(|e| {
            Error::invalid_argument(
                "namespace",
                format!("Could not find valid org_id and bucket_id: {e}"),
            )
        })?;

        let max_concurrent_uploads: usize = self.max_concurrent_uploads.into();
        let max_request_payload_size_bytes = self.max_request_payload_size_bytes;

        // make a stream and process in parallel
        let results = sources
            // split each input source in parallel, if possible
            .flat_map(|source| {
                split_lp(
                    source,
                    max_request_payload_size_bytes,
                    max_concurrent_uploads,
                )
            })
            // do the actual write
            .map(|source| {
                let org_id = org_id.to_string();
                let bucket_id = bucket_id.to_string();
                let inner = Arc::clone(&self.inner);

                tokio::task::spawn(
                    async move { inner.write_source(org_id, bucket_id, source).await },
                )
            })
            // Do the uploads in parallel
            .buffered(max_concurrent_uploads)
            .try_collect::<Vec<_>>()
            // handle panics in tasks
            .await
            .map_err(Error::client)?
            // find / return any errors
            .into_iter()
            .collect::<Result<Vec<_>, Error>>()?;

        Ok(results.into_iter().sum())
    }
}

/// Something that knows how to send http data. Exists so it can be
/// mocked out for testing
trait RequestMaker: Debug + Send + Sync {
    /// Write the body data to the specified org, bucket, and
    /// returning the number of bytes written
    ///
    /// (this is implemented manually to avoid `async_trait`)
    fn write_source(
        &self,
        org_id: String,
        bucket_id: String,
        body: String,
    ) -> BoxFuture<'_, Result<usize, Error>>;
}

impl RequestMaker for HttpConnection {
    fn write_source(
        &self,
        org_id: String,
        bucket_id: String,
        body: String,
    ) -> BoxFuture<'_, Result<usize, Error>> {
        let write_url = format!("{}api/v2/write", self.uri());

        async move {
            let body: Body = body.into();

            let data_len = body.as_bytes().map(|b| b.len()).unwrap_or(0);

            let response = self
                .client()
                .request(Method::POST, &write_url)
                .query(&[("bucket", bucket_id), ("org", org_id)])
                .body(body)
                .send()
                .await
                .map_err(Error::client)?;

            translate_response(response).await?;

            Ok(data_len)
        }
        .boxed()
    }
}

/// splits input line protocol into one or more sizes of at most
/// `max_chunk` on line breaks in a separte tokio task
fn split_lp(
    input: String,
    max_chunk_size: Option<usize>,
    max_concurrent_uploads: usize,
) -> impl Stream<Item = String> {
    let (tx, rx) = tokio::sync::mpsc::channel(max_concurrent_uploads);

    tokio::task::spawn(async move {
        match max_chunk_size {
            None => {
                // ignore errors (means the receiver hung up but nothing to communicate
                tx.send(input).await.ok();
            }
            Some(max_chunk_size) => {
                // use the actual line protocol parser to split on valid boundaries
                let mut acc = LineAccumulator::new(max_chunk_size);
                for l in influxdb_line_protocol::split_lines(&input) {
                    if let Some(chunk) = acc.push(l) {
                        // abort if receiver has hungup
                        if tx.send(chunk).await.is_err() {
                            return;
                        }
                    }
                }
                if let Some(chunk) = acc.flush() {
                    tx.send(chunk).await.ok();
                }
            }
        }
    });

    tokio_stream::wrappers::ReceiverStream::new(rx)
}
#[derive(Debug)]
struct LineAccumulator {
    current_chunk: String,
    max_chunk_size: usize,
}

impl LineAccumulator {
    fn new(max_chunk_size: usize) -> Self {
        Self {
            current_chunk: String::with_capacity(max_chunk_size),
            max_chunk_size,
        }
    }

    // Add data `l` to the current chunk being created, returning the
    // current chunk if complete.
    fn push(&mut self, l: &str) -> Option<String> {
        let chunk = if self.current_chunk.len() + l.len() + 1 > self.max_chunk_size {
            self.flush()
        } else {
            None
        };

        if !self.current_chunk.is_empty() {
            self.current_chunk += "\n";
        }

        self.current_chunk += l;
        chunk
    }

    /// allocate a new chunk with the right size, returning the currently built chunk if it has non zero length
    /// `self.current_chunk.len()` is zero
    fn flush(&mut self) -> Option<String> {
        if !self.current_chunk.is_empty() {
            let mut new_chunk = String::with_capacity(self.max_chunk_size);
            std::mem::swap(&mut new_chunk, &mut self.current_chunk);
            Some(new_chunk)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[tokio::test]
    async fn test() {
        let mock = Arc::new(MockRequestMaker::new());

        let namespace = "orgname_bucketname";
        let data = "m,t=foo f=4";

        let expected = vec![MockRequest {
            org_id: "orgname".into(),
            bucket_id: "bucketname".into(),
            body: data.into(),
        }];

        let num_bytes = Client::new_with_maker(Arc::clone(&mock) as _)
            .write_lp(namespace, data)
            .await
            .unwrap();
        assert_eq!(expected, mock.requests());
        assert_eq!(num_bytes, 11);
    }

    #[tokio::test]
    async fn test_max_request_payload_size() {
        let mock = Arc::new(MockRequestMaker::new());

        let namespace = "orgname_bucketname";
        let data = "m,t=foo f=4\n\
                    m,t=bar f=3\n\
                    m,t=fooddddddd f=4";

        // expect the data to be broken up into two chunks:
        let expected = vec![
            MockRequest {
                org_id: "orgname".into(),
                bucket_id: "bucketname".into(),
                body: "m,t=foo f=4\nm,t=bar f=3".into(),
            },
            MockRequest {
                org_id: "orgname".into(),
                bucket_id: "bucketname".into(),
                body: "m,t=fooddddddd f=4".into(),
            },
        ];

        let num_bytes = Client::new_with_maker(Arc::clone(&mock) as _)
            // enough to get first two lines, but not last
            .with_max_request_payload_size_bytes(Some(30))
            .write_lp(namespace, data)
            .await
            .unwrap();
        assert_eq!(expected, mock.requests());
        assert_eq!(num_bytes, 41);
    }

    #[tokio::test]
    async fn test_write_lp_stream() {
        let mock = Arc::new(MockRequestMaker::new());

        let namespace = "orgname_bucketname";
        let data = futures_util::stream::iter(
            vec!["m,t=foo f=4", "m,t=bar f=3"]
                .into_iter()
                .map(|s| s.to_string()),
        );

        // expect the data to come in two chunks
        let expected = vec![
            MockRequest {
                org_id: "orgname".into(),
                bucket_id: "bucketname".into(),
                body: "m,t=foo f=4".into(),
            },
            MockRequest {
                org_id: "orgname".into(),
                bucket_id: "bucketname".into(),
                body: "m,t=bar f=3".into(),
            },
        ];

        let num_bytes = Client::new_with_maker(Arc::clone(&mock) as _)
            .write_lp_stream(namespace, data)
            .await
            .unwrap();
        assert_eq!(expected, mock.requests());
        assert_eq!(num_bytes, 22);
    }

    #[derive(Debug, Clone, PartialEq)]
    struct MockRequest {
        org_id: String,
        bucket_id: String,
        body: String,
    }

    #[derive(Debug)]
    struct MockRequestMaker {
        requests: Mutex<Vec<MockRequest>>,
    }

    impl MockRequestMaker {
        fn new() -> Self {
            Self {
                requests: Mutex::new(vec![]),
            }
        }

        /// get a copy of the requests that were made using this mock
        fn requests(&self) -> Vec<MockRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl RequestMaker for MockRequestMaker {
        fn write_source(
            &self,
            org_id: String,
            bucket_id: String,
            body: String,
        ) -> BoxFuture<'_, Result<usize, Error>> {
            let sz = body.len();

            self.requests.lock().unwrap().push(MockRequest {
                org_id,
                bucket_id,
                body,
            });

            async move { Ok(sz) }.boxed()
        }
    }
}
