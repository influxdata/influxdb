use std::{collections::HashMap, fmt::Display, string::FromUtf8Error};

use bytes::Bytes;
use iox_query_params::StatementParam;
use reqwest::{Body, IntoUrl, StatusCode};
use secrecy::{ExposeSecret, Secret};
use serde::Serialize;
use url::Url;

/// Primary error type for the [`Client`]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("base URL error: {0}")]
    BaseUrl(#[source] reqwest::Error),

    #[error("request URL error: {0}")]
    RequestUrl(#[from] url::ParseError),

    #[error("failed to send /api/v3/write_lp request: {0}")]
    WriteLpSend(#[source] reqwest::Error),

    #[error("failed to read the API response bytes: {0}")]
    Bytes(#[source] reqwest::Error),

    #[error("failed to send /api/v3/query_{kind} request: {source}")]
    QuerySend {
        kind: QueryKind,
        #[source]
        source: reqwest::Error,
    },

    #[error(
        "provided parameter ('{name}') could not be converted \
        to a statment parameter"
    )]
    ConvertQueryParam {
        name: String,
        #[source]
        source: iox_query_params::Error,
    },

    #[error("invalid UTF8 in response: {0}")]
    InvalidUtf8(#[from] FromUtf8Error),

    #[error("server responded with error [{code}]: {message}")]
    ApiError { code: StatusCode, message: String },
}

pub type Result<T> = std::result::Result<T, Error>;

/// The InfluxDB 3.0 Client
///
/// For programmatic access to the HTTP API of InfluxDB 3.0
#[derive(Debug, Clone)]
pub struct Client {
    /// The base URL for making requests to a running InfluxDB 3.0 server
    base_url: Url,
    /// The `Bearer` token to use for authenticating on each request to the server
    auth_token: Option<Secret<String>>,
    /// A [`reqwest::Client`] for handling HTTP requests
    http_client: reqwest::Client,
}

impl Client {
    /// Create a new [`Client`]
    pub fn new<U: IntoUrl>(base_url: U) -> Result<Self> {
        Ok(Self {
            base_url: base_url.into_url().map_err(Error::BaseUrl)?,
            auth_token: None,
            http_client: reqwest::Client::new(),
        })
    }

    /// Set the `Bearer` token that will be sent with each request to the server
    ///
    /// # Example
    /// ```
    /// # use influxdb3_client::Client;
    /// # use influxdb3_client::Precision;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let token = "secret-token-string";
    /// let client = Client::new("http://localhost:8181")?
    ///     .with_auth_token(token);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_auth_token<S: Into<String>>(mut self, auth_token: S) -> Self {
        self.auth_token = Some(Secret::new(auth_token.into()));
        self
    }

    /// Compose a request to the `/api/v3/write_lp` API
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # use influxdb3_client::Precision;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let client = Client::new("http://localhost:8181")?;
    /// client
    ///     .api_v3_write_lp("db_name")
    ///     .precision(Precision::Millisecond)
    ///     .accept_partial(true)
    ///     .body("cpu,host=s1 usage=0.5")
    ///     .send()
    ///     .await
    ///     .expect("send write_lp request");
    /// # Ok(())
    /// # }
    /// ```
    pub fn api_v3_write_lp<S: Into<String>>(&self, db: S) -> WriteRequestBuilder<'_, NoBody> {
        WriteRequestBuilder {
            client: self,
            db: db.into(),
            precision: None,
            accept_partial: None,
            body: NoBody,
        }
    }

    /// Compose a request to the `/api/v3/query_sql` API
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let client = Client::new("http://localhost:8181")?;
    /// let response_bytes = client
    ///     .api_v3_query_sql("db_name", "SELECT * FROM foo")
    ///     .send()
    ///     .await
    ///     .expect("send query_sql request");
    /// # Ok(())
    /// # }
    /// ```
    pub fn api_v3_query_sql<D: Into<String>, Q: Into<String>>(
        &self,
        db: D,
        query: Q,
    ) -> QueryRequestBuilder<'_> {
        QueryRequestBuilder {
            client: self,
            kind: QueryKind::Sql,
            db: db.into(),
            query: query.into(),
            format: None,
            params: None,
        }
    }

    /// Compose a request to the `/api/v3/query_influxql` API
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let client = Client::new("http://localhost:8181")?;
    /// let response_bytes = client
    ///     .api_v3_query_influxql("db_name", "SELECT * FROM foo")
    ///     .send()
    ///     .await
    ///     .expect("send query_influxql request");
    /// # Ok(())
    /// # }
    /// ```
    pub fn api_v3_query_influxql<D: Into<String>, Q: Into<String>>(
        &self,
        db: D,
        query: Q,
    ) -> QueryRequestBuilder<'_> {
        QueryRequestBuilder {
            client: self,
            kind: QueryKind::InfluxQl,
            db: db.into(),
            query: query.into(),
            format: None,
            params: None,
        }
    }
}

/// The URL parameters of the request to the `/api/v3/write_lp` API
// TODO - this should re-use a type defined in the server code, or a separate crate,
//        central to both.
#[derive(Debug, Serialize)]
struct WriteParams<'a> {
    db: &'a str,
    precision: Option<Precision>,
    accept_partial: Option<bool>,
}

impl<'a, B> From<&'a WriteRequestBuilder<'a, B>> for WriteParams<'a> {
    fn from(builder: &'a WriteRequestBuilder<'a, B>) -> Self {
        Self {
            db: &builder.db,
            precision: builder.precision,
            accept_partial: builder.accept_partial,
        }
    }
}

/// Time series precision
// TODO - this should re-use a type defined in the server code, or a separate crate,
//        central to both.
#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Precision {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

/// Builder type for composing a request to `/api/v3/write_lp`
///
/// Produced by [`Client::api_v3_write_lp`]
#[derive(Debug)]
pub struct WriteRequestBuilder<'c, B> {
    client: &'c Client,
    db: String,
    precision: Option<Precision>,
    accept_partial: Option<bool>,
    body: B,
}

impl<'c, B> WriteRequestBuilder<'c, B> {
    /// Set the precision
    pub fn precision(mut self, set_to: Precision) -> Self {
        self.precision = Some(set_to);
        self
    }

    /// Set the `accept_partial` parameter
    pub fn accept_partial(mut self, set_to: bool) -> Self {
        self.accept_partial = Some(set_to);
        self
    }
}

impl<'c> WriteRequestBuilder<'c, NoBody> {
    /// Set the body of the request to the `/api/v3/write_lp` API
    ///
    /// This essentially wraps `reqwest`'s [`body`][reqwest::RequestBuilder::body]
    /// method, and puts the responsibility on the caller for now.
    pub fn body<T: Into<Body>>(self, body: T) -> WriteRequestBuilder<'c, Body> {
        WriteRequestBuilder {
            client: self.client,
            db: self.db,
            precision: self.precision,
            accept_partial: self.accept_partial,
            body: body.into(),
        }
    }
}

impl<'c> WriteRequestBuilder<'c, Body> {
    /// Send the request to the server
    pub async fn send(self) -> Result<()> {
        let url = self.client.base_url.join("/api/v3/write_lp")?;
        let params = WriteParams::from(&self);
        let mut req = self.client.http_client.post(url).query(&params);
        if let Some(token) = &self.client.auth_token {
            req = req.bearer_auth(token.expose_secret());
        }
        let resp = req
            .body(self.body)
            .send()
            .await
            .map_err(Error::WriteLpSend)?;
        let status = resp.status();
        let content = resp.bytes().await.map_err(Error::Bytes)?;
        match status {
            // TODO - handle the OK response content, return to caller, etc.
            StatusCode::OK => Ok(()),
            code => Err(Error::ApiError {
                code,
                message: String::from_utf8(content.to_vec())?,
            }),
        }
    }
}

#[doc(hidden)]
/// Typestate type for [`WriteRequestBuilder`]
#[derive(Debug, Copy, Clone)]
pub struct NoBody;

/// Used to compose a request to the `/api/v3/query_sql` API
///
/// Produced by [`Client::api_v3_query_sql`] method.
#[derive(Debug)]
pub struct QueryRequestBuilder<'c> {
    client: &'c Client,
    kind: QueryKind,
    db: String,
    query: String,
    format: Option<Format>,
    params: Option<HashMap<String, StatementParam>>,
}

// TODO - for now the send method just returns the bytes from the response.
//   It may be nicer to have the format parameter dictate how we return from
//   send, e.g., using types more specific to the format selected.
impl<'c> QueryRequestBuilder<'c> {
    /// Specify the format, `json`, `csv`, `pretty`, or `parquet`
    pub fn format(mut self, format: Format) -> Self {
        self.format = Some(format);
        self
    }

    /// Set a query parameter value with the given `name`
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let client = Client::new("http://localhost:8181")?;
    /// let response_bytes = client
    ///     .api_v3_query_sql("db_name", "SELECT * FROM foo WHERE bar = $bar AND baz > $baz")
    ///     .with_param("bar", "bop")
    ///     .with_param("baz", 0.5)
    ///     .send()
    ///     .await
    ///     .expect("send query_sql request");
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_param<S: Into<String>, P: Into<StatementParam>>(
        mut self,
        name: S,
        param: P,
    ) -> Self {
        self.params
            .get_or_insert_with(Default::default)
            .insert(name.into(), param.into());
        self
    }

    /// Try to set a query parameter value with the given `name`
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use serde_json::json;
    ///
    /// let client = Client::new("http://localhost:8181")?;
    /// let response_bytes = client
    ///     .api_v3_query_sql("db_name", "SELECT * FROM foo WHERE bar = $bar AND baz > $baz")
    ///     .with_try_param("bar", json!("baz"))?
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_try_param<S, P>(mut self, name: S, param: P) -> Result<Self>
    where
        S: Into<String> + Clone,
        P: TryInto<StatementParam, Error = iox_query_params::Error>,
    {
        let name = name.into();
        let param = param
            .try_into()
            .map_err(|source| Error::ConvertQueryParam {
                name: name.clone(),
                source,
            })?;
        self.params
            .get_or_insert_with(Default::default)
            .insert(name, param);
        Ok(self)
    }

    /// Send the request to `/api/v3/query_sql` or `/api/v3/query_influxql`
    pub async fn send(self) -> Result<Bytes> {
        let url = match self.kind {
            QueryKind::Sql => self.client.base_url.join("/api/v3/query_sql")?,
            QueryKind::InfluxQl => self.client.base_url.join("/api/v3/query_influxql")?,
        };
        let params = QueryParams::from(&self);
        let mut req = self.client.http_client.post(url).json(&params);
        if let Some(token) = &self.client.auth_token {
            req = req.bearer_auth(token.expose_secret());
        }
        let resp = req.send().await.map_err(|source| Error::QuerySend {
            kind: self.kind,
            source,
        })?;
        let status = resp.status();
        let content = resp.bytes().await.map_err(Error::Bytes)?;

        match status {
            StatusCode::OK => Ok(content),
            code => Err(Error::ApiError {
                code,
                message: String::from_utf8(content.to_vec()).map_err(Error::InvalidUtf8)?,
            }),
        }
    }
}

/// Query parameters for the `/api/v3/query_sql` API
#[derive(Debug, Serialize)]
pub struct QueryParams<'a> {
    db: &'a str,
    #[serde(rename = "q")]
    query: &'a str,
    format: Option<Format>,
    params: Option<&'a HashMap<String, StatementParam>>,
}

impl<'a> From<&'a QueryRequestBuilder<'a>> for QueryParams<'a> {
    fn from(builder: &'a QueryRequestBuilder<'a>) -> Self {
        Self {
            db: &builder.db,
            query: &builder.query,
            format: builder.format,
            params: builder.params.as_ref(),
        }
    }
}

/// The type of query, SQL or InfluxQL
#[derive(Debug, Copy, Clone)]
pub enum QueryKind {
    Sql,
    InfluxQl,
}

impl Display for QueryKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryKind::Sql => write!(f, "sql"),
            QueryKind::InfluxQl => write!(f, "influxql"),
        }
    }
}

/// Output format to request from the server in the `/api/v3/query_sql` API
#[derive(Debug, Serialize, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    Json,
    Csv,
    Parquet,
    Pretty,
}

#[cfg(test)]
mod tests {
    use mockito::{Matcher, Server};

    use crate::{Client, Format, Precision};

    #[tokio::test]
    async fn api_v3_write_lp() {
        let token = "super-secret-token";
        let db = "stats";
        let body = "\
            cpu,host=s1 usage=0.5
            cpu,host=s1,region=us-west usage=0.7";

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/write_lp")
            .match_header("Authorization", format!("Bearer {token}").as_str())
            .match_query(Matcher::AllOf(vec![
                Matcher::UrlEncoded("precision".into(), "millisecond".into()),
                Matcher::UrlEncoded("db".into(), db.into()),
                Matcher::UrlEncoded("accept_partial".into(), "true".into()),
            ]))
            .match_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url())
            .expect("create client")
            .with_auth_token(token);

        client
            .api_v3_write_lp(db)
            .precision(Precision::Millisecond)
            .accept_partial(true)
            .body(body)
            .send()
            .await
            .expect("send write_lp request");

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn api_v3_query_sql() {
        let token = "super-secret-token";
        let db = "stats";
        let query = "SELECT * FROM foo";
        let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/query_sql")
            .match_header("Authorization", format!("Bearer {token}").as_str())
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "q": query,
                "format": "json",
                "params": null,
            })))
            .with_status(200)
            // TODO - could add content-type header but that may be too brittle
            //        at the moment
            //      - this will be JSON Lines at some point
            .with_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url())
            .expect("create client")
            .with_auth_token(token);

        let r = client
            .api_v3_query_sql(db, query)
            .format(Format::Json)
            .send()
            .await
            .expect("send request to server");

        assert_eq!(&r, body);

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn api_v3_query_sql_params() {
        let db = "stats";
        let query = "SELECT * FROM foo WHERE bar = $bar";
        let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/query_sql")
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "q": query,
                "params": {
                    "bar": "baz",
                    "baz": false,
                },
                "format": null
            })))
            .with_status(200)
            .with_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url()).expect("create client");

        let r = client
            .api_v3_query_sql(db, query)
            .with_param("bar", "baz")
            .with_param("baz", false)
            .send()
            .await;

        mock.assert_async().await;

        r.expect("sent request successfully");
    }

    #[tokio::test]
    async fn api_v3_query_influxql() {
        let db = "stats";
        let query = "SELECT * FROM foo";
        let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/query_influxql")
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "q": query,
                "format": "json",
                "params": null,
            })))
            .with_status(200)
            // TODO - could add content-type header but that may be too brittle
            //        at the moment
            //      - this will be JSON Lines at some point
            .with_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url()).expect("create client");

        let r = client
            .api_v3_query_influxql(db, query)
            .format(Format::Json)
            .send()
            .await
            .expect("send request to server");

        assert_eq!(&r, body);

        mock.assert_async().await;
    }
}
