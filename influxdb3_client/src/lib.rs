use bytes::Bytes;
use hashbrown::HashMap;
use influxdb3_catalog::log::{OrderedCatalogBatch, TriggerSettings};
use iox_query_params::StatementParam;
use reqwest::{
    Body, Certificate, IntoUrl, Method, StatusCode,
    header::{CONTENT_TYPE, HeaderMap, HeaderValue},
    tls::Version,
};
use secrecy::{ExposeSecret, Secret};
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt::Display, num::NonZeroUsize, path::PathBuf, string::FromUtf8Error, time::Duration};
use url::Url;

use influxdb3_types::http::*;
pub use influxdb3_types::write::Precision;

/// Primary error type for the [`Client`]
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("base URL error: {0}")]
    BaseUrl(#[source] reqwest::Error),

    #[error("request URL error: {0}")]
    RequestUrl(#[from] url::ParseError),

    #[error("failed to read the API response bytes: {0}")]
    Bytes(#[source] reqwest::Error),

    #[error("failed to serialize the request body: {0}")]
    RequestSerialization(#[source] serde_json::Error),

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

    #[error("failed to parse JSON response: {0}")]
    Json(#[source] reqwest::Error),

    #[error("failed to parse plaintext response: {0}")]
    Text(#[source] reqwest::Error),

    #[error("server responded with error [{code}]: {message}")]
    ApiError { code: StatusCode, message: String },

    #[error("failed to send {method} {url} request: {source}")]
    RequestSend {
        method: Method,
        url: String,
        #[source]
        source: reqwest::Error,
    },

    #[error("failed to build an http client: {0}")]
    Builder(#[source] reqwest::Error),

    #[error("io error: {0}")]
    IO(#[from] std::io::Error),
}

impl Error {
    fn request_send(method: Method, url: impl Into<String>, source: reqwest::Error) -> Self {
        Self::RequestSend {
            method,
            url: url.into(),
            source,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// The InfluxDB 3 Core Client
///
/// For programmatic access to the HTTP API of InfluxDB 3 Core
#[derive(Debug, Clone)]
pub struct Client {
    /// The base URL for making requests to a running InfluxDB 3 Core server
    base_url: Url,
    /// The `Bearer` token to use for authenticating on each request to the server
    auth_token: Option<Secret<String>>,
    /// A [`reqwest::Client`] for handling HTTP requests
    http_client: reqwest::Client,
}

impl Client {
    /// Create a new [`Client`]
    pub fn new<U: IntoUrl>(base_url: U, ca_cert: Option<PathBuf>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .min_tls_version(Version::TLS_1_3)
            .use_rustls_tls();

        let http_client = if let Some(ca_cert) = ca_cert {
            let cert = std::fs::read(&ca_cert)?;
            let cert = match ca_cert.extension().and_then(|s| s.to_str()) {
                Some("der") => Certificate::from_der(&cert),
                Some("pem") | Some(_) | None => Certificate::from_pem(&cert),
            }
            .map_err(Error::Builder)?;
            client
                .add_root_certificate(cert)
                .build()
                .map_err(Error::Builder)?
        } else {
            client.build().map_err(Error::Builder)?
        };

        Ok(Self {
            base_url: base_url.into_url().map_err(Error::BaseUrl)?,
            auth_token: None,
            http_client,
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
            params: WriteParams {
                db: db.into(),
                precision: None,
                accept_partial: None,
                no_sync: None,
            },
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
            request: ClientQueryRequest {
                database: db.into(),
                query_str: query.into(),
                format: None,
                params: None,
            },
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
            request: ClientQueryRequest {
                database: db.into(),
                query_str: query.into(),
                format: None,
                params: None,
            },
        }
    }

    /// Compose a request to the `POST /api/v3/configure/last_cache` API
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let client = Client::new("http://localhost:8181")?;
    /// let resp = client
    ///     .api_v3_configure_last_cache_create("db_name", "table_name")
    ///     .ttl(120)
    ///     .name("cache_name")
    ///     .count(5)
    ///     .key_columns(["col1", "col2"])
    ///     .send()
    ///     .await
    ///     .expect("send create last cache request");
    /// # Ok(())
    /// # }
    /// ```
    pub fn api_v3_configure_last_cache_create(
        &self,
        db: impl Into<String>,
        table: impl Into<String>,
    ) -> CreateLastCacheRequestBuilder<'_> {
        CreateLastCacheRequestBuilder::new(self, db, table)
    }

    /// Make a request to the `DELETE /api/v3/configure/last_cache` API
    pub async fn api_v3_configure_last_cache_delete(
        &self,
        db: impl Into<String> + Send,
        table: impl Into<String> + Send,
        name: impl Into<String> + Send,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::DELETE,
                "/api/v3/configure/last_cache",
                Some(LastCacheDeleteRequest {
                    db: db.into(),
                    table: table.into(),
                    name: name.into(),
                }),
                None::<()>,
                None,
            )
            .await?;
        Ok(())
    }

    /// Compose a request to the `POST /api/v3/configure/distinct_cache` API
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # use std::num::NonZeroUsize;
    /// # use std::time::Duration;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// let client = Client::new("http://localhost:8181")?;
    /// let resp = client
    ///     .api_v3_configure_distinct_cache_create("db_name", "table_name", ["col1", "col2"])
    ///     .name("cache_name")
    ///     .max_cardinality(NonZeroUsize::new(1_000).unwrap())
    ///     .max_age(Duration::from_secs(3_600))
    ///     .send()
    ///     .await
    ///     .expect("send create distinct cache request");
    /// # Ok(())
    /// # }
    /// ```
    pub fn api_v3_configure_distinct_cache_create(
        &self,
        db: impl Into<String>,
        table: impl Into<String>,
        columns: impl IntoIterator<Item: Into<String>>,
    ) -> CreateDistinctCacheRequestBuilder<'_> {
        CreateDistinctCacheRequestBuilder::new(self, db, table, columns)
    }

    /// Make a request to the `DELETE /api/v3/configure/distinct_cache` API
    pub async fn api_v3_configure_distinct_cache_delete(
        &self,
        db: impl Into<String> + Send,
        table: impl Into<String> + Send,
        name: impl Into<String> + Send,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::DELETE,
                "/api/v3/configure/distinct_cache",
                Some(DistinctCacheDeleteRequest {
                    db: db.into(),
                    table: table.into(),
                    name: name.into(),
                }),
                None::<()>,
                None,
            )
            .await?;
        Ok(())
    }

    /// Compose a request to the `GET /api/v3/configure/database` API
    pub fn api_v3_configure_db_show(&self) -> ShowDatabasesRequestBuilder<'_> {
        ShowDatabasesRequestBuilder {
            client: self,
            request: ShowDatabasesRequest {
                show_deleted: false,
                format: QueryFormat::Json,
            },
        }
    }

    /// Make a request to the `POST /api/v3/configure/database` API
    pub async fn api_v3_configure_db_create(
        &self,
        db: impl Into<String> + Send,
        retention_period: Option<Duration>,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::POST,
                "/api/v3/configure/database",
                Some(CreateDatabaseRequest {
                    db: db.into(),
                    retention_period,
                }),
                None::<()>,
                None,
            )
            .await?;
        Ok(())
    }

    /// Make a request to the `PUT /api/v3/configure/database` API
    pub async fn api_v3_configure_db_update(
        &self,
        db: impl Into<String> + Send,
        retention_period: Option<Duration>,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::PUT,
                "/api/v3/configure/database",
                Some(UpdateDatabaseRequest {
                    db: db.into(),
                    retention_period,
                }),
                None::<()>,
                None,
            )
            .await?;
        Ok(())
    }

    /// Make a request to the `DELETE /api/v3/configure/database?db=foo` API
    pub async fn api_v3_configure_db_delete(&self, db: impl AsRef<str> + Send) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::DELETE,
                "/api/v3/configure/database",
                None::<()>,
                Some(DeleteDatabaseRequest {
                    db: db.as_ref().to_string(),
                    hard_delete_at: None,
                }),
                None,
            )
            .await?;
        Ok(())
    }

    /// Make a request to the `DELETE /api/v3/configure/database` API with hard delete option
    pub async fn api_v3_configure_db_delete_with_hard_delete(
        &self,
        db: impl AsRef<str> + Send,
        hard_delete_at: Option<HardDeletionTime>,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::DELETE,
                "/api/v3/configure/database",
                None::<()>,
                Some(DeleteDatabaseRequest {
                    db: db.as_ref().to_string(),
                    hard_delete_at,
                }),
                None,
            )
            .await?;
        Ok(())
    }

    /// Make a request to the `DELETE /api/v3/configure/table?db=foo&table=bar` API
    pub async fn api_v3_configure_table_delete<T: AsRef<str> + Send>(
        &self,
        db: T,
        table: T,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::DELETE,
                "/api/v3/configure/table",
                None::<()>,
                Some(DeleteTableRequest {
                    db: db.as_ref().to_string(),
                    table: table.as_ref().to_string(),
                    hard_delete_at: None,
                }),
                None,
            )
            .await?;
        Ok(())
    }

    /// Make a request to the `DELETE /api/v3/configure/table` API with hard delete option
    pub async fn api_v3_configure_table_delete_with_hard_delete<T: AsRef<str> + Send>(
        &self,
        db: T,
        table: T,
        hard_delete_at: Option<HardDeletionTime>,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::DELETE,
                "/api/v3/configure/table",
                None::<()>,
                Some(DeleteTableRequest {
                    db: db.as_ref().to_string(),
                    table: table.as_ref().to_string(),
                    hard_delete_at,
                }),
                None,
            )
            .await?;
        Ok(())
    }

    /// Make a request to the `POST /api/v3/configure/table` API
    pub async fn api_v3_configure_table_create(
        &self,
        db: impl Into<String> + Send,
        table: impl Into<String> + Send,
        tags: Vec<impl Into<String> + Send>,
        fields: Vec<(impl Into<String> + Send, impl Into<FieldType> + Send)>,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::POST,
                "/api/v3/configure/table",
                Some(CreateTableRequest {
                    db: db.into(),
                    table: table.into(),
                    tags: tags.into_iter().map(Into::into).collect(),
                    fields: fields
                        .into_iter()
                        .map(|(name, r#type)| CreateTableField {
                            name: name.into(),
                            r#type: r#type.into(),
                        })
                        .collect(),
                }),
                None::<()>,
                None,
            )
            .await?;
        Ok(())
    }

    /// Make a request to the `POST /api/v3/configure/processing_engine_plugin` API
    pub async fn api_v3_configure_processing_engine_plugin_create(
        &self,
        db: impl Into<String> + Send,
        plugin_name: impl Into<String> + Send,
        file_name: impl Into<String> + Send,
        plugin_type: impl Into<String> + Send,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::POST,
                "/api/v3/configure/processing_engine_plugin",
                Some(ProcessingEnginePluginCreateRequest {
                    db: db.into(),
                    plugin_name: plugin_name.into(),
                    file_name: file_name.into(),
                    plugin_type: plugin_type.into(),
                }),
                None::<()>,
                None,
            )
            .await?;
        Ok(())
    }

    /// Make a request to the `DELETE /api/v3/configure/processing_engine_plugin` API
    pub async fn api_v3_configure_processing_engine_plugin_delete(
        &self,
        db: impl Into<String> + Send,
        plugin_name: impl Into<String> + Send,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::DELETE,
                "/api/v3/configure/processing_engine_plugin",
                Some(ProcessingEnginePluginDeleteRequest {
                    db: db.into(),
                    plugin_name: plugin_name.into(),
                }),
                None::<()>,
                None,
            )
            .await?;
        Ok(())
    }

    /// Make a request to `POST /api/v3/configure/processing_engine_trigger`
    #[allow(clippy::too_many_arguments)]
    pub async fn api_v3_configure_processing_engine_trigger_create(
        &self,
        db: impl Into<String> + Send,
        trigger_name: impl Into<String> + Send,
        plugin_filename: impl Into<String> + Send,
        trigger_spec: impl Into<String> + Send,
        trigger_arguments: Option<HashMap<String, String>>,
        disabled: bool,
        trigger_settings: TriggerSettings,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::POST,
                "/api/v3/configure/processing_engine_trigger",
                Some(ProcessingEngineTriggerCreateRequest {
                    db: db.into(),
                    trigger_name: trigger_name.into(),
                    plugin_filename: plugin_filename.into(),
                    trigger_specification: trigger_spec.into(),
                    trigger_settings,
                    trigger_arguments,
                    disabled,
                }),
                None::<()>,
                None,
            )
            .await?;
        Ok(())
    }

    /// Make a request to `DELETE /api/v3/configure/processing_engine_trigger`
    pub async fn api_v3_configure_processing_engine_trigger_delete(
        &self,
        db: impl Into<String> + Send,
        trigger_name: impl Into<String> + Send,
        force: bool,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::DELETE,
                "/api/v3/configure/processing_engine_trigger",
                Some(ProcessingEngineTriggerDeleteRequest {
                    db: db.into(),
                    trigger_name: trigger_name.into(),
                    force,
                }),
                None::<()>,
                None,
            )
            .await?;
        Ok(())
    }

    /// Make a request to `POST /api/v3/configure/processing_engine_trigger/enable`
    pub async fn api_v3_configure_processing_engine_trigger_enable(
        &self,
        db: impl Into<String> + Send,
        trigger_name: impl Into<String> + Send,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::POST,
                "/api/v3/configure/processing_engine_trigger/enable",
                None::<()>,
                Some(ProcessingEngineTriggerIdentifier {
                    db: db.into(),
                    trigger_name: trigger_name.into(),
                }),
                None,
            )
            .await?;
        Ok(())
    }

    /// Make a request to `POST /api/v3/configure/plugin_environment/install_packages`
    pub async fn api_v3_configure_plugin_environment_install_packages(
        &self,
        packages: Vec<String>,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::POST,
                "/api/v3/configure/plugin_environment/install_packages",
                Some(ProcessingEngineInstallPackagesRequest { packages }),
                None::<()>,
                None,
            )
            .await?;
        Ok(())
    }

    /// Make a request to `POST /api/v3/configure/plugin_environment/install_requirements`
    pub async fn api_v3_configure_processing_engine_trigger_install_requirements(
        &self,
        requirements_location: impl Into<String> + Send,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::POST,
                "/api/v3/configure/plugin_environment/install_requirements",
                Some(ProcessingEngineInstallRequirementsRequest {
                    requirements_location: requirements_location.into(),
                }),
                None::<()>,
                None,
            )
            .await?;
        Ok(())
    }

    /// Make a request to `POST /api/v3/configure/processing_engine_trigger/disable`
    pub async fn api_v3_configure_processing_engine_trigger_disable(
        &self,
        db: impl Into<String> + Send,
        trigger_name: impl Into<String> + Send,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::POST,
                "/api/v3/configure/processing_engine_trigger/disable",
                None::<()>,
                Some(ProcessingEngineTriggerIdentifier {
                    db: db.into(),
                    trigger_name: trigger_name.into(),
                }),
                None,
            )
            .await?;
        Ok(())
    }

    async fn save_plugin_file(
        &self,
        method: Method,
        db: impl Into<String> + Send,
        trigger_name: impl Into<String> + Send,
        content: impl Into<String> + Send,
    ) -> Result<()> {
        self.send_json_get_bytes(
            method,
            &format!("/api/v3/plugins/files?db={}", db.into()),
            Some(UpdatePluginFileRequest {
                plugin_name: trigger_name.into(),
                content: content.into(),
            }),
            None::<()>,
            None,
        )
        .await?;
        Ok(())
    }

    /// Create a plugin file
    pub async fn api_v3_create_plugin_file(
        &self,
        db: impl Into<String> + Send,
        trigger_name: impl Into<String> + Send,
        content: impl Into<String> + Send,
    ) -> Result<()> {
        self.save_plugin_file(Method::POST, db, trigger_name, content)
            .await
    }

    /// Update a plugin file
    pub async fn api_v3_update_plugin_file(
        &self,
        db: impl Into<String> + Send,
        trigger_name: impl Into<String> + Send,
        content: impl Into<String> + Send,
    ) -> Result<()> {
        self.save_plugin_file(Method::PUT, db, trigger_name, content)
            .await
    }

    /// Make a request to the `POST /api/v3/plugin_test/wal` API
    pub async fn wal_plugin_test(
        &self,
        wal_plugin_test_request: WalPluginTestRequest,
    ) -> Result<WalPluginTestResponse> {
        self.send_json(
            Method::POST,
            "/api/v3/plugin_test/wal",
            Some(wal_plugin_test_request),
            None::<()>,
        )
        .await
    }

    /// Make a request to the `POST /api/v3/plugin_test/schedule` API
    pub async fn schedule_plugin_test(
        &self,
        schedule_plugin_test_request: SchedulePluginTestRequest,
    ) -> Result<SchedulePluginTestResponse> {
        self.send_json(
            Method::POST,
            "/api/v3/plugin_test/schedule",
            Some(schedule_plugin_test_request),
            None::<()>,
        )
        .await
    }

    /// Send a `/ping` request to the target `influxdb3` server to check its
    /// status and gather `version` and `revision` information
    pub async fn ping(&self) -> Result<PingResponse> {
        let url = self.base_url.join("/ping")?;
        let mut req = self.http_client.get(url);
        if let Some(t) = &self.auth_token {
            req = req.bearer_auth(t.expose_secret());
        }
        let resp = req
            .send()
            .await
            .map_err(|src| Error::request_send(Method::GET, "/ping", src))?;
        if resp.status().is_success() {
            resp.json().await.map_err(Error::Json)
        } else {
            Err(Error::ApiError {
                code: resp.status(),
                message: resp.text().await.map_err(Error::Text)?,
            })
        }
    }

    /// Create an admin token
    pub async fn api_v3_configure_create_admin_token(
        &self,
    ) -> Result<Option<CreateTokenWithPermissionsResponse>> {
        let response_json: Result<Option<CreateTokenWithPermissionsResponse>> = self
            .send_create(
                Method::POST,
                "/api/v3/configure/token/admin",
                None::<()>,
                None::<()>,
            )
            .await;
        response_json
    }

    /// Create "named" admin tokens
    pub async fn api_v3_configure_create_named_admin_token(
        &self,
        token_name: impl Into<String> + Send,
        expiry_secs: Option<u64>,
    ) -> Result<Option<CreateTokenWithPermissionsResponse>> {
        let response_json: Result<Option<CreateTokenWithPermissionsResponse>> = self
            .send_create(
                Method::POST,
                "/api/v3/configure/token/named_admin",
                Some(CreateNamedAdminTokenRequest {
                    token_name: token_name.into(),
                    expiry_secs,
                }),
                None::<()>,
            )
            .await;
        response_json
    }

    /// regenerate admin token
    pub async fn api_v3_configure_regenerate_admin_token(
        &self,
    ) -> Result<Option<CreateTokenWithPermissionsResponse>> {
        let response_json: Result<Option<CreateTokenWithPermissionsResponse>> = self
            .send_create(
                Method::POST,
                "/api/v3/configure/token/admin/regenerate",
                None::<()>,
                None::<()>,
            )
            .await;
        response_json
    }

    /// Delete token `DELETE /api/v3/configure/token?token_name=foo` API
    pub async fn api_v3_configure_token_delete(
        &self,
        token_name: impl AsRef<str> + Send,
    ) -> Result<()> {
        let _bytes = self
            .send_json_get_bytes(
                Method::DELETE,
                "/api/v3/configure/token",
                None::<()>,
                Some(TokenDeleteRequest {
                    token_name: token_name.as_ref().to_owned(),
                }),
                None,
            )
            .await?;
        Ok(())
    }

    /// Serialize the given `B` to json then send the request and return the resulting bytes.
    async fn send_json_get_bytes<B, Q>(
        &self,
        method: Method,
        url_path: &str,
        body: Option<B>,
        query: Option<Q>,
        mut headers: Option<HeaderMap>,
    ) -> Result<Bytes>
    where
        B: Serialize + Send + Sync,
        Q: Serialize + Send + Sync,
    {
        let b = body
            .map(|body| serde_json::to_string(&body))
            .transpose()
            .map_err(Error::RequestSerialization)?
            .map(Into::into);
        let hs = headers.get_or_insert_default();
        hs.insert(
            CONTENT_TYPE,
            HeaderValue::from_str("application/json").unwrap(),
        );
        self.send_get_bytes(method, url_path, b, query, headers)
            .await
    }

    /// Send an HTTP request with the specified parameters, return the bytes read from the response
    /// body.
    async fn send_get_bytes<Q>(
        &self,
        method: Method,
        url_path: &str,
        body: Option<Body>,
        query: Option<Q>,
        headers: Option<HeaderMap>,
    ) -> Result<Bytes>
    where
        Q: Serialize + Send + Sync,
    {
        let url = self.base_url.join(url_path)?;
        let mut req = self.http_client.request(method.clone(), url.clone());
        if let Some(token) = &self.auth_token {
            req = req.bearer_auth(token.expose_secret());
        }
        if let Some(body) = body {
            req = req.body(body);
        }
        if let Some(query) = query {
            req = req.query(&query);
        }
        if let Some(headers) = headers {
            req = req.headers(headers);
        }
        let resp = req
            .send()
            .await
            .map_err(|src| Error::request_send(method, url, src))?;
        let status = resp.status();
        let content = resp.bytes().await.map_err(Error::Bytes)?;

        match status {
            s if s.is_success() => Ok(content),
            code => Err(Error::ApiError {
                code,
                message: String::from_utf8(content.to_vec()).map_err(Error::InvalidUtf8)?,
            }),
        }
    }

    /// Send an HTTP request and return `Some(O)` if the response status is HTTP 201 Created.
    async fn send_create<B, Q, O>(
        &self,
        method: Method,
        url_path: &str,
        body: Option<B>,
        query: Option<Q>,
    ) -> Result<Option<O>>
    where
        B: Serialize + Send + Sync,
        Q: Serialize + Send + Sync,
        O: DeserializeOwned + Send + Sync,
    {
        let url = self.base_url.join(url_path)?;
        let mut req = self.http_client.request(method.clone(), url.clone());
        if let Some(token) = &self.auth_token {
            req = req.bearer_auth(token.expose_secret());
        }
        if let Some(body) = body {
            req = req.json(&body);
        }
        if let Some(query) = query {
            req = req.query(&query);
        }
        let resp = req
            .send()
            .await
            .map_err(|src| Error::request_send(method, url, src))?;
        let status = resp.status();
        match status {
            StatusCode::CREATED => {
                let content = resp.json::<O>().await.map_err(Error::Json)?;
                Ok(Some(content))
            }
            StatusCode::NO_CONTENT => Ok(None),
            code => Err(Error::ApiError {
                code,
                message: resp.text().await.map_err(Error::Text)?,
            }),
        }
    }

    /// Send an HTTP request and return `O` on success.
    async fn send_json<B, Q, O>(
        &self,
        method: Method,
        url_path: &str,
        body: Option<B>,
        query: Option<Q>,
    ) -> Result<O>
    where
        B: Serialize + Send + Sync,
        Q: Serialize + Send + Sync,
        O: DeserializeOwned + Send + Sync,
    {
        let url = self.base_url.join(url_path)?;
        let mut req = self.http_client.request(method.clone(), url.clone());
        if let Some(token) = &self.auth_token {
            req = req.bearer_auth(token.expose_secret());
        }
        if let Some(body) = body {
            req = req.json(&body);
        }
        if let Some(query) = query {
            req = req.query(&query);
        }
        let resp = req
            .send()
            .await
            .map_err(|src| Error::request_send(method, url, src))?;
        let status = resp.status();
        if status.is_success() {
            resp.json().await.map_err(Error::Json)
        } else {
            Err(Error::ApiError {
                code: resp.status(),
                message: resp.text().await.map_err(Error::Text)?,
            })
        }
    }
}

/// Builder type for composing a request to `/api/v3/write_lp`
///
/// Produced by [`Client::api_v3_write_lp`]
#[derive(Debug)]
pub struct WriteRequestBuilder<'c, B> {
    client: &'c Client,
    params: WriteParams,
    body: B,
}

impl<B> WriteRequestBuilder<'_, B> {
    /// Set the precision
    pub fn precision(mut self, set_to: Precision) -> Self {
        self.params.precision = Some(set_to);
        self
    }

    /// Set the `accept_partial` parameter
    pub fn accept_partial(mut self, set_to: bool) -> Self {
        self.params.accept_partial = Some(set_to);
        self
    }

    /// Set the `no_sync` parameter
    pub fn no_sync(mut self, set_to: bool) -> Self {
        self.params.no_sync = Some(set_to);
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
            params: self.params,
            body: body.into(),
        }
    }
}

impl WriteRequestBuilder<'_, Body> {
    /// Send the request to the server
    pub async fn send(self) -> Result<()> {
        // ignore the returned value since we don't expect a response body
        let _bytes = self
            .client
            .send_get_bytes(
                Method::POST,
                "/api/v3/write_lp",
                Some(self.body),
                Some(self.params),
                None,
            )
            .await?;

        Ok(())
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
    request: ClientQueryRequest,
}

// TODO - for now the send method just returns the bytes from the response.
//   It may be nicer to have the format parameter dictate how we return from
//   send, e.g., using types more specific to the format selected.
impl QueryRequestBuilder<'_> {
    /// Specify the format, `json`, `csv`, `pretty`, or `parquet`
    pub fn format(mut self, format: QueryFormat) -> Self {
        self.request.format = Some(format);
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
        self.request
            .params
            .get_or_insert_with(Default::default)
            .insert(name.into(), param.into());
        self
    }

    /// Set a query parameters from the given collection of pairs
    ///
    /// # Example
    /// ```no_run
    /// # use influxdb3_client::Client;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    /// use serde_json::json;
    /// use std::collections::HashMap;
    ///
    /// let client = Client::new("http://localhost:8181")?;
    /// let response_bytes = client
    ///     .api_v3_query_sql("db_name", "SELECT * FROM foo WHERE bar = $bar AND foo > $foo")
    ///     .with_params_from([
    ///         ("bar", json!(false)),
    ///         ("foo", json!(10)),
    ///     ])?
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_params_from<S, P, C>(mut self, params: C) -> Result<Self>
    where
        S: Into<String>,
        P: TryInto<StatementParam, Error = iox_query_params::Error>,
        C: IntoIterator<Item = (S, P)>,
    {
        for (name, param) in params.into_iter() {
            let name = name.into();
            let param = param
                .try_into()
                .map_err(|source| Error::ConvertQueryParam {
                    name: name.clone(),
                    source,
                })?;

            self.request
                .params
                .get_or_insert_with(Default::default)
                .insert(name, param);
        }
        Ok(self)
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
        S: Into<String>,
        P: TryInto<StatementParam, Error = iox_query_params::Error>,
    {
        let name = name.into();
        let param = param
            .try_into()
            .map_err(|source| Error::ConvertQueryParam {
                name: name.clone(),
                source,
            })?;
        self.request
            .params
            .get_or_insert_with(Default::default)
            .insert(name, param);
        Ok(self)
    }

    /// Send the request to `/api/v3/query_sql` or `/api/v3/query_influxql`
    pub async fn send(self) -> Result<Bytes> {
        let url = match self.kind {
            QueryKind::Sql => "/api/v3/query_sql",
            QueryKind::InfluxQl => "/api/v3/query_influxql",
        };
        self.client
            .send_json_get_bytes(Method::POST, url, Some(self.request), None::<()>, None)
            .await
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

#[derive(Debug)]
pub struct ShowDatabasesRequestBuilder<'c> {
    client: &'c Client,
    request: ShowDatabasesRequest,
}

impl ShowDatabasesRequestBuilder<'_> {
    /// Specify whether or not to show deleted databases in the output
    pub fn with_show_deleted(mut self, show_deleted: bool) -> Self {
        self.request.show_deleted = show_deleted;
        self
    }

    /// Specify the [`QueryFormat`] of the returned `Bytes`
    pub fn with_format(mut self, format: QueryFormat) -> Self {
        self.request.format = format;
        self
    }

    /// Send the request, returning the raw [`Bytes`] in the response from the server
    pub async fn send(self) -> Result<Bytes> {
        let url = "/api/v3/configure/database";
        self.client
            .send_json_get_bytes(Method::GET, url, None::<()>, Some(self.request), None)
            .await
    }
}

#[derive(Debug, Serialize)]
pub struct CreateLastCacheRequestBuilder<'c> {
    #[serde(skip_serializing)]
    client: &'c Client,
    request: LastCacheCreateRequest,
}

impl<'c> CreateLastCacheRequestBuilder<'c> {
    /// Create a new [`CreateLastCacheRequestBuilder`]
    fn new(client: &'c Client, db: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            client,
            request: LastCacheCreateRequest {
                db: db.into(),
                table: table.into(),
                name: None,
                key_columns: None,
                value_columns: None,
                count: Default::default(),
                ttl: Default::default(),
            },
        }
    }

    /// Specify a cache name
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.request.name = Some(name.into());
        self
    }

    /// Speciffy the key columns for the cache
    pub fn key_columns(mut self, column_names: impl IntoIterator<Item: Into<String>>) -> Self {
        self.request.key_columns = Some(column_names.into_iter().map(Into::into).collect());
        self
    }

    /// Specify the value columns for the cache
    pub fn value_columns(mut self, column_names: impl IntoIterator<Item: Into<String>>) -> Self {
        self.request.value_columns = Some(column_names.into_iter().map(Into::into).collect());
        self
    }

    /// Specify the size, or number of new entries a cache will hold before evicting old ones
    pub fn count(mut self, count: LastCacheSize) -> Self {
        self.request.count = count;
        self
    }

    /// Specify the time-to-live (TTL) in seconds for entries in the cache
    pub fn ttl(mut self, ttl: LastCacheTtl) -> Self {
        self.request.ttl = ttl;
        self
    }

    /// Send the request to `POST /api/v3/configure/last_cache`
    pub async fn send(self) -> Result<Option<OrderedCatalogBatch>> {
        self.client
            .send_create(
                Method::POST,
                "/api/v3/configure/last_cache",
                Some(self.request),
                None::<()>,
            )
            .await
    }
}

/// Type for composing requests to the `POST /api/v3/configure/distinct_cache` API created by the
/// [`Client::api_v3_configure_distinct_cache_create`] method
#[derive(Debug)]
pub struct CreateDistinctCacheRequestBuilder<'c> {
    client: &'c Client,
    request: DistinctCacheCreateRequest,
}

impl<'c> CreateDistinctCacheRequestBuilder<'c> {
    fn new(
        client: &'c Client,
        db: impl Into<String>,
        table: impl Into<String>,
        columns: impl IntoIterator<Item: Into<String>>,
    ) -> Self {
        Self {
            client,
            request: DistinctCacheCreateRequest {
                db: db.into(),
                table: table.into(),
                columns: columns.into_iter().map(Into::into).collect(),
                name: None,
                max_cardinality: Default::default(),
                max_age: Default::default(),
            },
        }
    }

    /// Specify the name of the cache to be created, `snake_case` names are encouraged
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.request.name = Some(name.into());
        self
    }

    /// Specify the maximum cardinality for the cache as a non-zero unsigned integer
    pub fn max_cardinality(mut self, max_cardinality: NonZeroUsize) -> Self {
        self.request.max_cardinality = max_cardinality.into();
        self
    }

    /// Specify the maximum age for entries in the cache
    pub fn max_age(mut self, max_age: Duration) -> Self {
        self.request.max_age = max_age.into();
        self
    }

    /// Send the create cache request
    pub async fn send(self) -> Result<Option<OrderedCatalogBatch>> {
        self.client
            .send_create(
                Method::POST,
                "/api/v3/configure/distinct_cache",
                Some(self.request),
                None::<()>,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use influxdb3_types::http::{LastCacheSize, LastCacheTtl};
    use mockito::{Matcher, Server};
    use serde_json::json;

    use crate::{Client, Precision, QueryFormat};

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

        let client = Client::new(mock_server.url(), None)
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

        let client = Client::new(mock_server.url(), None)
            .expect("create client")
            .with_auth_token(token);

        let r = client
            .api_v3_query_sql(db, query)
            .format(QueryFormat::Json)
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

        let client = Client::new(mock_server.url(), None).expect("create client");

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
            .with_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), None).expect("create client");

        let r = client
            .api_v3_query_influxql(db, query)
            .format(QueryFormat::Json)
            .send()
            .await
            .expect("send request to server");

        assert_eq!(&r, body);

        mock.assert_async().await;
    }
    #[tokio::test]
    async fn api_v3_query_influxql_params() {
        let db = "stats";
        let query = "SELECT * FROM foo WHERE a = $a AND b < $b AND c > $c AND d = $d";
        let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/query_influxql")
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "q": query,
                "params": {
                    "a": "bar",
                    "b": 123,
                    "c": 1.5,
                    "d": false
                },
                "format": null
            })))
            .with_status(200)
            .with_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), None).expect("create client");

        let mut builder = client.api_v3_query_influxql(db, query);

        for (name, value) in [
            ("a", json!("bar")),
            ("b", json!(123)),
            ("c", json!(1.5)),
            ("d", json!(false)),
        ] {
            builder = builder.with_try_param(name, value).unwrap();
        }
        let r = builder.send().await;

        mock.assert_async().await;

        r.expect("sent request successfully");
    }
    #[tokio::test]
    async fn api_v3_query_influxql_with_params_from() {
        let db = "stats";
        let query = "SELECT * FROM foo WHERE a = $a AND b < $b AND c > $c AND d = $d";
        let body = r#"[{"host": "foo", "time": "1990-07-23T06:00:00:000", "val": 1}]"#;

        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/query_influxql")
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "q": query,
                "params": {
                    "a": "bar",
                    "b": 123,
                    "c": 1.5,
                    "d": false
                },
                "format": null
            })))
            .with_status(200)
            .with_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), None).expect("create client");

        let r = client
            .api_v3_query_influxql(db, query)
            .with_params_from([
                ("a", json!("bar")),
                ("b", json!(123)),
                ("c", json!(1.5)),
                ("d", json!(false)),
            ])
            .unwrap()
            .send()
            .await;

        mock.assert_async().await;

        r.expect("sent request successfully");
    }

    // NOTE(trevor): these tests are flaky since we need to fabricate the mock response, considering
    // removing them in favour of integration tests that use the actual APIs
    #[tokio::test]
    #[ignore]
    async fn api_v3_configure_last_cache_create_201() {
        let db = "db";
        let table = "table";
        let name = "cache_name";
        let key_columns = ["col1", "col2"];
        let val_columns = vec!["col3", "col4"];
        let ttl = LastCacheTtl::from_secs(120);
        let count = LastCacheSize::new(5).unwrap();
        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/configure/last_cache")
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "table": table,
                "name": name,
                "key_columns": key_columns,
                "value_columns": val_columns,
                "count": count,
                "ttl": ttl,
            })))
            .with_status(201)
            .with_body(
                r#"{
                    "table": "table",
                    "name": "cache_name",
                    "key_columns": [0, 1],
                    "value_columns": {
                        "explicit": {
                            "columns": [2, 3]
                        }
                    },
                    "ttl": 120,
                    "count": 5
                }"#,
            )
            .create_async()
            .await;
        let client = Client::new(mock_server.url(), None).unwrap();
        client
            .api_v3_configure_last_cache_create(db, table)
            .name(name)
            .key_columns(key_columns)
            .value_columns(val_columns)
            .ttl(ttl)
            .count(count)
            .send()
            .await
            .expect("creates last cache and parses response");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn api_v3_configure_last_cache_create_204() {
        let db = "db";
        let table = "table";
        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("POST", "/api/v3/configure/last_cache")
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "table": table,
                "ttl": 14400,
                "count": 1,
            })))
            .with_status(204)
            .create_async()
            .await;
        let client = Client::new(mock_server.url(), None).unwrap();
        let resp = client
            .api_v3_configure_last_cache_create(db, table)
            .send()
            .await
            .unwrap();
        mock.assert_async().await;
        assert!(resp.is_none());
    }

    #[tokio::test]
    async fn api_v3_configure_last_cache_delete() {
        let db = "db";
        let table = "table";
        let name = "cache_name";
        let mut mock_server = Server::new_async().await;
        let mock = mock_server
            .mock("DELETE", "/api/v3/configure/last_cache")
            .match_body(Matcher::Json(serde_json::json!({
                "db": db,
                "table": table,
                "name": name,
            })))
            .with_status(200)
            .create_async()
            .await;
        let client = Client::new(mock_server.url(), None).unwrap();
        client
            .api_v3_configure_last_cache_delete(db, table, name)
            .await
            .unwrap();
        mock.assert_async().await;
    }
}
