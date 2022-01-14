//! This module contains the HTTP api for InfluxDB IOx, including a
//! partial implementation of the /v2 HTTP api routes from InfluxDB
//! for compatibility.
//!
//! Note that these routes are designed to be just helpers for now,
//! and "close enough" to the real /v2 api to be able to test InfluxDB IOx
//! without needing to create and manage a mapping layer from name -->
//! id (this is done by other services in the influx cloud)
//!
//! Long term, we expect to create IOx specific api in terms of
//! database names and may remove this quasi /v2 API.

// Influx crates
use data_types::{names::OrgBucketMappingError, DatabaseName};
use influxdb_iox_client::format::QueryOutputFormat;
use query::{exec::ExecutionContextProvider, QueryDatabase};
use server::Error;

// External crates
use async_trait::async_trait;
use http::header::CONTENT_TYPE;
use hyper::{Body, Method, Request, Response};
use observability_deps::tracing::{debug, error};
use serde::Deserialize;
use snafu::{OptionExt, ResultExt, Snafu};

use crate::influxdb_ioxd::{
    http::{
        dml::{HttpDrivenDml, InnerDmlError, RequestOrResponse},
        error::{HttpApiError, HttpApiErrorExt, HttpApiErrorSource},
        metrics::LineProtocolMetrics,
    },
    planner::Planner,
};
use dml::DmlOperation;
use std::{
    fmt::Debug,
    str::{self, FromStr},
    sync::Arc,
};

use super::DatabaseServerType;

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
pub enum ApplicationError {
    #[snafu(display("Internal error mapping org & bucket: {}", source))]
    BucketMappingError { source: OrgBucketMappingError },

    #[snafu(display("Internal error reading points from database {}:  {}", db_name, source))]
    Query {
        db_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Expected query string in request, but none was provided"))]
    ExpectedQueryString {},

    /// Error for when we could not parse the http query uri (e.g.
    /// `?foo=bar&bar=baz)`
    #[snafu(display("Invalid query string in HTTP URI '{}': {}", query_string, source))]
    InvalidQueryString {
        query_string: String,
        source: serde_urlencoded::de::Error,
    },

    #[snafu(display("No handler for {:?} {}", method, path))]
    RouteNotFound { method: Method, path: String },

    #[snafu(display("Invalid database name: {}", source))]
    DatabaseNameError {
        source: data_types::DatabaseNameError,
    },

    #[snafu(display("Database {} not found", db_name))]
    DatabaseNotFound { db_name: String },

    #[snafu(display("Internal error creating HTTP response:  {}", source))]
    CreatingResponse { source: http::Error },

    #[snafu(display("Invalid format '{}': : {}", format, source))]
    ParsingFormat {
        format: String,
        source: influxdb_iox_client::format::Error,
    },

    #[snafu(display(
        "Error formatting results of SQL query '{}' using '{:?}': {}",
        q,
        format,
        source
    ))]
    FormattingResult {
        q: String,
        format: QueryOutputFormat,
        source: influxdb_iox_client::format::Error,
    },

    #[snafu(display("Error while planning query: {}", source))]
    Planning {
        source: crate::influxdb_ioxd::planner::Error,
    },

    #[snafu(display("Server id not set"))]
    ServerIdNotSet,

    #[snafu(display("Server not initialized"))]
    ServerNotInitialized,

    #[snafu(display("Database {} not found", db_name))]
    DatabaseNotInitialized { db_name: String },

    #[snafu(display("Internal server error"))]
    InternalServerError,

    #[snafu(display("Cannot perform DML operation: {}", source))]
    DmlError {
        source: crate::influxdb_ioxd::http::dml::HttpDmlError,
    },
}

type Result<T, E = ApplicationError> = std::result::Result<T, E>;

impl HttpApiErrorSource for ApplicationError {
    fn to_http_api_error(&self) -> HttpApiError {
        match self {
            e @ Self::BucketMappingError { .. } => e.internal_error(),
            e @ Self::Query { .. } => e.internal_error(),
            e @ Self::ExpectedQueryString { .. } => e.invalid(),
            e @ Self::InvalidQueryString { .. } => e.invalid(),
            e @ Self::RouteNotFound { .. } => e.not_found(),
            e @ Self::DatabaseNameError { .. } => e.invalid(),
            e @ Self::DatabaseNotFound { .. } => e.not_found(),
            e @ Self::CreatingResponse { .. } => e.internal_error(),
            e @ Self::FormattingResult { .. } => e.internal_error(),
            e @ Self::ParsingFormat { .. } => e.invalid(),
            e @ Self::Planning { .. } => e.invalid(),
            e @ Self::ServerIdNotSet => e.invalid(),
            e @ Self::ServerNotInitialized => e.invalid(),
            e @ Self::DatabaseNotInitialized { .. } => e.invalid(),
            e @ Self::InternalServerError => e.internal_error(),
            Self::DmlError { source } => source.to_http_api_error(),
        }
    }
}

impl From<server::Error> for ApplicationError {
    fn from(e: Error) -> Self {
        match e {
            Error::IdNotSet => Self::ServerIdNotSet,
            Error::ServerNotInitialized { .. } => Self::ServerNotInitialized,
            Error::DatabaseNotInitialized { db_name } => Self::DatabaseNotInitialized { db_name },
            Error::DatabaseNotFound { db_name } => Self::DatabaseNotFound { db_name },
            Error::InvalidDatabaseName { source } => Self::DatabaseNameError { source },
            e => {
                error!(%e, "unexpected server error");
                // Don't return potentially sensitive information in response
                Self::InternalServerError
            }
        }
    }
}

#[async_trait]
impl HttpDrivenDml for DatabaseServerType {
    fn max_request_size(&self) -> usize {
        self.max_request_size
    }

    fn lp_metrics(&self) -> Arc<LineProtocolMetrics> {
        Arc::clone(&self.lp_metrics)
    }

    async fn write(
        &self,
        db_name: &DatabaseName<'_>,
        op: DmlOperation,
    ) -> Result<(), InnerDmlError> {
        let db = self
            .server
            .db(db_name)
            .map_err(|_| InnerDmlError::DatabaseNotFound {
                db_name: db_name.to_string(),
            })?;

        db.store_operation(&op)
            .map_err(|e| InnerDmlError::UserError {
                db_name: db_name.to_string(),
                source: Box::new(e),
            })
    }
}

pub async fn route_request(
    server_type: &DatabaseServerType,
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    match server_type
        .route_dml_http_request(req)
        .await
        .context(DmlSnafu)?
    {
        RequestOrResponse::Response(resp) => Ok(resp),
        RequestOrResponse::Request(req) => {
            let method = req.method().clone();
            let uri = req.uri().clone();

            match (method.clone(), uri.path()) {
                (Method::GET, "/api/v3/query") => query(req, server_type).await,

                (method, path) => Err(ApplicationError::RouteNotFound {
                    method,
                    path: path.to_string(),
                }),
            }
        }
    }
}

#[derive(Deserialize, Debug, PartialEq)]
/// Parsed URI Parameters of the request to the .../query endpoint
struct QueryParams {
    #[serde(alias = "database")]
    d: String,
    #[serde(alias = "query")]
    q: String,
    #[serde(default = "default_format")]
    format: String,
}

fn default_format() -> String {
    QueryOutputFormat::default().to_string()
}

async fn query(
    req: Request<Body>,
    server_type: &DatabaseServerType,
) -> Result<Response<Body>, ApplicationError> {
    let server = &server_type.server;

    let uri_query = req.uri().query().context(ExpectedQueryStringSnafu {})?;

    let QueryParams { d, q, format } =
        serde_urlencoded::from_str(uri_query).context(InvalidQueryStringSnafu {
            query_string: uri_query,
        })?;

    let format = QueryOutputFormat::from_str(&format).context(ParsingFormatSnafu { format })?;

    let db_name = DatabaseName::new(&d).context(DatabaseNameSnafu)?;
    debug!(uri = ?req.uri(), %q, ?format, %db_name, "running SQL query");

    let db = server.db(&db_name)?;

    let _query_completed_token = db.record_query("sql", &q);

    let ctx = db.new_query_context(req.extensions().get().cloned());
    let physical_plan = Planner::new(&ctx).sql(&q).await.context(PlanningSnafu)?;

    // TODO: stream read results out rather than rendering the
    // whole thing in mem
    let batches = ctx
        .collect(physical_plan)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(QuerySnafu { db_name })?;

    let results = format
        .format(&batches)
        .context(FormattingResultSnafu { q, format })?;

    let body = Body::from(results.into_bytes());

    let response = Response::builder()
        .header(CONTENT_TYPE, format.content_type())
        .body(body)
        .context(CreatingResponseSnafu)?;

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::influxdb_ioxd::{
        http::{
            dml::test_utils::{
                assert_delete_bad_request, assert_delete_unknown_database,
                assert_delete_unknown_table, assert_gzip_write, assert_write, assert_write_metrics,
                assert_write_to_invalid_database,
            },
            test_utils::{
                assert_health, assert_metrics, assert_tracing, check_response, get_content_type,
                TestServer,
            },
        },
        server_type::common_state::CommonServerState,
    };
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_eq;
    use data_types::{database_rules::DatabaseRules, server_id::ServerId, DatabaseName};
    use db::Db;
    use dml::DmlWrite;
    use http::StatusCode;
    use object_store::ObjectStore;
    use reqwest::Client;
    use schema::selection::Selection;
    use server::{rules::ProvidedDatabaseRules, ApplicationState, Server};
    use std::convert::TryFrom;
    use trace::RingBufferTraceCollector;

    fn make_application() -> Arc<ApplicationState> {
        Arc::new(ApplicationState::new(
            Arc::new(ObjectStore::new_in_memory()),
            None,
            Some(Arc::new(RingBufferTraceCollector::new(5))),
        ))
    }

    fn make_server(application: Arc<ApplicationState>) -> Arc<Server> {
        Arc::new(Server::new(application, Default::default()))
    }

    #[tokio::test]
    async fn test_health() {
        assert_health(setup_server().await).await;
    }

    #[tokio::test]
    async fn test_metrics() {
        assert_metrics(setup_server().await).await;
    }

    #[tokio::test]
    async fn test_tracing() {
        assert_tracing(setup_server().await).await;
    }

    async fn assert_dbwrite(test_server: TestServer<DatabaseServerType>, write: DmlWrite) {
        let (table_name, mutable_batch) = write.tables().next().unwrap();

        let test_db = test_server
            .server_type()
            .server
            .db(&DatabaseName::new("MyOrg_MyBucket").unwrap())
            .expect("Database exists");
        let batches = run_query(test_db, &format!("select * from {}", table_name)).await;

        let expected = arrow_util::display::pretty_format_batches(&[mutable_batch
            .to_arrow(Selection::All)
            .unwrap()])
        .unwrap();
        let expected = expected.split('\n');

        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_write() {
        let test_server = setup_server().await;
        let write = assert_write(&test_server).await;

        assert_dbwrite(test_server, write).await;
    }

    #[tokio::test]
    async fn test_write_metrics() {
        assert_write_metrics(setup_server().await, true).await;
    }

    #[tokio::test]
    async fn test_gzip_write() {
        let test_server = setup_server().await;
        let write = assert_gzip_write(&test_server).await;

        assert_dbwrite(test_server, write).await;
    }

    #[tokio::test]
    async fn write_to_invalid_database() {
        assert_write_to_invalid_database(setup_server().await).await;
    }

    #[tokio::test]
    async fn test_delete() {
        // Set up server
        let test_server = setup_server().await;

        // Set up client
        let client = Client::new();
        let bucket_name = "MyBucket";
        let org_name = "MyOrg";

        // Client requests delete something from an empty DB
        let delete_line = r#"{"start":"1970-01-01T00:00:00Z","stop":"2070-01-02T00:00:00Z", "predicate":"host=\"Orient.local\""}"#;
        let response = client
            .post(&format!(
                "{}/api/v2/delete?bucket={}&org={}",
                test_server.url(),
                bucket_name,
                org_name
            ))
            .body(delete_line)
            .send()
            .await;
        check_response("delete", response, StatusCode::NO_CONTENT, Some("")).await;

        // Client writes data to the server
        let lp_data = r#"h2o_temperature,location=santa_monica,state=CA surface_degrees=65.2,bottom_degrees=50.4 1617286224000000000
               h2o_temperature,location=Boston,state=MA surface_degrees=47.5,bottom_degrees=35 1617286224000000123"#;
        let response = client
            .post(&format!(
                "{}/api/v2/write?bucket={}&org={}",
                test_server.url(),
                bucket_name,
                org_name
            ))
            .body(lp_data)
            .send()
            .await;
        check_response("write", response, StatusCode::NO_CONTENT, Some("")).await;

        // Check that the data got into the right bucket
        let test_db = test_server
            .server_type()
            .server
            .db(&DatabaseName::new("MyOrg_MyBucket").unwrap())
            .expect("Database exists");
        let batches = run_query(
            Arc::clone(&test_db),
            "select * from h2o_temperature order by location",
        )
        .await;
        let expected = vec![
            "+----------------+--------------+-------+-----------------+--------------------------------+",
            "| bottom_degrees | location     | state | surface_degrees | time                           |",
            "+----------------+--------------+-------+-----------------+--------------------------------+",
            "| 35             | Boston       | MA    | 47.5            | 2021-04-01T14:10:24.000000123Z |",
            "| 50.4           | santa_monica | CA    | 65.2            | 2021-04-01T14:10:24Z           |",
            "+----------------+--------------+-------+-----------------+--------------------------------+",
        ];
        assert_batches_eq!(expected, &batches);

        // Now delete something
        let delete_line = r#"{"start":"2021-04-01T14:00:00Z","stop":"2021-04-02T14:00:00Z", "predicate":"location=Boston"}"#;
        let response = client
            .post(&format!(
                "{}/api/v2/delete?bucket={}&org={}",
                test_server.url(),
                bucket_name,
                org_name
            ))
            .body(delete_line)
            .send()
            .await;
        check_response("delete", response, StatusCode::NO_CONTENT, Some("")).await;

        // query again and should not get the deleted data
        let batches = run_query(test_db, "select * from h2o_temperature").await;
        let expected = vec![
            "+----------------+--------------+-------+-----------------+----------------------+",
            "| bottom_degrees | location     | state | surface_degrees | time                 |",
            "+----------------+--------------+-------+-----------------+----------------------+",
            "| 50.4           | santa_monica | CA    | 65.2            | 2021-04-01T14:10:24Z |",
            "+----------------+--------------+-------+-----------------+----------------------+",
        ];
        assert_batches_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn test_delete_unknown_database() {
        assert_delete_unknown_database(setup_server().await).await;
    }

    #[tokio::test]
    async fn test_delete_unknown_table() {
        assert_delete_unknown_table(setup_server().await).await;
    }

    #[tokio::test]
    async fn test_delete_bad_request() {
        assert_delete_bad_request(setup_server().await).await;
    }

    /// Sets up a test database with some data for testing the query endpoint
    /// returns a client for communicating with the server, and the server
    /// endpoint
    async fn setup_test_data() -> (Client, TestServer<DatabaseServerType>) {
        let test_server = setup_server().await;

        let client = Client::new();

        let lp_data = "h2o_temperature,location=santa_monica,state=CA surface_degrees=65.2,bottom_degrees=50.4 1617286224000000000";

        // send write data
        let bucket_name = "MyBucket";
        let org_name = "MyOrg";
        let response = client
            .post(&format!(
                "{}/api/v2/write?bucket={}&org={}",
                test_server.url(),
                bucket_name,
                org_name
            ))
            .body(lp_data)
            .send()
            .await;

        check_response("write", response, StatusCode::NO_CONTENT, Some("")).await;
        (client, test_server)
    }

    #[tokio::test]
    async fn test_query_pretty() {
        let (client, test_server) = setup_test_data().await;

        // send query data
        let response = client
            .get(&format!(
                "{}/api/v3/query?d=MyOrg_MyBucket&q={}",
                test_server.url(),
                "select%20*%20from%20h2o_temperature"
            ))
            .send()
            .await;

        assert_eq!(get_content_type(&response), "text/plain");

        let expected = r#"+----------------+--------------+-------+-----------------+----------------------+
| bottom_degrees | location     | state | surface_degrees | time                 |
+----------------+--------------+-------+-----------------+----------------------+
| 50.4           | santa_monica | CA    | 65.2            | 2021-04-01T14:10:24Z |
+----------------+--------------+-------+-----------------+----------------------+"#;

        check_response("query", response, StatusCode::OK, Some(expected)).await;

        // same response is expected if we explicitly request 'format=pretty'
        let response = client
            .get(&format!(
                "{}/api/v3/query?d=MyOrg_MyBucket&q={}&format=pretty",
                test_server.url(),
                "select%20*%20from%20h2o_temperature"
            ))
            .send()
            .await;
        assert_eq!(get_content_type(&response), "text/plain");

        check_response("query", response, StatusCode::OK, Some(expected)).await;
    }

    #[tokio::test]
    async fn test_query_csv() {
        let (client, test_server) = setup_test_data().await;

        // send query data
        let response = client
            .get(&format!(
                "{}/api/v3/query?d=MyOrg_MyBucket&q={}&format=csv",
                test_server.url(),
                "select%20*%20from%20h2o_temperature"
            ))
            .send()
            .await;

        assert_eq!(get_content_type(&response), "text/csv");

        let res = "bottom_degrees,location,state,surface_degrees,time\n\
                   50.4,santa_monica,CA,65.2,2021-04-01T14:10:24.000000000\n";
        check_response("query", response, StatusCode::OK, Some(res)).await;
    }

    #[tokio::test]
    async fn test_query_json() {
        let (client, test_server) = setup_test_data().await;

        // send a second line of data to demonstrate how that works
        let lp_data =
            "h2o_temperature,location=Boston,state=MA surface_degrees=50.2 1617286224000000000";

        // send write data
        let bucket_name = "MyBucket";
        let org_name = "MyOrg";
        let response = client
            .post(&format!(
                "{}/api/v2/write?bucket={}&org={}",
                test_server.url(),
                bucket_name,
                org_name
            ))
            .body(lp_data)
            .send()
            .await;

        check_response("write", response, StatusCode::NO_CONTENT, Some("")).await;

        // send query data
        let response = client
            .get(&format!(
                "{}/api/v3/query?d=MyOrg_MyBucket&q={}&format=json",
                test_server.url(),
                "select%20*%20from%20h2o_temperature%20order%20by%20surface_degrees"
            ))
            .send()
            .await;

        assert_eq!(get_content_type(&response), "application/json");

        // Note two json records: one record on each line
        let res = r#"[{"location":"Boston","state":"MA","surface_degrees":50.2,"time":"2021-04-01 14:10:24"},{"bottom_degrees":50.4,"location":"santa_monica","state":"CA","surface_degrees":65.2,"time":"2021-04-01 14:10:24"}]"#;
        check_response("query", response, StatusCode::OK, Some(res)).await;
    }

    #[tokio::test]
    async fn test_query_invalid_name() {
        let (client, test_server) = setup_test_data().await;

        // send query data
        let response = client
            .get(&format!(
                "{}/api/v3/query?d=&q={}",
                test_server.url(),
                "select%20*%20from%20h2o_temperature%20order%20by%20surface_degrees"
            ))
            .send()
            .await;

        check_response(
            "query",
            response,
            StatusCode::BAD_REQUEST,
            Some(r#"{"code":"invalid","message":"Invalid database name: Database name  length must be between 1 and 64 characters"}"#),
        )
        .await;
    }

    /// Run the specified SQL query and return formatted results as a string
    async fn run_query(db: Arc<Db>, query: &str) -> Vec<RecordBatch> {
        let ctx = db.new_query_context(None);
        let physical_plan = Planner::new(&ctx).sql(query).await.unwrap();

        ctx.collect(physical_plan).await.unwrap()
    }

    /// return a test server and the url to contact it for `MyOrg_MyBucket`
    async fn setup_server() -> TestServer<DatabaseServerType> {
        let application = make_application();

        let app_server = make_server(Arc::clone(&application));
        app_server.set_id(ServerId::try_from(1).unwrap()).unwrap();
        app_server.wait_for_init().await.unwrap();
        app_server
            .create_database(make_rules("MyOrg_MyBucket"))
            .await
            .unwrap();

        let server_type =
            DatabaseServerType::new(application, app_server, &CommonServerState::for_testing());

        TestServer::new(Arc::new(server_type))
    }

    fn make_rules(db_name: impl Into<String>) -> ProvidedDatabaseRules {
        let db_name = DatabaseName::new(db_name.into()).unwrap();
        ProvidedDatabaseRules::new_rules(DatabaseRules::new(db_name).into())
            .expect("Tests should create valid DatabaseRules")
    }
}
