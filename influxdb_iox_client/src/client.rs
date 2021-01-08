use data_types::database_rules::DatabaseRules;
use reqwest::{Method, Url};

use crate::errors::RequestError;
use crate::errors::{CreateDatabaseError, ServerErrorResponse};

// TODO: move DatabaseRules / WriterId into API client
// TODO: move API errors? Definitely map API errors

/// An IOx HTTP API client.
///
/// ```
/// #[tokio::test]
/// # async fn test() {
/// use data_types::database_rules::DatabaseRules;
/// use influxdb_iox_client::ClientBuilder;
///
/// let client = ClientBuilder::default()
///     .build("http://127.0.0.1:8080")
///     .unwrap();
///
/// // Ping the IOx server
/// client.ping().await.expect("server is down :(");
///
/// // Create a new database!
/// client
///     .create_database("bananas", &DatabaseRules::default())
///     .await
///     .expect("failed to create database");
/// # }
/// ```
#[derive(Debug)]
pub struct Client {
    pub(crate) http: reqwest::Client,

    /// The base URL to which request paths are joined.
    ///
    /// A base path of:
    ///
    /// ```text
    ///     https://www.influxdata.com/maybe-proxy/
    /// ```
    ///
    /// Joined with a request path of `/a/reg/` would result in:
    ///
    /// ```text
    ///     https://www.influxdata.com/maybe-proxy/a/req/
    /// ```
    ///
    /// Paths joined to this `base` MUST be relative to be appended to the base
    /// path. Absolute paths joined to `base` are still absolute.
    pub(crate) base: Url,
}

impl std::default::Default for Client {
    fn default() -> Self {
        crate::ClientBuilder::default()
            .build("http://127.0.0.1:8080")
            .expect("default client builder is invalid")
    }
}

impl Client {
    /// Ping the IOx server, checking for a HTTP 200 response.
    pub async fn ping(&self) -> Result<(), RequestError> {
        const PING_PATH: &'static str = "ping";

        let resp = self
            .http
            .request(Method::GET, self.url_for(PING_PATH))
            .send()
            .await;

        match resp {
            Ok(r) if r.status() == 200 => Ok(()),
            Ok(r) => Err(RequestError::UnexpectedStatusCode {
                code: r.status().as_u16(),
            }
            .into()),
            Err(e) => Err(e.into()),
        }
    }

    /// Creates a new IOx database.
    pub async fn create_database(
        &self,
        name: impl AsRef<str>,
        rules: &DatabaseRules,
    ) -> Result<(), CreateDatabaseError> {
        const DB_PATH: &'static str = "iox/api/v1/databases/";

        let url = self.url_for(DB_PATH).join(name.as_ref()).map_err(|_| {
            CreateDatabaseError::InvalidName {
                name: name.as_ref().to_string(),
            }
        })?;

        let resp = self.http.request(Method::PUT, url).json(rules).send().await;

        // TODO: map response error to application error
        //
        // For example, an "invalid database name" error needs mapping to
        // InvalidName

        match resp {
            Ok(r) if r.status() == 200 => Ok(()),
            Ok(r) if r.status() == 400 => ServerErrorResponse::try_from_body(r)
                .await
                .map(|e| Err(RequestError::BadRequest { source: e }.into()))?,
            Ok(r) => Err(RequestError::UnexpectedStatusCode {
                code: r.status().as_u16(),
            }
            .into()),
            Err(e) => Err(RequestError::HttpRequestError { source: e.into() }.into()),
        }
    }

    /// Build the request path for relative `path`.
    ///
    /// # Safety
    ///
    /// Panics in debug builds if `path` contains an absolute path.
    fn url_for(&self, path: &str) -> Url {
        // In non-release builds, assert the path is not an absolute path.
        //
        // Paths should be relative so the full base path is used.
        debug_assert_ne!(
            path.chars().nth(0).unwrap(),
            '/',
            "should not join absolute paths to base URL"
        );
        self.base
            .join(path)
            .expect("failed to construct request URL")
    }
}

#[cfg(test)]
mod tests {
    use crate::ClientBuilder;
    use rand::{distributions::Alphanumeric, thread_rng, Rng};

    use super::*;

    /// If `TEST_IOX_ENDPOINT` is set, load the value and return it to the
    /// caller.
    ///
    /// If `TEST_IOX_ENDPOINT` is not set, skip the calling test by returning
    /// early. Additionally if `TEST_INTEGRATION` is set, turn this early return
    /// into a panic to force a hard fail for skipped integration tests.
    macro_rules! maybe_skip_integration {
        () => {
            match (
                std::env::var("TEST_IOX_ENDPOINT").is_ok(),
                std::env::var("TEST_INTEGRATION").is_ok(),
            ) {
                (true, _) => std::env::var("TEST_IOX_ENDPOINT").unwrap(),
                (false, true) => {
                    panic!("TEST_INTEGRATION is set, but TEST_IOX_ENDPOINT are not")
                }
                _ => {
                    eprintln!("skipping integration test - set TEST_IOX_ENDPOINT to run");
                    return;
                }
            }
        };
    }

    #[tokio::test]
    async fn test_ping() {
        let endpoint = maybe_skip_integration!();
        let c = ClientBuilder::default().build(endpoint).unwrap();
        c.ping().await.expect("ping failed");
    }

    #[tokio::test]
    async fn test_create_database() {
        let endpoint = maybe_skip_integration!();
        let c = ClientBuilder::default().build(endpoint).unwrap();

        let rand_name: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();

        c.create_database(rand_name, &DatabaseRules::default())
            .await
            .expect("create database failed");
    }

    #[tokio::test]
    async fn test_create_database_invalid_name() {
        let endpoint = maybe_skip_integration!();
        let c = ClientBuilder::default().build(endpoint).unwrap();

        let err = c
            .create_database("bananas!", &DatabaseRules::default())
            .await
            .expect_err("expected request to fail");

        assert!(matches!(err, CreateDatabaseError::Unknown{source: RequestError::BadRequest{..}}))
    }

    #[test]
    fn test_default() {
        // Ensures the Default impl does not panic
        let c = Client::default();
        assert_eq!(c.base.as_str(), "http://127.0.0.1:8080/");
    }

    #[test]
    fn test_paths() {
        let c = ClientBuilder::default()
            .build("http://127.0.0.2:8081/proxy")
            .unwrap();

        assert_eq!(
            c.url_for("bananas").as_str(),
            "http://127.0.0.2:8081/proxy/bananas"
        );
    }

    #[test]
    #[should_panic(expected = "absolute paths")]
    fn test_absolute_path_panics() {
        let c = ClientBuilder::default()
            .build("http://127.0.0.2:8081/proxy")
            .unwrap();

        c.url_for("/bananas");
    }
}
