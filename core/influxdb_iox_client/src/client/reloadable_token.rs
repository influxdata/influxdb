use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use client_util::connection::HttpConnection;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use reqwest::header::AUTHORIZATION;
use reqwest::{Body, Method};
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::error::{Error, translate_response};
use crate::write::RequestMaker;

/// A request maker that wraps an [`HttpConnection`] and dynamically adds
/// an authorization header from a periodically reloaded token file.
///
/// The token is loaded from a file at startup and can be configured to
/// reload at a specified interval. If the token file cannot be read during
/// a reload, the previous token is retained and a warning is logged.
#[derive(Debug)]
pub struct ReloadableTokenRequestMaker {
    /// The underlying HTTP connection
    connection: HttpConnection,

    /// The current token, if any
    token: Arc<RwLock<Option<String>>>,

    /// Handle to the background reload task (if configured)
    _reload_task: JoinSet<()>,
}

impl ReloadableTokenRequestMaker {
    /// Creates a new [`ReloadableTokenRequestMaker`] with the given connection and token path.
    ///
    /// If `reload_interval` is `Some`, a background task is spawned to periodically
    /// reload the token from the file. If `None`, the token is only loaded once.
    ///
    /// # Arguments
    ///
    /// * `connection` - The HTTP connection to wrap
    /// * `token_path` - Path to the file containing the authentication token
    /// * `reload_interval` - Optional interval at which to reload the token
    ///
    /// # Returns
    ///
    /// Returns `Ok(Self)` if the initial token was loaded successfully, or `Err` if
    /// the token file could not be read.
    pub async fn new(
        connection: HttpConnection,
        token_path: PathBuf,
        reload_interval: Option<Duration>,
    ) -> Result<Self, std::io::Error> {
        // Load initial token
        let initial_token = load_token_from_file(&token_path).await?;
        let token = Arc::new(RwLock::new(initial_token));

        let mut join_set = JoinSet::new();

        // Spawn background reload task if interval is configured
        reload_interval.map(|interval| {
            let token = Arc::clone(&token);
            let token_path = token_path.clone();

            join_set.spawn(async move {
                info!(
                    "Starting authentication token reload task for {:?} with interval {:?}",
                    token_path, interval
                );

                loop {
                    tokio::time::sleep(interval).await;

                    match load_token_from_file(&token_path).await {
                        Ok(new_token) => {
                            let mut guard = token.write().await;
                            let changed = *guard != new_token;
                            *guard = new_token;
                            if changed {
                                info!("Reloaded authentication token from {:?}", token_path);
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Failed to reload authentication token from {:?}: {}. \
                                 Keeping previous token.",
                                token_path, e
                            );
                        }
                    }
                }
            })
        });

        Ok(Self {
            connection,
            token,
            _reload_task: join_set,
        })
    }

    /// Creates a new [`ReloadableTokenRequestMaker`] without any token.
    ///
    /// This is useful when no authentication is configured.
    pub fn new_without_token(connection: HttpConnection) -> Self {
        Self {
            connection,
            token: Arc::new(RwLock::new(None)),
            _reload_task: JoinSet::new(),
        }
    }
}

impl RequestMaker for ReloadableTokenRequestMaker {
    fn write_source(
        &self,
        org_id: String,
        bucket_id: String,
        body: String,
    ) -> BoxFuture<'_, Result<usize, Error>> {
        let write_url = format!("{}api/v2/write", self.connection.uri());
        let token = Arc::clone(&self.token);
        let client = self.connection.client();

        async move {
            let body: Body = body.into();
            let data_len = body.as_bytes().map(|b| b.len()).unwrap_or(0);

            let mut request = client
                .request(Method::POST, &write_url)
                .query(&[("bucket", bucket_id), ("org", org_id)])
                .body(body);

            // Add authorization header if token is available
            if let Some(ref token_value) = *token.read().await {
                request = request.header(AUTHORIZATION, format!("Bearer {}", token_value));
            }

            let response = request.send().await.map_err(Error::client)?;

            translate_response(response).await?;

            Ok(data_len)
        }
        .boxed()
    }
}

/// Load the token from a file, trimming whitespace.
///
/// Returns `Ok(None)` if the file is empty after trimming.
/// Returns `Ok(Some(token))` if a non-empty token was read.
/// Returns `Err` if the file could not be read.
async fn load_token_from_file(path: &PathBuf) -> Result<Option<String>, std::io::Error> {
    let content = tokio::fs::read_to_string(path).await?;
    let trimmed = content.trim().to_string();

    if trimmed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(trimmed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_load_token_from_file() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "  my-secret-token  ").unwrap();

        let token = load_token_from_file(&file.path().to_path_buf())
            .await
            .unwrap();
        assert_eq!(token, Some("my-secret-token".to_string()));
    }

    #[tokio::test]
    async fn test_load_empty_token_file() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "   ").unwrap();

        let token = load_token_from_file(&file.path().to_path_buf())
            .await
            .unwrap();
        assert_eq!(token, None);
    }

    #[tokio::test]
    async fn test_load_token_file_not_found() {
        let result = load_token_from_file(&PathBuf::from("/nonexistent/path/to/token")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_token_reload() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "initial-token").unwrap();
        file.flush().unwrap();

        // Create a mock connection - we only need to test token loading behavior
        // For this test, we'll directly test the token state
        let token_path = file.path().to_path_buf();

        // Load initial token
        let initial = load_token_from_file(&token_path).await.unwrap();
        assert_eq!(initial, Some("initial-token".to_string()));

        // Update the file
        std::fs::write(&token_path, "updated-token\n").unwrap();

        // Reload and verify
        let updated = load_token_from_file(&token_path).await.unwrap();
        assert_eq!(updated, Some("updated-token".to_string()));
    }
}
