use reqwest::{Method, StatusCode};
use secrecy::ExposeSecret;
use serde::Serialize;

use crate::{Client, Error, Result};

impl Client {
    pub async fn api_v3_configure_file_index_create_or_update(
        &self,
        db: impl Into<String> + Send,
        table: Option<impl Into<String> + Send>,
        columns: Vec<impl Into<String> + Send>,
    ) -> Result<()> {
        let api_path = "/api/v3/enterprise/configure/file_index";
        let url = self.base_url.join(api_path)?;
        #[derive(Serialize)]
        struct Req {
            db: String,
            table: Option<String>,
            columns: Vec<String>,
        }
        let mut req = self.http_client.post(url).json(&Req {
            db: db.into(),
            table: table.map(Into::into),
            columns: columns.into_iter().map(Into::into).collect(),
        });
        if let Some(token) = &self.auth_token {
            req = req.bearer_auth(token.expose_secret());
        }
        let resp = req
            .send()
            .await
            .map_err(|src| Error::request_send(Method::POST, api_path, src))?;
        let status = resp.status();
        match status {
            StatusCode::OK => Ok(()),
            code => Err(Error::ApiError {
                code,
                message: resp.text().await.map_err(Error::Text)?,
            }),
        }
    }

    pub async fn api_v3_configure_file_index_delete(
        &self,
        db: impl Into<String> + Send,
        table: Option<impl Into<String> + Send>,
    ) -> Result<()> {
        let api_path = "/api/v3/enterprise/configure/file_index";
        let url = self.base_url.join(api_path)?;
        #[derive(Serialize)]
        struct Req {
            db: String,
            table: Option<String>,
        }
        let mut req = self.http_client.delete(url).json(&Req {
            db: db.into(),
            table: table.map(Into::into),
        });
        if let Some(token) = &self.auth_token {
            req = req.bearer_auth(token.expose_secret());
        }
        let resp = req
            .send()
            .await
            .map_err(|src| Error::request_send(Method::DELETE, api_path, src))?;
        let status = resp.status();
        match status {
            StatusCode::OK => Ok(()),
            code => Err(Error::ApiError {
                code,
                message: resp.text().await.map_err(Error::Text)?,
            }),
        }
    }
}
