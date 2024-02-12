use reqwest::{Body, IntoUrl};
use secrecy::{ExposeSecret, Secret};
use serde::Serialize;
use url::Url;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("base URL error: {0}")]
    BaseUrl(#[source] reqwest::Error),

    #[error("request URL error: {0}")]
    RequestUrl(#[from] url::ParseError),

    #[error("failed to send /api/v3/write_lp request: {0}")]
    WriteLpSend(#[source] reqwest::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Client {
    base_url: Url,
    auth_header: Option<Secret<String>>,
    http_client: reqwest::Client,
}

impl Client {
    pub fn new<U: IntoUrl>(base_url: U, http_client: reqwest::Client) -> Result<Self> {
        Ok(Self {
            base_url: base_url.into_url().map_err(Error::BaseUrl)?,
            auth_header: None,
            http_client,
        })
    }

    pub fn with_auth_header<S: Into<String>>(mut self, auth_header: S) -> Self {
        self.auth_header = Some(Secret::new(auth_header.into()));
        self
    }

    pub fn api_v3_write_lp<S: Into<String>>(&self, db: S) -> WriteRequestBuilder<NoBody> {
        WriteRequestBuilder {
            client: self.clone(),
            db: db.into(),
            precision: None,
            accept_partial: None,
            body: NoBody,
        }
    }
}

#[derive(Debug, Serialize)]
struct WriteRequest<'a> {
    db: &'a str,
    precision: Option<Precision>,
    accept_partial: Option<bool>,
}

impl<'a, B> From<&'a WriteRequestBuilder<B>> for WriteRequest<'a> {
    fn from(builder: &'a WriteRequestBuilder<B>) -> Self {
        Self {
            db: &builder.db,
            precision: builder.precision,
            accept_partial: builder.accept_partial,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum Precision {
    Second,
    Milli,
    Micro,
    Nano,
}

#[derive(Debug)]
pub struct WriteRequestBuilder<B> {
    client: Client,
    db: String,
    precision: Option<Precision>,
    accept_partial: Option<bool>,
    body: B,
}

impl<B> WriteRequestBuilder<B> {
    pub fn precision_seconds(mut self) -> Self {
        self.precision = Some(Precision::Second);
        self
    }

    pub fn precision_milli(mut self) -> Self {
        self.precision = Some(Precision::Milli);
        self
    }

    pub fn precision_micro(mut self) -> Self {
        self.precision = Some(Precision::Micro);
        self
    }

    pub fn precision_nano(mut self) -> Self {
        self.precision = Some(Precision::Nano);
        self
    }

    pub fn accept_partial(mut self, set_to: bool) -> Self {
        self.accept_partial = Some(set_to);
        self
    }
}

impl WriteRequestBuilder<NoBody> {
    pub fn body<T: Into<Body>>(self, body: T) -> WriteRequestBuilder<Body> {
        WriteRequestBuilder {
            client: self.client,
            db: self.db,
            precision: self.precision,
            accept_partial: self.accept_partial,
            body: body.into(),
        }
    }
}

impl WriteRequestBuilder<Body> {
    pub async fn send(self) -> Result<()> {
        let url = self.client.base_url.join("/api/v3/write_lp")?;
        let req = WriteRequest::from(&self);
        let mut b = self.client.http_client.post(url).query(&req);
        if let Some(token) = self.client.auth_header {
            b = b.bearer_auth(token.expose_secret());
        }
        // TODO - handle the response, return to caller, etc.
        b.body(self.body).send().await.map_err(Error::WriteLpSend)?;
        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub struct NoBody;

#[cfg(test)]
mod tests {
    use mockito::{Matcher, Server};

    use crate::Client;

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
                Matcher::UrlEncoded("precision".into(), "milli".into()),
                Matcher::UrlEncoded("db".into(), db.into()),
                Matcher::UrlEncoded("accept_partial".into(), "true".into()),
            ]))
            .match_body(body)
            .create_async()
            .await;

        let client = Client::new(mock_server.url(), reqwest::Client::new())
            .expect("create client")
            .with_auth_header(token);

        let _ = client
            .api_v3_write_lp(db)
            .precision_milli()
            .accept_partial(true)
            .body(body)
            .send()
            .await
            .expect("send write_lp request");

        mock.assert_async().await;
    }

    #[tokio::test]
    async fn api_v3_write_lp_from_file() {
        todo!("invoke /api/v3/write_lp API by streaming an .lp file in the body");
    }
}
