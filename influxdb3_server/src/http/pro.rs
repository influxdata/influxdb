use super::Error;
use super::HttpApi;
use super::QueryExecutor;
use hyper::Body;
use hyper::Request;
use hyper::Response;
use iox_time::TimeProvider;

impl<Q, T> HttpApi<Q, T>
where
    Q: QueryExecutor,
    T: TimeProvider,
    Error: From<<Q as QueryExecutor>::Error>,
{
    pub async fn pro_echo(&self, req: Request<Body>) -> Result<Response<Body>, Error> {
        let body = req.into_body();
        Response::builder()
            .status(200)
            .body(body)
            .map_err(Into::into)
    }
}
