use super::Error;
use super::HttpApi;
use super::QueryExecutor;
use hyper::Body;
use hyper::Request;
use hyper::Response;
use influxdb3_write::WriteBuffer;
use iox_time::TimeProvider;

impl<W, Q, T> HttpApi<W, Q, T>
where
    W: WriteBuffer,
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
