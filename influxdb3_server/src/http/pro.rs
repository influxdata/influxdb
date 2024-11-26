use super::Error;
use super::HttpApi;
use hyper::Body;
use hyper::Request;
use hyper::Response;
use iox_time::TimeProvider;

impl<T> HttpApi<T>
where
    T: TimeProvider,
{
    pub async fn pro_echo(&self, req: Request<Body>) -> Result<Response<Body>, Error> {
        let body = req.into_body();
        Response::builder()
            .status(200)
            .body(body)
            .map_err(Into::into)
    }
}
