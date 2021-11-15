use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use http::header::CONTENT_ENCODING;
use hyper::Body;
use snafu::{ResultExt, Snafu};

use super::error::{HttpApiError, HttpApiErrorExt, HttpApiErrorSource};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
pub enum ParseBodyError {
    #[snafu(display("Body exceeds limit of {} bytes", max_body_size))]
    RequestSizeExceeded { max_body_size: usize },

    #[snafu(display("Invalid content encoding: {}", content_encoding))]
    InvalidContentEncoding { content_encoding: String },

    #[snafu(display("Error reading request header '{}' as Utf8: {}", header_name, source))]
    ReadingHeaderAsUtf8 {
        header_name: String,
        source: hyper::header::ToStrError,
    },

    #[snafu(display("Error decompressing body as gzip: {}", source))]
    ReadingBodyAsGzip { source: std::io::Error },

    #[snafu(display("Client hung up while sending body: {}", source))]
    ClientHangup { source: hyper::Error },
}

impl HttpApiErrorSource for ParseBodyError {
    fn to_http_api_error(&self) -> HttpApiError {
        match self {
            e @ Self::RequestSizeExceeded { .. } => e.invalid(),
            e @ Self::InvalidContentEncoding { .. } => e.invalid(),
            e @ Self::ReadingHeaderAsUtf8 { .. } => e.invalid(),
            e @ Self::ReadingBodyAsGzip { .. } => e.invalid(),
            e @ Self::ClientHangup { .. } => e.invalid(),
        }
    }
}

/// Parse the request's body into raw bytes, applying size limits and
/// content encoding as needed.
pub async fn parse_body(
    req: hyper::Request<Body>,
    max_size: usize,
) -> Result<Bytes, ParseBodyError> {
    // clippy says the const needs to be assigned to a local variable:
    // error: a `const` item with interior mutability should not be borrowed
    let header_name = CONTENT_ENCODING;
    let ungzip = match req.headers().get(&header_name) {
        None => false,
        Some(content_encoding) => {
            let content_encoding = content_encoding.to_str().context(ReadingHeaderAsUtf8 {
                header_name: header_name.as_str(),
            })?;
            match content_encoding {
                "gzip" => true,
                _ => InvalidContentEncoding { content_encoding }.fail()?,
            }
        }
    };

    let mut payload = req.into_body();

    let mut body = BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk.context(ClientHangup)?;
        // limit max size of in-memory payload
        if (body.len() + chunk.len()) > max_size {
            return Err(ParseBodyError::RequestSizeExceeded {
                max_body_size: max_size,
            });
        }
        body.extend_from_slice(&chunk);
    }
    let body = body.freeze();

    // apply any content encoding needed
    if ungzip {
        use std::io::Read;
        let decoder = flate2::read::GzDecoder::new(&body[..]);

        // Read at most max_size bytes to prevent a decompression bomb based
        // DoS.
        let mut decoder = decoder.take(max_size as u64);
        let mut decoded_data = Vec::new();
        decoder
            .read_to_end(&mut decoded_data)
            .context(ReadingBodyAsGzip)?;
        Ok(decoded_data.into())
    } else {
        Ok(body)
    }
}

#[cfg(test)]
mod tests {
    use hyper::Request;
    use tokio_stream::wrappers::ReceiverStream;

    use crate::influxdb_ioxd::http::test_utils::TEST_MAX_REQUEST_SIZE;

    use super::*;

    #[tokio::test]
    async fn client_hangup_during_parse() {
        #[derive(Debug, Snafu)]
        enum TestError {
            #[snafu(display("Blarg Error"))]
            Blarg {},
        }

        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let body = Body::wrap_stream(ReceiverStream::new(rx));

        tx.send(Ok("foo")).await.unwrap();
        tx.send(Err(TestError::Blarg {})).await.unwrap();

        let request = Request::builder()
            .uri("https://ye-olde-non-existent-server/")
            .body(body)
            .unwrap();

        let parse_result = parse_body(request, TEST_MAX_REQUEST_SIZE)
            .await
            .unwrap_err();
        assert_eq!(
            parse_result.to_string(),
            "Client hung up while sending body: error reading a body from connection: Blarg Error"
        );
    }
}
