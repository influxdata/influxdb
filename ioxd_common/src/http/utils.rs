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
            let content_encoding = content_encoding
                .to_str()
                .context(ReadingHeaderAsUtf8Snafu {
                    header_name: header_name.as_str(),
                })?;
            match content_encoding {
                "gzip" => true,
                "identity" => false,
                _ => InvalidContentEncodingSnafu { content_encoding }.fail()?,
            }
        }
    };

    let mut payload = req.into_body();

    let mut body = BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk.context(ClientHangupSnafu)?;
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
        //
        // In order to detect if the entire stream has been read, or truncated,
        // read an extra byte beyond the limit and check the resulting data
        // length - see test_read_gzipped_body_truncation.
        let mut decoder = decoder.take(max_size as u64 + 1);
        let mut decoded_data = Vec::new();
        decoder
            .read_to_end(&mut decoded_data)
            .context(ReadingBodyAsGzipSnafu)?;

        // If the length is max_size+1, the body is at least max_size+1 bytes in
        // length, and possibly longer, but truncated.
        if decoded_data.len() > max_size {
            return Err(ParseBodyError::RequestSizeExceeded {
                max_body_size: max_size,
            });
        }

        Ok(decoded_data.into())
    } else {
        Ok(body)
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, iter};

    use flate2::{write::GzEncoder, Compression};
    use futures::stream;
    use http::HeaderValue;
    use hyper::Request;
    use tokio_stream::wrappers::ReceiverStream;

    use crate::http::test_utils::TEST_MAX_REQUEST_SIZE;

    use super::*;

    const MAX_BYTES: usize = 1024;

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

    #[tokio::test]
    async fn test_read_gzipped_body_truncation() {
        // Generate a LP string in the form of:
        //
        //  bananas,A=AAAAAAAAAA(repeated)... B=42
        //                                  ^
        //                                  |
        //                         MAX_BYTES boundary
        //
        // So that reading MAX_BYTES number of bytes produces the string:
        //
        //  bananas,A=AAAAAAAAAA(repeated)...
        //
        // Effectively trimming off the " B=42" suffix.
        let body = "bananas,A=";
        let body = iter::once(body)
            .chain(iter::repeat("A").take(MAX_BYTES - body.len()))
            .chain(iter::once(" B=42\n"))
            .flat_map(|s| s.bytes())
            .collect::<Vec<u8>>();

        // Apply gzip compression to the body
        let mut e = GzEncoder::new(Vec::new(), Compression::default());
        e.write_all(&body).unwrap();
        let body = e.finish().expect("failed to compress test body");

        let body: Result<_, std::io::Error> = Ok(body);
        let body = Body::wrap_stream(stream::iter(iter::once(body)));

        let mut request = Request::builder()
            .uri("https://explosions.example/")
            .body(body)
            .unwrap();

        request
            .headers_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));

        let got = parse_body(request, MAX_BYTES).await;

        assert!(matches!(
            got,
            Err(ParseBodyError::RequestSizeExceeded { .. })
        ));
    }

    #[tokio::test]
    async fn test_accept_identity_content_encoding() {
        let request = Request::builder()
            .uri("https://explosions.example/")
            .header("Content-Encoding", "identity")
            .body(Body::from("bananas,A=12"))
            .unwrap();

        let got = parse_body(request, MAX_BYTES).await;
        assert!(got.is_ok());
    }
}
