use bytes::Bytes;
use http_body::{Body as HttpBody, Frame, SizeHint};
use http_body_util::combinators::UnsyncBoxBody;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::errors::ServiceError;

pin_project! {
    #[project = UnifiedBodyProj]
    pub(crate) enum UnifiedBody {
        Http {
            #[pin]
            body: iox_http_util::ResponseBody
        },
        Grpc {
            #[pin]
            body: UnsyncBoxBody<Bytes, ServiceError>
        },
    }
}

impl HttpBody for UnifiedBody {
    type Data = Bytes;
    type Error = ServiceError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match self.project() {
            UnifiedBodyProj::Http { body } => {
                body.poll_frame(cx).map_err(|e| ServiceError(e.to_string()))
            }
            UnifiedBodyProj::Grpc { body } => body.poll_frame(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Self::Http { body } => body.is_end_stream(),
            Self::Grpc { body } => body.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Http { body } => body.size_hint(),
            Self::Grpc { body } => body.size_hint(),
        }
    }
}
