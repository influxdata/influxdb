use data_types::NamespaceId;
use trace::ctx::SpanContext;

use super::write::WriteOperation;

/// The set of operations which the ingester can derive and process from wire
/// requests
#[derive(Clone, Debug)]
pub enum IngestOp {
    /// A write for ingest
    Write(WriteOperation),
}

impl IngestOp {
    /// The namespace which the ingest operation is for
    pub fn namespace(&self) -> NamespaceId {
        match self {
            Self::Write(w) => w.namespace(),
        }
    }

    /// An optional tracing context associated with the [`IngestOp`]
    pub fn span_context(&self) -> Option<&SpanContext> {
        match self {
            Self::Write(w) => w.span_context(),
        }
    }
}
