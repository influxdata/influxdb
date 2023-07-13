use data_types::{sequence_number_set::SequenceNumberSet, NamespaceId};
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

    /// Construct a new [`SequenceNumberSet`] containing all the sequence
    /// numbers in this op.
    pub fn sequence_number_set(&self) -> SequenceNumberSet {
        match self {
            Self::Write(w) => w
                .tables()
                .map(|(_, t)| t.partitioned_data().sequence_number())
                .collect(),
        }
    }
}
