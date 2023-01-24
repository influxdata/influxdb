//! Error handling.

use datafusion::{arrow::error::ArrowError, error::DataFusionError, parquet::errors::ParquetError};
use object_store::Error as ObjectStoreError;
use std::{error::Error, fmt::Display, sync::Arc};
use tokio::time::error::Elapsed;

/// What kind of error did we occur during compaction?
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ErrorKind {
    /// Could not access the object store.
    ///
    /// This may happen during K8s pod boot, e.g. when kube2iam is not started yet.
    ///
    /// See <https://github.com/influxdata/idpe/issues/16984>.
    ObjectStore,

    /// We ran out of memory (OOM).
    ///
    /// The compactor shall retry (if possible) with a smaller set of files.
    OutOfMemory,

    /// Partition took too long.
    Timeout,

    /// Unknown/unexpected error.
    ///
    /// This will likely mark the affected partition as "skipped" and the compactor will no longer touch it.
    Unknown,
}

impl ErrorKind {
    /// Return all variants.
    pub fn variants() -> &'static [Self] {
        &[
            Self::ObjectStore,
            Self::OutOfMemory,
            Self::Timeout,
            Self::Unknown,
        ]
    }

    /// Return static name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::ObjectStore => "object_store",
            Self::OutOfMemory => "out_of_memory",
            Self::Timeout => "timeout",
            Self::Unknown => "unknown",
        }
    }
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Extension trait for normal errors to support classification.
pub trait ErrorKindExt {
    /// Classify error.
    fn classify(&self) -> ErrorKind;
}

impl ErrorKindExt for ArrowError {
    fn classify(&self) -> ErrorKind {
        if let Some(source) = self.source() {
            return source.classify();
        }

        match self {
            Self::ExternalError(e) => e.classify(),
            // ArrowError is also mostly broken for many variants
            e => try_recover_unknown(e),
        }
    }
}

impl ErrorKindExt for DataFusionError {
    fn classify(&self) -> ErrorKind {
        match self.find_root() {
            Self::ArrowError(e) => e.classify(),
            Self::External(e) => e.classify(),
            Self::ObjectStore(e) => e.classify(),
            Self::ParquetError(e) => e.classify(),
            Self::ResourcesExhausted(_) => ErrorKind::OutOfMemory,
            e => try_recover_unknown(e),
        }
    }
}

impl ErrorKindExt for Elapsed {
    fn classify(&self) -> ErrorKind {
        if let Some(source) = self.source() {
            return source.classify();
        }

        ErrorKind::Timeout
    }
}

impl ErrorKindExt for ObjectStoreError {
    fn classify(&self) -> ErrorKind {
        if let Some(source) = self.source() {
            return source.classify();
        }

        ErrorKind::ObjectStore
    }
}

impl ErrorKindExt for ParquetError {
    fn classify(&self) -> ErrorKind {
        if let Some(source) = self.source() {
            return source.classify();
        }

        // ParquetError is completely broken and doesn't contain proper error chains
        try_recover_unknown(self)
    }
}

macro_rules! dispatch_body {
    ($self:ident) => {
        if let Some(e) = $self.downcast_ref::<ArrowError>() {
            e.classify()
        } else if let Some(e) = $self.downcast_ref::<Arc<ArrowError>>() {
            e.as_ref().classify()
        } else if let Some(e) = $self.downcast_ref::<Box<ArrowError>>() {
            e.as_ref().classify()
        } else if let Some(e) = $self.downcast_ref::<DataFusionError>() {
            e.classify()
        } else if let Some(e) = $self.downcast_ref::<Arc<DataFusionError>>() {
            e.as_ref().classify()
        } else if let Some(e) = $self.downcast_ref::<Box<DataFusionError>>() {
            e.as_ref().classify()
        } else if let Some(e) = $self.downcast_ref::<Elapsed>() {
            e.classify()
        } else if let Some(e) = $self.downcast_ref::<Arc<Elapsed>>() {
            e.as_ref().classify()
        } else if let Some(e) = $self.downcast_ref::<Box<Elapsed>>() {
            e.as_ref().classify()
        } else if let Some(e) = $self.downcast_ref::<ObjectStoreError>() {
            e.classify()
        } else if let Some(e) = $self.downcast_ref::<Arc<ObjectStoreError>>() {
            e.as_ref().classify()
        } else if let Some(e) = $self.downcast_ref::<Box<ObjectStoreError>>() {
            e.as_ref().classify()
        } else if let Some(e) = $self.downcast_ref::<ParquetError>() {
            e.classify()
        } else if let Some(e) = $self.downcast_ref::<Arc<ParquetError>>() {
            e.as_ref().classify()
        } else if let Some(e) = $self.downcast_ref::<Box<ParquetError>>() {
            e.as_ref().classify()
        } else if let Some(e) = $self.downcast_ref::<Arc<dyn std::error::Error>>() {
            e.as_ref().classify()
        } else if let Some(e) = $self.downcast_ref::<Arc<dyn std::error::Error + Send + Sync>>() {
            e.as_ref().classify()
        } else {
            try_recover_unknown($self)
        }
    };
}

impl ErrorKindExt for &(dyn std::error::Error + 'static) {
    fn classify(&self) -> ErrorKind {
        if let Some(source) = self.source() {
            return source.classify();
        }

        dispatch_body!(self)
    }
}

impl ErrorKindExt for &(dyn std::error::Error + Send + Sync + 'static) {
    fn classify(&self) -> ErrorKind {
        if let Some(source) = self.source() {
            return source.classify();
        }

        dispatch_body!(self)
    }
}

impl ErrorKindExt for Arc<dyn std::error::Error> {
    fn classify(&self) -> ErrorKind {
        if let Some(source) = self.source() {
            return source.classify();
        }

        self.as_ref().classify()
    }
}

impl ErrorKindExt for Arc<dyn std::error::Error + Send + Sync> {
    fn classify(&self) -> ErrorKind {
        if let Some(source) = self.source() {
            return source.classify();
        }

        self.as_ref().classify()
    }
}

impl ErrorKindExt for Box<dyn std::error::Error> {
    fn classify(&self) -> ErrorKind {
        if let Some(source) = self.source() {
            return source.classify();
        }

        self.as_ref().classify()
    }
}

impl ErrorKindExt for Box<dyn std::error::Error + Send + Sync> {
    fn classify(&self) -> ErrorKind {
        if let Some(source) = self.source() {
            return source.classify();
        }

        self.as_ref().classify()
    }
}

fn try_recover_unknown<E>(e: &E) -> ErrorKind
where
    E: std::error::Error,
{
    let s = e.to_string();
    if s.contains("Object Store error") {
        return ErrorKind::ObjectStore;
    }
    if s.contains("Resources exhausted") {
        return ErrorKind::OutOfMemory;
    }

    ErrorKind::Unknown
}
