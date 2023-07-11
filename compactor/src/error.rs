//! Error handling.

use compactor_scheduler::ErrorKind as SchedulerErrorKind;
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

impl From<ErrorKind> for SchedulerErrorKind {
    fn from(e: ErrorKind) -> Self {
        match e {
            ErrorKind::ObjectStore => Self::ObjectStore,
            ErrorKind::OutOfMemory => Self::OutOfMemory,
            ErrorKind::Timeout => Self::Timeout,
            ErrorKind::Unknown => Self::Unknown("".into()),
        }
    }
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// A simple error that can be used to convey information.
#[derive(Debug)]
pub struct SimpleError {
    kind: ErrorKind,
    msg: String,
}

impl SimpleError {
    pub fn new(kind: ErrorKind, msg: impl Into<String>) -> Self {
        Self {
            kind,
            msg: msg.into(),
        }
    }
}

impl Display for SimpleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for SimpleError {}

/// Dynamic error type that is used throughout the stack.
pub type DynError = Box<dyn std::error::Error + Send + Sync>;

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

impl ErrorKindExt for SimpleError {
    fn classify(&self) -> ErrorKind {
        self.kind
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
        } else if let Some(e) = $self.downcast_ref::<SimpleError>() {
            e.classify()
        } else if let Some(e) = $self.downcast_ref::<Arc<SimpleError>>() {
            e.as_ref().classify()
        } else if let Some(e) = $self.downcast_ref::<Box<SimpleError>>() {
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
    if s.contains("deadline has elapsed") {
        return ErrorKind::Timeout;
    }

    ErrorKind::Unknown
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_classify() {
        // simple root causes
        assert_eq!(
            ObjectStoreError::NotImplemented.classify(),
            ErrorKind::ObjectStore,
        );
        assert_eq!(
            DataFusionError::ResourcesExhausted(String::from("foo")).classify(),
            ErrorKind::OutOfMemory,
        );
        assert_eq!(elapsed().classify(), ErrorKind::Timeout,);
        assert_eq!(
            SimpleError::new(ErrorKind::Timeout, "foo").classify(),
            ErrorKind::Timeout,
        );
        assert_eq!(
            Box::<dyn std::error::Error>::from(String::from("foo")).classify(),
            ErrorKind::Unknown,
        );

        // string fallbacks
        assert_eq!(
            Box::<dyn std::error::Error>::from(
                DataFusionError::ObjectStore(ObjectStoreError::NotImplemented).to_string()
            )
            .classify(),
            ErrorKind::ObjectStore,
        );
        assert_eq!(
            Box::<dyn std::error::Error>::from(
                DataFusionError::ResourcesExhausted(String::from("foo")).to_string()
            )
            .classify(),
            ErrorKind::OutOfMemory,
        );
        assert_eq!(
            Box::<dyn std::error::Error>::from(elapsed().to_string()).classify(),
            ErrorKind::Timeout,
        );

        // dyn downcast
        assert_eq!(
            (Box::new(ObjectStoreError::NotImplemented) as Box<dyn std::error::Error>).classify(),
            ErrorKind::ObjectStore,
        );
        assert_eq!(
            (Box::new(DataFusionError::ResourcesExhausted(String::from("foo")))
                as Box<dyn std::error::Error>)
                .classify(),
            ErrorKind::OutOfMemory,
        );
        assert_eq!(
            (Box::new(elapsed()) as Box<dyn std::error::Error>).classify(),
            ErrorKind::Timeout,
        );
        assert_eq!(
            (Box::new(SimpleError::new(ErrorKind::Timeout, "foo")) as Box<dyn std::error::Error>)
                .classify(),
            ErrorKind::Timeout,
        );

        // dyn downcast in Arc
        assert_eq!(
            (Box::new(Arc::new(ObjectStoreError::NotImplemented)) as Box<dyn std::error::Error>)
                .classify(),
            ErrorKind::ObjectStore,
        );
        assert_eq!(
            (Box::new(Arc::new(DataFusionError::ResourcesExhausted(String::from(
                "foo"
            )))) as Box<dyn std::error::Error>)
                .classify(),
            ErrorKind::OutOfMemory,
        );
        assert_eq!(
            (Box::new(Arc::new(elapsed())) as Box<dyn std::error::Error>).classify(),
            ErrorKind::Timeout,
        );
        assert_eq!(
            (Box::new(Arc::new(SimpleError::new(ErrorKind::Timeout, "foo")))
                as Box<dyn std::error::Error>)
                .classify(),
            ErrorKind::Timeout,
        );

        // dyn downcast in Box
        assert_eq!(
            (Box::new(Box::new(ObjectStoreError::NotImplemented)) as Box<dyn std::error::Error>)
                .classify(),
            ErrorKind::ObjectStore,
        );
        assert_eq!(
            (Box::new(Box::new(DataFusionError::ResourcesExhausted(String::from(
                "foo"
            )))) as Box<dyn std::error::Error>)
                .classify(),
            ErrorKind::OutOfMemory,
        );
        assert_eq!(
            (Box::new(Box::new(elapsed())) as Box<dyn std::error::Error>).classify(),
            ErrorKind::Timeout,
        );
        assert_eq!(
            (Box::new(Box::new(SimpleError::new(ErrorKind::Timeout, "foo")))
                as Box<dyn std::error::Error>)
                .classify(),
            ErrorKind::Timeout,
        );
    }

    /// [`Elapsed`] has no public constructor, so we need to trigger it.
    fn elapsed() -> Elapsed {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(0), futures::future::pending::<()>()).await
            })
            .unwrap_err()
    }
}
