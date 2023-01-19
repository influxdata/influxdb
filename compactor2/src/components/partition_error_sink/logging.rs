use std::fmt::Display;

use async_trait::async_trait;
use data_types::PartitionId;
use observability_deps::tracing::error;

use super::PartitionErrorSink;

#[derive(Debug)]
pub struct LoggingPartitionErrorSinkWrapper<T>
where
    T: PartitionErrorSink,
{
    inner: T,
}

impl<T> LoggingPartitionErrorSinkWrapper<T>
where
    T: PartitionErrorSink,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> Display for LoggingPartitionErrorSinkWrapper<T>
where
    T: PartitionErrorSink,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logging({})", self.inner)
    }
}

#[async_trait]
impl<T> PartitionErrorSink for LoggingPartitionErrorSinkWrapper<T>
where
    T: PartitionErrorSink,
{
    async fn record(&self, partition: PartitionId, msg: &str) {
        error!(
            e = msg,
            partition_id = partition.get(),
            "Error while compacting partition",
        );
        self.inner.record(partition, msg).await;
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use test_helpers::tracing::TracingCapture;

    use crate::components::partition_error_sink::mock::MockPartitionErrorSink;

    use super::*;

    #[test]
    fn test_display() {
        let sink = LoggingPartitionErrorSinkWrapper::new(MockPartitionErrorSink::new());
        assert_eq!(sink.to_string(), "logging(mock)");
    }

    #[tokio::test]
    async fn test_record() {
        let inner = Arc::new(MockPartitionErrorSink::new());
        let sink = LoggingPartitionErrorSinkWrapper::new(Arc::clone(&inner));

        let capture = TracingCapture::new();

        sink.record(PartitionId::new(1), "msg 1").await;
        sink.record(PartitionId::new(2), "msg 2").await;
        sink.record(PartitionId::new(1), "msg 3").await;

        assert_eq!(
            capture.to_string(),
            "level = ERROR; message = Error while compacting partition; e = \"msg 1\"; partition_id = 1; \n\
level = ERROR; message = Error while compacting partition; e = \"msg 2\"; partition_id = 2; \n\
level = ERROR; message = Error while compacting partition; e = \"msg 3\"; partition_id = 1; ",
        );

        assert_eq!(
            inner.errors(),
            HashMap::from([
                (PartitionId::new(1), String::from("msg 3")),
                (PartitionId::new(2), String::from("msg 2")),
            ]),
        );
    }
}
