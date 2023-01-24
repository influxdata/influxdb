use std::{collections::HashMap, fmt::Display, sync::Mutex};

use async_trait::async_trait;
use data_types::PartitionId;

use super::PartitionErrorSink;

#[derive(Debug, Default)]
pub struct MockPartitionErrorSink {
    last: Mutex<HashMap<PartitionId, String>>,
}

impl MockPartitionErrorSink {
    #[allow(dead_code)] // not used anywhere
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)] // not used anywhere
    pub fn errors(&self) -> HashMap<PartitionId, String> {
        self.last.lock().expect("not poisoned").clone()
    }
}

impl Display for MockPartitionErrorSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl PartitionErrorSink for MockPartitionErrorSink {
    async fn record(&self, partition: PartitionId, e: Box<dyn std::error::Error + Send + Sync>) {
        self.last
            .lock()
            .expect("not poisoned")
            .insert(partition, e.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(MockPartitionErrorSink::new().to_string(), "mock",);
    }

    #[tokio::test]
    async fn test_record() {
        let sink = MockPartitionErrorSink::new();

        assert_eq!(sink.errors(), HashMap::default(),);

        sink.record(PartitionId::new(1), "msg 1".into()).await;
        sink.record(PartitionId::new(2), "msg 2".into()).await;
        sink.record(PartitionId::new(1), "msg 3".into()).await;

        assert_eq!(
            sink.errors(),
            HashMap::from([
                (PartitionId::new(1), String::from("msg 3")),
                (PartitionId::new(2), String::from("msg 2")),
            ]),
        );
    }
}
