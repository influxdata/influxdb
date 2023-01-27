use std::{collections::HashMap, fmt::Display, sync::Mutex};

use async_trait::async_trait;
use data_types::PartitionId;

use super::PartitionDoneSink;

#[derive(Debug, Default)]
pub struct MockPartitionDoneSink {
    last: Mutex<HashMap<PartitionId, Result<(), String>>>,
}

impl MockPartitionDoneSink {
    pub fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)] // not used anywhere
    pub fn errors(&self) -> HashMap<PartitionId, Result<(), String>> {
        self.last.lock().expect("not poisoned").clone()
    }
}

impl Display for MockPartitionDoneSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl PartitionDoneSink for MockPartitionDoneSink {
    async fn record(
        &self,
        partition: PartitionId,
        res: Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) {
        self.last
            .lock()
            .expect("not poisoned")
            .insert(partition, res.map_err(|e| e.to_string()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(MockPartitionDoneSink::new().to_string(), "mock",);
    }

    #[tokio::test]
    async fn test_record() {
        let sink = MockPartitionDoneSink::new();

        assert_eq!(sink.errors(), HashMap::default(),);

        sink.record(PartitionId::new(1), Err("msg 1".into())).await;
        sink.record(PartitionId::new(2), Err("msg 2".into())).await;
        sink.record(PartitionId::new(1), Err("msg 3".into())).await;
        sink.record(PartitionId::new(3), Ok(())).await;

        assert_eq!(
            sink.errors(),
            HashMap::from([
                (PartitionId::new(1), Err(String::from("msg 3"))),
                (PartitionId::new(2), Err(String::from("msg 2"))),
                (PartitionId::new(3), Ok(())),
            ]),
        );
    }
}
