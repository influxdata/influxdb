use std::{collections::HashMap, fmt::Display};

use async_trait::async_trait;
use data_types::PartitionId;
use parking_lot::Mutex;

use super::{DynError, Error, PartitionDoneSink};

/// Mock for [`PartitionDoneSink`].
#[derive(Debug, Default)]
pub(crate) struct MockPartitionDoneSink {
    last: Mutex<HashMap<PartitionId, Result<(), String>>>,
}

impl MockPartitionDoneSink {
    /// Create new mock.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Get the last recorded results.
    #[cfg(test)]
    pub(crate) fn results(&self) -> HashMap<PartitionId, Result<(), String>> {
        self.last.lock().clone()
    }
}

impl Display for MockPartitionDoneSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl PartitionDoneSink for MockPartitionDoneSink {
    async fn record(&self, partition: PartitionId, res: Result<(), DynError>) -> Result<(), Error> {
        self.last
            .lock()
            .insert(partition, res.map_err(|e| e.to_string()));
        Ok(())
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

        assert_eq!(sink.results(), HashMap::default(),);

        sink.record(PartitionId::new(1), Err("msg 1".into()))
            .await
            .expect("record failed");
        sink.record(PartitionId::new(2), Err("msg 2".into()))
            .await
            .expect("record failed");
        sink.record(PartitionId::new(1), Err("msg 3".into()))
            .await
            .expect("record failed");
        sink.record(PartitionId::new(3), Ok(()))
            .await
            .expect("record failed");

        assert_eq!(
            sink.results(),
            HashMap::from([
                (PartitionId::new(1), Err(String::from("msg 3"))),
                (PartitionId::new(2), Err(String::from("msg 2"))),
                (PartitionId::new(3), Ok(())),
            ]),
        );
    }
}
