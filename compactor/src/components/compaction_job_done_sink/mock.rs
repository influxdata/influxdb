use std::{collections::HashMap, fmt::Display, sync::Mutex};

use async_trait::async_trait;
use compactor_scheduler::CompactionJob;

use super::{CompactionJobDoneSink, DynError};

/// Mock for [`CompactionJobDoneSink`].
#[derive(Debug, Default)]
pub struct MockCompactionJobDoneSink {
    last: Mutex<HashMap<CompactionJob, Result<(), String>>>,
}

impl MockCompactionJobDoneSink {
    /// Create new mock.
    #[allow(dead_code)] // used for testing
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the last recorded results.
    #[allow(dead_code)] // used for testing
    pub fn results(&self) -> HashMap<CompactionJob, Result<(), String>> {
        self.last.lock().expect("not poisoned").clone()
    }
}

impl Display for MockCompactionJobDoneSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mock")
    }
}

#[async_trait]
impl CompactionJobDoneSink for MockCompactionJobDoneSink {
    async fn record(&self, job: CompactionJob, res: Result<(), DynError>) -> Result<(), DynError> {
        self.last
            .lock()
            .expect("not poisoned")
            .insert(job, res.map_err(|e| e.to_string()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use data_types::PartitionId;

    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(MockCompactionJobDoneSink::new().to_string(), "mock",);
    }

    #[tokio::test]
    async fn test_record() {
        let sink = MockCompactionJobDoneSink::new();

        assert_eq!(sink.results(), HashMap::default(),);

        let cj_1 = CompactionJob::new(PartitionId::new(1));
        let cj_2 = CompactionJob::new(PartitionId::new(2));
        let cj_3 = CompactionJob::new(PartitionId::new(3));

        sink.record(cj_1.clone(), Err("msg 1".into()))
            .await
            .expect("record failed");
        sink.record(cj_2.clone(), Err("msg 2".into()))
            .await
            .expect("record failed");
        sink.record(cj_1.clone(), Err("msg 3".into()))
            .await
            .expect("record failed");
        sink.record(cj_3.clone(), Ok(()))
            .await
            .expect("record failed");

        assert_eq!(
            sink.results(),
            HashMap::from([
                (cj_1, Err(String::from("msg 3"))),
                (cj_2, Err(String::from("msg 2"))),
                (cj_3, Ok(())),
            ]),
        );
    }
}
