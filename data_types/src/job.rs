use generated_types::influxdata::iox::management::v1 as management;

/// Metadata associated with a set of background tasks
/// Used in combination with TrackerRegistry
#[derive(Debug, Clone)]
pub enum Job {
    PersistSegment { writer_id: u32, segment_id: u64 },
    Dummy { nanos: Vec<u64> },
}

impl From<Job> for management::operation_metadata::Job {
    fn from(job: Job) -> Self {
        match job {
            Job::PersistSegment {
                writer_id,
                segment_id,
            } => Self::PersistSegment(management::PersistSegment {
                writer_id,
                segment_id,
            }),
            Job::Dummy { nanos } => Self::Dummy(management::Dummy { nanos }),
        }
    }
}
