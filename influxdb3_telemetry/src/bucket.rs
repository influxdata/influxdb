use observability_deps::tracing::debug;

use crate::stats::Stats;

#[derive(Debug, Default)]
pub(crate) struct EventsBucket {
    pub writes: PerMinuteWrites,
    pub queries: PerMinuteReads,

    pub(crate) num_writes: usize,
    pub(crate) num_queries: usize,
}

impl EventsBucket {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_write_sample(&mut self, num_lines: usize, size_bytes: usize) {
        self.writes.add_sample(num_lines, size_bytes);
        self.num_writes += 1;
    }

    pub fn update_num_queries(&mut self) {
        self.queries.add_sample();
        self.num_queries += 1;
    }

    pub(crate) fn reset(&mut self) {
        *self = EventsBucket::default();
        debug!(
            write_bucket = ?self,
            "Resetting write bucket"
        );
    }
}

#[derive(Debug, Default)]
pub(crate) struct PerMinuteWrites {
    pub lines: Stats<u64>,
    pub size_bytes: Stats<u64>,
}

impl PerMinuteWrites {
    pub fn add_sample(&mut self, num_lines: usize, size_bytes: usize) -> Option<()> {
        self.lines.update(num_lines as u64);
        self.size_bytes.update(size_bytes as u64)?;
        Some(())
    }
}

#[derive(Debug, Default)]
pub(crate) struct PerMinuteReads {
    pub num_queries: Stats<u64>,
}

impl PerMinuteReads {
    pub fn add_sample(&mut self) -> Option<()> {
        self.num_queries.update(1);
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use observability_deps::tracing::info;

    use super::*;

    #[test_log::test(test)]
    fn test_bucket_for_writes() {
        let mut bucket = EventsBucket::new();
        info!(bucket = ?bucket, "Events bucket empty");
        bucket.add_write_sample(1, 1);
        info!(bucket = ?bucket, "Events bucket added 1");
        bucket.add_write_sample(2, 2);
        info!(bucket = ?bucket, "Events bucket added 2");
        bucket.add_write_sample(3, 3);
        info!(bucket = ?bucket, "Events bucket added 3");
        bucket.add_write_sample(4, 4);
        info!(bucket = ?bucket, "Events bucket added 4");

        assert_eq!(4, bucket.num_writes);
        assert_eq!(1, bucket.writes.lines.avg);
        assert_eq!(1, bucket.writes.size_bytes.avg);

        assert_eq!(1, bucket.writes.lines.min);
        assert_eq!(1, bucket.writes.size_bytes.min);

        assert_eq!(4, bucket.writes.lines.max);
        assert_eq!(4, bucket.writes.size_bytes.max);

        bucket.add_write_sample(20, 20);
        info!(bucket = ?bucket, "Events bucket added 20");
        assert_eq!(4, bucket.writes.lines.avg);
        assert_eq!(4, bucket.writes.size_bytes.avg);

        bucket.update_num_queries();
        bucket.update_num_queries();
        bucket.update_num_queries();
        assert_eq!(3, bucket.num_queries);

        bucket.reset();
        assert_eq!(0, bucket.writes.lines.min);
        assert_eq!(0, bucket.writes.lines.max);
        assert_eq!(0, bucket.writes.lines.avg);
        assert_eq!(0, bucket.writes.size_bytes.min);
        assert_eq!(0, bucket.writes.size_bytes.max);
        assert_eq!(0, bucket.writes.size_bytes.avg);
        assert_eq!(0, bucket.num_writes);
        assert_eq!(0, bucket.num_queries);
    }
}
