use observability_deps::tracing::debug;

use crate::stats::Stats;

/// This bucket holds all the event metrics like reads/writes. As
/// new read or write comes in we update the stats for them. Then once
/// a minute when a sample is taken these metrics are reset to collect
/// the events again till next sample is taken.
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
    pub total_lines: u64,
    pub size_bytes: Stats<u64>,
    pub total_size_bytes: u64,
}

impl PerMinuteWrites {
    pub fn add_sample(&mut self, num_lines: usize, size_bytes: usize) -> Option<()> {
        let new_num_lines = num_lines as u64;
        self.lines.update(new_num_lines);
        self.total_lines += new_num_lines;

        let new_size_bytes = size_bytes as u64;
        self.size_bytes.update(new_size_bytes)?;
        self.total_size_bytes += new_size_bytes;
        Some(())
    }
}

#[derive(Debug, Default)]
pub(crate) struct PerMinuteReads {
    pub num_queries: Stats<u64>,
    pub total_num_queries: u64,
}

impl PerMinuteReads {
    pub fn add_sample(&mut self) -> Option<()> {
        self.num_queries.update(1);
        self.total_num_queries += 1;
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
