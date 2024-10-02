use crate::{
    bucket::EventsBucket,
    stats::{RollingStats, Stats},
};

#[derive(Debug, Default)]
pub(crate) struct Writes {
    pub lines: RollingStats<u64>,
    pub size_bytes: RollingStats<u64>,
    // num_writes is just Stats and not RollingStats as we don't
    // aggregate num_writes at the per minute interval, it can
    // just be taken from the events bucket.
    pub num_writes: Stats<u64>,
}

impl Writes {
    pub fn add_sample(&mut self, events_bucket: &EventsBucket) -> Option<()> {
        self.lines.update(&events_bucket.writes.lines);
        self.size_bytes.update(&events_bucket.writes.size_bytes);
        self.num_writes.update(events_bucket.num_writes as u64);
        Some(())
    }

    pub fn reset(&mut self) {
        self.lines.reset();
        self.size_bytes.reset();
    }
}

#[derive(Debug, Default)]
pub(crate) struct Queries {
    // We don't aggregate the num_queries at 1 min intervals
    pub num_queries: Stats<u64>,
}

impl Queries {
    pub fn add_sample(&mut self, events_bucket: &EventsBucket) -> Option<()> {
        self.num_queries.update(events_bucket.num_queries as u64);
        Some(())
    }

    pub fn reset(&mut self) {
        self.num_queries.reset();
    }
}

#[derive(Debug, Default)]
pub(crate) struct Cpu {
    pub utilization: Stats<f32>,
}

impl Cpu {
    pub fn add_sample(&mut self, cpu_used: f32) -> Option<()> {
        self.utilization.update(cpu_used)
    }

    pub fn reset(&mut self) {
        self.utilization.reset();
    }
}

#[derive(Debug, Default)]
pub(crate) struct Memory {
    pub usage: Stats<u64>,
}

impl Memory {
    pub fn add_sample(&mut self, mem_used: u64) -> Option<()> {
        self.usage.update(mem_used)
    }

    pub fn reset(&mut self) {
        self.usage.reset();
    }
}
