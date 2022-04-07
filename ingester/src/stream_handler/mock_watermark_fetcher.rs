use super::sink_instrumentation::WatermarkFetcher;

#[derive(Debug, Copy, Clone)]
pub struct MockWatermarkFetcher {
    ret: Option<u64>,
}

impl MockWatermarkFetcher {
    pub fn new(ret: Option<u64>) -> Self {
        Self { ret }
    }
}

impl WatermarkFetcher for MockWatermarkFetcher {
    fn watermark(&self) -> Option<u64> {
        self.ret
    }
}
