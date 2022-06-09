use super::sink_instrumentation::WatermarkFetcher;

#[derive(Debug, Copy, Clone)]
pub struct MockWatermarkFetcher {
    ret: Option<i64>,
}

impl MockWatermarkFetcher {
    pub fn new(ret: Option<i64>) -> Self {
        Self { ret }
    }
}

impl WatermarkFetcher for MockWatermarkFetcher {
    fn watermark(&self) -> Option<i64> {
        self.ret
    }
}
