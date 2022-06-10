/// This is a NoOp structure used in production builds. The Test
/// version of this structure has additional features used for
/// synchronized testing.
#[cfg(not(test))]
#[derive(Debug)]
pub(crate) struct TestTriggers {}

#[cfg(not(test))]
impl TestTriggers {
    pub(crate) fn new() -> Self {
        Self {}
    }

    /// Note that a write has been done
    pub(crate) async fn on_write(&self) {}
}
