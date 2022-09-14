use std::sync::Arc;

use tokio::sync::{Barrier, Mutex};

#[derive(Debug, Clone)]
/// Synchronizes waiting on some test event
struct EventBarrier {
    before: Arc<Barrier>,
    after: Arc<Barrier>,
}

impl EventBarrier {
    fn new() -> Self {
        Self {
            before: Arc::new(Barrier::new(2)),
            after: Arc::new(Barrier::new(2)),
        }
    }
}

/// Testing structure that allows coordinating the timing between an
/// async write and a test.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct TestTriggers {
    write: Mutex<Option<EventBarrier>>,
}

#[cfg(test)]
impl TestTriggers {
    pub(crate) fn new() -> Self {
        Self {
            write: Mutex::new(None),
        }
    }

    /// Note that the test should pause after the next write
    pub(crate) async fn enable_pause_after_write(&self) {
        let mut write = self.write.lock().await;
        assert!(write.is_none(), "previously configured pause");
        *write = Some(EventBarrier::new())
    }

    /// Waits until the write has been paused
    pub(crate) async fn wait_for_pause_after_write(&self) {
        self.write()
            .await
            .expect("write barrier not configured")
            .before
            .wait()
            .await;
    }

    /// Note that a write has been done
    pub(crate) async fn on_write(&self) {
        if let Some(write) = self.write().await {
            write.before.wait().await;
            write.after.wait().await;
        }
    }

    /// Release the waiting write and clear the pause

    pub(crate) async fn release_pause_after_write(&self) {
        // Hold lock the entire time so next write can't accidentally try and wait
        let mut write = self.write.lock().await;
        write
            .as_ref()
            .expect("write barrier not configured")
            .after
            .wait()
            .await;

        *write = None;
    }

    /// return the currently configured write event barrier
    async fn write(&self) -> Option<EventBarrier> {
        self.write.lock().await.as_ref().cloned()
    }
}
