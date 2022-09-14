use std::{
    sync::Arc,
    task::{Poll, Waker},
};

use data_types::ShardIndex;
use futures::Future;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use pin_project::pin_project;

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum PoisonPill {
    LifecyclePanic,
    LifecycleExit,
    StreamPanic(ShardIndex),
    StreamExit(ShardIndex),
}

#[derive(Debug)]
struct PoisonCabinetInner {
    pills: Vec<PoisonPill>,
    wait_list: Vec<Waker>,
}

impl PoisonCabinetInner {
    /// Register a waker to be notified when a new pill is added
    fn register_waker(&mut self, waker: &Waker) {
        for wait_waker in &self.wait_list {
            if wait_waker.will_wake(waker) {
                return;
            }
        }
        self.wait_list.push(waker.clone())
    }
}

#[derive(Debug)]
pub struct PoisonCabinet {
    inner: Arc<RwLock<PoisonCabinetInner>>,
}

impl PoisonCabinet {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(PoisonCabinetInner {
                pills: Vec::with_capacity(0),
                wait_list: Vec::with_capacity(0),
            })),
        }
    }

    #[allow(dead_code)]
    pub fn add(&self, pill: PoisonPill) {
        let mut inner = self.inner.write();
        inner.pills.push(pill);

        for waker in inner.wait_list.drain(..) {
            waker.wake()
        }
    }

    pub fn contains(&self, pill: &PoisonPill) -> bool {
        let inner = self.inner.read();

        inner.pills.contains(pill)
    }

    #[allow(dead_code)]
    pub fn wait_for(&self, pill: PoisonPill) -> PoisonWait {
        PoisonWait {
            pill,
            inner: Arc::clone(&self.inner),
        }
    }
}

#[pin_project]
pub struct PoisonWait {
    pill: PoisonPill,
    inner: Arc<RwLock<PoisonCabinetInner>>,
}

impl Future for PoisonWait {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let inner = this.inner.upgradable_read();
        if inner.pills.contains(this.pill) {
            return Poll::Ready(());
        }

        let mut inner = RwLockUpgradableReadGuard::upgrade(inner);
        inner.register_waker(cx.waker());
        Poll::Pending
    }
}
