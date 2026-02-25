use std::{
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use futures::Stream;

#[cfg(test)]
pub(crate) mod test_utils;

#[derive(Debug, Default)]
pub(crate) struct Mailbox {
    counter: AtomicUsize,
    wakers: Mutex<Vec<Waker>>,
}

impl Mailbox {
    /// Notify all notifiers
    pub(crate) fn notify(&self) {
        let mut guard = self.wakers.lock().expect("not poisoned");

        // bump counter AFTER acquiring lock but before notifying wakers
        self.counter.fetch_add(1, Ordering::SeqCst);

        for waker in guard.drain(..) {
            waker.wake();
        }
    }

    /// Notifier.
    pub(crate) fn notifier(self: &Arc<Self>) -> Notifier {
        Notifier {
            mailbox: Arc::downgrade(self),
            counter: 0,
        }
    }
}

/// Notification for certain events.
///
/// If there was an event between this and the previous call, this will return immediately, so the caller
/// will never miss an event.
///
/// Missed/past events are aggregated into a single notification.
#[derive(Debug, Clone)]
pub struct Notifier {
    mailbox: Weak<Mailbox>,
    counter: usize,
}

impl Stream for Notifier {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        let Some(mailbox) = this.mailbox.upgrade() else {
            return Poll::Ready(None);
        };

        let upstream = mailbox.counter.load(Ordering::SeqCst);
        if upstream != this.counter {
            this.counter = upstream;
            Poll::Ready(Some(()))
        } else {
            let mut guard = mailbox.wakers.lock().expect("not poisoned");

            // check upstream again because counter might have changed
            let upstream = mailbox.counter.load(Ordering::SeqCst);
            if upstream != this.counter {
                this.counter = upstream;
                Poll::Ready(Some(()))
            } else {
                guard.push(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use futures_test_utils::AssertFutureExt;

    use super::*;

    #[tokio::test]
    async fn test_mailbox_destruction() {
        let mailbox = Arc::new(Mailbox::default());
        assert_eq!(Arc::strong_count(&mailbox), 1);

        let mut notifier = mailbox.notifier();
        assert_eq!(Arc::strong_count(&mailbox), 1);

        let mut next_fut = notifier.next();
        next_fut.assert_pending().await;

        drop(mailbox);
        assert!(next_fut.poll_timeout().await.is_none());
    }

    #[tokio::test]
    async fn test_notify_before_notifier_creation() {
        let mailbox = Arc::new(Mailbox::default());
        mailbox.notify();

        let mut notifier = mailbox.notifier();
        notifier.next().poll_timeout().await.unwrap();

        let mut next_fut = notifier.next();
        next_fut.assert_pending().await;

        mailbox.notify();
        next_fut.poll_timeout().await.unwrap();
    }

    #[tokio::test]
    async fn test_notify_after_notifier_creation() {
        let mailbox = Arc::new(Mailbox::default());
        let mut notifier = mailbox.notifier();
        let mut next_fut = notifier.next();
        next_fut.assert_pending().await;

        mailbox.notify();
        next_fut.poll_timeout().await.unwrap();

        mailbox.notify();
        notifier.next().poll_timeout().await.unwrap();
    }

    #[tokio::test]
    async fn test_event_compression() {
        let mailbox = Arc::new(Mailbox::default());
        let mut notifier = mailbox.notifier();

        // these two triggers should only create ONE notification
        mailbox.notify();
        mailbox.notify();

        notifier.next().poll_timeout().await.unwrap();
        notifier.next().assert_pending().await;
    }
}
