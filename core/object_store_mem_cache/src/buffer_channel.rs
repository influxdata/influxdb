//! Channel to hand a buffer from an inner store to the in-mem cache.
//!
//! Normally we would just use [`bytes`], however the crate suffers from gate-keeping and even though many users would
//! like to see it, there is currently no proper way to build [`bytes`]-based buffers with proper alignment or a
//! custom vtable. So we work around it.

use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use futures::FutureExt;
use linear_buffer::Slice;
use tokio::sync::oneshot::{Receiver, Sender, error::RecvError};

/// Create channel that can be used ONCE to send a [`Slice`].
///
/// The sender may choose not to accept the transfer (by not calling [`accept`](BufferSender::accept)), i.e. if it does
/// not implement buffer handling.
pub fn channel() -> (BufferSender, BufferReceiver) {
    let accepted = Arc::new(AtomicBool::new(false));
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let sender = BufferSender {
        accepted: Arc::clone(&accepted),
        sender: Arc::new(Mutex::new(Some(sender))),
    };
    let receiver = BufferReceiver { accepted, receiver };
    (sender, receiver)
}

/// Sender-side for a [`Slice`].
///
/// The sender is clonable so it can be used with [`http::Extensions`], but you must only call [`accept`](Self::accept)
/// at most once.
#[derive(Debug, Clone)]
pub struct BufferSender {
    accepted: Arc<AtomicBool>,
    sender: Arc<Mutex<Option<Sender<Slice>>>>,
}

impl BufferSender {
    /// Accept that we will have a [`Slice`] available at some point.
    ///
    /// After calling this function, the sender MUST provide a slice at some point. Dropping the returned
    /// [handle](BufferSenderAccepted) without doing so will result in an error on the
    /// [receiver side](BufferReceiverAccepted).
    ///
    /// # Panic
    /// Across all clones, this method must only be called at most once.
    pub fn accept(self) -> BufferSenderAccepted {
        let Self { accepted, sender } = self;
        let maybe_sender = {
            let mut guard = sender.lock().unwrap();
            guard.take()
        };
        let sender = maybe_sender.expect("can only accept once");
        accepted.store(true, Ordering::SeqCst);
        BufferSenderAccepted { sender }
    }
}

/// Sender-side in an [accepted](BufferSender::accept) state.
#[derive(Debug)]
pub struct BufferSenderAccepted {
    sender: Sender<Slice>,
}

impl BufferSenderAccepted {
    /// Send slice.
    pub fn send(self, buffer: Slice) {
        let Self { sender } = self;
        sender.send(buffer).ok();
    }
}

/// Receiver side of a [`Slice`].
#[derive(Debug)]
pub struct BufferReceiver {
    accepted: Arc<AtomicBool>,
    receiver: Receiver<Slice>,
}

impl BufferReceiver {
    pub fn accepted(self) -> Option<BufferReceiverAccepted> {
        let Self { accepted, receiver } = self;
        accepted
            .load(Ordering::SeqCst)
            .then_some(BufferReceiverAccepted { receiver })
    }
}

/// Receiver side of the [`Slice`] for which the sender has accepted the transfer.
#[derive(Debug)]
pub struct BufferReceiverAccepted {
    receiver: Receiver<Slice>,
}

impl Future for BufferReceiverAccepted {
    type Output = Result<Slice, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.receiver.poll_unpin(cx)
    }
}

#[cfg(test)]
mod test {
    use linear_buffer::LinearBuffer;

    use super::*;

    #[test]
    #[should_panic(expected = "can only accept once")]
    fn panic_accept_twice() {
        let (tx, _rx) = channel();
        let tx2 = tx.clone();

        tx.accept();
        tx2.accept();
    }

    #[tokio::test]
    async fn err_accepted_sender_dropped() {
        let (tx, rx) = channel();
        let tx = tx.accept();
        let rx = rx.accepted().unwrap();
        drop(tx);
        rx.await.unwrap_err();
    }

    #[tokio::test]
    async fn accept_accepted_send_receive() {
        let buffer = LinearBuffer::new(0);
        let slice = buffer.slice_initialized_part(..);

        let (tx, rx) = channel();
        let tx = tx.accept();
        let rx = rx.accepted().unwrap();
        tx.send(slice.clone());
        let slice2 = rx.await.unwrap();

        assert_eq!(
            slice.as_ptr().expose_provenance(),
            slice2.as_ptr().expose_provenance(),
        );
    }

    #[tokio::test]
    async fn accept_send_accepted_receive() {
        let buffer = LinearBuffer::new(0);
        let slice = buffer.slice_initialized_part(..);

        let (tx, rx) = channel();
        let tx = tx.accept();
        tx.send(slice.clone());
        let rx = rx.accepted().unwrap();
        let slice2 = rx.await.unwrap();

        assert_eq!(
            slice.as_ptr().expose_provenance(),
            slice2.as_ptr().expose_provenance(),
        );
    }

    #[tokio::test]
    async fn not_accepted() {
        let (tx, rx) = channel();
        drop(tx);
        assert!(rx.accepted().is_none());
    }
}
