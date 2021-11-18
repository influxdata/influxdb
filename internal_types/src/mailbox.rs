use std::sync::Arc;

/// A mailbox consists of two arrays, an inbox and an outbox
///
/// * [`Mailbox::push`] can be used to add entries to the inbox
/// * [`Mailbox::consume`] rotates data into the outbox and allows it to be consumed
///
/// This achieves the following goals:
///
/// * Allows data to continue to arrive whilst data is being consumed
/// * Allows multiple consumers to coordinate preventing concurrent consumption
///
/// This is different from a tokio channel in the following ways:
///
/// 1. The contents can be inspected prior to removal from the mailbox
/// (important as catalog transactions are fallible)
///
/// 2. Potentially multiple consumers can acquire exclusive access for
/// the duration of a catalog transaction
///
/// 3. Users can ensure that everything in the mailbox is consumed
///
#[derive(Debug)]
pub struct Mailbox<T> {
    outbox: Arc<tokio::sync::Mutex<Vec<T>>>,
    inbox: parking_lot::Mutex<Vec<T>>,
}

impl<T> Default for Mailbox<T> {
    fn default() -> Self {
        Self {
            outbox: Default::default(),
            inbox: Default::default(),
        }
    }
}

impl<T> Mailbox<T> {
    /// Add an item to the inbox
    pub fn push(&self, item: T) {
        self.inbox.lock().push(item)
    }

    /// Get a handle to consume some data from this [`Mailbox`]
    ///
    /// Rotates the inbox into the outbox and returns a [`MailboxHandle`]
    pub async fn consume(&self) -> MailboxHandle<T> {
        let mut outbox = Arc::clone(&self.outbox).lock_owned().await;
        outbox.append(&mut self.inbox.lock());
        MailboxHandle { outbox }
    }
}

/// A [`MailboxHandle`] grants the ability to consume elements from the outbox of a [`Mailbox`]
///
/// Whilst a [`MailboxHandle`] is held for a [`Mailbox`]:
///
/// * Another [`MailboxHandle`] cannot be obtained
/// * Entries can be added to the [`Mailbox`]'s inbox
/// * Entries cannot be added to the [`Mailbox`]'s outbox
///
#[derive(Debug)]
pub struct MailboxHandle<T> {
    outbox: tokio::sync::OwnedMutexGuard<Vec<T>>,
}

impl<T> MailboxHandle<T> {
    /// Returns the outbox of the associated [`Mailbox`]
    pub fn outbox(&self) -> &[T] {
        &self.outbox
    }

    /// Flush the outbox of the [`Mailbox`]
    pub fn flush(mut self) {
        self.outbox.clear()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mailbox() {
        let mailbox: Mailbox<i32> = Default::default();

        mailbox.push(1);
        mailbox.push(2);

        let handle = mailbox.consume().await;

        // Can continue to push items
        mailbox.push(3);
        mailbox.push(4);

        assert_eq!(handle.outbox(), &[1, 2]);

        // Drop handle without flushing
        std::mem::drop(handle);

        mailbox.push(5);

        let handle = mailbox.consume().await;

        mailbox.push(6);

        assert_eq!(handle.outbox(), &[1, 2, 3, 4, 5]);

        handle.flush();

        let handle = mailbox.consume().await;

        assert_eq!(handle.outbox(), &[6]);
    }
}
