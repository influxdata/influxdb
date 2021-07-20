use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// A semaphore that produces [`Send`]able guards.
pub struct Semaphore {
    user_count: Arc<AtomicUsize>,
}

impl Semaphore {
    /// Creates new semaphore with a single permit.
    pub fn new() -> Self {
        Self {
            user_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Creates guard if no permit exists.
    ///
    /// To produce multiple guards, you can clone an existing one.
    pub fn guard(&self) -> Option<Guard> {
        let count = self.user_count.fetch_add(1, Ordering::SeqCst);
        if count > 0 {
            self.user_count.fetch_sub(1, Ordering::SeqCst);
            None
        } else {
            Some(Guard {
                user_count: Arc::clone(&self.user_count),
            })
        }
    }
}

/// Guard that hols a [`Semaphore`] permit.
///
/// New guards can be produced in two ways:
/// - cloning an existing one
/// - when no guard exists: using [`Semaphore::guard`].
pub struct Guard {
    user_count: Arc<AtomicUsize>,
}

impl Guard {
    /// Use a guard.
    ///
    /// This is a no-op but is helpful if you need to reference a guard within a closure.
    pub fn use_here(&self) {}
}

impl Clone for Guard {
    /// Clone guard and increase usage count.
    fn clone(&self) -> Self {
        self.user_count.fetch_add(1, Ordering::SeqCst);
        Self {
            user_count: Arc::clone(&self.user_count),
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.user_count.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let s = Semaphore::new();

        let g = s.guard().unwrap();
        assert!(s.guard().is_none());
        drop(g);

        let g1 = s.guard().unwrap();
        let g2 = g1.clone();
        assert!(s.guard().is_none());
        drop(g1);
        assert!(s.guard().is_none());
        drop(g2);
        s.guard().unwrap();
    }
}
