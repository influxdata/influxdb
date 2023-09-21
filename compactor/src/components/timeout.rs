//! Utilities for managing cases where what we want to do when future times out depends on whether
//! some notion of "progress" has been made.

use std::{fmt, future::Future, time::Duration};

use tokio::sync::watch::{self, Sender};

/// Returned information from a call to [`timeout_with_progress_checking`].
pub enum TimeoutWithProgress<R> {
    /// The inner future timed out and _no_ progress was reported.
    NoWorkTimeOutError,
    /// The inner future timed out and _some_ progress was reported.
    SomeWorkTryAgain,
    /// The inner future completed before the timeout and returned a value of type `R`.
    Completed(R),
}

impl<R: fmt::Debug> fmt::Debug for TimeoutWithProgress<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoWorkTimeOutError => write!(f, "TimeoutWithProgress::NoWorkTimeOutError"),
            Self::SomeWorkTryAgain => write!(f, "TimeoutWithProgress::SomeWorkTryAgain"),
            Self::Completed(r) => write!(f, "TimeoutWithProgress::Completed({:?})", r),
        }
    }
}

/// Set an overall timeout for a future that has some concept of making progress or not, and if the
/// future times out, send a different [`TimeoutWithProgress`] value depending on whether there
/// was no work done or some work done. This lets the calling code assess whether it might be worth
/// trying the operation again to make more progress, or whether the future is somehow stuck or
/// takes too long to ever work.
///
/// # Parameters
///
/// * `full_timeout`: The timeout duration the future is allowed to spend
/// * `inner_future`: A function taking a [`tokio::sync::watch::Sender<bool>`] that returns a
///   future. This function expects that the body of the future will call `send(true)` to indicate
///   that progress has been made, however the future defines "progress". If the future times out,
///   this function will return `TimeoutWithProgress::SomeWorkTryAgain` if it has received at least
///   one `true` value and `TimeoutWithProgress::NoWorkTimeOutError` if nothing was sent. If the
///   future finishes before `full_timeout`, this function will return
///   `TimeoutWithProgress::Completed` and pass along the returned value from the future.
pub async fn timeout_with_progress_checking<F, Fut>(
    full_timeout: Duration,
    inner_future: F,
) -> TimeoutWithProgress<Fut::Output>
where
    F: FnOnce(Sender<bool>) -> Fut + Send,
    Fut: Future + Send,
{
    let (transmit_progress_signal, receive_progress_signal) = watch::channel(false);

    let called_inner_future = inner_future(transmit_progress_signal);

    match tokio::time::timeout(full_timeout, called_inner_future).await {
        Ok(val) => TimeoutWithProgress::Completed(val),
        Err(_) => {
            let progress = *receive_progress_signal.borrow();
            if progress {
                TimeoutWithProgress::SomeWorkTryAgain
            } else {
                TimeoutWithProgress::NoWorkTimeOutError
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

    #[tokio::test]
    async fn reports_progress_completes_and_returns_ok_under_timeout() {
        let state = timeout_with_progress_checking(Duration::from_millis(5), |tx| async move {
            // No loop in this test; report progress and then return success to simulate
            // successfully completing all work before the timeout.
            let _ignore_send_errors = tx.send(true);
            Result::<(), String>::Ok(())
        })
        .await;

        assert_matches!(state, TimeoutWithProgress::Completed(Ok(())));
    }

    #[tokio::test]
    async fn reports_progress_completes_and_returns_err_under_timeout() {
        let state = timeout_with_progress_checking(Duration::from_millis(5), |tx| async move {
            // No loop in this test; report progress and then return an error to simulate
            // a problem occurring before the timeout.
            let _ignore_send_errors = tx.send(true);
            Result::<(), String>::Err(String::from("there was a problem"))
        })
        .await;

        assert_matches!(
            state,
            TimeoutWithProgress::Completed(Err(e)) if e == "there was a problem"
        );
    }

    #[tokio::test]
    async fn doesnt_report_progress_returns_err_under_timeout() {
        let state = timeout_with_progress_checking(Duration::from_millis(5), |_tx| async move {
            Result::<(), String>::Err(String::from("there was a problem"))
        })
        .await;

        assert_matches!(
            state,
            TimeoutWithProgress::Completed(Err(e)) if e == "there was a problem"
        );
    }

    #[tokio::test]
    async fn reports_progress_then_times_out() {
        let state = timeout_with_progress_checking(Duration::from_millis(5), |tx| async move {
            loop {
                // Sleep for 2 ms, which should be able to run and report progress and then timeout
                // because it will never complete
                tokio::time::sleep(Duration::from_millis(2)).await;
                let _ignore_send_errors = tx.send(true);
            }
        })
        .await;

        assert_matches!(state, TimeoutWithProgress::SomeWorkTryAgain);
    }

    #[tokio::test]
    async fn doesnt_report_progress_then_times_out() {
        let state = timeout_with_progress_checking(Duration::from_millis(5), |_tx| async move {
            // No loop in this test; don't report progress and then sleep enough that this will
            // time out.
            tokio::time::sleep(Duration::from_secs(1)).await;
            Result::<(), String>::Ok(())
        })
        .await;

        assert_matches!(state, TimeoutWithProgress::NoWorkTimeOutError);
    }
}
