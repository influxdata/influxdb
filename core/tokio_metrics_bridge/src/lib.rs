//! Integrates tokio runtime stats into the IOx metric system.
//!
//! This is NOT called `tokio-metrics` since this name is already taken.
#![warn(missing_docs)]

#[cfg(not(tokio_unstable))]
mod not_tokio_unstable {
    use metric as _;
    use parking_lot as _;
    use tokio as _;
    use workspace_hack as _;
}

#[cfg(tokio_unstable)]
mod bridge;
#[cfg(tokio_unstable)]
pub use bridge::*;

#[cfg(test)]
mod tests {
    #[test]
    #[cfg(not(tokio_unstable))]
    fn test_must_be_tested() {
        // If you are reading this after a test failure, you're trying to
        // perform a full test of everything ("integration" - probably in CI)
        // but the test runner is not configured to provide coverage of
        // `cfg(tokio_unstable)` gated code.
        //
        // Make your integration test runner compile in `cfg(tokio_unstable)`
        // code to make this failure go away, and get full test coverage.
        if std::env::var("TEST_INTEGRATION").is_ok() {
            panic!(
                "You're running integration tests, but you're not testing \
                `cfg(tokio_unstable)` code - this causes missing test coverage."
            );
        }
    }
}
