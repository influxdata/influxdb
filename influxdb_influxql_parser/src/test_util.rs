//! Test utilities

/// Asserts that the result of a nom parser is an error and a [`nom::Err::Failure`].
#[macro_export]
macro_rules! assert_failure {
    ($RESULT:expr) => {
        assert!(matches!($RESULT.unwrap_err(), nom::Err::Failure(_)));
    };
}
