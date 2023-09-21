//! Test utilities

/// Asserts that the result of a nom parser is an error and a [`nom::Err::Failure`].
#[macro_export]
macro_rules! assert_failure {
    ($RESULT:expr) => {
        assert_matches::assert_matches!($RESULT.unwrap_err(), nom::Err::Failure(_));
    };
}

/// Asserts that the result of a nom parser is an error and a [`nom::Err::Error`] of the specified
/// [`nom::error::ErrorKind`].
#[macro_export]
macro_rules! assert_error {
    ($RESULT:expr, $ERR:ident) => {
        assert_matches::assert_matches!(
            $RESULT.unwrap_err(),
            nom::Err::Error($crate::internal::Error::Nom(_, nom::error::ErrorKind::$ERR))
        );
    };
}

/// Asserts that the result of a nom parser is an [`crate::internal::Error::Syntax`] and a [`nom::Err::Failure`].
#[macro_export]
macro_rules! assert_expect_error {
    ($RESULT:expr, $MSG:expr) => {
        match $RESULT.unwrap_err() {
            nom::Err::Failure($crate::internal::Error::Syntax {
                input: _,
                message: got,
            }) => {
                assert_eq!(got.to_string(), $MSG)
            }
            e => panic!("Expected Failure(Syntax(_, msg), got {:?}", e),
        }
    };
}
