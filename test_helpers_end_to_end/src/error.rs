/// Check error returned by the Flight API.
pub fn check_flight_error(
    err: influxdb_iox_client::flight::Error,
    expected_error_code: tonic::Code,
    expected_message: Option<&str>,
) {
    if let Some(status) = err.tonic_status() {
        check_tonic_status(status, expected_error_code, expected_message);
    } else {
        panic!("Not a gRPC error: {err}");
    }
}

/// Check tonic status.
pub fn check_tonic_status(
    status: &tonic::Status,
    expected_error_code: tonic::Code,
    expected_message: Option<&str>,
) {
    assert_eq!(
        status.code(),
        expected_error_code,
        "Wrong status code: {}\n\nStatus:\n{}",
        status.code(),
        status,
    );
    if let Some(expected_message) = expected_message {
        let status_message = status.message();
        assert_eq!(
            status_message, expected_message,
            "\nActual status message:\n{status_message}\nExpected message:\n{expected_message}"
        );
    }
}
