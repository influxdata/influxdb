use std::f64;

/// A test helper function for asserting floating point numbers are within the machine epsilon
/// because strict comparison of floating point numbers is incorrect
pub fn approximately_equal(f1: f64, f2: f64) -> bool {
    (f1 - f2).abs() < f64::EPSILON
}

