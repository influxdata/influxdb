//! Logging utilities for use before the tracing system is initialized.
//!
//! These functions format output to look like the logging system, providing
//! consistent-looking output during early startup.

use std::io::IsTerminal;

use chrono::Utc;

// ANSI color codes
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RED: &str = "\x1b[31m";
const RESET: &str = "\x1b[0m";

fn log(level: &str, color: &str, target: &str, message: &str) {
    let now = Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ");
    if std::io::stderr().is_terminal() {
        eprintln!("{now} {color}{level:>5}{RESET} {target}: {message}");
    } else {
        eprintln!("{now} {level:>5} {target}: {message}");
    }
}

/// Print an info message formatted like the logging system output.
pub fn info(target: &str, message: &str) {
    log("INFO", GREEN, target, message);
}

/// Print a warning message formatted like the logging system output.
pub fn warn(target: &str, message: &str) {
    log("WARN", YELLOW, target, message);
}

/// Print an error message formatted like the logging system output.
pub fn error(target: &str, message: &str) {
    log("ERROR", RED, target, message);
}
