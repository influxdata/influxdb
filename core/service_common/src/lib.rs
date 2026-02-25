//! Common methods for RPC service implementations

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

mod error;

pub use error::{datafusion_error_to_tonic_code, flight_error_to_tonic_code};

// Included to avoid arrow in workspace-hack crate
use arrow as _;
