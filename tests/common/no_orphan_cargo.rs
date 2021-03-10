//! Like assert_cmd::cargo::CommandCargoExt
//! but with workaround for https://github.com/influxdata/influxdb_iox/issues/951

use std::env;
use std::path;
use std::process::Command;

const WRAPPER: &str = "scripts/cargo_bin_no_orphan.bash";

/// Create a Command to run a specific binary of the current crate.
///
/// The binary will be executed by a wrapper which ensures it gets
/// killed when the parent process (the caller) dies.
/// This is useful in test fixtures that lazily initialize background processes
/// in static Onces, which are never dropped and thus cannot be cleaned up with
/// RAII.
pub fn cargo_bin<S: AsRef<str>>(name: S) -> Command {
    let mut cmd = Command::new(WRAPPER);
    cmd.arg(cargo_bin_name(name.as_ref()));
    cmd
}

fn cargo_bin_name(name: &str) -> path::PathBuf {
    target_dir().join(format!("{}{}", name, env::consts::EXE_SUFFIX))
}

// Adapted from
// https://github.com/rust-lang/cargo/blob/485670b3983b52289a2f353d589c57fae2f60f82/tests/testsuite/support/mod.rs#L507
fn target_dir() -> path::PathBuf {
    env::current_exe()
        .ok()
        .map(|mut path| {
            path.pop();
            if path.ends_with("deps") {
                path.pop();
            }
            path
        })
        .unwrap()
}
