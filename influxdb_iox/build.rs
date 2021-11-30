// Include the GIT_HASH, if any, in `GIT_HASH` environment variable at build
// time
//
// https://stackoverflow.com/questions/43753491/include-git-commit-hash-as-string-into-rust-program
use std::process::Command;
fn main() {
    let output = Command::new("git").args(&["rev-parse", "HEAD"]).output();

    if let Ok(output) = output {
        if let Ok(git_hash) = String::from_utf8(output.stdout) {
            println!("cargo:rustc-env=GIT_HASH={}", git_hash);
        }
    }
}
