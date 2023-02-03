// Include the GIT_HASH, in `GIT_HASH` environment variable at build
// time, panic'ing if it can not be found
//
// https://stackoverflow.com/questions/43753491/include-git-commit-hash-as-string-into-rust-program
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=GIT_HASH");
    // Populate env!(GIT_HASH) with the current git commit
    println!("cargo:rustc-env=GIT_HASH={}", get_git_hash());

    Ok(())
}

fn get_git_hash() -> String {
    let out = match std::env::var("VERSION_HASH") {
        Ok(v) => v,
        Err(_) => {
            let output = Command::new("git")
                .args(["describe", "--always", "--dirty", "--abbrev=64"])
                .output()
                .expect("failed to execute git rev-parse to read the current git hash");

            String::from_utf8(output.stdout).expect("non-utf8 found in git hash")
        }
    };

    assert!(!out.is_empty(), "attempting to embed empty git hash");
    out
}
