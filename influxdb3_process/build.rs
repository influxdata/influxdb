// Include the GIT_HASH, in `GIT_HASH` environment variable at build
// time, panic'ing if it can not be found
//
// https://stackoverflow.com/questions/43753491/include-git-commit-hash-as-string-into-rust-program
use cargo_metadata::MetadataCommand;
use std::env;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=GIT_HASH");
    // Populate env!(GIT_HASH) with the current git commit
    let git_hash = get_git_hash();
    println!("cargo:rustc-env=GIT_HASH={git_hash}");
    // Populate env!(GIT_HASH_SHORT) with the current git commit
    let git_hash_short = get_git_hash_short();
    println!("cargo:rustc-env=GIT_HASH_SHORT={git_hash_short}");

    let path = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let meta = MetadataCommand::new()
        .manifest_path("./Cargo.toml")
        .current_dir(&path)
        .exec()
        .unwrap();

    let root = meta.root_package().unwrap();
    let build = root.metadata["influxdb3"]["build"]
        .as_str()
        .unwrap_or("Core");
    println!("cargo:rerun-if-env-changed=INFLUXDB3_BUILD_VERSION");
    println!("cargo:rustc-env=INFLUXDB3_BUILD_VERSION={build}");

    // Configure crypto backend based on target platform
    configure_crypto_backend()?;

    Ok(())
}

fn get_git_hash() -> String {
    let out = match std::env::var("GIT_HASH") {
        Ok(v) => v,
        Err(_) => {
            let output = Command::new("git")
                // We used `git describe`, but when you tag a build the commit hash goes missing when
                // using describe. So, switching it to use `rev-parse` which is consistent with the
                // `get_git_hash_short` below as well.
                //
                // And we already have cargo version appearing as a separate string so using `git
                // describe` looks redundant on tagged release builds
                .args(["rev-parse", "HEAD"])
                .output()
                .expect("failed to execute git rev-parse to read the current git hash");

            String::from_utf8(output.stdout).expect("non-utf8 found in git hash")
        }
    };

    assert!(!out.is_empty(), "attempting to embed empty git hash");
    out
}

fn get_git_hash_short() -> String {
    let out = match std::env::var("GIT_HASH_SHORT") {
        Ok(v) => v,
        Err(_) => {
            let output = Command::new("git")
                .args(["rev-parse", "--short", "HEAD"])
                .output()
                .expect("failed to execute git rev-parse to read the current git hash");
            String::from_utf8(output.stdout).expect("non-utf8 found in git hash")
        }
    };

    assert!(!out.is_empty(), "attempting to embed empty git hash");
    out
}

fn configure_crypto_backend() -> Result<(), Box<dyn std::error::Error>> {
    // Get the target triple from environment (set by Cargo during build)
    let target = env::var("TARGET").unwrap_or_else(|_| {
        panic!("TARGET environment variable not set by Cargo");
    });

    // Check if crypto backend is overridden via environment variable
    println!("cargo:rerun-if-env-changed=INFLUXDB_CRYPTO_BACKEND");
    let crypto_backend = if let Ok(backend) = env::var("INFLUXDB_CRYPTO_BACKEND") {
        println!(
            "cargo:warning=Using crypto backend from INFLUXDB_CRYPTO_BACKEND env: {}",
            backend
        );
        backend
    } else {
        // Determine based on target OS
        // Linux targets use aws-lc-rs for better performance
        // All other platforms use ring for better compatibility
        if target.contains("linux") {
            "aws-lc-rs".to_string()
        } else {
            "ring".to_string()
        }
    };

    println!(
        "cargo:warning=Building for target: {}, using crypto backend: {}",
        target, crypto_backend
    );

    // Set rustc-cfg flags that can be used in the code with #[cfg(...)]
    println!(
        "cargo:rustc-cfg=influxdb_crypto_backend=\"{}\"",
        crypto_backend
    );

    // Set specific flags for conditional compilation
    if crypto_backend == "ring" {
        println!("cargo:rustc-cfg=use_ring_crypto");
    } else if crypto_backend == "aws-lc-rs" {
        println!("cargo:rustc-cfg=use_aws_lc_rs_crypto");
    }

    // Also set as environment variable for other build scripts if needed
    println!("cargo:rustc-env=INFLUXDB_CRYPTO_BACKEND={}", crypto_backend);

    Ok(())
}
