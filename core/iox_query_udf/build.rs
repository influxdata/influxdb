//! Fetch pre-build binaries from our GitHub releases.
//!
//! # Updating
//! To update this to a new release, pick a (probably the latest) "WASM Binaries" release from
//! <https://github.com/influxdata/datafusion-udf-wasm/releases>. Based on that you update the constants in this
//! module as follows:
//!
//! - **[`COMMIT`]:** Under "Build Metadata" you find `Commit: ...`.
//! - **[`BUILD_TIMESTAMP`]**: Also under "Builder Metadata" you find `Build Timestamp: ...`.
//! - **[`SHA256_TXT_CHECKSUM`]**: Attached to the release itself you find the "Assets". You may need to click on
//!   "show more" / "show all". Find the asset named `sha256sum.txt`. For that asset the SHA256 checksum is listed next
//!   to it (it even has a small "copy" button). This is NOT the content of `sha256sum.txt`!
use std::{
    collections::HashMap,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};

use sha2::Digest;

/// GIT hash of the commit in <https://github.com/influxdata/datafusion-udf-wasm>.
///
/// See module level comment on how to update this.
const COMMIT: &str = "89ab4ae6312c3a44859ddd43d9df4d4300d3086a";

/// Timestamp of the WASM build.
///
/// See module level comment on how to update this.
const BUILD_TIMESTAMP: &str = "2026-01-28T11:46:19+00:00";

/// SHA256 checksum of the `sha256sum.txt` release artifact.
///
/// We gonna use that file to read the checksum of the target-triplet-specific `.elf` file.
///
/// See module level comment on how to update this.
const SHA256_TXT_CHECKSUM: &str =
    "sha256:1fcd991e5e03f9c2e56e7ba5e9d56af9343b53bf9c3383b19e7ca985a9d3d823";

fn main() {
    let sha256sum_txt_checksum = SHA256_TXT_CHECKSUM.to_lowercase().replace("sha256:", "");
    let sha256sum_txt = fetch_artifact("sha256sum.txt", &sha256sum_txt_checksum);
    let checksums = std::fs::read_to_string(&sha256sum_txt)
        .unwrap()
        .lines()
        .map(|l| {
            let (checksum, file) = l.split_once("  ").unwrap();
            (file.to_owned(), checksum.to_owned())
        })
        .collect::<HashMap<_, _>>();

    let target = std::env::var("TARGET").unwrap();
    let elf_file = format!("datafusion_udf_wasm_python.release.{target}.elf");
    let Some(elf_file_checksum) = checksums.get(&elf_file) else {
        panic!("no checksum found for {elf_file}")
    };
    let elf = fetch_artifact(&elf_file, elf_file_checksum);
    println!("cargo::rustc-env=BIN_PATH_PYTHON_WASM={}", elf.display(),);
}

/// Download release artifact/file.
fn fetch_artifact(name: &str, checksum: &str) -> PathBuf {
    let target_file = download_target_location(name);

    if target_file.exists() && sha256(&target_file) == checksum {
        return target_file;
    }

    // build timestamp in URL has a specific format:
    // - the TZ offset is not included (it's always UTC anyways)
    // - since `:` doesn't work in URLs, `-` is used
    let build_ts = BUILD_TIMESTAMP.replace("+00:00", "").replace(":", "-");

    let url = format!(
        "https://github.com/influxdata/datafusion-udf-wasm/releases/download/wasm-binaries%2F{build_ts}%2F{COMMIT}/{name}"
    );

    // retry loop for download
    for retry in 1.. {
        match download(&url, &target_file) {
            Ok(()) => {
                break;
            }
            Err(e) if retry < 100 => {
                println!("cargo::warning=download of `{url}` failed, retrying: {e}");
                std::thread::sleep(Duration::from_secs(1));
            }
            Err(e) => {
                panic!("download of `{url}` failed: {e}");
            }
        }
    }

    let actual_checksum = sha256(&target_file);
    assert!(
        actual_checksum == checksum,
        "Checksum mismatch for {name}:\n\nActual:\n{actual_checksum}\n\nExpected:\n{checksum}",
    );

    target_file
}

/// Download file to path.
fn download(url: &str, path: &Path) -> Result<(), std::io::Error> {
    let response = ureq::get(url).call().map_err(std::io::Error::other)?;
    let mut data = response.into_body().into_reader();
    let mut file = File::create(path)?;
    std::io::copy(&mut data, &mut file)?;
    file.sync_all()?;
    Ok(())
}

/// Get SHA256 of given file.
fn sha256(path: &Path) -> String {
    const READ_BUFFER_SIZE: usize = 8192;
    let mut hasher = sha2::Sha256::new();
    let mut file = File::open(path).unwrap();
    let mut buffer = [0u8; READ_BUFFER_SIZE];
    loop {
        let n = file.read(&mut buffer).unwrap();
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    let digest = hasher.finalize();
    digest
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
}

/// Get download location.
fn download_target_location(name: &str) -> PathBuf {
    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    let download_dir = out_dir.join("iox_query_udf_download");

    if !download_dir.is_dir() {
        std::fs::create_dir(&download_dir).unwrap();
    }

    download_dir.join(name)
}
