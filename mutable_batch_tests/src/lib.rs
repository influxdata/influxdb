//! This crate only exists for its tests and benchmarks

use flate2::read::GzDecoder;
use std::io::Read;
use std::path::Path;

/// Parses the BENCHMARK_LP environment variable for a list of semicolon delimited paths
/// to line protocol files. Returnss a list of (filename, line protocol) pairs for benchmarking
pub fn benchmark_lp() -> Vec<(String, String)> {
    let env = std::env::var("BENCHMARK_LP")
        .expect("set BENCHMARK_LP to a semicolon-delimited list of source files");

    env.split(';').map(read_path).collect()
}

fn read_path(path: &str) -> (String, String) {
    let path = Path::new(path);

    let filename = path.file_name().expect("file").to_string_lossy();

    //  Path::extension only returns `.gz` not `.lp.gz`
    let extension = match filename.split_once('.') {
        Some((_, extension)) => extension,
        None => "lp",
    };

    match extension {
        "lp.gz" => {
            let file = std::fs::File::open(path).unwrap();
            let mut decoded = GzDecoder::new(file);
            let mut ret = String::new();
            decoded.read_to_string(&mut ret).unwrap();
            (filename.to_string(), ret)
        }
        "lp" => (filename.to_string(), std::fs::read_to_string(path).unwrap()),
        ext => panic!("Unrecognised extension: {ext}"),
    }
}
