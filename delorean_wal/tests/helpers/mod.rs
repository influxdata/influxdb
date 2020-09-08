#![allow(unused_macros)]
#![allow(dead_code)]

use delorean_wal::{Entry, WalBuilder};
use std::{fs, path::PathBuf};

type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T = (), E = TestError> = std::result::Result<T, E>;

pub fn wal_file_names(dir: impl Into<PathBuf>) -> Vec<String> {
    wal_paths(dir)
        .iter()
        .filter_map(|path| path.file_name().map(|p| p.to_string_lossy().to_string()))
        .collect()
}

pub fn wal_paths(dir: impl Into<PathBuf>) -> Vec<PathBuf> {
    let mut paths: Vec<_> = fs::read_dir(&dir.into())
        .expect("Cannot read WAL directory")
        .flatten() // Ignore errors
        .map(|entry| entry.path())
        .collect();
    paths.sort();
    paths
}

pub fn total_size_on_disk(dir: impl Into<PathBuf>) -> u64 {
    wal_paths(&dir.into())
        .iter()
        .map(|file| {
            fs::metadata(file)
                .expect("Could not read file metadata")
                .len()
        })
        .sum()
}

pub fn file_name_for_sequence_number(id: u64) -> String {
    format!("wal_{:016x}.db", id)
}

pub fn all_entries(builder: &WalBuilder) -> Result<Vec<Entry>> {
    builder
        .clone()
        .entries()?
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

macro_rules! assert_filenames_for_sequence_numbers {
    ($dir:expr, [$($id:expr),* $(,)?] $(,)?) => {{
        let actual = wal_file_names(&$dir.as_ref());
        let expected = [$(file_name_for_sequence_number($id)),*];
        assert_eq!(actual, expected);
    }};
}

macro_rules! assert_entry {
    ($entry:expr, $seq_num:expr, $data: expr $(,)?) => {{
        assert_eq!($seq_num, $entry.sequence_number());
        assert_eq!($data.as_ref(), $entry.as_data());
    }};
}

macro_rules! create_and_sync_batch {
    ($wal:expr, [$($entry:expr),* $(,)?] $(,)?) => {{
        $({
            let data = Vec::from($entry);
            let data = WritePayload::new(data)?;
            $wal.append(data)?;
        })*

        $wal.sync_all()?;
    }};
}
