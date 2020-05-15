use delorean_wal::{Entry, WalBuilder};
use std::{fs, io::Write, path::PathBuf};

type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = TestError> = std::result::Result<T, E>;

fn wal_file_names(dir: impl Into<PathBuf>) -> Vec<String> {
    wal_paths(dir)
        .iter()
        .filter_map(|path| path.file_name().map(|p| p.to_string_lossy().to_string()))
        .collect()
}

fn wal_paths(dir: impl Into<PathBuf>) -> Vec<PathBuf> {
    let mut paths: Vec<_> = fs::read_dir(&dir.into())
        .expect("Cannot read WAL directory")
        .flatten() // Ignore errors
        .map(|entry| entry.path())
        .collect();
    paths.sort();
    paths
}

fn total_size_on_disk(dir: impl Into<PathBuf>) -> u64 {
    wal_paths(&dir.into())
        .iter()
        .map(|file| {
            fs::metadata(file)
                .expect("Could not read file metadata")
                .len()
        })
        .sum()
}

fn file_name_for_sequence_number(id: u64) -> String {
    format!("wal_{:016x}.db", id)
}

fn all_entries(builder: &WalBuilder) -> Result<Vec<Entry>> {
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
        let mut count = 0;

        $({
            let mut w = $wal.append();
            w.write_all($entry)?;
            w.finalize(())?;
            count += 1;
        })*

        let (to_notify, outcome) = $wal.sync();
        assert!(matches!(outcome, Ok(())));
        assert_eq!(count, to_notify.len());
    }};
}

#[test]
#[allow(clippy::cognitive_complexity)]
fn file_rollover() -> Result {
    let dir = delorean_test_helpers::tmp_dir()?;

    // Set the file rollover size limit low to test rollover
    let builder = WalBuilder::new(dir.as_ref()).file_rollover_size(100);
    let wal = builder.clone().wal()?;

    // Should start without existing WAL files
    let wal_files = wal_file_names(&dir.as_ref());
    assert!(wal_files.is_empty());

    // Total size should be 0
    assert_eq!(wal.total_size(), 0);

    // Reading the WAL should return Ok(empty vec)
    let wal_entries = all_entries(&builder)?;
    assert!(wal_entries.is_empty());

    // Write one WAL entry when there are no existing WAL files
    create_and_sync_batch!(wal, [b"some data within the file limit"]);

    // There should now be one existing WAL file
    assert_filenames_for_sequence_numbers!(dir, [0]);

    // Total size should be that of all the files
    assert_eq!(wal.total_size(), total_size_on_disk(&dir.as_ref()));

    // Should be able to read the entry back out
    let wal_entries = all_entries(&builder)?;
    assert_eq!(1, wal_entries.len());
    assert_entry!(wal_entries[0], 0, b"some data within the file limit");

    // Write one WAL entry when there is an existing WAL file that is currently under the size
    // limit, should end up in the same WAL file
    create_and_sync_batch!(wal, [b"some more data that puts the file over the limit"]);

    // There should still be one existing WAL file
    assert_filenames_for_sequence_numbers!(dir, [0]);

    // Total size should be that of all the files
    assert_eq!(wal.total_size(), total_size_on_disk(&dir.as_ref()));

    // Should be able to read the entries back out
    let wal_entries = all_entries(&builder)?;
    assert_eq!(2, wal_entries.len());
    assert_entry!(wal_entries[0], 0, b"some data within the file limit");
    assert_entry!(
        wal_entries[1],
        1,
        b"some more data that puts the file over the limit",
    );

    // Write one WAL entry, and because the existing file is over the size limit, this entry
    // should end up in a new WAL file
    create_and_sync_batch!(
        wal,
        [b"some more data, this should now be rolled over into the next WAL file"]
    );

    // There should now be two existing WAL files
    assert_filenames_for_sequence_numbers!(dir, [0, 2]);

    // Total size should be that of all the files
    assert_eq!(wal.total_size(), total_size_on_disk(&dir.as_ref()));

    // Should be able to read the entries back out
    let wal_entries = all_entries(&builder)?;
    assert_eq!(3, wal_entries.len());
    assert_entry!(wal_entries[0], 0, b"some data within the file limit");
    assert_entry!(
        wal_entries[1],
        1,
        b"some more data that puts the file over the limit"
    );
    assert_entry!(
        wal_entries[2],
        2,
        b"some more data, this should now be rolled over into the next WAL file"
    );

    // Write two WAL entries, one that could fit in the existing file but puts the file over the
    // limit. Because the two entries are in one sync batch, they both will end up in the existing
    // file even though it's over the limit after the first entry.
    create_and_sync_batch!(
        wal,
        [
            b"one entry that puts the existing file over the limit",
            b"another entry"
        ]
    );

    // There should still be two existing WAL files
    assert_filenames_for_sequence_numbers!(dir, [0, 2]);

    // Total size should be that of all the files
    assert_eq!(wal.total_size(), total_size_on_disk(&dir.as_ref()));

    // Should be able to read the entries back out
    let wal_entries = all_entries(&builder)?;
    assert_eq!(5, wal_entries.len());
    assert_entry!(wal_entries[0], 0, b"some data within the file limit");
    assert_entry!(
        wal_entries[1],
        1,
        b"some more data that puts the file over the limit"
    );
    assert_entry!(
        wal_entries[2],
        2,
        b"some more data, this should now be rolled over into the next WAL file"
    );
    assert_entry!(
        wal_entries[3],
        3,
        b"one entry that puts the existing file over the limit"
    );
    assert_entry!(wal_entries[4], 4, b"another entry");

    let total_file_size_before_delete = total_size_on_disk(&dir.as_ref());

    // Some process deletes the first WAL file
    let path = dir.path().join(file_name_for_sequence_number(0));
    fs::remove_file(path)?;

    // Total size isn't aware of the out-of-band deletion
    assert_eq!(wal.total_size(), total_file_size_before_delete);

    // Should be able to read the remaining entries back out
    let wal_entries = all_entries(&builder)?;
    assert_eq!(3, wal_entries.len());
    assert_entry!(
        wal_entries[0],
        2,
        b"some more data, this should now be rolled over into the next WAL file"
    );
    assert_entry!(
        wal_entries[1],
        3,
        b"one entry that puts the existing file over the limit"
    );
    assert_entry!(wal_entries[2], 4, b"another entry");

    // Pretend the process restarts
    let wal: delorean_wal::Wal<()> = builder.wal()?;

    // Total size should be that of all the files, so without the file deleted out-of-band
    assert_eq!(wal.total_size(), total_size_on_disk(&dir.as_ref()));

    Ok(())
}
