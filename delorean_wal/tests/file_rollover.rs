use delorean_wal::{WalBuilder, WritePayload};
use std::fs;

#[macro_use]
mod helpers;
use crate::helpers::*;

#[test]
#[allow(clippy::cognitive_complexity)]
fn file_rollover() -> Result {
    let dir = delorean_test_helpers::tmp_dir()?;

    // Set the file rollover size limit low to test rollover
    let builder = WalBuilder::new(dir.as_ref()).file_rollover_size(100);
    let mut wal = builder.clone().wal()?;

    // Should start without existing WAL files
    let wal_files = wal_file_names(&dir.as_ref());
    assert!(wal_files.is_empty());

    // Reading the WAL should return Ok(empty vec)
    let wal_entries = all_entries(&builder)?;
    assert!(wal_entries.is_empty());

    // Write one WAL entry when there are no existing WAL files
    create_and_sync_batch!(wal, ["some data within the file limit"]);

    // There should now be one existing WAL file
    assert_filenames_for_sequence_numbers!(dir, [0]);

    // Should be able to read the entry back out
    let wal_entries = all_entries(&builder)?;
    assert_eq!(1, wal_entries.len());
    assert_entry!(wal_entries[0], 0, b"some data within the file limit");

    // Write one WAL entry when there is an existing WAL file that is currently under the size
    // limit, should end up in the same WAL file
    create_and_sync_batch!(wal, ["some more data that puts the file over the limit"]);

    // There should still be one existing WAL file
    assert_filenames_for_sequence_numbers!(dir, [0]);

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
        ["some more data, this should now be rolled over into the next WAL file"]
    );

    // There should now be two existing WAL files
    assert_filenames_for_sequence_numbers!(dir, [0, 2]);

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
            "one entry that puts the existing file over the limit",
            "another entry"
        ]
    );

    // There should still be two existing WAL files
    assert_filenames_for_sequence_numbers!(dir, [0, 2]);

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

    // Some process deletes the first WAL file
    let path = dir.path().join(file_name_for_sequence_number(0));
    fs::remove_file(path)?;

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

    Ok(())
}
