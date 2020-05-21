use delorean_wal::WalBuilder;
use std::io::Write;

#[macro_use]
mod helpers;
use crate::helpers::*;

#[test]
#[allow(clippy::cognitive_complexity)]
fn delete_up_to() -> Result {
    let dir = delorean_test_helpers::tmp_dir()?;

    // Set the file rollover size limit low to test interaction with file rollover
    let builder = WalBuilder::new(dir.as_ref()).file_rollover_size(100);
    let wal = builder.clone().wal()?;

    create_and_sync_batch!(
        wal,
        [
            b"some data within the file limit",
            b"some more data that puts the file over the limit"
        ]
    );

    // Write one WAL entry, and because the existing file is over the size limit, this entry
    // should end up in a new WAL file
    create_and_sync_batch!(
        wal,
        [b"some more data, this should now be rolled over into the next WAL file"]
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

    // There should be two existing WAL files
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

    // Not including 3, is this expected?
    wal.delete_up_to_entry(3)?;

    // There should be one existing WAL file
    assert_filenames_for_sequence_numbers!(dir, [2]);

    // Add another entry; the sequence numbers continue
    create_and_sync_batch!(wal, [b"entry after deletion"]);

    // Should be able to read the entries back out
    let wal_entries = all_entries(&builder)?;
    assert_eq!(4, wal_entries.len());

    // Is it expected that 2 is still readable, because we asked to delete it but couldn't because
    // it was in a file with 3?
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
    assert_entry!(wal_entries[3], 5, b"entry after deletion");

    Ok(())
}
