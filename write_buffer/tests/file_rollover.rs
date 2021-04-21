use std::fs;
use write_buffer::{WriteBufferBuilder, WritePayload};

#[macro_use]
mod helpers;
use crate::helpers::*;

#[test]
#[allow(clippy::cognitive_complexity)]
fn file_rollover() -> Result {
    let dir = test_helpers::tmp_dir()?;

    // Set the file rollover size limit low to test rollover
    let builder = WriteBufferBuilder::new(dir.as_ref()).file_rollover_size(100);
    let mut write_buffer = builder.clone().write_buffer()?;

    // Should start without existing Write Buffer files
    let write_buffer_files = write_buffer_file_names(&dir.as_ref());
    assert!(write_buffer_files.is_empty());

    // Reading the Write Buffer should return Ok(empty vec)
    let write_buffer_entries = all_entries(&builder)?;
    assert!(write_buffer_entries.is_empty());

    // Write one Write Buffer entry when there are no existing Write Buffer files
    create_and_sync_batch!(write_buffer, ["some data within the file limit"]);

    // There should now be one existing Write Buffer file
    assert_filenames_for_sequence_numbers!(dir, [0]);

    // Should be able to read the entry back out
    let write_buffer_entries = all_entries(&builder)?;
    assert_eq!(1, write_buffer_entries.len());
    assert_entry!(
        write_buffer_entries[0],
        0,
        b"some data within the file limit"
    );

    // Write one Write Buffer entry when there is an existing Write Buffer file
    // that is currently under the size limit, should end up in the same Write
    // Buffer file
    create_and_sync_batch!(
        write_buffer,
        ["some more data that puts the file over the limit"]
    );

    // There should still be one existing Write Buffer file
    assert_filenames_for_sequence_numbers!(dir, [0]);

    // Should be able to read the entries back out
    let write_buffer_entries = all_entries(&builder)?;
    assert_eq!(2, write_buffer_entries.len());
    assert_entry!(
        write_buffer_entries[0],
        0,
        b"some data within the file limit"
    );
    assert_entry!(
        write_buffer_entries[1],
        1,
        b"some more data that puts the file over the limit",
    );

    // Write one Write Buffer entry, and because the existing file is over the size
    // limit, this entry should end up in a new Write Buffer file
    create_and_sync_batch!(
        write_buffer,
        ["some more data, this should now be rolled over into the next Write Buffer file"]
    );

    // There should now be two existing Write Buffer files
    assert_filenames_for_sequence_numbers!(dir, [0, 2]);

    // Should be able to read the entries back out
    let write_buffer_entries = all_entries(&builder)?;
    assert_eq!(3, write_buffer_entries.len());
    assert_entry!(
        write_buffer_entries[0],
        0,
        b"some data within the file limit"
    );
    assert_entry!(
        write_buffer_entries[1],
        1,
        b"some more data that puts the file over the limit"
    );
    assert_entry!(
        write_buffer_entries[2],
        2,
        b"some more data, this should now be rolled over into the next Write Buffer file"
    );

    // Write two Write Buffer entries, one that could fit in the existing file but
    // puts the file over the limit. Because the two entries are in one sync
    // batch, they both will end up in the existing file even though it's over
    // the limit after the first entry.
    create_and_sync_batch!(
        write_buffer,
        [
            "one entry that puts the existing file over the limit",
            "another entry"
        ]
    );

    // There should still be two existing Write Buffer files
    assert_filenames_for_sequence_numbers!(dir, [0, 2]);

    // Should be able to read the entries back out
    let write_buffer_entries = all_entries(&builder)?;
    assert_eq!(5, write_buffer_entries.len());
    assert_entry!(
        write_buffer_entries[0],
        0,
        b"some data within the file limit"
    );
    assert_entry!(
        write_buffer_entries[1],
        1,
        b"some more data that puts the file over the limit"
    );
    assert_entry!(
        write_buffer_entries[2],
        2,
        b"some more data, this should now be rolled over into the next Write Buffer file"
    );
    assert_entry!(
        write_buffer_entries[3],
        3,
        b"one entry that puts the existing file over the limit"
    );
    assert_entry!(write_buffer_entries[4], 4, b"another entry");

    // Some process deletes the first Write Buffer file
    let path = dir.path().join(file_name_for_sequence_number(0));
    fs::remove_file(path)?;

    // Should be able to read the remaining entries back out
    let write_buffer_entries = all_entries(&builder)?;
    assert_eq!(3, write_buffer_entries.len());
    assert_entry!(
        write_buffer_entries[0],
        2,
        b"some more data, this should now be rolled over into the next Write Buffer file"
    );
    assert_entry!(
        write_buffer_entries[1],
        3,
        b"one entry that puts the existing file over the limit"
    );
    assert_entry!(write_buffer_entries[2], 4, b"another entry");

    Ok(())
}
