use write_buffer::{WriteBufferBuilder, WritePayload};

#[macro_use]
mod helpers;
use crate::helpers::*;

#[test]
#[allow(clippy::cognitive_complexity)]
fn delete_up_to() -> Result {
    let dir = test_helpers::tmp_dir()?;

    // Set the file rollover size limit low to test interaction with file rollover
    let builder = WriteBufferBuilder::new(dir.as_ref()).file_rollover_size(100);
    let mut write_buffer = builder.clone().write_buffer()?;

    create_and_sync_batch!(
        write_buffer,
        [
            "some data within the file limit",
            "some more data that puts the file over the limit"
        ]
    );

    // Write one Write Buffer entry, and because the existing file is over the size
    // limit, this entry should end up in a new Write Buffer file
    create_and_sync_batch!(
        write_buffer,
        ["some more data, this should now be rolled over into the next Write Buffer file"]
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

    // There should be two existing Write Buffer files
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

    // Not including 3!
    write_buffer.delete_up_to_entry(3)?;

    // There should be one existing Write Buffer file
    assert_filenames_for_sequence_numbers!(dir, [2]);

    // Add another entry; the sequence numbers continue
    create_and_sync_batch!(write_buffer, ["entry after deletion"]);

    // Should be able to read the entries back out
    let write_buffer_entries = all_entries(&builder)?;
    assert_eq!(4, write_buffer_entries.len());

    // 2 is still readable, because we asked to delete it but couldn't because it
    // was in a file with 3.
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
    assert_entry!(write_buffer_entries[3], 5, b"entry after deletion");

    Ok(())
}
