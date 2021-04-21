use std::fs;
use write_buffer::{WriteBufferBuilder, WritePayload};

#[macro_use]
mod helpers;
use helpers::Result;

#[test]
#[allow(clippy::cognitive_complexity)]
fn total_size() -> Result {
    let dir = test_helpers::tmp_dir()?;

    // Set the file rollover size limit low to test how rollover interacts with
    // total size
    let builder = WriteBufferBuilder::new(dir.as_ref()).file_rollover_size(100);
    let mut write_buffer = builder.clone().write_buffer()?;

    // Should start without existing Write Buffer files; this implies total file
    // size on disk is 0
    let write_buffer_files = helpers::write_buffer_file_names(&dir.as_ref());
    assert!(write_buffer_files.is_empty());

    // Total size should be 0
    assert_eq!(write_buffer.total_size(), 0);

    create_and_sync_batch!(write_buffer, ["some data within the file limit"]);

    // Total size should be that of all the files
    assert_eq!(
        write_buffer.total_size(),
        helpers::total_size_on_disk(&dir.as_ref())
    );

    // Write one Write Buffer entry that ends up in the same Write Buffer file
    create_and_sync_batch!(
        write_buffer,
        ["some more data that puts the file over the limit"]
    );

    // Total size should be that of all the files
    assert_eq!(
        write_buffer.total_size(),
        helpers::total_size_on_disk(&dir.as_ref())
    );

    // Write one Write Buffer entry, and because the existing file is over the size
    // limit, this entry should end up in a new Write Buffer file
    create_and_sync_batch!(
        write_buffer,
        ["some more data, this should now be rolled over into the next Write Buffer file"]
    );

    // Total size should be that of all the files
    assert_eq!(
        write_buffer.total_size(),
        helpers::total_size_on_disk(&dir.as_ref())
    );

    let total_file_size_before_delete = helpers::total_size_on_disk(&dir.as_ref());

    // Some process deletes the first Write Buffer file
    let path = dir.path().join(helpers::file_name_for_sequence_number(0));
    fs::remove_file(path)?;

    // Total size isn't aware of the out-of-band deletion
    assert_eq!(write_buffer.total_size(), total_file_size_before_delete);

    // Pretend the process restarts
    let write_buffer = builder.write_buffer()?;

    // Total size should be that of all the files, so without the file deleted
    // out-of-band
    assert_eq!(
        write_buffer.total_size(),
        helpers::total_size_on_disk(&dir.as_ref())
    );

    Ok(())
}
