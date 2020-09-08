use delorean_wal::{WalBuilder, WritePayload};
use std::fs;

#[macro_use]
mod helpers;
use helpers::Result;

#[test]
#[allow(clippy::cognitive_complexity)]
fn total_size() -> Result {
    let dir = delorean_test_helpers::tmp_dir()?;

    // Set the file rollover size limit low to test how rollover interacts with total size
    let builder = WalBuilder::new(dir.as_ref()).file_rollover_size(100);
    let mut wal = builder.clone().wal()?;

    // Should start without existing WAL files; this implies total file size on disk is 0
    let wal_files = helpers::wal_file_names(&dir.as_ref());
    assert!(wal_files.is_empty());

    // Total size should be 0
    assert_eq!(wal.total_size(), 0);

    create_and_sync_batch!(wal, ["some data within the file limit"]);

    // Total size should be that of all the files
    assert_eq!(wal.total_size(), helpers::total_size_on_disk(&dir.as_ref()));

    // Write one WAL entry that ends up in the same WAL file
    create_and_sync_batch!(wal, ["some more data that puts the file over the limit"]);

    // Total size should be that of all the files
    assert_eq!(wal.total_size(), helpers::total_size_on_disk(&dir.as_ref()));

    // Write one WAL entry, and because the existing file is over the size limit, this entry
    // should end up in a new WAL file
    create_and_sync_batch!(
        wal,
        ["some more data, this should now be rolled over into the next WAL file"]
    );

    // Total size should be that of all the files
    assert_eq!(wal.total_size(), helpers::total_size_on_disk(&dir.as_ref()));

    let total_file_size_before_delete = helpers::total_size_on_disk(&dir.as_ref());

    // Some process deletes the first WAL file
    let path = dir.path().join(helpers::file_name_for_sequence_number(0));
    fs::remove_file(path)?;

    // Total size isn't aware of the out-of-band deletion
    assert_eq!(wal.total_size(), total_file_size_before_delete);

    // Pretend the process restarts
    let wal: delorean_wal::Wal = builder.wal()?;

    // Total size should be that of all the files, so without the file deleted out-of-band
    assert_eq!(wal.total_size(), helpers::total_size_on_disk(&dir.as_ref()));

    Ok(())
}
