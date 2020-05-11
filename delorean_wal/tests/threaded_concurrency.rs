use delorean_wal::WalBuilder;
use std::{io::Write, sync::mpsc, thread};

type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = TestError> = std::result::Result<T, E>;

#[test]
fn threaded_concurrency() -> Result {
    let dir = delorean_test_helpers::tmp_dir()?;
    let builder = WalBuilder::new(dir.as_ref());
    let wal = builder.clone().wal();

    let (ingest_done_tx, ingest_done_rx) = mpsc::sync_channel(1);

    let ingest_thread = thread::spawn({
        let wal = wal.clone();
        move || {
            let mut w = wal.append();
            w.write_all(b"some")?;
            w.write_all(b"data")?;
            w.finalize(ingest_done_tx)?;
            Ok(())
        }
    });

    let sync_thread = thread::spawn({
        let wal = wal;
        move || {
            let r: Result<Result, _> = ingest_thread.join();
            r.unwrap()?;

            let (to_notify, outcome) = wal.sync();

            for notify in to_notify {
                notify.send(outcome.clone())?;
            }

            Ok(())
        }
    });

    let r: Result<Result, _> = sync_thread.join();
    r.unwrap()?;

    assert!(matches!(ingest_done_rx.into_iter().next(), Some(Ok(()))));

    let wal_entries: Result<Vec<_>, _> = builder.entries()?.collect();
    let wal_entries = wal_entries?;
    assert_eq!(1, wal_entries.len());
    assert_eq!(b"somedata".as_ref(), &*wal_entries[0].as_data());

    Ok(())
}
