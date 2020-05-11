use delorean_wal::WalBuilder;
use futures::{channel::mpsc, SinkExt, StreamExt};
use std::io::Write;
use tokio::task;

type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = TestError> = std::result::Result<T, E>;

#[tokio::test(threaded_scheduler)]
async fn async_concurrency() -> Result {
    let dir = delorean_test_helpers::tmp_dir()?.into_path();
    let builder = WalBuilder::new(dir.clone());
    let wal = builder.clone().wal();

    let (ingest_done_tx, mut ingest_done_rx) = mpsc::channel(1);

    let ingest_task = tokio::spawn({
        let wal = wal.clone();
        async move {
            let mut w = wal.append();
            w.write_all(b"some")?;
            w.write_all(b"data")?;
            w.finalize(ingest_done_tx)?;
            Ok(())
        }
    });

    let sync_task = tokio::spawn({
        let wal = wal.clone();
        async move {
            let r: Result<Result, _> = ingest_task.await;
            r??;

            let (to_notify, outcome) = task::block_in_place(|| wal.sync());

            for mut notify in to_notify {
                notify.send(outcome.clone()).await?;
            }

            Ok(())
        }
    });

    let r: Result<Result, _> = sync_task.await;
    r??;

    assert!(matches!(ingest_done_rx.next().await, Some(Ok(()))));

    let wal_entries: Result<Vec<_>, _> = builder.entries()?.collect();
    let wal_entries = wal_entries?;
    assert_eq!(1, wal_entries.len());
    assert_eq!(b"somedata".as_ref(), &*wal_entries[0].as_data());

    Ok(())
}
