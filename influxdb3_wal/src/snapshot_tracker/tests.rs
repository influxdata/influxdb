use super::*;

#[test]
fn snapshot() {
    let mut tracker = SnapshotTracker::new(2, Gen1Duration::new_1m(), None);
    let p1 = WalPeriod::new(
        WalFileSequenceNumber::new(1),
        Timestamp::new(0),
        Timestamp::new(60_000000000),
    );
    let p2 = WalPeriod::new(
        WalFileSequenceNumber::new(2),
        Timestamp::new(800000001),
        Timestamp::new(119_900000000),
    );
    let p3 = WalPeriod::new(
        WalFileSequenceNumber::new(3),
        Timestamp::new(60_600000000),
        Timestamp::new(120_900000000),
    );
    let p4 = WalPeriod::new(
        WalFileSequenceNumber::new(4),
        Timestamp::new(120_800000001),
        Timestamp::new(240_000000000),
    );
    let p5 = WalPeriod::new(
        WalFileSequenceNumber::new(5),
        Timestamp::new(180_800000001),
        Timestamp::new(299_000000000),
    );
    let p6 = WalPeriod::new(
        WalFileSequenceNumber::new(6),
        Timestamp::new(240_800000001),
        Timestamp::new(360_100000000),
    );

    assert!(tracker.snapshot(false).is_none());
    tracker.add_wal_period(p1.clone());
    assert!(tracker.snapshot(false).is_none());
    tracker.add_wal_period(p2.clone());
    assert!(tracker.snapshot(false).is_none());
    tracker.add_wal_period(p3.clone());
    assert_eq!(
        tracker.snapshot(false),
        Some(SnapshotDetails {
            snapshot_sequence_number: SnapshotSequenceNumber::new(1),
            end_time_marker: 120_000000000,
            first_wal_sequence_number: WalFileSequenceNumber::new(1),
            last_wal_sequence_number: WalFileSequenceNumber::new(2),
            forced: false,
        })
    );
    tracker.add_wal_period(p4.clone());
    assert_eq!(tracker.snapshot(false), None);
    tracker.add_wal_period(p5.clone());
    assert_eq!(
        tracker.snapshot(false),
        Some(SnapshotDetails {
            snapshot_sequence_number: SnapshotSequenceNumber::new(2),
            end_time_marker: 240_000000000,
            first_wal_sequence_number: WalFileSequenceNumber::new(3),
            last_wal_sequence_number: WalFileSequenceNumber::new(3),
            forced: false,
        })
    );

    assert_eq!(tracker.wal_periods, vec![p4.clone(), p5.clone()]);

    tracker.add_wal_period(p6.clone());
    assert_eq!(
        tracker.snapshot(false),
        Some(SnapshotDetails {
            snapshot_sequence_number: SnapshotSequenceNumber::new(3),
            end_time_marker: 360_000000000,
            first_wal_sequence_number: WalFileSequenceNumber::new(4),
            last_wal_sequence_number: WalFileSequenceNumber::new(5),
            forced: false,
        })
    );

    assert!(tracker.snapshot(false).is_none());
}

#[test]
fn snapshot_future_data_forces_snapshot() {
    let mut tracker = SnapshotTracker::new(2, Gen1Duration::new_1m(), None);
    let p1 = WalPeriod::new(
        WalFileSequenceNumber::new(1),
        Timestamp::new(0),
        Timestamp::new(300_100000000),
    );
    let p2 = WalPeriod::new(
        WalFileSequenceNumber::new(2),
        Timestamp::new(30_000000000),
        Timestamp::new(59_900000000),
    );
    let p3 = WalPeriod::new(
        WalFileSequenceNumber::new(3),
        Timestamp::new(60_000000000),
        Timestamp::new(60_900000000),
    );
    let p4 = WalPeriod::new(
        WalFileSequenceNumber::new(4),
        Timestamp::new(90_000000000),
        Timestamp::new(120_000000000),
    );
    let p5 = WalPeriod::new(
        WalFileSequenceNumber::new(5),
        Timestamp::new(120_000000000),
        Timestamp::new(150_000000000),
    );
    let p6 = WalPeriod::new(
        WalFileSequenceNumber::new(6),
        Timestamp::new(150_000000000),
        Timestamp::new(180_100000000),
    );

    tracker.add_wal_period(p1.clone());
    tracker.add_wal_period(p2.clone());
    tracker.add_wal_period(p3.clone());
    assert!(tracker.snapshot(false).is_none());
    tracker.add_wal_period(p4.clone());
    assert!(tracker.snapshot(false).is_none());
    tracker.add_wal_period(p5.clone());
    assert!(tracker.snapshot(false).is_none());
    tracker.add_wal_period(p6.clone());

    assert_eq!(
        tracker.snapshot(false),
        Some(SnapshotDetails {
            snapshot_sequence_number: SnapshotSequenceNumber::new(1),
            end_time_marker: 360000000000,
            first_wal_sequence_number: WalFileSequenceNumber::new(1),
            last_wal_sequence_number: WalFileSequenceNumber::new(6),
            forced: true,
        })
    );
}

/// `oldest_period_added_at` follows the oldest period still waiting: none
/// while empty or unstamped, the first stamped period's time otherwise, and
/// the next remaining period's after a snapshot drains the older ones.
#[test]
fn oldest_period_added_at_tracks_the_oldest_waiting_period() {
    let mut tracker = SnapshotTracker::new(2, Gen1Duration::new_1m(), None);
    assert_eq!(tracker.oldest_period_added_at(), None);

    let t0 = Time::from_timestamp_nanos(1_000);
    let t1 = Time::from_timestamp_nanos(2_000);
    // periods 1-2 sit in the first gen1 minute, period 3 in the next
    tracker.add_wal_period(
        WalPeriod::new(
            WalFileSequenceNumber::new(1),
            Timestamp::new(0),
            Timestamp::new(1),
        )
        .added_at(t0),
    );
    tracker.add_wal_period(
        WalPeriod::new(
            WalFileSequenceNumber::new(2),
            Timestamp::new(2),
            Timestamp::new(3),
        )
        .added_at(t1),
    );
    assert_eq!(tracker.oldest_period_added_at(), Some(t0));

    // A snapshot of everything leaves nothing waiting.
    let details = tracker.snapshot(true).expect("forced snapshot");
    assert_eq!(
        details.last_wal_sequence_number,
        WalFileSequenceNumber::new(2)
    );
    assert_eq!(tracker.oldest_period_added_at(), None);

    // An unstamped period reports nothing, even when older ones did.
    tracker.add_wal_period(WalPeriod::new(
        WalFileSequenceNumber::new(3),
        Timestamp::new(4),
        Timestamp::new(5),
    ));
    assert_eq!(tracker.oldest_period_added_at(), None);
}
