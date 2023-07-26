//! layout test for scenario shown to get stuck compacting.
//! The original of this set of files is querying a catalog of a partition stuck doing
//! non-productive compactions (which needs veritical splitting to resolve the impasse).
//!
//! See [crate::layout] module for detailed documentation

use data_types::CompactionLevel;
use iox_time::Time;
use std::time::Duration;

use crate::layouts::{layout_setup_builder, parquet_builder, run_layout_scenario, ONE_MB};
const MAX_DESIRED_FILE_SIZE: u64 = 100 * ONE_MB;

#[tokio::test]
async fn stuck_l0() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_num_files_per_plan(20)
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .with_partition_timeout(Duration::from_millis(10000))
        .with_suppress_run_output() // remove this to debug
        .build()
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686853019000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686930563065100652))
                .with_file_size_bytes(149933875),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686845579000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928811433793899))
                .with_file_size_bytes(103205619),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686853319000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686935546047601759))
                .with_file_size_bytes(150536767),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686871559000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686936871554969451))
                .with_file_size_bytes(102393626),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686854759000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686935947459465643))
                .with_file_size_bytes(87151809),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686845579000000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928854574095806))
                .with_file_size_bytes(5682010),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686852839000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686935742511199929))
                .with_file_size_bytes(75607192),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686855419000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686935151542899174))
                .with_file_size_bytes(87166408),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686855059000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686929965334855957))
                .with_file_size_bytes(88035623),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686855659000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686931893702512591))
                .with_file_size_bytes(90543489),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686852899000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686934966479515832))
                .with_file_size_bytes(75851382),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686853079000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686931336078719452))
                .with_file_size_bytes(149692663),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686853319000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686929421018268948))
                .with_file_size_bytes(150619037),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686853379000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686930780953922120))
                .with_file_size_bytes(58021414),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686852839000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686929712329555892))
                .with_file_size_bytes(75536272),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686853019000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686933271571861107))
                .with_file_size_bytes(149014949),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686852899000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686931600579333716))
                .with_file_size_bytes(72914229),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686852959000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686933528170895870))
                .with_file_size_bytes(74896171),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686855119000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686933830062404735))
                .with_file_size_bytes(89245536),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686852119000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686934254955029762))
                .with_file_size_bytes(105905115),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686849719000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686932458050354802))
                .with_file_size_bytes(104819243),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686853679000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686934759745855254))
                .with_file_size_bytes(150386578),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686841379000000000)
                .with_max_time(1686854219000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686932677391046778))
                .with_file_size_bytes(67069745),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686845639000000000)
                .with_max_time(1686849779000000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928854574095806))
                .with_file_size_bytes(5526463),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686845639000000000)
                .with_max_time(1686849779000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928811433793899))
                .with_file_size_bytes(101878097),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686849779000000000)
                .with_max_time(1686858119000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686932458050354802))
                .with_file_size_bytes(104808702),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686849839000000000)
                .with_max_time(1686850559000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928811433793899))
                .with_file_size_bytes(21186155),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686849839000000000)
                .with_max_time(1686850559000000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928854574095806))
                .with_file_size_bytes(998505),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686850619000000000)
                .with_max_time(1686854819000000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928854574095806))
                .with_file_size_bytes(5580685),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686850619000000000)
                .with_max_time(1686854819000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928811433793899))
                .with_file_size_bytes(103246896),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686852179000000000)
                .with_max_time(1686862859000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686934254955029762))
                .with_file_size_bytes(105513447),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686852899000000000)
                .with_max_time(1686864359000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686935742511199929))
                .with_file_size_bytes(139541880),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686852899000000000)
                .with_max_time(1686864359000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686929712329555892))
                .with_file_size_bytes(139400211),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686852959000000000)
                .with_max_time(1686864419000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686931600579333716))
                .with_file_size_bytes(136888003),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686852959000000000)
                .with_max_time(1686864419000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686934966479515832))
                .with_file_size_bytes(139953230),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686853019000000000)
                .with_max_time(1686864599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686933528170895870))
                .with_file_size_bytes(138845602),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686853079000000000)
                .with_max_time(1686864659000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686933271571861107))
                .with_file_size_bytes(84174642),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686853079000000000)
                .with_max_time(1686864659000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686930563065100652))
                .with_file_size_bytes(83486810),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686853139000000000)
                .with_max_time(1686864839000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686931336078719452))
                .with_file_size_bytes(83035926),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686853379000000000)
                .with_max_time(1686865259000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686929421018268948))
                .with_file_size_bytes(80749475),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686853379000000000)
                .with_max_time(1686865259000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686935546047601759))
                .with_file_size_bytes(80622284),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686853439000000000)
                .with_max_time(1686865439000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686930780953922120))
                .with_file_size_bytes(130471302),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686853739000000000)
                .with_max_time(1686866039000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686934759745855254))
                .with_file_size_bytes(76518641),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686854279000000000)
                .with_max_time(1686867059000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686932677391046778))
                .with_file_size_bytes(81222708),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686854819000000000)
                .with_max_time(1686868199000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686935947459465643))
                .with_file_size_bytes(93828618),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686854879000000000)
                .with_max_time(1686859019000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928811433793899))
                .with_file_size_bytes(101899966),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686854879000000000)
                .with_max_time(1686859019000000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928854574095806))
                .with_file_size_bytes(5444939),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686855119000000000)
                .with_max_time(1686868739000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686929965334855957))
                .with_file_size_bytes(97364742),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686855179000000000)
                .with_max_time(1686868859000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686933830062404735))
                .with_file_size_bytes(96919046),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686855479000000000)
                .with_max_time(1686869519000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686935151542899174))
                .with_file_size_bytes(101734904),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686855719000000000)
                .with_max_time(1686869939000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686931893702512591))
                .with_file_size_bytes(100008012),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686858179000000000)
                .with_max_time(1686865979000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686932458050354802))
                .with_file_size_bytes(98556380),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686859079000000000)
                .with_max_time(1686859499000000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928854574095806))
                .with_file_size_bytes(593319),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686859079000000000)
                .with_max_time(1686859499000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928811433793899))
                .with_file_size_bytes(14403989),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686859559000000000)
                .with_max_time(1686863699000000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928854574095806))
                .with_file_size_bytes(5423734),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686859559000000000)
                .with_max_time(1686863699000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928811433793899))
                .with_file_size_bytes(101893482),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686862919000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686934254955029762))
                .with_file_size_bytes(102580493),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686863759000000000)
                .with_max_time(1686867659000000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928854574095806))
                .with_file_size_bytes(5026731),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686863759000000000)
                .with_max_time(1686867839000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928811433793899))
                .with_file_size_bytes(100495018),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686864419000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686929712329555892))
                .with_file_size_bytes(78503529),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686864419000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686935742511199929))
                .with_file_size_bytes(78149265),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686864479000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686934966479515832))
                .with_file_size_bytes(77391966),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686864479000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686931600579333716))
                .with_file_size_bytes(83215868),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686864659000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686933528170895870))
                .with_file_size_bytes(76904008),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686864719000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686930563065100652))
                .with_file_size_bytes(56776838),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686864719000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686933271571861107))
                .with_file_size_bytes(56708180),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686864899000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686931336078719452))
                .with_file_size_bytes(55114047),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686865319000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686929421018268948))
                .with_file_size_bytes(51263308),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686865319000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686935546047601759))
                .with_file_size_bytes(51157926),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686865499000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686930780953922120))
                .with_file_size_bytes(92510190),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686866099000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686934759745855254))
                .with_file_size_bytes(46749740),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686867119000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686932677391046778))
                .with_file_size_bytes(114531826),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686867719000000000)
                .with_max_time(1686867839000000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928854574095806))
                .with_file_size_bytes(229903),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686867899000000000)
                .with_max_time(1686868319000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928811433793899))
                .with_file_size_bytes(14513946),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686867899000000000)
                .with_max_time(1686868319000000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928854574095806))
                .with_file_size_bytes(602054),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686868259000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686935947459465643))
                .with_file_size_bytes(70522099),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686868379000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928854574095806))
                .with_file_size_bytes(93408439),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686868379000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928118432114258))
                .with_file_size_bytes(41089381),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686868799000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686929965334855957))
                .with_file_size_bytes(61094135),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686868919000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686933830062404735))
                .with_file_size_bytes(59466261),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686869579000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686935151542899174))
                .with_file_size_bytes(51024344),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686869999000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686931893702512591))
                .with_file_size_bytes(45632935),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686871619000000000)
                .with_max_time(1686873599000000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686936871554969451))
                .with_file_size_bytes(9380799),
        )
        .await;

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.1[1686841379000000000,1686853019000000000] 1686930563.07s 143mb|-------------L0.1-------------|                                                          "
    - "L0.3[1686841379000000000,1686853319000000000] 1686935546.05s 144mb|-------------L0.3--------------|                                                         "
    - "L0.4[1686841379000000000,1686871559000000000] 1686936871.55s 98mb|---------------------------------------L0.4---------------------------------------|      "
    - "L0.5[1686841379000000000,1686854759000000000] 1686935947.46s 83mb|---------------L0.5----------------|                                                     "
    - "L0.7[1686841379000000000,1686852839000000000] 1686935742.51s 72mb|-------------L0.7-------------|                                                          "
    - "L0.8[1686841379000000000,1686855419000000000] 1686935151.54s 83mb|----------------L0.8-----------------|                                                   "
    - "L0.9[1686841379000000000,1686855059000000000] 1686929965.33s 84mb|----------------L0.9----------------|                                                    "
    - "L0.10[1686841379000000000,1686855659000000000] 1686931893.7s 86mb|----------------L0.10----------------|                                                   "
    - "L0.11[1686841379000000000,1686852899000000000] 1686934966.48s 72mb|------------L0.11-------------|                                                          "
    - "L0.12[1686841379000000000,1686853079000000000] 1686931336.08s 143mb|------------L0.12-------------|                                                          "
    - "L0.13[1686841379000000000,1686853319000000000] 1686929421.02s 144mb|-------------L0.13-------------|                                                         "
    - "L0.14[1686841379000000000,1686853379000000000] 1686930780.95s 55mb|-------------L0.14-------------|                                                         "
    - "L0.15[1686841379000000000,1686852839000000000] 1686929712.33s 72mb|------------L0.15-------------|                                                          "
    - "L0.16[1686841379000000000,1686853019000000000] 1686933271.57s 142mb|------------L0.16-------------|                                                          "
    - "L0.17[1686841379000000000,1686852899000000000] 1686931600.58s 70mb|------------L0.17-------------|                                                          "
    - "L0.18[1686841379000000000,1686852959000000000] 1686933528.17s 71mb|------------L0.18-------------|                                                          "
    - "L0.19[1686841379000000000,1686855119000000000] 1686933830.06s 85mb|---------------L0.19----------------|                                                    "
    - "L0.20[1686841379000000000,1686852119000000000] 1686934254.96s 101mb|-----------L0.20------------|                                                            "
    - "L0.21[1686841379000000000,1686849719000000000] 1686932458.05s 100mb|--------L0.21--------|                                                                   "
    - "L0.22[1686841379000000000,1686853679000000000] 1686934759.75s 143mb|-------------L0.22--------------|                                                        "
    - "L0.23[1686841379000000000,1686854219000000000] 1686932677.39s 64mb|--------------L0.23--------------|                                                       "
    - "L0.26[1686849779000000000,1686858119000000000] 1686932458.05s 100mb                       |--------L0.26--------|                                            "
    - "L0.31[1686852179000000000,1686862859000000000] 1686934254.96s 101mb                              |-----------L0.31-----------|                               "
    - "L0.32[1686852899000000000,1686864359000000000] 1686935742.51s 133mb                                |------------L0.32-------------|                          "
    - "L0.33[1686852899000000000,1686864359000000000] 1686929712.33s 133mb                                |------------L0.33-------------|                          "
    - "L0.34[1686852959000000000,1686864419000000000] 1686931600.58s 131mb                                |------------L0.34-------------|                          "
    - "L0.35[1686852959000000000,1686864419000000000] 1686934966.48s 133mb                                |------------L0.35-------------|                          "
    - "L0.36[1686853019000000000,1686864599000000000] 1686933528.17s 132mb                                |------------L0.36-------------|                          "
    - "L0.37[1686853079000000000,1686864659000000000] 1686933271.57s 80mb                                |------------L0.37-------------|                          "
    - "L0.38[1686853079000000000,1686864659000000000] 1686930563.07s 80mb                                |------------L0.38-------------|                          "
    - "L0.39[1686853139000000000,1686864839000000000] 1686931336.08s 79mb                                |------------L0.39-------------|                          "
    - "L0.40[1686853379000000000,1686865259000000000] 1686929421.02s 77mb                                 |-------------L0.40-------------|                        "
    - "L0.41[1686853379000000000,1686865259000000000] 1686935546.05s 77mb                                 |-------------L0.41-------------|                        "
    - "L0.42[1686853439000000000,1686865439000000000] 1686930780.95s 124mb                                 |-------------L0.42-------------|                        "
    - "L0.43[1686853739000000000,1686866039000000000] 1686934759.75s 73mb                                  |-------------L0.43--------------|                      "
    - "L0.44[1686854279000000000,1686867059000000000] 1686932677.39s 77mb                                    |--------------L0.44--------------|                   "
    - "L0.45[1686854819000000000,1686868199000000000] 1686935947.46s 89mb                                     |---------------L0.45---------------|                "
    - "L0.48[1686855119000000000,1686868739000000000] 1686929965.33s 93mb                                      |---------------L0.48----------------|              "
    - "L0.49[1686855179000000000,1686868859000000000] 1686933830.06s 92mb                                      |---------------L0.49----------------|              "
    - "L0.50[1686855479000000000,1686869519000000000] 1686935151.54s 97mb                                       |----------------L0.50----------------|            "
    - "L0.51[1686855719000000000,1686869939000000000] 1686931893.7s 95mb                                        |----------------L0.51----------------|           "
    - "L0.52[1686858179000000000,1686865979000000000] 1686932458.05s 94mb                                              |-------L0.52-------|                       "
    - "L0.57[1686862919000000000,1686873599000000000] 1686934254.96s 98mb                                                            |-----------L0.57-----------| "
    - "L0.60[1686864419000000000,1686873599000000000] 1686929712.33s 75mb                                                                |---------L0.60---------| "
    - "L0.61[1686864419000000000,1686873599000000000] 1686935742.51s 75mb                                                                |---------L0.61---------| "
    - "L0.62[1686864479000000000,1686873599000000000] 1686934966.48s 74mb                                                                |---------L0.62---------| "
    - "L0.63[1686864479000000000,1686873599000000000] 1686931600.58s 79mb                                                                |---------L0.63---------| "
    - "L0.64[1686864659000000000,1686873599000000000] 1686933528.17s 73mb                                                                 |--------L0.64---------| "
    - "L0.65[1686864719000000000,1686873599000000000] 1686930563.07s 54mb                                                                 |--------L0.65---------| "
    - "L0.66[1686864719000000000,1686873599000000000] 1686933271.57s 54mb                                                                 |--------L0.66---------| "
    - "L0.67[1686864899000000000,1686873599000000000] 1686931336.08s 53mb                                                                 |--------L0.67---------| "
    - "L0.68[1686865319000000000,1686873599000000000] 1686929421.02s 49mb                                                                  |--------L0.68--------| "
    - "L0.69[1686865319000000000,1686873599000000000] 1686935546.05s 49mb                                                                  |--------L0.69--------| "
    - "L0.70[1686865499000000000,1686873599000000000] 1686930780.95s 88mb                                                                   |-------L0.70--------| "
    - "L0.71[1686866099000000000,1686873599000000000] 1686934759.75s 45mb                                                                     |------L0.71-------| "
    - "L0.72[1686867119000000000,1686873599000000000] 1686932677.39s 109mb                                                                       |-----L0.72------| "
    - "L0.76[1686868259000000000,1686873599000000000] 1686935947.46s 67mb                                                                           |---L0.76----| "
    - "L0.79[1686868799000000000,1686873599000000000] 1686929965.33s 58mb                                                                            |---L0.79---| "
    - "L0.80[1686868919000000000,1686873599000000000] 1686933830.06s 57mb                                                                            |---L0.80---| "
    - "L0.81[1686869579000000000,1686873599000000000] 1686935151.54s 49mb                                                                              |--L0.81--| "
    - "L0.82[1686869999000000000,1686873599000000000] 1686931893.7s 44mb                                                                               |-L0.82--| "
    - "L0.83[1686871619000000000,1686873599000000000] 1686936871.55s 9mb                                                                                    |L0.83|"
    - "L1                                                                                                                 "
    - "L1.6[1686841379000000000,1686845579000000000] 1686928854.57s 5mb|--L1.6---|                                                                               "
    - "L1.24[1686845639000000000,1686849779000000000] 1686928854.57s 5mb           |--L1.24--|                                                                    "
    - "L1.28[1686849839000000000,1686850559000000000] 1686928854.57s 975kb                       |L1.28|                                                            "
    - "L1.29[1686850619000000000,1686854819000000000] 1686928854.57s 5mb                         |--L1.29--|                                                      "
    - "L1.47[1686854879000000000,1686859019000000000] 1686928854.57s 5mb                                     |--L1.47--|                                          "
    - "L1.53[1686859079000000000,1686859499000000000] 1686928854.57s 579kb                                                 |L1.53|                                  "
    - "L1.55[1686859559000000000,1686863699000000000] 1686928854.57s 5mb                                                  |--L1.55--|                             "
    - "L1.58[1686863759000000000,1686867659000000000] 1686928854.57s 5mb                                                              |-L1.58--|                  "
    - "L1.73[1686867719000000000,1686867839000000000] 1686928854.57s 225kb                                                                         |L1.73|          "
    - "L1.75[1686867899000000000,1686868319000000000] 1686928854.57s 588kb                                                                          |L1.75|         "
    - "L1.77[1686868379000000000,1686873599000000000] 1686928854.57s 89mb                                                                           |---L1.77----| "
    - "L2                                                                                                                 "
    - "L2.2[1686841379000000000,1686845579000000000] 1686928811.43s 98mb|--L2.2---|                                                                               "
    - "L2.25[1686845639000000000,1686849779000000000] 1686928811.43s 97mb           |--L2.25--|                                                                    "
    - "L2.27[1686849839000000000,1686850559000000000] 1686928811.43s 20mb                       |L2.27|                                                            "
    - "L2.30[1686850619000000000,1686854819000000000] 1686928811.43s 98mb                         |--L2.30--|                                                      "
    - "L2.46[1686854879000000000,1686859019000000000] 1686928811.43s 97mb                                     |--L2.46--|                                          "
    - "L2.54[1686859079000000000,1686859499000000000] 1686928811.43s 14mb                                                 |L2.54|                                  "
    - "L2.56[1686859559000000000,1686863699000000000] 1686928811.43s 97mb                                                  |--L2.56--|                             "
    - "L2.59[1686863759000000000,1686867839000000000] 1686928811.43s 96mb                                                              |--L2.59--|                 "
    - "L2.74[1686867899000000000,1686868319000000000] 1686928811.43s 14mb                                                                          |L2.74|         "
    - "L2.78[1686868379000000000,1686873599000000000] 1686928118.43s 39mb                                                                           |---L2.78----| "
    - "**** Final Output Files (49.67gb written)"
    - "L1                                                                                                                 "
    - "L1.1867[1686873206669502294,1686873599000000000] 1686936871.55s 83mb                                                                                        |L1.1867|"
    - "L2                                                                                                                 "
    - "L2.1889[1686841379000000000,1686842394885830950] 1686936871.55s 100mb|L2.1889|                                                                                 "
    - "L2.1890[1686842394885830951,1686842786390926609] 1686936871.55s 39mb  |L2.1890|                                                                               "
    - "L2.1891[1686842786390926610,1686843316416264410] 1686936871.55s 100mb   |L2.1891|                                                                              "
    - "L2.1896[1686843316416264411,1686843684225379958] 1686936871.55s 100mb     |L2.1896|                                                                            "
    - "L2.1897[1686843684225379959,1686844052034495505] 1686936871.55s 100mb      |L2.1897|                                                                           "
    - "L2.1898[1686844052034495506,1686844193781853218] 1686936871.55s 39mb       |L2.1898|                                                                          "
    - "L2.1899[1686844193781853219,1686844848044941008] 1686936871.55s 100mb       |L2.1899|                                                                          "
    - "L2.1904[1686844848044941009,1686845161518308591] 1686936871.55s 100mb         |L2.1904|                                                                        "
    - "L2.1905[1686845161518308592,1686845474991676173] 1686936871.55s 100mb          |L2.1905|                                                                       "
    - "L2.1906[1686845474991676174,1686845579000000000] 1686936871.55s 33mb           |L2.1906|                                                                      "
    - "L2.1914[1686845579000000001,1686846586992441776] 1686936871.55s 100mb           |L2.1914|                                                                      "
    - "L2.1919[1686846586992441777,1686847213091270336] 1686936871.55s 100mb              |L2.1919|                                                                   "
    - "L2.1920[1686847213091270337,1686847594984883551] 1686936871.55s 61mb                |L2.1920|                                                                 "
    - "L2.1921[1686847594984883552,1686847967133302591] 1686936871.55s 100mb                 |L2.1921|                                                                "
    - "L2.1922[1686847967133302592,1686848339281721630] 1686936871.55s 100mb                  |L2.1922|                                                               "
    - "L2.1923[1686848339281721631,1686848602977306099] 1686936871.55s 71mb                   |L2.1923|                                                              "
    - "L2.1924[1686848602977306100,1686849027096194358] 1686936871.55s 100mb                    |L2.1924|                                                             "
    - "L2.1925[1686849027096194359,1686849451215082616] 1686936871.55s 100mb                     |L2.1925|                                                            "
    - "L2.1926[1686849451215082617,1686849779000000000] 1686936871.55s 77mb                      |L2.1926|                                                           "
    - "L2.1934[1686849779000000001,1686851019138093591] 1686936871.55s 100mb                       |L2.1934|                                                          "
    - "L2.1939[1686851019138093592,1686851845896821338] 1686936871.55s 100mb                          |L2.1939|                                                       "
    - "L2.1944[1686851845896821339,1686852397069307378] 1686936871.55s 100mb                             |L2.1944|                                                    "
    - "L2.1949[1686852397069307379,1686852764517630237] 1686936871.55s 100mb                              |L2.1949|                                                   "
    - "L2.1950[1686852764517630238,1686853131965953095] 1686936871.55s 100mb                               |L2.1950|                                                  "
    - "L2.1952[1686853131965953096,1686853754631187460] 1686936871.55s 100mb                                |L2.1952|                                                 "
    - "L2.1958[1686853754631187461,1686854074019438293] 1686936871.55s 100mb                                  |L2.1958|                                               "
    - "L2.1959[1686854074019438294,1686854377296421824] 1686936871.55s 95mb                                   |L2.1959|                                              "
    - "L2.1960[1686854377296421825,1686854689009102312] 1686936871.55s 100mb                                    |L2.1960|                                             "
    - "L2.1961[1686854689009102313,1686854840368603300] 1686936871.55s 49mb                                     |L2.1961|                                            "
    - "L2.1969[1686854840368603301,1686855548468013502] 1686936871.55s 100mb                                     |L2.1969|                                            "
    - "L2.1970[1686855548468013503,1686856256567423703] 1686936871.55s 100mb                                       |L2.1970|                                          "
    - "L2.1971[1686856256567423704,1686856536982788349] 1686936871.55s 40mb                                         |L2.1971|                                        "
    - "L2.1972[1686856536982788350,1686857111714340888] 1686936871.55s 100mb                                          |L2.1972|                                       "
    - "L2.1977[1686857111714340889,1686857492337275263] 1686936871.55s 100mb                                           |L2.1977|                                      "
    - "L2.1978[1686857492337275264,1686857872960209637] 1686936871.55s 100mb                                             |L2.1978|                                    "
    - "L2.1979[1686857872960209638,1686858233596973397] 1686936871.55s 95mb                                              |L2.1979|                                   "
    - "L2.1980[1686858233596973398,1686858684102523465] 1686936871.55s 100mb                                               |L2.1980|                                  "
    - "L2.1981[1686858684102523466,1686859134608073532] 1686936871.55s 100mb                                                |L2.1981|                                 "
    - "L2.1982[1686859134608073533,1686859499000000000] 1686936871.55s 81mb                                                 |L2.1982|                                "
    - "L2.1990[1686859499000000001,1686860134536077779] 1686936871.55s 100mb                                                  |L2.1990|                               "
    - "L2.1991[1686860134536077780,1686860770072155557] 1686936871.55s 100mb                                                    |L2.1991|                             "
    - "L2.1993[1686860770072155558,1686861617453595289] 1686936871.55s 100mb                                                      |L2.1993|                           "
    - "L2.1998[1686861617453595290,1686862007484245554] 1686936871.55s 100mb                                                        |L2.1998|                         "
    - "L2.1999[1686862007484245555,1686862397514895818] 1686936871.55s 100mb                                                         |L2.1999|                        "
    - "L2.2000[1686862397514895819,1686862464835035020] 1686936871.55s 17mb                                                          |L2.2000|                       "
    - "L2.2001[1686862464835035021,1686862789833508891] 1686936871.55s 100mb                                                          |L2.2001|                       "
    - "L2.2002[1686862789833508892,1686863114831982761] 1686936871.55s 100mb                                                           |L2.2002|                      "
    - "L2.2003[1686863114831982762,1686863312216466667] 1686936871.55s 61mb                                                            |L2.2003|                     "
    - "L2.2011[1686863312216466668,1686864075006108740] 1686936871.55s 100mb                                                             |L2.2011|                    "
    - "L2.2012[1686864075006108741,1686864837795750812] 1686936871.55s 100mb                                                               |L2.2012|                  "
    - "L2.2013[1686864837795750813,1686864893143269255] 1686936871.55s 7mb                                                                 |L2.2013|                "
    - "L2.2014[1686864893143269256,1686865484385600871] 1686936871.55s 100mb                                                                 |L2.2014|                "
    - "L2.2015[1686865484385600872,1686866075627932486] 1686936871.55s 100mb                                                                   |L2.2015|              "
    - "L2.2024[1686866075627932487,1686866607164120163] 1686936871.55s 100mb                                                                    |L2.2024|             "
    - "L2.2025[1686866607164120164,1686867138700307839] 1686936871.55s 100mb                                                                      |L2.2025|           "
    - "L2.2027[1686867138700307840,1686867545868269737] 1686936871.55s 100mb                                                                       |L2.2027|          "
    - "L2.2028[1686867545868269738,1686867953036231634] 1686936871.55s 100mb                                                                         |L2.2028|        "
    - "L2.2029[1686867953036231635,1686868319000000000] 1686936871.55s 90mb                                                                          |L2.2029|       "
    - "L2.2037[1686868319000000001,1686869079291584458] 1686936871.55s 100mb                                                                           |L2.2037|      "
    - "L2.2038[1686869079291584459,1686869839583168915] 1686936871.55s 100mb                                                                             |L2.2038|    "
    - "L2.2039[1686869839583168916,1686870526488100069] 1686936871.55s 90mb                                                                               |L2.2039|  "
    - "L2.2047[1686870526488100070,1686871158741704859] 1686936871.55s 100mb                                                                                 |L2.2047|"
    - "L2.2048[1686871158741704860,1686871790995309648] 1686936871.55s 100mb                                                                                   |L2.2048|"
    - "L2.2050[1686871790995309649,1686872061589130317] 1686936871.55s 100mb                                                                                    |L2.2050|"
    - "L2.2051[1686872061589130318,1686872332182950985] 1686936871.55s 100mb                                                                                     |L2.2051|"
    - "L2.2052[1686872332182950986,1686872423248908406] 1686936871.55s 34mb                                                                                      |L2.2052|"
    - "L2.2053[1686872423248908407,1686872916648819217] 1686936871.55s 100mb                                                                                      |L2.2053|"
    - "L2.2054[1686872916648819218,1686873410048730027] 1686936871.55s 100mb                                                                                        |L2.2054|"
    - "L2.2055[1686873410048730028,1686873599000000000] 1686936871.55s 38mb                                                                                         |L2.2055|"
    "###
    );
}

#[tokio::test]
async fn stuck_l1() {
    test_helpers::maybe_start_logging();

    let setup = layout_setup_builder()
        .await
        .with_max_num_files_per_plan(20)
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .with_partition_timeout(Duration::from_millis(100))
        .build()
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686873630000000000)
                .with_max_time(1686879712000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686927078592450239))
                .with_file_size_bytes(104071379),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686873630000000000)
                .with_max_time(1686920683000000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928116935534089))
                .with_file_size_bytes(74761432),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686879750000000000)
                .with_max_time(1686885832000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686927078592450239))
                .with_file_size_bytes(104046636),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686885870000000000)
                .with_max_time(1686888172000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686927078592450239))
                .with_file_size_bytes(39504848),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686888210000000000)
                .with_max_time(1686894292000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686927078592450239))
                .with_file_size_bytes(104068640),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686894330000000000)
                .with_max_time(1686900412000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686927078592450239))
                .with_file_size_bytes(104024462),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686900450000000000)
                .with_max_time(1686901072000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686927078592450239))
                .with_file_size_bytes(12847477),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686901110000000000)
                .with_max_time(1686907132000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686927078592450239))
                .with_file_size_bytes(103082698),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686907170000000000)
                .with_max_time(1686910072000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686927078592450239))
                .with_file_size_bytes(51292692),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686910110000000000)
                .with_max_time(1686919792000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686926864318936602))
                .with_file_size_bytes(105671599),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686919830000000000)
                .with_max_time(1686926803000000000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686926864318936602))
                .with_file_size_bytes(71282156),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1686920730000000000)
                .with_max_time(1686926803000000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1686928116935534089))
                .with_file_size_bytes(38566243),
        )
        .await;

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L1                                                                                                                 "
    - "L1.2[1686873630000000000,1686920683000000000] 1686928116.94s 71mb|------------------------------------L1.2-------------------------------------|           "
    - "L1.12[1686920730000000000,1686926803000000000] 1686928116.94s 37mb                                                                               |-L1.12--| "
    - "L2                                                                                                                 "
    - "L2.1[1686873630000000000,1686879712000000000] 1686927078.59s 99mb|--L2.1--|                                                                                "
    - "L2.3[1686879750000000000,1686885832000000000] 1686927078.59s 99mb          |--L2.3--|                                                                      "
    - "L2.4[1686885870000000000,1686888172000000000] 1686927078.59s 38mb                    |L2.4|                                                                "
    - "L2.5[1686888210000000000,1686894292000000000] 1686927078.59s 99mb                        |--L2.5--|                                                        "
    - "L2.6[1686894330000000000,1686900412000000000] 1686927078.59s 99mb                                   |--L2.6--|                                             "
    - "L2.7[1686900450000000000,1686901072000000000] 1686927078.59s 12mb                                             |L2.7|                                       "
    - "L2.8[1686901110000000000,1686907132000000000] 1686927078.59s 98mb                                              |--L2.8--|                                  "
    - "L2.9[1686907170000000000,1686910072000000000] 1686927078.59s 49mb                                                        |L2.9|                            "
    - "L2.10[1686910110000000000,1686919792000000000] 1686926864.32s 101mb                                                             |----L2.10-----|             "
    - "L2.11[1686919830000000000,1686926803000000000] 1686926864.32s 68mb                                                                              |--L2.11--| "
    - "**** Simulation run 0, type=split(ReduceOverlap)(split_times=[1686879712000000000, 1686885832000000000, 1686888172000000000, 1686894292000000000, 1686900412000000000, 1686901072000000000, 1686907132000000000, 1686910072000000000, 1686919792000000000]). 1 Input Files, 71mb total:"
    - "L1, all files 71mb                                                                                                 "
    - "L1.2[1686873630000000000,1686920683000000000] 1686928116.94s|------------------------------------------L1.2------------------------------------------|"
    - "**** 10 Output Files (parquet_file_id not yet assigned), 71mb total:"
    - "L1                                                                                                                 "
    - "L1.?[1686873630000000000,1686879712000000000] 1686928116.94s 9mb|--L1.?---|                                                                               "
    - "L1.?[1686879712000000001,1686885832000000000] 1686928116.94s 9mb           |--L1.?---|                                                                    "
    - "L1.?[1686885832000000001,1686888172000000000] 1686928116.94s 4mb                       |L1.?|                                                             "
    - "L1.?[1686888172000000001,1686894292000000000] 1686928116.94s 9mb                           |--L1.?---|                                                    "
    - "L1.?[1686894292000000001,1686900412000000000] 1686928116.94s 9mb                                       |--L1.?---|                                        "
    - "L1.?[1686900412000000001,1686901072000000000] 1686928116.94s 1mb                                                   |L1.?|                                 "
    - "L1.?[1686901072000000001,1686907132000000000] 1686928116.94s 9mb                                                    |--L1.?---|                           "
    - "L1.?[1686907132000000001,1686910072000000000] 1686928116.94s 4mb                                                                |L1.?|                    "
    - "L1.?[1686910072000000001,1686919792000000000] 1686928116.94s 15mb                                                                     |------L1.?------|   "
    - "L1.?[1686919792000000001,1686920683000000000] 1686928116.94s 1mb                                                                                        |L1.?|"
    - "Committing partition 1:"
    - "  Soft Deleting 1 files: L1.2"
    - "  Creating 10 files"
    - "**** Simulation run 1, type=split(CompactAndSplitOutput(FoundSubsetLessThanMaxCompactSize))(split_times=[1686879262359644750, 1686884894719289500]). 6 Input Files, 258mb total:"
    - "L1                                                                                                                 "
    - "L1.13[1686873630000000000,1686879712000000000] 1686928116.94s 9mb|---------------L1.13---------------|                                                     "
    - "L1.14[1686879712000000001,1686885832000000000] 1686928116.94s 9mb                                     |---------------L1.14---------------|                "
    - "L1.15[1686885832000000001,1686888172000000000] 1686928116.94s 4mb                                                                           |---L1.15----| "
    - "L2                                                                                                                 "
    - "L2.1[1686873630000000000,1686879712000000000] 1686927078.59s 99mb|---------------L2.1----------------|                                                     "
    - "L2.3[1686879750000000000,1686885832000000000] 1686927078.59s 99mb                                     |---------------L2.3----------------|                "
    - "L2.4[1686885870000000000,1686888172000000000] 1686927078.59s 38mb                                                                           |----L2.4----| "
    - "**** 3 Output Files (parquet_file_id not yet assigned), 258mb total:"
    - "L2                                                                                                                 "
    - "L2.?[1686873630000000000,1686879262359644750] 1686928116.94s 100mb|--------------L2.?--------------|                                                        "
    - "L2.?[1686879262359644751,1686884894719289500] 1686928116.94s 100mb                                  |--------------L2.?--------------|                      "
    - "L2.?[1686884894719289501,1686888172000000000] 1686928116.94s 58mb                                                                     |-------L2.?-------| "
    - "Committing partition 1:"
    - "  Soft Deleting 6 files: L2.1, L2.3, L2.4, L1.13, L1.14, L1.15"
    - "  Creating 3 files"
    - "**** Final Output Files (329mb written)"
    - "L1                                                                                                                 "
    - "L1.12[1686920730000000000,1686926803000000000] 1686928116.94s 37mb                                                                               |-L1.12--| "
    - "L1.16[1686888172000000001,1686894292000000000] 1686928116.94s 9mb                        |-L1.16--|                                                        "
    - "L1.17[1686894292000000001,1686900412000000000] 1686928116.94s 9mb                                  |-L1.17--|                                              "
    - "L1.18[1686900412000000001,1686901072000000000] 1686928116.94s 1mb                                             |L1.18|                                      "
    - "L1.19[1686901072000000001,1686907132000000000] 1686928116.94s 9mb                                              |-L1.19--|                                  "
    - "L1.20[1686907132000000001,1686910072000000000] 1686928116.94s 4mb                                                        |L1.20|                           "
    - "L1.21[1686910072000000001,1686919792000000000] 1686928116.94s 15mb                                                             |----L1.21-----|             "
    - "L1.22[1686919792000000001,1686920683000000000] 1686928116.94s 1mb                                                                              |L1.22|     "
    - "L2                                                                                                                 "
    - "L2.5[1686888210000000000,1686894292000000000] 1686927078.59s 99mb                        |--L2.5--|                                                        "
    - "L2.6[1686894330000000000,1686900412000000000] 1686927078.59s 99mb                                   |--L2.6--|                                             "
    - "L2.7[1686900450000000000,1686901072000000000] 1686927078.59s 12mb                                             |L2.7|                                       "
    - "L2.8[1686901110000000000,1686907132000000000] 1686927078.59s 98mb                                              |--L2.8--|                                  "
    - "L2.9[1686907170000000000,1686910072000000000] 1686927078.59s 49mb                                                        |L2.9|                            "
    - "L2.10[1686910110000000000,1686919792000000000] 1686926864.32s 101mb                                                             |----L2.10-----|             "
    - "L2.11[1686919830000000000,1686926803000000000] 1686926864.32s 68mb                                                                              |--L2.11--| "
    - "L2.23[1686873630000000000,1686879262359644750] 1686928116.94s 100mb|-L2.23-|                                                                                 "
    - "L2.24[1686879262359644751,1686884894719289500] 1686928116.94s 100mb         |-L2.24-|                                                                        "
    - "L2.25[1686884894719289501,1686888172000000000] 1686928116.94s 58mb                   |L2.25|                                                                "
    "###
    );
    // TODO(maybe): see matching comment in files_to_compact.rs/limit_files_to_compact
    // The L1s left above are less than ideal, but maybe not bad.  This scenario initially just barely met the criteria for compaction and started with splits
    // to remove overlaps between L1 and L2 (that's good).  Due to the grouping of files they didn't all get to compact in the first round (that's ok).
    // But the first few that got compacted in the first round were enough to make the partition no longer meet the criteria for compaction, so the rest
    // are left sitting there ready to compact with their L2s, but not quite getting to.
    // The critical point is that this case doesn't loop forever anymore.
}

#[tokio::test]
async fn stuck_l0_large_l0s() {
    test_helpers::maybe_start_logging();

    let max_files = 20;
    let setup = layout_setup_builder()
        .await
        .with_max_num_files_per_plan(max_files)
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .with_partition_timeout(Duration::from_millis(100000))
        .with_suppress_run_output() // remove this to debug
        .build()
        .await;

    // This will be a big set of overlapping L0s which lands in a single chain, forcing sorting by max_l0_created_at
    // within the chain.  We'll use that forced sorting by max_l0_created_at to try to force the first 20 files (which
    // are max sized), to be repeatedly compacted with themselves.

    // Three things to notice about the first set of files:
    // 1) Everything in this test will be forced into one overlapping chain, which forces sorting by max_lo_created_at
    //    within the chain.
    // 2) The first/earliest/left-most (by max_l0_created_at) 20 files are already max sized so L0->L0 compaction won't
    //    change them.  Compacting the first 20 will be an unproductive compaction (20 files -> 20 files).
    // 3) All of these files are time range 1->2000.  Compacting these then together will give us 20 max sized files
    //    that cover 100 ns each. All of those alternately split files will still overlap the rest of the chain, so
    //    compacting the first set doesn't kick anything out of the chain, and doesn't accomplish anything.
    for i in 0..max_files {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(1)
                    .with_max_time(2000)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i as i64))
                    .with_file_size_bytes(MAX_DESIRED_FILE_SIZE),
            )
            .await;
    }
    // Things to notice about the second set of files:
    // 1) We're adding enough smaller files to bring down the average size for the chain so it looks like we need
    //    ManySmallFiles compactions.
    // 2) These files overlap the problem set of big files above (by min_time), but their max_l0_created_at puts
    //    them all after the problem set of big files.
    for i in max_files..(max_files * 10) {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i as i64)
                    .with_max_time(i as i64 * 10000)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i as i64))
                    .with_file_size_bytes(10),
            )
            .await;
    }
    // In Summary:
    // Without special handling, this scenario is catagorized as ManySmallFiles, and repeatedly & unproductively compacts
    // the first set of files, which never accomplishes anything.
    // This test demonstrates the need for `file_classification_for_many_files` skipping oversized chunks of files.
    // Without that clause, this test loops forever with unproductive compactions.
    // With that clause, the first set of large files gets set aside during ManySmallFiles mode, then gets later compacted
    // into L1s when the rest of the L0s are reduced to fewer/larger files.

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.1[1,2000] 0ns 100mb   |L0.1|                                                                                    "
    - "L0.2[1,2000] 1ns 100mb   |L0.2|                                                                                    "
    - "L0.3[1,2000] 2ns 100mb   |L0.3|                                                                                    "
    - "L0.4[1,2000] 3ns 100mb   |L0.4|                                                                                    "
    - "L0.5[1,2000] 4ns 100mb   |L0.5|                                                                                    "
    - "L0.6[1,2000] 5ns 100mb   |L0.6|                                                                                    "
    - "L0.7[1,2000] 6ns 100mb   |L0.7|                                                                                    "
    - "L0.8[1,2000] 7ns 100mb   |L0.8|                                                                                    "
    - "L0.9[1,2000] 8ns 100mb   |L0.9|                                                                                    "
    - "L0.10[1,2000] 9ns 100mb  |L0.10|                                                                                   "
    - "L0.11[1,2000] 10ns 100mb |L0.11|                                                                                   "
    - "L0.12[1,2000] 11ns 100mb |L0.12|                                                                                   "
    - "L0.13[1,2000] 12ns 100mb |L0.13|                                                                                   "
    - "L0.14[1,2000] 13ns 100mb |L0.14|                                                                                   "
    - "L0.15[1,2000] 14ns 100mb |L0.15|                                                                                   "
    - "L0.16[1,2000] 15ns 100mb |L0.16|                                                                                   "
    - "L0.17[1,2000] 16ns 100mb |L0.17|                                                                                   "
    - "L0.18[1,2000] 17ns 100mb |L0.18|                                                                                   "
    - "L0.19[1,2000] 18ns 100mb |L0.19|                                                                                   "
    - "L0.20[1,2000] 19ns 100mb |L0.20|                                                                                   "
    - "L0.21[20,200000] 20ns 10b|-L0.21-|                                                                                 "
    - "L0.22[21,210000] 21ns 10b|-L0.22-|                                                                                 "
    - "L0.23[22,220000] 22ns 10b|-L0.23-|                                                                                 "
    - "L0.24[23,230000] 23ns 10b|-L0.24--|                                                                                "
    - "L0.25[24,240000] 24ns 10b|-L0.25--|                                                                                "
    - "L0.26[25,250000] 25ns 10b|--L0.26--|                                                                               "
    - "L0.27[26,260000] 26ns 10b|--L0.27--|                                                                               "
    - "L0.28[27,270000] 27ns 10b|--L0.28---|                                                                              "
    - "L0.29[28,280000] 28ns 10b|--L0.29---|                                                                              "
    - "L0.30[29,290000] 29ns 10b|---L0.30---|                                                                             "
    - "L0.31[30,300000] 30ns 10b|---L0.31---|                                                                             "
    - "L0.32[31,310000] 31ns 10b|---L0.32----|                                                                            "
    - "L0.33[32,320000] 32ns 10b|---L0.33----|                                                                            "
    - "L0.34[33,330000] 33ns 10b|---L0.34----|                                                                            "
    - "L0.35[34,340000] 34ns 10b|----L0.35----|                                                                           "
    - "L0.36[35,350000] 35ns 10b|----L0.36----|                                                                           "
    - "L0.37[36,360000] 36ns 10b|----L0.37-----|                                                                          "
    - "L0.38[37,370000] 37ns 10b|----L0.38-----|                                                                          "
    - "L0.39[38,380000] 38ns 10b|-----L0.39-----|                                                                         "
    - "L0.40[39,390000] 39ns 10b|-----L0.40-----|                                                                         "
    - "L0.41[40,400000] 40ns 10b|-----L0.41------|                                                                        "
    - "L0.42[41,410000] 41ns 10b|-----L0.42------|                                                                        "
    - "L0.43[42,420000] 42ns 10b|-----L0.43------|                                                                        "
    - "L0.44[43,430000] 43ns 10b|------L0.44------|                                                                       "
    - "L0.45[44,440000] 44ns 10b|------L0.45------|                                                                       "
    - "L0.46[45,450000] 45ns 10b|------L0.46-------|                                                                      "
    - "L0.47[46,460000] 46ns 10b|------L0.47-------|                                                                      "
    - "L0.48[47,470000] 47ns 10b|-------L0.48-------|                                                                     "
    - "L0.49[48,480000] 48ns 10b|-------L0.49-------|                                                                     "
    - "L0.50[49,490000] 49ns 10b|-------L0.50--------|                                                                    "
    - "L0.51[50,500000] 50ns 10b|-------L0.51--------|                                                                    "
    - "L0.52[51,510000] 51ns 10b|--------L0.52--------|                                                                   "
    - "L0.53[52,520000] 52ns 10b|--------L0.53--------|                                                                   "
    - "L0.54[53,530000] 53ns 10b|--------L0.54--------|                                                                   "
    - "L0.55[54,540000] 54ns 10b|--------L0.55---------|                                                                  "
    - "L0.56[55,550000] 55ns 10b|--------L0.56---------|                                                                  "
    - "L0.57[56,560000] 56ns 10b|---------L0.57---------|                                                                 "
    - "L0.58[57,570000] 57ns 10b|---------L0.58---------|                                                                 "
    - "L0.59[58,580000] 58ns 10b|---------L0.59----------|                                                                "
    - "L0.60[59,590000] 59ns 10b|---------L0.60----------|                                                                "
    - "L0.61[60,600000] 60ns 10b|----------L0.61----------|                                                               "
    - "L0.62[61,610000] 61ns 10b|----------L0.62----------|                                                               "
    - "L0.63[62,620000] 62ns 10b|----------L0.63-----------|                                                              "
    - "L0.64[63,630000] 63ns 10b|----------L0.64-----------|                                                              "
    - "L0.65[64,640000] 64ns 10b|----------L0.65-----------|                                                              "
    - "L0.66[65,650000] 65ns 10b|-----------L0.66-----------|                                                             "
    - "L0.67[66,660000] 66ns 10b|-----------L0.67-----------|                                                             "
    - "L0.68[67,670000] 67ns 10b|-----------L0.68------------|                                                            "
    - "L0.69[68,680000] 68ns 10b|-----------L0.69------------|                                                            "
    - "L0.70[69,690000] 69ns 10b|------------L0.70------------|                                                           "
    - "L0.71[70,700000] 70ns 10b|------------L0.71------------|                                                           "
    - "L0.72[71,710000] 71ns 10b|------------L0.72-------------|                                                          "
    - "L0.73[72,720000] 72ns 10b|------------L0.73-------------|                                                          "
    - "L0.74[73,730000] 73ns 10b|-------------L0.74-------------|                                                         "
    - "L0.75[74,740000] 74ns 10b|-------------L0.75-------------|                                                         "
    - "L0.76[75,750000] 75ns 10b|-------------L0.76-------------|                                                         "
    - "L0.77[76,760000] 76ns 10b|-------------L0.77--------------|                                                        "
    - "L0.78[77,770000] 77ns 10b|-------------L0.78--------------|                                                        "
    - "L0.79[78,780000] 78ns 10b|--------------L0.79--------------|                                                       "
    - "L0.80[79,790000] 79ns 10b|--------------L0.80--------------|                                                       "
    - "L0.81[80,800000] 80ns 10b|--------------L0.81---------------|                                                      "
    - "L0.82[81,810000] 81ns 10b|--------------L0.82---------------|                                                      "
    - "L0.83[82,820000] 82ns 10b|---------------L0.83---------------|                                                     "
    - "L0.84[83,830000] 83ns 10b|---------------L0.84---------------|                                                     "
    - "L0.85[84,840000] 84ns 10b|---------------L0.85---------------|                                                     "
    - "L0.86[85,850000] 85ns 10b|---------------L0.86----------------|                                                    "
    - "L0.87[86,860000] 86ns 10b|---------------L0.87----------------|                                                    "
    - "L0.88[87,870000] 87ns 10b|----------------L0.88----------------|                                                   "
    - "L0.89[88,880000] 88ns 10b|----------------L0.89----------------|                                                   "
    - "L0.90[89,890000] 89ns 10b|----------------L0.90-----------------|                                                  "
    - "L0.91[90,900000] 90ns 10b|----------------L0.91-----------------|                                                  "
    - "L0.92[91,910000] 91ns 10b|-----------------L0.92-----------------|                                                 "
    - "L0.93[92,920000] 92ns 10b|-----------------L0.93-----------------|                                                 "
    - "L0.94[93,930000] 93ns 10b|-----------------L0.94------------------|                                                "
    - "L0.95[94,940000] 94ns 10b|-----------------L0.95------------------|                                                "
    - "L0.96[95,950000] 95ns 10b|-----------------L0.96------------------|                                                "
    - "L0.97[96,960000] 96ns 10b|------------------L0.97------------------|                                               "
    - "L0.98[97,970000] 97ns 10b|------------------L0.98------------------|                                               "
    - "L0.99[98,980000] 98ns 10b|------------------L0.99-------------------|                                              "
    - "L0.100[99,990000] 99ns 10b|------------------L0.100------------------|                                              "
    - "L0.101[100,1000000] 100ns 10b|------------------L0.101-------------------|                                             "
    - "L0.102[101,1010000] 101ns 10b|------------------L0.102-------------------|                                             "
    - "L0.103[102,1020000] 102ns 10b|-------------------L0.103-------------------|                                            "
    - "L0.104[103,1030000] 103ns 10b|-------------------L0.104-------------------|                                            "
    - "L0.105[104,1040000] 104ns 10b|-------------------L0.105--------------------|                                           "
    - "L0.106[105,1050000] 105ns 10b|-------------------L0.106--------------------|                                           "
    - "L0.107[106,1060000] 106ns 10b|-------------------L0.107--------------------|                                           "
    - "L0.108[107,1070000] 107ns 10b|--------------------L0.108--------------------|                                          "
    - "L0.109[108,1080000] 108ns 10b|--------------------L0.109--------------------|                                          "
    - "L0.110[109,1090000] 109ns 10b|--------------------L0.110---------------------|                                         "
    - "L0.111[110,1100000] 110ns 10b|--------------------L0.111---------------------|                                         "
    - "L0.112[111,1110000] 111ns 10b|---------------------L0.112---------------------|                                        "
    - "L0.113[112,1120000] 112ns 10b|---------------------L0.113---------------------|                                        "
    - "L0.114[113,1130000] 113ns 10b|---------------------L0.114----------------------|                                       "
    - "L0.115[114,1140000] 114ns 10b|---------------------L0.115----------------------|                                       "
    - "L0.116[115,1150000] 115ns 10b|----------------------L0.116----------------------|                                      "
    - "L0.117[116,1160000] 116ns 10b|----------------------L0.117----------------------|                                      "
    - "L0.118[117,1170000] 117ns 10b|----------------------L0.118----------------------|                                      "
    - "L0.119[118,1180000] 118ns 10b|----------------------L0.119-----------------------|                                     "
    - "L0.120[119,1190000] 119ns 10b|----------------------L0.120-----------------------|                                     "
    - "L0.121[120,1200000] 120ns 10b|-----------------------L0.121-----------------------|                                    "
    - "L0.122[121,1210000] 121ns 10b|-----------------------L0.122-----------------------|                                    "
    - "L0.123[122,1220000] 122ns 10b|-----------------------L0.123------------------------|                                   "
    - "L0.124[123,1230000] 123ns 10b|-----------------------L0.124------------------------|                                   "
    - "L0.125[124,1240000] 124ns 10b|------------------------L0.125------------------------|                                  "
    - "L0.126[125,1250000] 125ns 10b|------------------------L0.126------------------------|                                  "
    - "L0.127[126,1260000] 126ns 10b|------------------------L0.127------------------------|                                  "
    - "L0.128[127,1270000] 127ns 10b|------------------------L0.128-------------------------|                                 "
    - "L0.129[128,1280000] 128ns 10b|------------------------L0.129-------------------------|                                 "
    - "L0.130[129,1290000] 129ns 10b|-------------------------L0.130-------------------------|                                "
    - "L0.131[130,1300000] 130ns 10b|-------------------------L0.131-------------------------|                                "
    - "L0.132[131,1310000] 131ns 10b|-------------------------L0.132--------------------------|                               "
    - "L0.133[132,1320000] 132ns 10b|-------------------------L0.133--------------------------|                               "
    - "L0.134[133,1330000] 133ns 10b|--------------------------L0.134--------------------------|                              "
    - "L0.135[134,1340000] 134ns 10b|--------------------------L0.135--------------------------|                              "
    - "L0.136[135,1350000] 135ns 10b|--------------------------L0.136---------------------------|                             "
    - "L0.137[136,1360000] 136ns 10b|--------------------------L0.137---------------------------|                             "
    - "L0.138[137,1370000] 137ns 10b|--------------------------L0.138---------------------------|                             "
    - "L0.139[138,1380000] 138ns 10b|---------------------------L0.139---------------------------|                            "
    - "L0.140[139,1390000] 139ns 10b|---------------------------L0.140---------------------------|                            "
    - "L0.141[140,1400000] 140ns 10b|---------------------------L0.141----------------------------|                           "
    - "L0.142[141,1410000] 141ns 10b|---------------------------L0.142----------------------------|                           "
    - "L0.143[142,1420000] 142ns 10b|----------------------------L0.143----------------------------|                          "
    - "L0.144[143,1430000] 143ns 10b|----------------------------L0.144----------------------------|                          "
    - "L0.145[144,1440000] 144ns 10b|----------------------------L0.145-----------------------------|                         "
    - "L0.146[145,1450000] 145ns 10b|----------------------------L0.146-----------------------------|                         "
    - "L0.147[146,1460000] 146ns 10b|-----------------------------L0.147-----------------------------|                        "
    - "L0.148[147,1470000] 147ns 10b|-----------------------------L0.148-----------------------------|                        "
    - "L0.149[148,1480000] 148ns 10b|-----------------------------L0.149-----------------------------|                        "
    - "L0.150[149,1490000] 149ns 10b|-----------------------------L0.150------------------------------|                       "
    - "L0.151[150,1500000] 150ns 10b|-----------------------------L0.151------------------------------|                       "
    - "L0.152[151,1510000] 151ns 10b|------------------------------L0.152------------------------------|                      "
    - "L0.153[152,1520000] 152ns 10b|------------------------------L0.153------------------------------|                      "
    - "L0.154[153,1530000] 153ns 10b|------------------------------L0.154-------------------------------|                     "
    - "L0.155[154,1540000] 154ns 10b|------------------------------L0.155-------------------------------|                     "
    - "L0.156[155,1550000] 155ns 10b|-------------------------------L0.156-------------------------------|                    "
    - "L0.157[156,1560000] 156ns 10b|-------------------------------L0.157-------------------------------|                    "
    - "L0.158[157,1570000] 157ns 10b|-------------------------------L0.158-------------------------------|                    "
    - "L0.159[158,1580000] 158ns 10b|-------------------------------L0.159--------------------------------|                   "
    - "L0.160[159,1590000] 159ns 10b|-------------------------------L0.160--------------------------------|                   "
    - "L0.161[160,1600000] 160ns 10b|--------------------------------L0.161--------------------------------|                  "
    - "L0.162[161,1610000] 161ns 10b|--------------------------------L0.162--------------------------------|                  "
    - "L0.163[162,1620000] 162ns 10b|--------------------------------L0.163---------------------------------|                 "
    - "L0.164[163,1630000] 163ns 10b|--------------------------------L0.164---------------------------------|                 "
    - "L0.165[164,1640000] 164ns 10b|---------------------------------L0.165---------------------------------|                "
    - "L0.166[165,1650000] 165ns 10b|---------------------------------L0.166---------------------------------|                "
    - "L0.167[166,1660000] 166ns 10b|---------------------------------L0.167----------------------------------|               "
    - "L0.168[167,1670000] 167ns 10b|---------------------------------L0.168----------------------------------|               "
    - "L0.169[168,1680000] 168ns 10b|---------------------------------L0.169----------------------------------|               "
    - "L0.170[169,1690000] 169ns 10b|----------------------------------L0.170----------------------------------|              "
    - "L0.171[170,1700000] 170ns 10b|----------------------------------L0.171----------------------------------|              "
    - "L0.172[171,1710000] 171ns 10b|----------------------------------L0.172-----------------------------------|             "
    - "L0.173[172,1720000] 172ns 10b|----------------------------------L0.173-----------------------------------|             "
    - "L0.174[173,1730000] 173ns 10b|-----------------------------------L0.174-----------------------------------|            "
    - "L0.175[174,1740000] 174ns 10b|-----------------------------------L0.175-----------------------------------|            "
    - "L0.176[175,1750000] 175ns 10b|-----------------------------------L0.176------------------------------------|           "
    - "L0.177[176,1760000] 176ns 10b|-----------------------------------L0.177------------------------------------|           "
    - "L0.178[177,1770000] 177ns 10b|------------------------------------L0.178------------------------------------|          "
    - "L0.179[178,1780000] 178ns 10b|------------------------------------L0.179------------------------------------|          "
    - "L0.180[179,1790000] 179ns 10b|------------------------------------L0.180------------------------------------|          "
    - "L0.181[180,1800000] 180ns 10b|------------------------------------L0.181-------------------------------------|         "
    - "L0.182[181,1810000] 181ns 10b|------------------------------------L0.182-------------------------------------|         "
    - "L0.183[182,1820000] 182ns 10b|-------------------------------------L0.183-------------------------------------|        "
    - "L0.184[183,1830000] 183ns 10b|-------------------------------------L0.184-------------------------------------|        "
    - "L0.185[184,1840000] 184ns 10b|-------------------------------------L0.185--------------------------------------|       "
    - "L0.186[185,1850000] 185ns 10b|-------------------------------------L0.186--------------------------------------|       "
    - "L0.187[186,1860000] 186ns 10b|--------------------------------------L0.187--------------------------------------|      "
    - "L0.188[187,1870000] 187ns 10b|--------------------------------------L0.188--------------------------------------|      "
    - "L0.189[188,1880000] 188ns 10b|--------------------------------------L0.189---------------------------------------|     "
    - "L0.190[189,1890000] 189ns 10b|--------------------------------------L0.190---------------------------------------|     "
    - "L0.191[190,1900000] 190ns 10b|--------------------------------------L0.191---------------------------------------|     "
    - "L0.192[191,1910000] 191ns 10b|---------------------------------------L0.192---------------------------------------|    "
    - "L0.193[192,1920000] 192ns 10b|---------------------------------------L0.193---------------------------------------|    "
    - "L0.194[193,1930000] 193ns 10b|---------------------------------------L0.194----------------------------------------|   "
    - "L0.195[194,1940000] 194ns 10b|---------------------------------------L0.195----------------------------------------|   "
    - "L0.196[195,1950000] 195ns 10b|----------------------------------------L0.196----------------------------------------|  "
    - "L0.197[196,1960000] 196ns 10b|----------------------------------------L0.197----------------------------------------|  "
    - "L0.198[197,1970000] 197ns 10b|----------------------------------------L0.198-----------------------------------------| "
    - "L0.199[198,1980000] 198ns 10b|----------------------------------------L0.199-----------------------------------------| "
    - "L0.200[199,1990000] 199ns 10b|----------------------------------------L0.200-----------------------------------------| "
    - "**** Final Output Files (14.37gb written)"
    - "L1                                                                                                                 "
    - "L1.3823[1047695,1801539] 199ns 72mb                                               |------------L1.3823-------------|         "
    - "L1.3824[1801540,1990000] 199ns 18mb                                                                                 |L1.3824|"
    - "L2                                                                                                                 "
    - "L2.3801[1,102] 199ns 100mb|L2.3801|                                                                                 "
    - "L2.3802[103,203] 199ns 100mb|L2.3802|                                                                                 "
    - "L2.3803[204,305] 199ns 100mb|L2.3803|                                                                                 "
    - "L2.3820[1553,1652] 199ns 101mb|L2.3820|                                                                                 "
    - "L2.3825[306,407] 199ns 101mb|L2.3825|                                                                                 "
    - "L2.3826[408,508] 199ns 100mb|L2.3826|                                                                                 "
    - "L2.3827[509,513] 199ns 6mb|L2.3827|                                                                                 "
    - "L2.3828[514,614] 199ns 101mb|L2.3828|                                                                                 "
    - "L2.3829[615,714] 199ns 100mb|L2.3829|                                                                                 "
    - "L2.3830[715,719] 199ns 6mb|L2.3830|                                                                                 "
    - "L2.3831[720,820] 199ns 101mb|L2.3831|                                                                                 "
    - "L2.3832[821,920] 199ns 100mb|L2.3832|                                                                                 "
    - "L2.3833[921,1017] 199ns 99mb|L2.3833|                                                                                 "
    - "L2.3834[1018,1118] 199ns 100mb|L2.3834|                                                                                 "
    - "L2.3835[1119,1218] 199ns 99mb|L2.3835|                                                                                 "
    - "L2.3836[1219,1310] 199ns 93mb|L2.3836|                                                                                 "
    - "L2.3837[1311,1411] 199ns 100mb|L2.3837|                                                                                 "
    - "L2.3838[1412,1511] 199ns 99mb|L2.3838|                                                                                 "
    - "L2.3839[1512,1552] 199ns 42mb|L2.3839|                                                                                 "
    - "L2.3840[1653,400658] 199ns 100mb|----L2.3840-----|                                                                        "
    - "L2.3841[400659,799663] 199ns 100mb                  |----L2.3841-----|                                                      "
    - "L2.3842[799664,1047694] 199ns 62mb                                    |-L2.3842-|                                           "
    "###
    );
}

// This case is taken from a catalog where the partition was stuck doing single file L0->L0 compactions with a ManySmallFiles classification.
// The key point is that there is 1 L0 file, and enough overlapping L1 files such that the sum of L0 and overlapping L1s are too many for
// a single compaction.  So it it tried to do L0->L0 compaction, but you can't get less than 1 L0 file...
#[tokio::test]
async fn single_file_compaction() {
    test_helpers::maybe_start_logging();

    let max_files = 20;
    let setup = layout_setup_builder()
        .await
        .with_max_num_files_per_plan(max_files)
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .with_partition_timeout(Duration::from_millis(1000))
        .with_suppress_run_output() // remove this to debug
        .build()
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681776057065884000)
                .with_max_time(1681848094846357000)
                .with_compaction_level(CompactionLevel::Final)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681848108803007952))
                .with_file_size_bytes(148352),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681848059723530000)
                .with_max_time(1681849022292840000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681849158083717413))
                .with_file_size_bytes(8532),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681849256770938000)
                .with_max_time(1681849612137939000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681849758018522867))
                .with_file_size_bytes(7180),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681849857540998000)
                .with_max_time(1681849933405747000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681850058063700468))
                .with_file_size_bytes(6354),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681850155949687000)
                .with_max_time(1681850525337964000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681850658095040165))
                .with_file_size_bytes(7224),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681850533564810000)
                .with_max_time(1681850800324334000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681850958072081740))
                .with_file_size_bytes(6442),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681850807902300000)
                .with_max_time(1681851109057342000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681851258099471556))
                .with_file_size_bytes(6467),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681851356697599000)
                .with_max_time(1681851731606438000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681851858069516381))
                .with_file_size_bytes(7202),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681851768198276000)
                .with_max_time(1681852656555310000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681852758025054620))
                .with_file_size_bytes(7901),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681852858788440000)
                .with_max_time(1681853202074816000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681853358030917913))
                .with_file_size_bytes(7175),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681853216031150000)
                .with_max_time(1681853533814380000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681853658084495307))
                .with_file_size_bytes(6461),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681853755089369000)
                .with_max_time(1681854114135030000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681854258102937522))
                .with_file_size_bytes(7172),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681854158528835000)
                .with_max_time(1681854411758250000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681854558107269518))
                .with_file_size_bytes(6445),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681854656198860000)
                .with_max_time(1681855901530453000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681856058068217803))
                .with_file_size_bytes(9388),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681855930016632000)
                .with_max_time(1681856215951881000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681856358077776391))
                .with_file_size_bytes(6411),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681856457094364000)
                .with_max_time(1681856572199715000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681856658099983774))
                .with_file_size_bytes(6471),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681856755669647000)
                .with_max_time(1681856797376786000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681856959540758502))
                .with_file_size_bytes(6347),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681857059467239000)
                .with_max_time(1681857411709822000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681857559463607724))
                .with_file_size_bytes(7179),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681857658708732000)
                .with_max_time(1681858001258834000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681858159653340111))
                .with_file_size_bytes(7171),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681858259089021000)
                .with_max_time(1681858311972651000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681858459694290981))
                .with_file_size_bytes(6417),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681858336136281000)
                .with_max_time(1681858611711634000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681858759770566450))
                .with_file_size_bytes(6432),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681858613076367000)
                .with_max_time(1681859207290151000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681859359651203045))
                .with_file_size_bytes(7211),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681859212497834000)
                .with_max_time(1681859549996540000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681859659796715205))
                .with_file_size_bytes(6408),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681859755984961000)
                .with_max_time(1681860397139689000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681860559596560745))
                .with_file_size_bytes(7919),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681860656403220000)
                .with_max_time(1681861312602593000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681861463769557785))
                .with_file_size_bytes(7920),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681861557592893000)
                .with_max_time(1681861592762435000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681861760075293126))
                .with_file_size_bytes(6432),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681861612304587000)
                .with_max_time(1681861928505695000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681862059957822724))
                .with_file_size_bytes(6456),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681862008720364000)
                .with_max_time(1681862268794595000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681862511938856063))
                .with_file_size_bytes(6453),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1681776002714783000)
                .with_max_time(1681862102913137000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1683039505904263771))
                .with_file_size_bytes(7225),
        )
        .await;

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.29[1681776002714783000,1681862102913137000] 1683039505.9s 7kb|-----------------------------------------L0.29-----------------------------------------| "
    - "L1                                                                                                                 "
    - "L1.2[1681848059723530000,1681849022292840000] 1681849158.08s 8kb                                                                           |L1.2|         "
    - "L1.3[1681849256770938000,1681849612137939000] 1681849758.02s 7kb                                                                            |L1.3|        "
    - "L1.4[1681849857540998000,1681849933405747000] 1681850058.06s 6kb                                                                             |L1.4|       "
    - "L1.5[1681850155949687000,1681850525337964000] 1681850658.1s 7kb                                                                             |L1.5|       "
    - "L1.6[1681850533564810000,1681850800324334000] 1681850958.07s 6kb                                                                             |L1.6|       "
    - "L1.7[1681850807902300000,1681851109057342000] 1681851258.1s 6kb                                                                              |L1.7|      "
    - "L1.8[1681851356697599000,1681851731606438000] 1681851858.07s 7kb                                                                              |L1.8|      "
    - "L1.9[1681851768198276000,1681852656555310000] 1681852758.03s 8kb                                                                               |L1.9|     "
    - "L1.10[1681852858788440000,1681853202074816000] 1681853358.03s 7kb                                                                                |L1.10|   "
    - "L1.11[1681853216031150000,1681853533814380000] 1681853658.08s 6kb                                                                                |L1.11|   "
    - "L1.12[1681853755089369000,1681854114135030000] 1681854258.1s 7kb                                                                                 |L1.12|  "
    - "L1.13[1681854158528835000,1681854411758250000] 1681854558.11s 6kb                                                                                 |L1.13|  "
    - "L1.14[1681854656198860000,1681855901530453000] 1681856058.07s 9kb                                                                                  |L1.14| "
    - "L1.15[1681855930016632000,1681856215951881000] 1681856358.08s 6kb                                                                                   |L1.15|"
    - "L1.16[1681856457094364000,1681856572199715000] 1681856658.1s 6kb                                                                                   |L1.16|"
    - "L1.17[1681856755669647000,1681856797376786000] 1681856959.54s 6kb                                                                                    |L1.17|"
    - "L1.18[1681857059467239000,1681857411709822000] 1681857559.46s 7kb                                                                                    |L1.18|"
    - "L1.19[1681857658708732000,1681858001258834000] 1681858159.65s 7kb                                                                                     |L1.19|"
    - "L1.20[1681858259089021000,1681858311972651000] 1681858459.69s 6kb                                                                                     |L1.20|"
    - "L1.21[1681858336136281000,1681858611711634000] 1681858759.77s 6kb                                                                                     |L1.21|"
    - "L1.22[1681858613076367000,1681859207290151000] 1681859359.65s 7kb                                                                                      |L1.22|"
    - "L1.23[1681859212497834000,1681859549996540000] 1681859659.8s 6kb                                                                                      |L1.23|"
    - "L1.24[1681859755984961000,1681860397139689000] 1681860559.6s 8kb                                                                                       |L1.24|"
    - "L1.25[1681860656403220000,1681861312602593000] 1681861463.77s 8kb                                                                                        |L1.25|"
    - "L1.26[1681861557592893000,1681861592762435000] 1681861760.08s 6kb                                                                                         |L1.26|"
    - "L1.27[1681861612304587000,1681861928505695000] 1681862059.96s 6kb                                                                                         |L1.27|"
    - "L1.28[1681862008720364000,1681862268794595000] 1681862511.94s 6kb                                                                                         |L1.28|"
    - "L2                                                                                                                 "
    - "L2.1[1681776057065884000,1681848094846357000] 1681848108.8s 145kb|----------------------------------L2.1-----------------------------------|               "
    - "**** Final Output Files (192kb written)"
    - "L1                                                                                                                 "
    - "L1.30[1681776002714783000,1681862268794595000] 1683039505.9s 192kb|-----------------------------------------L1.30------------------------------------------|"
    - "L2                                                                                                                 "
    - "L2.1[1681776057065884000,1681848094846357000] 1681848108.8s 145kb|----------------------------------L2.1-----------------------------------|               "
    "###
    );
}

// This test comes from a real world catalog scenario where the configured split percentage caused a loop.  ManySmallFiles decided to compact just 2 files, which
// happen to already be split at the target percentage.  So the compaction creates 2 output files that are the same as the input files, resulting in a loop.
#[tokio::test]
async fn split_precent_loop() {
    test_helpers::maybe_start_logging();

    let max_files = 20;
    let setup = layout_setup_builder()
        .await
        .with_max_num_files_per_plan(max_files)
        .with_max_desired_file_size_bytes(MAX_DESIRED_FILE_SIZE)
        .with_partition_timeout(Duration::from_millis(1000))
        .with_percentage_max_file_size(5)
        .with_split_percentage(80)
        .with_suppress_run_output() // remove this to debug
        .build()
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987200001000000)
                .with_max_time(1675996179137000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676010160053162493))
                .with_file_size_bytes(103403616),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675996179142000000)
                .with_max_time(1676005158275000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676010160053162493))
                .with_file_size_bytes(102072124),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1676005158277000000)
                .with_max_time(1676010156669000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676010160053162493))
                .with_file_size_bytes(61186631),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675989300563000000)
                .with_max_time(1676036409167000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676036411377096481))
                .with_file_size_bytes(2219347),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987777260000000)
                .with_max_time(1676036474324000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676036476572081862))
                .with_file_size_bytes(2159488),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987902254000000)
                .with_max_time(1676036529744000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676036533523024586))
                .with_file_size_bytes(2267826),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987264233000000)
                .with_max_time(1676036708522000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676036711284678620))
                .with_file_size_bytes(2262710),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987208765000000)
                .with_max_time(1676036773664000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676036776492734973))
                .with_file_size_bytes(2283847),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987969189000000)
                .with_max_time(1676036830287000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676036833578815748))
                .with_file_size_bytes(2173838),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987448630000000)
                .with_max_time(1676037009945000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676037011333912856))
                .with_file_size_bytes(2215286),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675991332100000000)
                .with_max_time(1676037072975000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676037076612171888))
                .with_file_size_bytes(2175613),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675989374650000000)
                .with_max_time(1676037129342000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676037133683428336))
                .with_file_size_bytes(2244289),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987252382000000)
                .with_max_time(1676037308408000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676037311292474524))
                .with_file_size_bytes(2217991),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987574435000000)
                .with_max_time(1676037374115000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676037376589707454))
                .with_file_size_bytes(2188472),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675989488901000000)
                .with_max_time(1676037430277000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676037433529280795))
                .with_file_size_bytes(2247953),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987956301000000)
                .with_max_time(1676037608139000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676037611337404983))
                .with_file_size_bytes(2230257),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987840745000000)
                .with_max_time(1676037673346000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676037676565165201))
                .with_file_size_bytes(2197670),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987620819000000)
                .with_max_time(1676037730350000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676037733819595619))
                .with_file_size_bytes(2181963),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987267649000000)
                .with_max_time(1676037909084000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676037911429564851))
                .with_file_size_bytes(2225185),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675988167750000000)
                .with_max_time(1676037975214000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676037976761976812))
                .with_file_size_bytes(2241751),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675995240778000000)
                .with_max_time(1676063934345000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676063936517933405))
                .with_file_size_bytes(2117926),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987292063000000)
                .with_max_time(1676064071432000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676064075612113418))
                .with_file_size_bytes(2197086),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675991856673000000)
                .with_max_time(1676064136664000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676064139132278475))
                .with_file_size_bytes(2179185),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675990277246000000)
                .with_max_time(1676064234591000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676064236557583838))
                .with_file_size_bytes(2229863),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675989676787000000)
                .with_max_time(1676064371697000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676064375723383965))
                .with_file_size_bytes(2164138),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675992075734000000)
                .with_max_time(1676064437297000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676064439064292184))
                .with_file_size_bytes(2139050),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675991814786000000)
                .with_max_time(1676064533585000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676064536460879736))
                .with_file_size_bytes(2215298),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675994514058000000)
                .with_max_time(1676064670409000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676064675911178179))
                .with_file_size_bytes(2081641),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675989994664000000)
                .with_max_time(1676064736678000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676064740172902173))
                .with_file_size_bytes(2270347),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675989093150000000)
                .with_max_time(1676064834639000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676064836484625744))
                .with_file_size_bytes(2241366),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987304054000000)
                .with_max_time(1676064970327000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676064975861286528))
                .with_file_size_bytes(2127038),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987787688000000)
                .with_max_time(1676065036871000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676065039959254669))
                .with_file_size_bytes(2234389),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675994030979000000)
                .with_max_time(1676065133988000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676065136539751838))
                .with_file_size_bytes(2162239),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675988375191000000)
                .with_max_time(1676065272216000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676065275804926272))
                .with_file_size_bytes(2225432),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987584851000000)
                .with_max_time(1676065337320000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676065339850486840))
                .with_file_size_bytes(2199543),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987883656000000)
                .with_max_time(1676065434070000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676065436477743987))
                .with_file_size_bytes(2189675),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987838080000000)
                .with_max_time(1676065568770000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676065575902973989))
                .with_file_size_bytes(2240286),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987203524000000)
                .with_max_time(1676003982168000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676036233843843417))
                .with_file_size_bytes(249698),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1676003983105000000)
                .with_max_time(1676020762353000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676036233843843417))
                .with_file_size_bytes(118322672),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1676020762355000000)
                .with_max_time(1676036230752000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676036233843843417))
                .with_file_size_bytes(167000529),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987206125000000)
                .with_max_time(1676013525882000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676063839068577846))
                .with_file_size_bytes(299209),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1676013530331000000)
                .with_max_time(1676039845772000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676063839068577846))
                .with_file_size_bytes(29163092),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1676039845773000000)
                .with_max_time(1676063836202000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676063839068577846))
                .with_file_size_bytes(253799912),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1675987825375000000)
                .with_max_time(1676050466145000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676066475259188285))
                .with_file_size_bytes(20949),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1676050539639000000)
                .with_max_time(1676066212011000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1676066475259188285))
                .with_file_size_bytes(13133322),
        )
        .await;

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.4[1675989300563000000,1676036409167000000] 1676036411.38s 2mb  |-----------------------L0.4------------------------|                                   "
    - "L0.5[1675987777260000000,1676036474324000000] 1676036476.57s 2mb|------------------------L0.5-------------------------|                                   "
    - "L0.6[1675987902254000000,1676036529744000000] 1676036533.52s 2mb|------------------------L0.6-------------------------|                                   "
    - "L0.7[1675987264233000000,1676036708522000000] 1676036711.28s 2mb|-------------------------L0.7-------------------------|                                  "
    - "L0.8[1675987208765000000,1676036773664000000] 1676036776.49s 2mb|-------------------------L0.8-------------------------|                                  "
    - "L0.9[1675987969189000000,1676036830287000000] 1676036833.58s 2mb|------------------------L0.9-------------------------|                                   "
    - "L0.10[1675987448630000000,1676037009945000000] 1676037011.33s 2mb|------------------------L0.10-------------------------|                                  "
    - "L0.11[1675991332100000000,1676037072975000000] 1676037076.61s 2mb    |----------------------L0.11-----------------------|                                  "
    - "L0.12[1675989374650000000,1676037129342000000] 1676037133.68s 2mb  |-----------------------L0.12------------------------|                                  "
    - "L0.13[1675987252382000000,1676037308408000000] 1676037311.29s 2mb|-------------------------L0.13-------------------------|                                 "
    - "L0.14[1675987574435000000,1676037374115000000] 1676037376.59s 2mb|------------------------L0.14-------------------------|                                  "
    - "L0.15[1675989488901000000,1676037430277000000] 1676037433.53s 2mb  |-----------------------L0.15------------------------|                                  "
    - "L0.16[1675987956301000000,1676037608139000000] 1676037611.34s 2mb|------------------------L0.16-------------------------|                                  "
    - "L0.17[1675987840745000000,1676037673346000000] 1676037676.57s 2mb|------------------------L0.17-------------------------|                                  "
    - "L0.18[1675987620819000000,1676037730350000000] 1676037733.82s 2mb|-------------------------L0.18-------------------------|                                 "
    - "L0.19[1675987267649000000,1676037909084000000] 1676037911.43s 2mb|-------------------------L0.19-------------------------|                                 "
    - "L0.20[1675988167750000000,1676037975214000000] 1676037976.76s 2mb |------------------------L0.20-------------------------|                                 "
    - "L0.21[1675995240778000000,1676063934345000000] 1676063936.52s 2mb         |-----------------------------------L0.21------------------------------------|   "
    - "L0.22[1675987292063000000,1676064071432000000] 1676064075.61s 2mb|----------------------------------------L0.22----------------------------------------|   "
    - "L0.23[1675991856673000000,1676064136664000000] 1676064139.13s 2mb     |-------------------------------------L0.23--------------------------------------|   "
    - "L0.24[1675990277246000000,1676064234591000000] 1676064236.56s 2mb   |--------------------------------------L0.24---------------------------------------|   "
    - "L0.25[1675989676787000000,1676064371697000000] 1676064375.72s 2mb  |---------------------------------------L0.25---------------------------------------|   "
    - "L0.26[1675992075734000000,1676064437297000000] 1676064439.06s 2mb     |-------------------------------------L0.26--------------------------------------|   "
    - "L0.27[1675991814786000000,1676064533585000000] 1676064536.46s 2mb     |-------------------------------------L0.27--------------------------------------|   "
    - "L0.28[1675994514058000000,1676064670409000000] 1676064675.91s 2mb        |------------------------------------L0.28------------------------------------|   "
    - "L0.29[1675989994664000000,1676064736678000000] 1676064740.17s 2mb   |---------------------------------------L0.29---------------------------------------|  "
    - "L0.30[1675989093150000000,1676064834639000000] 1676064836.48s 2mb  |---------------------------------------L0.30----------------------------------------|  "
    - "L0.31[1675987304054000000,1676064970327000000] 1676064975.86s 2mb|----------------------------------------L0.31-----------------------------------------|  "
    - "L0.32[1675987787688000000,1676065036871000000] 1676065039.96s 2mb|----------------------------------------L0.32----------------------------------------|   "
    - "L0.33[1675994030979000000,1676065133988000000] 1676065136.54s 2mb       |------------------------------------L0.33-------------------------------------|   "
    - "L0.34[1675988375191000000,1676065272216000000] 1676065275.8s 2mb |----------------------------------------L0.34----------------------------------------|  "
    - "L0.35[1675987584851000000,1676065337320000000] 1676065339.85s 2mb|----------------------------------------L0.35-----------------------------------------|  "
    - "L0.36[1675987883656000000,1676065434070000000] 1676065436.48s 2mb|----------------------------------------L0.36-----------------------------------------|  "
    - "L0.37[1675987838080000000,1676065568770000000] 1676065575.9s 2mb|----------------------------------------L0.37-----------------------------------------|  "
    - "L0.38[1675987203524000000,1676003982168000000] 1676036233.84s 244kb|------L0.38------|                                                                       "
    - "L0.39[1676003983105000000,1676020762353000000] 1676036233.84s 113mb                   |------L0.39------|                                                    "
    - "L0.40[1676020762355000000,1676036230752000000] 1676036233.84s 159mb                                      |-----L0.40-----|                                   "
    - "L0.41[1675987206125000000,1676013525882000000] 1676063839.07s 292kb|-----------L0.41-----------|                                                             "
    - "L0.42[1676013530331000000,1676039845772000000] 1676063839.07s 28mb                             |-----------L0.42-----------|                                "
    - "L0.43[1676039845773000000,1676063836202000000] 1676063839.07s 242mb                                                           |----------L0.43----------|    "
    - "L0.44[1675987825375000000,1676050466145000000] 1676066475.26s 20kb|--------------------------------L0.44--------------------------------|                   "
    - "L0.45[1676050539639000000,1676066212011000000] 1676066475.26s 13mb                                                                        |-----L0.45-----| "
    - "L1                                                                                                                 "
    - "L1.1[1675987200001000000,1675996179137000000] 1676010160.05s 99mb|--L1.1--|                                                                                "
    - "L1.2[1675996179142000000,1676005158275000000] 1676010160.05s 97mb          |--L1.2--|                                                                      "
    - "L1.3[1676005158277000000,1676010156669000000] 1676010160.05s 58mb                    |L1.3|                                                                "
    - "WARNING: file L0.40[1676020762355000000,1676036230752000000] 1676036233.84s 159mb exceeds soft limit 100mb by more than 50%"
    - "WARNING: file L0.43[1676039845773000000,1676063836202000000] 1676063839.07s 242mb exceeds soft limit 100mb by more than 50%"
    - "**** Final Output Files (4.17gb written)"
    - "L2                                                                                                                 "
    - "L2.304[1676034607207000001,1676066212011000000] 1676066475.26s 286mb                                                      |-------------L2.304--------------| "
    - "L2.321[1675987200001000000,1675993675383098793] 1676066475.26s 100mb|L2.321|                                                                                  "
    - "L2.329[1675993675383098794,1676001131291506471] 1676066475.26s 100mb       |L2.329|                                                                           "
    - "L2.330[1676001131291506472,1676008587199914148] 1676066475.26s 100mb               |L2.330|                                                                   "
    - "L2.331[1676008587199914149,1676014352965372887] 1676066475.26s 77mb                        |L2.331|                                                          "
    - "L2.332[1676014352965372888,1676023648281062782] 1676066475.26s 100mb                              |-L2.332-|                                                  "
    - "L2.333[1676023648281062783,1676032943596752676] 1676066475.26s 100mb                                         |-L2.333-|                                       "
    - "L2.334[1676032943596752677,1676034607207000000] 1676066475.26s 18mb                                                    |L2.334|                              "
    - "WARNING: file L2.304[1676034607207000001,1676066212011000000] 1676066475.26s 286mb exceeds soft limit 100mb by more than 50%"
    "###
    );
}
