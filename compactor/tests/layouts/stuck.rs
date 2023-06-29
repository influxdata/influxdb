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
