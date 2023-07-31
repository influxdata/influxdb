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
    - "**** Final Output Files (43.43gb written)"
    - "L2                                                                                                                 "
    - "L2.1463[1686841379000000000,1686842332558996896] 1686936871.55s 100mb|L2.1463|                                                                                 "
    - "L2.1468[1686842332558996897,1686842963082388201] 1686936871.55s 100mb  |L2.1468|                                                                               "
    - "L2.1469[1686842963082388202,1686843593605779505] 1686936871.55s 100mb    |L2.1469|                                                                             "
    - "L2.1471[1686843593605779506,1686844269803455042] 1686936871.55s 100mb      |L2.1471|                                                                           "
    - "L2.1476[1686844269803455043,1686844592758068181] 1686936871.55s 100mb        |L2.1476|                                                                         "
    - "L2.1477[1686844592758068182,1686844915712681319] 1686936871.55s 100mb        |L2.1477|                                                                         "
    - "L2.1478[1686844915712681320,1686844946001130578] 1686936871.55s 9mb         |L2.1478|                                                                        "
    - "L2.1479[1686844946001130579,1686845261138897644] 1686936871.55s 100mb         |L2.1479|                                                                        "
    - "L2.1480[1686845261138897645,1686845576276664709] 1686936871.55s 100mb          |L2.1480|                                                                       "
    - "L2.1481[1686845576276664710,1686845579000000000] 1686936871.55s 885kb           |L2.1481|                                                                      "
    - "L2.1489[1686845579000000001,1686846612945515506] 1686936871.55s 100mb           |L2.1489|                                                                      "
    - "L2.1494[1686846612945515507,1686847302242526939] 1686936871.55s 100mb              |L2.1494|                                                                   "
    - "L2.1499[1686847302242526940,1686847769313756192] 1686936871.55s 100mb                |L2.1499|                                                                 "
    - "L2.1500[1686847769313756193,1686848236384985444] 1686936871.55s 100mb                 |L2.1500|                                                                "
    - "L2.1502[1686848236384985445,1686848816392993760] 1686936871.55s 100mb                   |L2.1502|                                                              "
    - "L2.1507[1686848816392993761,1686849165082054031] 1686936871.55s 100mb                    |L2.1507|                                                             "
    - "L2.1508[1686849165082054032,1686849513771114301] 1686936871.55s 100mb                     |L2.1508|                                                            "
    - "L2.1509[1686849513771114302,1686849779000000000] 1686936871.55s 76mb                      |L2.1509|                                                           "
    - "L2.1510[1686849779000000001,1686850288711664442] 1686936871.55s 100mb                       |L2.1510|                                                          "
    - "L2.1511[1686850288711664443,1686850559000000000] 1686936871.55s 53mb                        |L2.1511|                                                         "
    - "L2.1519[1686850559000000001,1686851147210677461] 1686936871.55s 100mb                         |L2.1519|                                                        "
    - "L2.1520[1686851147210677462,1686851735421354921] 1686936871.55s 100mb                           |L2.1520|                                                      "
    - "L2.1521[1686851735421354922,1686852240527466641] 1686936871.55s 86mb                            |L2.1521|                                                     "
    - "L2.1522[1686852240527466642,1686852812866488092] 1686936871.55s 100mb                              |L2.1522|                                                   "
    - "L2.1523[1686852812866488093,1686853385205509542] 1686936871.55s 100mb                               |L2.1523|                                                  "
    - "L2.1525[1686853385205509543,1686853965359592641] 1686936871.55s 100mb                                 |L2.1525|                                                "
    - "L2.1530[1686853965359592642,1686854382616966390] 1686936871.55s 100mb                                   |L2.1530|                                              "
    - "L2.1531[1686854382616966391,1686854799874340138] 1686936871.55s 100mb                                    |L2.1531|                                             "
    - "L2.1532[1686854799874340139,1686854819000000000] 1686936871.55s 5mb                                     |L2.1532|                                            "
    - "L2.1540[1686854819000000001,1686855555092837650] 1686936871.55s 100mb                                     |L2.1540|                                            "
    - "L2.1541[1686855555092837651,1686856291185675299] 1686936871.55s 100mb                                       |L2.1541|                                          "
    - "L2.1542[1686856291185675300,1686856502561319445] 1686936871.55s 29mb                                         |L2.1542|                                        "
    - "L2.1543[1686856502561319446,1686857135555597834] 1686936871.55s 100mb                                          |L2.1543|                                       "
    - "L2.1548[1686857135555597835,1686857409590080289] 1686936871.55s 100mb                                            |L2.1548|                                     "
    - "L2.1549[1686857409590080290,1686857683624562743] 1686936871.55s 100mb                                            |L2.1549|                                     "
    - "L2.1550[1686857683624562744,1686857768549876222] 1686936871.55s 31mb                                             |L2.1550|                                    "
    - "L2.1551[1686857768549876223,1686858217039101175] 1686936871.55s 100mb                                             |L2.1551|                                    "
    - "L2.1552[1686858217039101176,1686858665528326127] 1686936871.55s 100mb                                               |L2.1552|                                  "
    - "L2.1554[1686858665528326128,1686859092257365665] 1686936871.55s 100mb                                                |L2.1554|                                 "
    - "L2.1555[1686859092257365666,1686859499000000000] 1686936871.55s 95mb                                                 |L2.1555|                                "
    - "L2.1563[1686859499000000001,1686860075111679382] 1686936871.55s 100mb                                                  |L2.1563|                               "
    - "L2.1564[1686860075111679383,1686860651223358763] 1686936871.55s 100mb                                                    |L2.1564|                             "
    - "L2.1565[1686860651223358764,1686861094471633615] 1686936871.55s 77mb                                                     |L2.1565|                            "
    - "L2.1573[1686861094471633616,1686861597923995018] 1686936871.55s 100mb                                                       |L2.1573|                          "
    - "L2.1574[1686861597923995019,1686862101376356420] 1686936871.55s 100mb                                                        |L2.1574|                         "
    - "L2.1582[1686862101376356421,1686862532038334079] 1686936871.55s 100mb                                                         |L2.1582|                        "
    - "L2.1583[1686862532038334080,1686862962700311737] 1686936871.55s 100mb                                                           |L2.1583|                      "
    - "L2.1584[1686862962700311738,1686863391646317185] 1686936871.55s 100mb                                                            |L2.1584|                     "
    - "L2.1592[1686863391646317186,1686864106500589152] 1686936871.55s 100mb                                                             |L2.1592|                    "
    - "L2.1593[1686864106500589153,1686864821354861118] 1686936871.55s 100mb                                                               |L2.1593|                  "
    - "L2.1594[1686864821354861119,1686865116328768791] 1686936871.55s 41mb                                                                 |L2.1594|                "
    - "L2.1595[1686865116328768792,1686865695735321041] 1686936871.55s 100mb                                                                  |L2.1595|               "
    - "L2.1596[1686865695735321042,1686866275141873290] 1686936871.55s 100mb                                                                   |L2.1596|              "
    - "L2.1598[1686866275141873291,1686866862140786963] 1686936871.55s 100mb                                                                     |L2.1598|            "
    - "L2.1603[1686866862140786964,1686867205172843469] 1686936871.55s 100mb                                                                       |L2.1603|          "
    - "L2.1604[1686867205172843470,1686867548204899974] 1686936871.55s 100mb                                                                        |L2.1604|         "
    - "L2.1605[1686867548204899975,1686867839000000000] 1686936871.55s 85mb                                                                         |L2.1605|        "
    - "L2.1613[1686867839000000001,1686869156057291877] 1686936871.55s 100mb                                                                         |L2.1613|        "
    - "L2.1618[1686869156057291878,1686869793900296627] 1686936871.55s 100mb                                                                             |L2.1618|    "
    - "L2.1619[1686869793900296628,1686870431743301376] 1686936871.55s 100mb                                                                               |L2.1619|  "
    - "L2.1620[1686870431743301377,1686870473114583753] 1686936871.55s 6mb                                                                                 |L2.1620|"
    - "L2.1628[1686870473114583754,1686870911305888930] 1686936871.55s 100mb                                                                                 |L2.1628|"
    - "L2.1629[1686870911305888931,1686871349497194106] 1686936871.55s 100mb                                                                                  |L2.1629|"
    - "L2.1638[1686871349497194107,1686871921260674819] 1686936871.55s 100mb                                                                                   |L2.1638|"
    - "L2.1643[1686871921260674820,1686872342671651604] 1686936871.55s 100mb                                                                                     |L2.1643|"
    - "L2.1648[1686872342671651605,1686872646602704048] 1686936871.55s 100mb                                                                                      |L2.1648|"
    - "L2.1649[1686872646602704049,1686872950533756491] 1686936871.55s 100mb                                                                                       |L2.1649|"
    - "L2.1650[1686872950533756492,1686873064787630789] 1686936871.55s 38mb                                                                                        |L2.1650|"
    - "L2.1651[1686873064787630790,1686873383892289532] 1686936871.55s 100mb                                                                                        |L2.1651|"
    - "L2.1652[1686873383892289533,1686873599000000000] 1686936871.55s 67mb                                                                                         |L2.1652|"
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
    - "**** Final Output Files (11.25gb written)"
    - "L2                                                                                                                 "
    - "L2.3439[1812,2716] 199ns 190mb|L2.3439|                                                                                 "
    - "L2.3523[1,102] 199ns 101mb|L2.3523|                                                                                 "
    - "L2.3546[103,204] 199ns 101mb|L2.3546|                                                                                 "
    - "L2.3547[205,305] 199ns 100mb|L2.3547|                                                                                 "
    - "L2.3548[306,361] 199ns 57mb|L2.3548|                                                                                 "
    - "L2.3549[362,463] 199ns 101mb|L2.3549|                                                                                 "
    - "L2.3550[464,564] 199ns 100mb|L2.3550|                                                                                 "
    - "L2.3551[565,619] 199ns 56mb|L2.3551|                                                                                 "
    - "L2.3552[620,720] 199ns 101mb|L2.3552|                                                                                 "
    - "L2.3553[721,820] 199ns 100mb|L2.3553|                                                                                 "
    - "L2.3554[821,875] 199ns 56mb|L2.3554|                                                                                 "
    - "L2.3555[876,975] 199ns 100mb|L2.3555|                                                                                 "
    - "L2.3556[976,1074] 199ns 99mb|L2.3556|                                                                                 "
    - "L2.3557[1075,1138] 199ns 66mb|L2.3557|                                                                                 "
    - "L2.3558[1139,1240] 199ns 101mb|L2.3558|                                                                                 "
    - "L2.3559[1241,1341] 199ns 100mb|L2.3559|                                                                                 "
    - "L2.3560[1342,1396] 199ns 56mb|L2.3560|                                                                                 "
    - "L2.3561[1397,1497] 199ns 101mb|L2.3561|                                                                                 "
    - "L2.3562[1498,1597] 199ns 100mb|L2.3562|                                                                                 "
    - "L2.3563[1598,1652] 199ns 56mb|L2.3563|                                                                                 "
    - "L2.3564[1653,1751] 199ns 101mb|L2.3564|                                                                                 "
    - "L2.3565[1752,1811] 199ns 62mb|L2.3565|                                                                                 "
    - "L2.3566[2717,1990000] 199ns 2kb|----------------------------------------L2.3566----------------------------------------| "
    - "WARNING: file L2.3439[1812,2716] 199ns 190mb exceeds soft limit 100mb by more than 50%"
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

// Another case from a real world catalog.  Originally this case resulted in (appropriately) splitting the L0s so they don't overlap so many L1s, then (inapproprately)
// compacting the L0s together again as a ManySmallFiles operation, then the cycle repeated.
#[tokio::test]
async fn split_then_undo_it() {
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
                .with_min_time(1680045637389000000)
                .with_max_time(1680046202520000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1680564436898219406))
                .with_file_size_bytes(106355502),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1680046202521000000)
                .with_max_time(1680046767652000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1680564436898219406))
                .with_file_size_bytes(104204199),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1680046767653000000)
                .with_max_time(1680047223526000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1680564436898219406))
                .with_file_size_bytes(84022852),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1680047223527000000)
                .with_max_time(1680047793776000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1680564436898219406))
                .with_file_size_bytes(105366839),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1680047793777000000)
                .with_max_time(1680047999999000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1680564436898219406))
                .with_file_size_bytes(37340524),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1679962892196000000)
                .with_max_time(1679969727828000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681186614522129445))
                .with_file_size_bytes(585995),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1679979814583000000)
                .with_max_time(1679989863127000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681186614522129445))
                .with_file_size_bytes(124967),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1679994942502000000)
                .with_max_time(1679996159985000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681186614522129445))
                .with_file_size_bytes(174089),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1679996160115000000)
                .with_max_time(1680013439626000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681186614522129445))
                .with_file_size_bytes(1448943),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1680013440066000000)
                .with_max_time(1680019937530000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681186614522129445))
                .with_file_size_bytes(443531),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1680019960376000000)
                .with_max_time(1680030670313000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681186614522129445))
                .with_file_size_bytes(187534),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1680030903802000000)
                .with_max_time(1680033957192000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681186614522129445))
                .with_file_size_bytes(50882),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1680035266427000000)
                .with_max_time(1680037607284000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681186614522129445))
                .with_file_size_bytes(62993),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1680037696661000000)
                .with_max_time(1680041087999000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681186614522129445))
                .with_file_size_bytes(9732222),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1680041088000000000)
                .with_max_time(1680044543999000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681186614522129445))
                .with_file_size_bytes(116659999),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1680044544000000000)
                .with_max_time(1680045637388000000)
                .with_compaction_level(CompactionLevel::FileNonOverlapped)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681186614522129445))
                .with_file_size_bytes(177095940),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1679961600071000000)
                .with_max_time(1680030719900000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681420678891928705))
                .with_file_size_bytes(11208773),
        )
        .await;

    setup
        .partition
        .create_parquet_file(
            parquet_builder()
                .with_min_time(1680030720000000000)
                .with_max_time(1680047999900000000)
                .with_compaction_level(CompactionLevel::Initial)
                .with_max_l0_created_at(Time::from_timestamp_nanos(1681420678891928705))
                .with_file_size_bytes(2806765),
        )
        .await;

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.17[1679961600071000000,1680030719900000000] 1681420678.89s 11mb|--------------------------------L0.17--------------------------------|                   "
    - "L0.18[1680030720000000000,1680047999900000000] 1681420678.89s 3mb                                                                       |-----L0.18-----|  "
    - "L1                                                                                                                 "
    - "L1.1[1680045637389000000,1680046202520000000] 1680564436.9s 101mb                                                                                       |L1.1|"
    - "L1.2[1680046202521000000,1680046767652000000] 1680564436.9s 99mb                                                                                        |L1.2|"
    - "L1.3[1680046767653000000,1680047223526000000] 1680564436.9s 80mb                                                                                        |L1.3|"
    - "L1.4[1680047223527000000,1680047793776000000] 1680564436.9s 100mb                                                                                         |L1.4|"
    - "L1.5[1680047793777000000,1680047999999000000] 1680564436.9s 36mb                                                                                         |L1.5|"
    - "L1.6[1679962892196000000,1679969727828000000] 1681186614.52s 572kb |L1.6-|                                                                                  "
    - "L1.7[1679979814583000000,1679989863127000000] 1681186614.52s 122kb                  |--L1.7--|                                                              "
    - "L1.8[1679994942502000000,1679996159985000000] 1681186614.52s 170kb                                  |L1.8|                                                  "
    - "L1.9[1679996160115000000,1680013439626000000] 1681186614.52s 1mb                                    |-----L1.9------|                                     "
    - "L1.10[1680013440066000000,1680019937530000000] 1681186614.52s 433kb                                                      |L1.10|                             "
    - "L1.11[1680019960376000000,1680030670313000000] 1681186614.52s 183kb                                                            |--L1.11--|                   "
    - "L1.12[1680030903802000000,1680033957192000000] 1681186614.52s 50kb                                                                        |L1.12|           "
    - "L1.13[1680035266427000000,1680037607284000000] 1681186614.52s 62kb                                                                            |L1.13|       "
    - "L1.14[1680037696661000000,1680041087999000000] 1681186614.52s 9mb                                                                               |L1.14|    "
    - "L1.15[1680041088000000000,1680044543999000000] 1681186614.52s 111mb                                                                                  |L1.15| "
    - "L1.16[1680044544000000000,1680045637388000000] 1681186614.52s 169mb                                                                                      |L1.16|"
    - "WARNING: file L1.16[1680044544000000000,1680045637388000000] 1681186614.52s 169mb exceeds soft limit 100mb by more than 50%"
    - "**** Final Output Files (1.51gb written)"
    - "L2                                                                                                                 "
    - "L2.46[1679961600071000000,1680022452125054234] 1681420678.89s 100mb|----------------------------L2.46----------------------------|                           "
    - "L2.56[1680022452125054235,1680032319822461332] 1681420678.89s 100mb                                                               |-L2.56--|                 "
    - "L2.57[1680032319822461333,1680042187519868429] 1681420678.89s 100mb                                                                         |-L2.57--|       "
    - "L2.58[1680042187519868430,1680045769912063525] 1681420678.89s 36mb                                                                                   |L2.58|"
    - "L2.59[1680045769912063526,1680046349505534795] 1681420678.89s 100mb                                                                                       |L2.59|"
    - "L2.60[1680046349505534796,1680046929099006064] 1681420678.89s 100mb                                                                                        |L2.60|"
    - "L2.61[1680046929099006065,1680047338160274709] 1681420678.89s 71mb                                                                                        |L2.61|"
    - "L2.62[1680047338160274710,1680047867631254942] 1681420678.89s 93mb                                                                                         |L2.62|"
    - "L2.63[1680047867631254943,1680047999999000000] 1681420678.89s 23mb                                                                                         |L2.63|"
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
    - "**** Final Output Files (3.4gb written)"
    - "L1                                                                                                                 "
    - "L1.260[1676045833054395546,1676050409609000000] 1676066475.26s 41mb                                                                  |L1.260|                "
    - "L2                                                                                                                 "
    - "L2.228[1676050409609000001,1676066212011000000] 1676066475.26s 145mb                                                                        |----L2.228-----| "
    - "L2.251[1675987200001000000,1675995209209749739] 1676066475.26s 100mb|L2.251-|                                                                                 "
    - "L2.261[1675995209209749740,1676003044683020379] 1676066475.26s 100mb         |L2.261|                                                                         "
    - "L2.262[1676003044683020380,1676010880156291018] 1676066475.26s 100mb                  |L2.262|                                                                "
    - "L2.263[1676010880156291019,1676018715629412205] 1676066475.26s 100mb                          |L2.263|                                                        "
    - "L2.264[1676018715629412206,1676027900853050774] 1676066475.26s 100mb                                   |-L2.264-|                                             "
    - "L2.265[1676027900853050775,1676037086076689342] 1676066475.26s 100mb                                              |-L2.265-|                                  "
    - "L2.266[1676037086076689343,1676045833054395545] 1676066475.26s 95mb                                                        |L2.266-|                         "
    "###
    );
}

// This is a simplified version of a test generated from actual catalog contents (which was thousands of lines).
// The key attributes are:
//  - there are enough bytes of L0 to trigger vertical splitting
//  - there are enough L0 files that the individual files are tiny
//  - there are lots of L1s that make it a pain to merge down from L0
//  - when the L0s get split, they're split into enough pieces that the algorigthm (pre-fix) would put the L0s back together in a single file.
// The result, prior to the fix motivating this test case, is that the L0s would be vertically split, then regrouped together in a single chain,
// so they get recompacted together, which again prompts the need for vertical splitting, resulting in an unproductive cycle.
#[tokio::test]
async fn very_big_overlapped_backlog() {
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

    let max_time: i64 = 200000;
    let l0_cnt: i64 = 200;
    let l0_interval = max_time / l0_cnt;
    let l0_size = MAX_DESIRED_FILE_SIZE * 4 / l0_cnt as u64;
    let l1_cnt = 100;
    let l1_interval = max_time / l1_cnt;

    // Create 100s of overlapping L0s
    for i in 0..l0_cnt {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i * l0_interval)
                    .with_max_time((i + 1) * l0_interval)
                    .with_compaction_level(CompactionLevel::Initial)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(l1_cnt + i))
                    .with_file_size_bytes(l0_size),
            )
            .await;
    }

    // Create a lot of L1s, on the same time range as the L0s
    for i in 0..l1_cnt {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i * l1_interval)
                    .with_max_time((i + 1) * l1_interval - 1)
                    .with_compaction_level(CompactionLevel::FileNonOverlapped)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i))
                    .with_file_size_bytes(MAX_DESIRED_FILE_SIZE),
            )
            .await;
    }

    // Create a lot of L2s, on the same time range as the L0s and L1s
    for i in 0..l1_cnt {
        setup
            .partition
            .create_parquet_file(
                parquet_builder()
                    .with_min_time(i * l1_interval)
                    .with_max_time((i + 1) * l1_interval - 1)
                    .with_compaction_level(CompactionLevel::Final)
                    .with_max_l0_created_at(Time::from_timestamp_nanos(i))
                    .with_file_size_bytes(MAX_DESIRED_FILE_SIZE),
            )
            .await;
    }

    insta::assert_yaml_snapshot!(
        run_layout_scenario(&setup).await,
        @r###"
    ---
    - "**** Input Files "
    - "L0                                                                                                                 "
    - "L0.1[0,1000] 100ns 2mb   |L0.1|                                                                                    "
    - "L0.2[1000,2000] 101ns 2mb|L0.2|                                                                                    "
    - "L0.3[2000,3000] 102ns 2mb|L0.3|                                                                                    "
    - "L0.4[3000,4000] 103ns 2mb |L0.4|                                                                                   "
    - "L0.5[4000,5000] 104ns 2mb |L0.5|                                                                                   "
    - "L0.6[5000,6000] 105ns 2mb  |L0.6|                                                                                  "
    - "L0.7[6000,7000] 106ns 2mb  |L0.7|                                                                                  "
    - "L0.8[7000,8000] 107ns 2mb   |L0.8|                                                                                 "
    - "L0.9[8000,9000] 108ns 2mb   |L0.9|                                                                                 "
    - "L0.10[9000,10000] 109ns 2mb    |L0.10|                                                                               "
    - "L0.11[10000,11000] 110ns 2mb    |L0.11|                                                                               "
    - "L0.12[11000,12000] 111ns 2mb    |L0.12|                                                                               "
    - "L0.13[12000,13000] 112ns 2mb     |L0.13|                                                                              "
    - "L0.14[13000,14000] 113ns 2mb     |L0.14|                                                                              "
    - "L0.15[14000,15000] 114ns 2mb      |L0.15|                                                                             "
    - "L0.16[15000,16000] 115ns 2mb      |L0.16|                                                                             "
    - "L0.17[16000,17000] 116ns 2mb       |L0.17|                                                                            "
    - "L0.18[17000,18000] 117ns 2mb       |L0.18|                                                                            "
    - "L0.19[18000,19000] 118ns 2mb        |L0.19|                                                                           "
    - "L0.20[19000,20000] 119ns 2mb        |L0.20|                                                                           "
    - "L0.21[20000,21000] 120ns 2mb         |L0.21|                                                                          "
    - "L0.22[21000,22000] 121ns 2mb         |L0.22|                                                                          "
    - "L0.23[22000,23000] 122ns 2mb         |L0.23|                                                                          "
    - "L0.24[23000,24000] 123ns 2mb          |L0.24|                                                                         "
    - "L0.25[24000,25000] 124ns 2mb          |L0.25|                                                                         "
    - "L0.26[25000,26000] 125ns 2mb           |L0.26|                                                                        "
    - "L0.27[26000,27000] 126ns 2mb           |L0.27|                                                                        "
    - "L0.28[27000,28000] 127ns 2mb            |L0.28|                                                                       "
    - "L0.29[28000,29000] 128ns 2mb            |L0.29|                                                                       "
    - "L0.30[29000,30000] 129ns 2mb             |L0.30|                                                                      "
    - "L0.31[30000,31000] 130ns 2mb             |L0.31|                                                                      "
    - "L0.32[31000,32000] 131ns 2mb             |L0.32|                                                                      "
    - "L0.33[32000,33000] 132ns 2mb              |L0.33|                                                                     "
    - "L0.34[33000,34000] 133ns 2mb              |L0.34|                                                                     "
    - "L0.35[34000,35000] 134ns 2mb               |L0.35|                                                                    "
    - "L0.36[35000,36000] 135ns 2mb               |L0.36|                                                                    "
    - "L0.37[36000,37000] 136ns 2mb                |L0.37|                                                                   "
    - "L0.38[37000,38000] 137ns 2mb                |L0.38|                                                                   "
    - "L0.39[38000,39000] 138ns 2mb                 |L0.39|                                                                  "
    - "L0.40[39000,40000] 139ns 2mb                 |L0.40|                                                                  "
    - "L0.41[40000,41000] 140ns 2mb                  |L0.41|                                                                 "
    - "L0.42[41000,42000] 141ns 2mb                  |L0.42|                                                                 "
    - "L0.43[42000,43000] 142ns 2mb                  |L0.43|                                                                 "
    - "L0.44[43000,44000] 143ns 2mb                   |L0.44|                                                                "
    - "L0.45[44000,45000] 144ns 2mb                   |L0.45|                                                                "
    - "L0.46[45000,46000] 145ns 2mb                    |L0.46|                                                               "
    - "L0.47[46000,47000] 146ns 2mb                    |L0.47|                                                               "
    - "L0.48[47000,48000] 147ns 2mb                     |L0.48|                                                              "
    - "L0.49[48000,49000] 148ns 2mb                     |L0.49|                                                              "
    - "L0.50[49000,50000] 149ns 2mb                      |L0.50|                                                             "
    - "L0.51[50000,51000] 150ns 2mb                      |L0.51|                                                             "
    - "L0.52[51000,52000] 151ns 2mb                      |L0.52|                                                             "
    - "L0.53[52000,53000] 152ns 2mb                       |L0.53|                                                            "
    - "L0.54[53000,54000] 153ns 2mb                       |L0.54|                                                            "
    - "L0.55[54000,55000] 154ns 2mb                        |L0.55|                                                           "
    - "L0.56[55000,56000] 155ns 2mb                        |L0.56|                                                           "
    - "L0.57[56000,57000] 156ns 2mb                         |L0.57|                                                          "
    - "L0.58[57000,58000] 157ns 2mb                         |L0.58|                                                          "
    - "L0.59[58000,59000] 158ns 2mb                          |L0.59|                                                         "
    - "L0.60[59000,60000] 159ns 2mb                          |L0.60|                                                         "
    - "L0.61[60000,61000] 160ns 2mb                           |L0.61|                                                        "
    - "L0.62[61000,62000] 161ns 2mb                           |L0.62|                                                        "
    - "L0.63[62000,63000] 162ns 2mb                           |L0.63|                                                        "
    - "L0.64[63000,64000] 163ns 2mb                            |L0.64|                                                       "
    - "L0.65[64000,65000] 164ns 2mb                            |L0.65|                                                       "
    - "L0.66[65000,66000] 165ns 2mb                             |L0.66|                                                      "
    - "L0.67[66000,67000] 166ns 2mb                             |L0.67|                                                      "
    - "L0.68[67000,68000] 167ns 2mb                              |L0.68|                                                     "
    - "L0.69[68000,69000] 168ns 2mb                              |L0.69|                                                     "
    - "L0.70[69000,70000] 169ns 2mb                               |L0.70|                                                    "
    - "L0.71[70000,71000] 170ns 2mb                               |L0.71|                                                    "
    - "L0.72[71000,72000] 171ns 2mb                               |L0.72|                                                    "
    - "L0.73[72000,73000] 172ns 2mb                                |L0.73|                                                   "
    - "L0.74[73000,74000] 173ns 2mb                                |L0.74|                                                   "
    - "L0.75[74000,75000] 174ns 2mb                                 |L0.75|                                                  "
    - "L0.76[75000,76000] 175ns 2mb                                 |L0.76|                                                  "
    - "L0.77[76000,77000] 176ns 2mb                                  |L0.77|                                                 "
    - "L0.78[77000,78000] 177ns 2mb                                  |L0.78|                                                 "
    - "L0.79[78000,79000] 178ns 2mb                                   |L0.79|                                                "
    - "L0.80[79000,80000] 179ns 2mb                                   |L0.80|                                                "
    - "L0.81[80000,81000] 180ns 2mb                                    |L0.81|                                               "
    - "L0.82[81000,82000] 181ns 2mb                                    |L0.82|                                               "
    - "L0.83[82000,83000] 182ns 2mb                                    |L0.83|                                               "
    - "L0.84[83000,84000] 183ns 2mb                                     |L0.84|                                              "
    - "L0.85[84000,85000] 184ns 2mb                                     |L0.85|                                              "
    - "L0.86[85000,86000] 185ns 2mb                                      |L0.86|                                             "
    - "L0.87[86000,87000] 186ns 2mb                                      |L0.87|                                             "
    - "L0.88[87000,88000] 187ns 2mb                                       |L0.88|                                            "
    - "L0.89[88000,89000] 188ns 2mb                                       |L0.89|                                            "
    - "L0.90[89000,90000] 189ns 2mb                                        |L0.90|                                           "
    - "L0.91[90000,91000] 190ns 2mb                                        |L0.91|                                           "
    - "L0.92[91000,92000] 191ns 2mb                                        |L0.92|                                           "
    - "L0.93[92000,93000] 192ns 2mb                                         |L0.93|                                          "
    - "L0.94[93000,94000] 193ns 2mb                                         |L0.94|                                          "
    - "L0.95[94000,95000] 194ns 2mb                                          |L0.95|                                         "
    - "L0.96[95000,96000] 195ns 2mb                                          |L0.96|                                         "
    - "L0.97[96000,97000] 196ns 2mb                                           |L0.97|                                        "
    - "L0.98[97000,98000] 197ns 2mb                                           |L0.98|                                        "
    - "L0.99[98000,99000] 198ns 2mb                                            |L0.99|                                       "
    - "L0.100[99000,100000] 199ns 2mb                                            |L0.100|                                      "
    - "L0.101[100000,101000] 200ns 2mb                                             |L0.101|                                     "
    - "L0.102[101000,102000] 201ns 2mb                                             |L0.102|                                     "
    - "L0.103[102000,103000] 202ns 2mb                                             |L0.103|                                     "
    - "L0.104[103000,104000] 203ns 2mb                                              |L0.104|                                    "
    - "L0.105[104000,105000] 204ns 2mb                                              |L0.105|                                    "
    - "L0.106[105000,106000] 205ns 2mb                                               |L0.106|                                   "
    - "L0.107[106000,107000] 206ns 2mb                                               |L0.107|                                   "
    - "L0.108[107000,108000] 207ns 2mb                                                |L0.108|                                  "
    - "L0.109[108000,109000] 208ns 2mb                                                |L0.109|                                  "
    - "L0.110[109000,110000] 209ns 2mb                                                 |L0.110|                                 "
    - "L0.111[110000,111000] 210ns 2mb                                                 |L0.111|                                 "
    - "L0.112[111000,112000] 211ns 2mb                                                 |L0.112|                                 "
    - "L0.113[112000,113000] 212ns 2mb                                                  |L0.113|                                "
    - "L0.114[113000,114000] 213ns 2mb                                                  |L0.114|                                "
    - "L0.115[114000,115000] 214ns 2mb                                                   |L0.115|                               "
    - "L0.116[115000,116000] 215ns 2mb                                                   |L0.116|                               "
    - "L0.117[116000,117000] 216ns 2mb                                                    |L0.117|                              "
    - "L0.118[117000,118000] 217ns 2mb                                                    |L0.118|                              "
    - "L0.119[118000,119000] 218ns 2mb                                                     |L0.119|                             "
    - "L0.120[119000,120000] 219ns 2mb                                                     |L0.120|                             "
    - "L0.121[120000,121000] 220ns 2mb                                                      |L0.121|                            "
    - "L0.122[121000,122000] 221ns 2mb                                                      |L0.122|                            "
    - "L0.123[122000,123000] 222ns 2mb                                                      |L0.123|                            "
    - "L0.124[123000,124000] 223ns 2mb                                                       |L0.124|                           "
    - "L0.125[124000,125000] 224ns 2mb                                                       |L0.125|                           "
    - "L0.126[125000,126000] 225ns 2mb                                                        |L0.126|                          "
    - "L0.127[126000,127000] 226ns 2mb                                                        |L0.127|                          "
    - "L0.128[127000,128000] 227ns 2mb                                                         |L0.128|                         "
    - "L0.129[128000,129000] 228ns 2mb                                                         |L0.129|                         "
    - "L0.130[129000,130000] 229ns 2mb                                                          |L0.130|                        "
    - "L0.131[130000,131000] 230ns 2mb                                                          |L0.131|                        "
    - "L0.132[131000,132000] 231ns 2mb                                                          |L0.132|                        "
    - "L0.133[132000,133000] 232ns 2mb                                                           |L0.133|                       "
    - "L0.134[133000,134000] 233ns 2mb                                                           |L0.134|                       "
    - "L0.135[134000,135000] 234ns 2mb                                                            |L0.135|                      "
    - "L0.136[135000,136000] 235ns 2mb                                                            |L0.136|                      "
    - "L0.137[136000,137000] 236ns 2mb                                                             |L0.137|                     "
    - "L0.138[137000,138000] 237ns 2mb                                                             |L0.138|                     "
    - "L0.139[138000,139000] 238ns 2mb                                                              |L0.139|                    "
    - "L0.140[139000,140000] 239ns 2mb                                                              |L0.140|                    "
    - "L0.141[140000,141000] 240ns 2mb                                                               |L0.141|                   "
    - "L0.142[141000,142000] 241ns 2mb                                                               |L0.142|                   "
    - "L0.143[142000,143000] 242ns 2mb                                                               |L0.143|                   "
    - "L0.144[143000,144000] 243ns 2mb                                                                |L0.144|                  "
    - "L0.145[144000,145000] 244ns 2mb                                                                |L0.145|                  "
    - "L0.146[145000,146000] 245ns 2mb                                                                 |L0.146|                 "
    - "L0.147[146000,147000] 246ns 2mb                                                                 |L0.147|                 "
    - "L0.148[147000,148000] 247ns 2mb                                                                  |L0.148|                "
    - "L0.149[148000,149000] 248ns 2mb                                                                  |L0.149|                "
    - "L0.150[149000,150000] 249ns 2mb                                                                   |L0.150|               "
    - "L0.151[150000,151000] 250ns 2mb                                                                   |L0.151|               "
    - "L0.152[151000,152000] 251ns 2mb                                                                   |L0.152|               "
    - "L0.153[152000,153000] 252ns 2mb                                                                    |L0.153|              "
    - "L0.154[153000,154000] 253ns 2mb                                                                    |L0.154|              "
    - "L0.155[154000,155000] 254ns 2mb                                                                     |L0.155|             "
    - "L0.156[155000,156000] 255ns 2mb                                                                     |L0.156|             "
    - "L0.157[156000,157000] 256ns 2mb                                                                      |L0.157|            "
    - "L0.158[157000,158000] 257ns 2mb                                                                      |L0.158|            "
    - "L0.159[158000,159000] 258ns 2mb                                                                       |L0.159|           "
    - "L0.160[159000,160000] 259ns 2mb                                                                       |L0.160|           "
    - "L0.161[160000,161000] 260ns 2mb                                                                        |L0.161|          "
    - "L0.162[161000,162000] 261ns 2mb                                                                        |L0.162|          "
    - "L0.163[162000,163000] 262ns 2mb                                                                        |L0.163|          "
    - "L0.164[163000,164000] 263ns 2mb                                                                         |L0.164|         "
    - "L0.165[164000,165000] 264ns 2mb                                                                         |L0.165|         "
    - "L0.166[165000,166000] 265ns 2mb                                                                          |L0.166|        "
    - "L0.167[166000,167000] 266ns 2mb                                                                          |L0.167|        "
    - "L0.168[167000,168000] 267ns 2mb                                                                           |L0.168|       "
    - "L0.169[168000,169000] 268ns 2mb                                                                           |L0.169|       "
    - "L0.170[169000,170000] 269ns 2mb                                                                            |L0.170|      "
    - "L0.171[170000,171000] 270ns 2mb                                                                            |L0.171|      "
    - "L0.172[171000,172000] 271ns 2mb                                                                            |L0.172|      "
    - "L0.173[172000,173000] 272ns 2mb                                                                             |L0.173|     "
    - "L0.174[173000,174000] 273ns 2mb                                                                             |L0.174|     "
    - "L0.175[174000,175000] 274ns 2mb                                                                              |L0.175|    "
    - "L0.176[175000,176000] 275ns 2mb                                                                              |L0.176|    "
    - "L0.177[176000,177000] 276ns 2mb                                                                               |L0.177|   "
    - "L0.178[177000,178000] 277ns 2mb                                                                               |L0.178|   "
    - "L0.179[178000,179000] 278ns 2mb                                                                                |L0.179|  "
    - "L0.180[179000,180000] 279ns 2mb                                                                                |L0.180|  "
    - "L0.181[180000,181000] 280ns 2mb                                                                                 |L0.181| "
    - "L0.182[181000,182000] 281ns 2mb                                                                                 |L0.182| "
    - "L0.183[182000,183000] 282ns 2mb                                                                                 |L0.183| "
    - "L0.184[183000,184000] 283ns 2mb                                                                                  |L0.184|"
    - "L0.185[184000,185000] 284ns 2mb                                                                                  |L0.185|"
    - "L0.186[185000,186000] 285ns 2mb                                                                                   |L0.186|"
    - "L0.187[186000,187000] 286ns 2mb                                                                                   |L0.187|"
    - "L0.188[187000,188000] 287ns 2mb                                                                                    |L0.188|"
    - "L0.189[188000,189000] 288ns 2mb                                                                                    |L0.189|"
    - "L0.190[189000,190000] 289ns 2mb                                                                                     |L0.190|"
    - "L0.191[190000,191000] 290ns 2mb                                                                                     |L0.191|"
    - "L0.192[191000,192000] 291ns 2mb                                                                                     |L0.192|"
    - "L0.193[192000,193000] 292ns 2mb                                                                                      |L0.193|"
    - "L0.194[193000,194000] 293ns 2mb                                                                                      |L0.194|"
    - "L0.195[194000,195000] 294ns 2mb                                                                                       |L0.195|"
    - "L0.196[195000,196000] 295ns 2mb                                                                                       |L0.196|"
    - "L0.197[196000,197000] 296ns 2mb                                                                                        |L0.197|"
    - "L0.198[197000,198000] 297ns 2mb                                                                                        |L0.198|"
    - "L0.199[198000,199000] 298ns 2mb                                                                                         |L0.199|"
    - "L0.200[199000,200000] 299ns 2mb                                                                                         |L0.200|"
    - "L1                                                                                                                 "
    - "L1.201[0,1999] 0ns 100mb |L1.201|                                                                                  "
    - "L1.202[2000,3999] 1ns 100mb|L1.202|                                                                                  "
    - "L1.203[4000,5999] 2ns 100mb |L1.203|                                                                                 "
    - "L1.204[6000,7999] 3ns 100mb  |L1.204|                                                                                "
    - "L1.205[8000,9999] 4ns 100mb   |L1.205|                                                                               "
    - "L1.206[10000,11999] 5ns 100mb    |L1.206|                                                                              "
    - "L1.207[12000,13999] 6ns 100mb     |L1.207|                                                                             "
    - "L1.208[14000,15999] 7ns 100mb      |L1.208|                                                                            "
    - "L1.209[16000,17999] 8ns 100mb       |L1.209|                                                                           "
    - "L1.210[18000,19999] 9ns 100mb        |L1.210|                                                                          "
    - "L1.211[20000,21999] 10ns 100mb         |L1.211|                                                                         "
    - "L1.212[22000,23999] 11ns 100mb         |L1.212|                                                                         "
    - "L1.213[24000,25999] 12ns 100mb          |L1.213|                                                                        "
    - "L1.214[26000,27999] 13ns 100mb           |L1.214|                                                                       "
    - "L1.215[28000,29999] 14ns 100mb            |L1.215|                                                                      "
    - "L1.216[30000,31999] 15ns 100mb             |L1.216|                                                                     "
    - "L1.217[32000,33999] 16ns 100mb              |L1.217|                                                                    "
    - "L1.218[34000,35999] 17ns 100mb               |L1.218|                                                                   "
    - "L1.219[36000,37999] 18ns 100mb                |L1.219|                                                                  "
    - "L1.220[38000,39999] 19ns 100mb                 |L1.220|                                                                 "
    - "L1.221[40000,41999] 20ns 100mb                  |L1.221|                                                                "
    - "L1.222[42000,43999] 21ns 100mb                  |L1.222|                                                                "
    - "L1.223[44000,45999] 22ns 100mb                   |L1.223|                                                               "
    - "L1.224[46000,47999] 23ns 100mb                    |L1.224|                                                              "
    - "L1.225[48000,49999] 24ns 100mb                     |L1.225|                                                             "
    - "L1.226[50000,51999] 25ns 100mb                      |L1.226|                                                            "
    - "L1.227[52000,53999] 26ns 100mb                       |L1.227|                                                           "
    - "L1.228[54000,55999] 27ns 100mb                        |L1.228|                                                          "
    - "L1.229[56000,57999] 28ns 100mb                         |L1.229|                                                         "
    - "L1.230[58000,59999] 29ns 100mb                          |L1.230|                                                        "
    - "L1.231[60000,61999] 30ns 100mb                           |L1.231|                                                       "
    - "L1.232[62000,63999] 31ns 100mb                           |L1.232|                                                       "
    - "L1.233[64000,65999] 32ns 100mb                            |L1.233|                                                      "
    - "L1.234[66000,67999] 33ns 100mb                             |L1.234|                                                     "
    - "L1.235[68000,69999] 34ns 100mb                              |L1.235|                                                    "
    - "L1.236[70000,71999] 35ns 100mb                               |L1.236|                                                   "
    - "L1.237[72000,73999] 36ns 100mb                                |L1.237|                                                  "
    - "L1.238[74000,75999] 37ns 100mb                                 |L1.238|                                                 "
    - "L1.239[76000,77999] 38ns 100mb                                  |L1.239|                                                "
    - "L1.240[78000,79999] 39ns 100mb                                   |L1.240|                                               "
    - "L1.241[80000,81999] 40ns 100mb                                    |L1.241|                                              "
    - "L1.242[82000,83999] 41ns 100mb                                    |L1.242|                                              "
    - "L1.243[84000,85999] 42ns 100mb                                     |L1.243|                                             "
    - "L1.244[86000,87999] 43ns 100mb                                      |L1.244|                                            "
    - "L1.245[88000,89999] 44ns 100mb                                       |L1.245|                                           "
    - "L1.246[90000,91999] 45ns 100mb                                        |L1.246|                                          "
    - "L1.247[92000,93999] 46ns 100mb                                         |L1.247|                                         "
    - "L1.248[94000,95999] 47ns 100mb                                          |L1.248|                                        "
    - "L1.249[96000,97999] 48ns 100mb                                           |L1.249|                                       "
    - "L1.250[98000,99999] 49ns 100mb                                            |L1.250|                                      "
    - "L1.251[100000,101999] 50ns 100mb                                             |L1.251|                                     "
    - "L1.252[102000,103999] 51ns 100mb                                             |L1.252|                                     "
    - "L1.253[104000,105999] 52ns 100mb                                              |L1.253|                                    "
    - "L1.254[106000,107999] 53ns 100mb                                               |L1.254|                                   "
    - "L1.255[108000,109999] 54ns 100mb                                                |L1.255|                                  "
    - "L1.256[110000,111999] 55ns 100mb                                                 |L1.256|                                 "
    - "L1.257[112000,113999] 56ns 100mb                                                  |L1.257|                                "
    - "L1.258[114000,115999] 57ns 100mb                                                   |L1.258|                               "
    - "L1.259[116000,117999] 58ns 100mb                                                    |L1.259|                              "
    - "L1.260[118000,119999] 59ns 100mb                                                     |L1.260|                             "
    - "L1.261[120000,121999] 60ns 100mb                                                      |L1.261|                            "
    - "L1.262[122000,123999] 61ns 100mb                                                      |L1.262|                            "
    - "L1.263[124000,125999] 62ns 100mb                                                       |L1.263|                           "
    - "L1.264[126000,127999] 63ns 100mb                                                        |L1.264|                          "
    - "L1.265[128000,129999] 64ns 100mb                                                         |L1.265|                         "
    - "L1.266[130000,131999] 65ns 100mb                                                          |L1.266|                        "
    - "L1.267[132000,133999] 66ns 100mb                                                           |L1.267|                       "
    - "L1.268[134000,135999] 67ns 100mb                                                            |L1.268|                      "
    - "L1.269[136000,137999] 68ns 100mb                                                             |L1.269|                     "
    - "L1.270[138000,139999] 69ns 100mb                                                              |L1.270|                    "
    - "L1.271[140000,141999] 70ns 100mb                                                               |L1.271|                   "
    - "L1.272[142000,143999] 71ns 100mb                                                               |L1.272|                   "
    - "L1.273[144000,145999] 72ns 100mb                                                                |L1.273|                  "
    - "L1.274[146000,147999] 73ns 100mb                                                                 |L1.274|                 "
    - "L1.275[148000,149999] 74ns 100mb                                                                  |L1.275|                "
    - "L1.276[150000,151999] 75ns 100mb                                                                   |L1.276|               "
    - "L1.277[152000,153999] 76ns 100mb                                                                    |L1.277|              "
    - "L1.278[154000,155999] 77ns 100mb                                                                     |L1.278|             "
    - "L1.279[156000,157999] 78ns 100mb                                                                      |L1.279|            "
    - "L1.280[158000,159999] 79ns 100mb                                                                       |L1.280|           "
    - "L1.281[160000,161999] 80ns 100mb                                                                        |L1.281|          "
    - "L1.282[162000,163999] 81ns 100mb                                                                        |L1.282|          "
    - "L1.283[164000,165999] 82ns 100mb                                                                         |L1.283|         "
    - "L1.284[166000,167999] 83ns 100mb                                                                          |L1.284|        "
    - "L1.285[168000,169999] 84ns 100mb                                                                           |L1.285|       "
    - "L1.286[170000,171999] 85ns 100mb                                                                            |L1.286|      "
    - "L1.287[172000,173999] 86ns 100mb                                                                             |L1.287|     "
    - "L1.288[174000,175999] 87ns 100mb                                                                              |L1.288|    "
    - "L1.289[176000,177999] 88ns 100mb                                                                               |L1.289|   "
    - "L1.290[178000,179999] 89ns 100mb                                                                                |L1.290|  "
    - "L1.291[180000,181999] 90ns 100mb                                                                                 |L1.291| "
    - "L1.292[182000,183999] 91ns 100mb                                                                                 |L1.292| "
    - "L1.293[184000,185999] 92ns 100mb                                                                                  |L1.293|"
    - "L1.294[186000,187999] 93ns 100mb                                                                                   |L1.294|"
    - "L1.295[188000,189999] 94ns 100mb                                                                                    |L1.295|"
    - "L1.296[190000,191999] 95ns 100mb                                                                                     |L1.296|"
    - "L1.297[192000,193999] 96ns 100mb                                                                                      |L1.297|"
    - "L1.298[194000,195999] 97ns 100mb                                                                                       |L1.298|"
    - "L1.299[196000,197999] 98ns 100mb                                                                                        |L1.299|"
    - "L1.300[198000,199999] 99ns 100mb                                                                                         |L1.300|"
    - "L2                                                                                                                 "
    - "L2.301[0,1999] 0ns 100mb |L2.301|                                                                                  "
    - "L2.302[2000,3999] 1ns 100mb|L2.302|                                                                                  "
    - "L2.303[4000,5999] 2ns 100mb |L2.303|                                                                                 "
    - "L2.304[6000,7999] 3ns 100mb  |L2.304|                                                                                "
    - "L2.305[8000,9999] 4ns 100mb   |L2.305|                                                                               "
    - "L2.306[10000,11999] 5ns 100mb    |L2.306|                                                                              "
    - "L2.307[12000,13999] 6ns 100mb     |L2.307|                                                                             "
    - "L2.308[14000,15999] 7ns 100mb      |L2.308|                                                                            "
    - "L2.309[16000,17999] 8ns 100mb       |L2.309|                                                                           "
    - "L2.310[18000,19999] 9ns 100mb        |L2.310|                                                                          "
    - "L2.311[20000,21999] 10ns 100mb         |L2.311|                                                                         "
    - "L2.312[22000,23999] 11ns 100mb         |L2.312|                                                                         "
    - "L2.313[24000,25999] 12ns 100mb          |L2.313|                                                                        "
    - "L2.314[26000,27999] 13ns 100mb           |L2.314|                                                                       "
    - "L2.315[28000,29999] 14ns 100mb            |L2.315|                                                                      "
    - "L2.316[30000,31999] 15ns 100mb             |L2.316|                                                                     "
    - "L2.317[32000,33999] 16ns 100mb              |L2.317|                                                                    "
    - "L2.318[34000,35999] 17ns 100mb               |L2.318|                                                                   "
    - "L2.319[36000,37999] 18ns 100mb                |L2.319|                                                                  "
    - "L2.320[38000,39999] 19ns 100mb                 |L2.320|                                                                 "
    - "L2.321[40000,41999] 20ns 100mb                  |L2.321|                                                                "
    - "L2.322[42000,43999] 21ns 100mb                  |L2.322|                                                                "
    - "L2.323[44000,45999] 22ns 100mb                   |L2.323|                                                               "
    - "L2.324[46000,47999] 23ns 100mb                    |L2.324|                                                              "
    - "L2.325[48000,49999] 24ns 100mb                     |L2.325|                                                             "
    - "L2.326[50000,51999] 25ns 100mb                      |L2.326|                                                            "
    - "L2.327[52000,53999] 26ns 100mb                       |L2.327|                                                           "
    - "L2.328[54000,55999] 27ns 100mb                        |L2.328|                                                          "
    - "L2.329[56000,57999] 28ns 100mb                         |L2.329|                                                         "
    - "L2.330[58000,59999] 29ns 100mb                          |L2.330|                                                        "
    - "L2.331[60000,61999] 30ns 100mb                           |L2.331|                                                       "
    - "L2.332[62000,63999] 31ns 100mb                           |L2.332|                                                       "
    - "L2.333[64000,65999] 32ns 100mb                            |L2.333|                                                      "
    - "L2.334[66000,67999] 33ns 100mb                             |L2.334|                                                     "
    - "L2.335[68000,69999] 34ns 100mb                              |L2.335|                                                    "
    - "L2.336[70000,71999] 35ns 100mb                               |L2.336|                                                   "
    - "L2.337[72000,73999] 36ns 100mb                                |L2.337|                                                  "
    - "L2.338[74000,75999] 37ns 100mb                                 |L2.338|                                                 "
    - "L2.339[76000,77999] 38ns 100mb                                  |L2.339|                                                "
    - "L2.340[78000,79999] 39ns 100mb                                   |L2.340|                                               "
    - "L2.341[80000,81999] 40ns 100mb                                    |L2.341|                                              "
    - "L2.342[82000,83999] 41ns 100mb                                    |L2.342|                                              "
    - "L2.343[84000,85999] 42ns 100mb                                     |L2.343|                                             "
    - "L2.344[86000,87999] 43ns 100mb                                      |L2.344|                                            "
    - "L2.345[88000,89999] 44ns 100mb                                       |L2.345|                                           "
    - "L2.346[90000,91999] 45ns 100mb                                        |L2.346|                                          "
    - "L2.347[92000,93999] 46ns 100mb                                         |L2.347|                                         "
    - "L2.348[94000,95999] 47ns 100mb                                          |L2.348|                                        "
    - "L2.349[96000,97999] 48ns 100mb                                           |L2.349|                                       "
    - "L2.350[98000,99999] 49ns 100mb                                            |L2.350|                                      "
    - "L2.351[100000,101999] 50ns 100mb                                             |L2.351|                                     "
    - "L2.352[102000,103999] 51ns 100mb                                             |L2.352|                                     "
    - "L2.353[104000,105999] 52ns 100mb                                              |L2.353|                                    "
    - "L2.354[106000,107999] 53ns 100mb                                               |L2.354|                                   "
    - "L2.355[108000,109999] 54ns 100mb                                                |L2.355|                                  "
    - "L2.356[110000,111999] 55ns 100mb                                                 |L2.356|                                 "
    - "L2.357[112000,113999] 56ns 100mb                                                  |L2.357|                                "
    - "L2.358[114000,115999] 57ns 100mb                                                   |L2.358|                               "
    - "L2.359[116000,117999] 58ns 100mb                                                    |L2.359|                              "
    - "L2.360[118000,119999] 59ns 100mb                                                     |L2.360|                             "
    - "L2.361[120000,121999] 60ns 100mb                                                      |L2.361|                            "
    - "L2.362[122000,123999] 61ns 100mb                                                      |L2.362|                            "
    - "L2.363[124000,125999] 62ns 100mb                                                       |L2.363|                           "
    - "L2.364[126000,127999] 63ns 100mb                                                        |L2.364|                          "
    - "L2.365[128000,129999] 64ns 100mb                                                         |L2.365|                         "
    - "L2.366[130000,131999] 65ns 100mb                                                          |L2.366|                        "
    - "L2.367[132000,133999] 66ns 100mb                                                           |L2.367|                       "
    - "L2.368[134000,135999] 67ns 100mb                                                            |L2.368|                      "
    - "L2.369[136000,137999] 68ns 100mb                                                             |L2.369|                     "
    - "L2.370[138000,139999] 69ns 100mb                                                              |L2.370|                    "
    - "L2.371[140000,141999] 70ns 100mb                                                               |L2.371|                   "
    - "L2.372[142000,143999] 71ns 100mb                                                               |L2.372|                   "
    - "L2.373[144000,145999] 72ns 100mb                                                                |L2.373|                  "
    - "L2.374[146000,147999] 73ns 100mb                                                                 |L2.374|                 "
    - "L2.375[148000,149999] 74ns 100mb                                                                  |L2.375|                "
    - "L2.376[150000,151999] 75ns 100mb                                                                   |L2.376|               "
    - "L2.377[152000,153999] 76ns 100mb                                                                    |L2.377|              "
    - "L2.378[154000,155999] 77ns 100mb                                                                     |L2.378|             "
    - "L2.379[156000,157999] 78ns 100mb                                                                      |L2.379|            "
    - "L2.380[158000,159999] 79ns 100mb                                                                       |L2.380|           "
    - "L2.381[160000,161999] 80ns 100mb                                                                        |L2.381|          "
    - "L2.382[162000,163999] 81ns 100mb                                                                        |L2.382|          "
    - "L2.383[164000,165999] 82ns 100mb                                                                         |L2.383|         "
    - "L2.384[166000,167999] 83ns 100mb                                                                          |L2.384|        "
    - "L2.385[168000,169999] 84ns 100mb                                                                           |L2.385|       "
    - "L2.386[170000,171999] 85ns 100mb                                                                            |L2.386|      "
    - "L2.387[172000,173999] 86ns 100mb                                                                             |L2.387|     "
    - "L2.388[174000,175999] 87ns 100mb                                                                              |L2.388|    "
    - "L2.389[176000,177999] 88ns 100mb                                                                               |L2.389|   "
    - "L2.390[178000,179999] 89ns 100mb                                                                                |L2.390|  "
    - "L2.391[180000,181999] 90ns 100mb                                                                                 |L2.391| "
    - "L2.392[182000,183999] 91ns 100mb                                                                                 |L2.392| "
    - "L2.393[184000,185999] 92ns 100mb                                                                                  |L2.393|"
    - "L2.394[186000,187999] 93ns 100mb                                                                                   |L2.394|"
    - "L2.395[188000,189999] 94ns 100mb                                                                                    |L2.395|"
    - "L2.396[190000,191999] 95ns 100mb                                                                                     |L2.396|"
    - "L2.397[192000,193999] 96ns 100mb                                                                                      |L2.397|"
    - "L2.398[194000,195999] 97ns 100mb                                                                                       |L2.398|"
    - "L2.399[196000,197999] 98ns 100mb                                                                                        |L2.399|"
    - "L2.400[198000,199999] 99ns 100mb                                                                                         |L2.400|"
    - "**** Final Output Files (45.56gb written)"
    - "L2                                                                                                                 "
    - "L2.1173[0,983] 104ns 100mb|L2.1173|                                                                                 "
    - "L2.1174[984,1966] 104ns 100mb|L2.1174|                                                                                 "
    - "L2.1175[1967,1999] 104ns 3mb|L2.1175|                                                                                 "
    - "L2.1176[2000,2983] 108ns 100mb|L2.1176|                                                                                 "
    - "L2.1177[2984,3966] 108ns 100mb |L2.1177|                                                                                "
    - "L2.1178[3967,3999] 108ns 3mb |L2.1178|                                                                                "
    - "L2.1179[4000,4978] 108ns 100mb |L2.1179|                                                                                "
    - "L2.1180[4979,5956] 108ns 100mb  |L2.1180|                                                                               "
    - "L2.1181[5957,5999] 108ns 4mb  |L2.1181|                                                                               "
    - "L2.1182[6000,6978] 108ns 100mb  |L2.1182|                                                                               "
    - "L2.1183[6979,7956] 108ns 100mb   |L2.1183|                                                                              "
    - "L2.1184[7957,7999] 108ns 5mb   |L2.1184|                                                                              "
    - "L2.1185[8000,8979] 113ns 100mb   |L2.1185|                                                                              "
    - "L2.1186[8980,9958] 113ns 100mb    |L2.1186|                                                                             "
    - "L2.1187[9959,9999] 113ns 4mb    |L2.1187|                                                                             "
    - "L2.1188[10000,10980] 113ns 100mb    |L2.1188|                                                                             "
    - "L2.1189[10981,11960] 113ns 100mb    |L2.1189|                                                                             "
    - "L2.1190[11961,11999] 113ns 4mb     |L2.1190|                                                                            "
    - "L2.1191[12000,12980] 113ns 100mb     |L2.1191|                                                                            "
    - "L2.1192[12981,13960] 113ns 100mb     |L2.1192|                                                                            "
    - "L2.1193[13961,13999] 113ns 4mb      |L2.1193|                                                                           "
    - "L2.1194[14000,14981] 117ns 100mb      |L2.1194|                                                                           "
    - "L2.1195[14982,15962] 117ns 100mb      |L2.1195|                                                                           "
    - "L2.1196[15963,15999] 117ns 4mb       |L2.1196|                                                                          "
    - "L2.1197[16000,16980] 117ns 100mb       |L2.1197|                                                                          "
    - "L2.1198[16981,17960] 117ns 100mb       |L2.1198|                                                                          "
    - "L2.1199[17961,17999] 117ns 4mb        |L2.1199|                                                                         "
    - "L2.1200[18000,18981] 123ns 100mb        |L2.1200|                                                                         "
    - "L2.1201[18982,19962] 123ns 100mb        |L2.1201|                                                                         "
    - "L2.1202[19963,19999] 123ns 4mb        |L2.1202|                                                                         "
    - "L2.1203[20000,20981] 123ns 100mb         |L2.1203|                                                                        "
    - "L2.1204[20982,21962] 123ns 100mb         |L2.1204|                                                                        "
    - "L2.1205[21963,21999] 123ns 4mb         |L2.1205|                                                                        "
    - "L2.1206[22000,22981] 127ns 100mb         |L2.1206|                                                                        "
    - "L2.1207[22982,23962] 127ns 100mb          |L2.1207|                                                                       "
    - "L2.1208[23963,23999] 127ns 4mb          |L2.1208|                                                                       "
    - "L2.1209[24000,24984] 127ns 100mb          |L2.1209|                                                                       "
    - "L2.1210[24985,25968] 127ns 100mb           |L2.1210|                                                                      "
    - "L2.1211[25969,25999] 127ns 3mb           |L2.1211|                                                                      "
    - "L2.1212[26000,26981] 131ns 100mb           |L2.1212|                                                                      "
    - "L2.1213[26982,27962] 131ns 100mb            |L2.1213|                                                                     "
    - "L2.1214[27963,27999] 131ns 4mb            |L2.1214|                                                                     "
    - "L2.1215[28000,28978] 131ns 100mb            |L2.1215|                                                                     "
    - "L2.1216[28979,29956] 131ns 100mb             |L2.1216|                                                                    "
    - "L2.1217[29957,29999] 131ns 5mb             |L2.1217|                                                                    "
    - "L2.1218[30000,30977] 131ns 100mb             |L2.1218|                                                                    "
    - "L2.1219[30978,31954] 131ns 100mb             |L2.1219|                                                                    "
    - "L2.1220[31955,31999] 131ns 5mb              |L2.1220|                                                                   "
    - "L2.1221[32000,32981] 137ns 100mb              |L2.1221|                                                                   "
    - "L2.1222[32982,33962] 137ns 100mb              |L2.1222|                                                                   "
    - "L2.1223[33963,33999] 137ns 4mb               |L2.1223|                                                                  "
    - "L2.1224[34000,34981] 137ns 100mb               |L2.1224|                                                                  "
    - "L2.1225[34982,35962] 137ns 100mb               |L2.1225|                                                                  "
    - "L2.1226[35963,35999] 137ns 4mb                |L2.1226|                                                                 "
    - "L2.1227[36000,36981] 141ns 100mb                |L2.1227|                                                                 "
    - "L2.1228[36982,37962] 141ns 100mb                |L2.1228|                                                                 "
    - "L2.1229[37963,37999] 141ns 4mb                 |L2.1229|                                                                "
    - "L2.1230[38000,38985] 141ns 100mb                 |L2.1230|                                                                "
    - "L2.1231[38986,39970] 141ns 100mb                 |L2.1231|                                                                "
    - "L2.1232[39971,39999] 141ns 3mb                 |L2.1232|                                                                "
    - "L2.1233[40000,40983] 146ns 100mb                  |L2.1233|                                                               "
    - "L2.1234[40984,41966] 146ns 100mb                  |L2.1234|                                                               "
    - "L2.1235[41967,41999] 146ns 3mb                  |L2.1235|                                                               "
    - "L2.1236[42000,42977] 146ns 100mb                  |L2.1236|                                                               "
    - "L2.1237[42978,43954] 146ns 100mb                   |L2.1237|                                                              "
    - "L2.1238[43955,43999] 146ns 5mb                   |L2.1238|                                                              "
    - "L2.1239[44000,44976] 146ns 100mb                   |L2.1239|                                                              "
    - "L2.1240[44977,45952] 146ns 100mb                    |L2.1240|                                                             "
    - "L2.1241[45953,45999] 146ns 5mb                    |L2.1241|                                                             "
    - "L2.1242[46000,46980] 151ns 100mb                    |L2.1242|                                                             "
    - "L2.1243[46981,47960] 151ns 100mb                     |L2.1243|                                                            "
    - "L2.1244[47961,47999] 151ns 4mb                     |L2.1244|                                                            "
    - "L2.1245[48000,48980] 151ns 100mb                     |L2.1245|                                                            "
    - "L2.1246[48981,49960] 151ns 100mb                      |L2.1246|                                                           "
    - "L2.1247[49961,49999] 151ns 4mb                      |L2.1247|                                                           "
    - "L2.1248[50000,50980] 151ns 100mb                      |L2.1248|                                                           "
    - "L2.1249[50981,51960] 151ns 100mb                      |L2.1249|                                                           "
    - "L2.1250[51961,51999] 151ns 4mb                       |L2.1250|                                                          "
    - "L2.1251[52000,52981] 155ns 100mb                       |L2.1251|                                                          "
    - "L2.1252[52982,53962] 155ns 100mb                       |L2.1252|                                                          "
    - "L2.1253[53963,53999] 155ns 4mb                        |L2.1253|                                                         "
    - "L2.1254[54000,54980] 155ns 100mb                        |L2.1254|                                                         "
    - "L2.1255[54981,55960] 155ns 100mb                        |L2.1255|                                                         "
    - "L2.1256[55961,55999] 155ns 4mb                         |L2.1256|                                                        "
    - "L2.1257[56000,56981] 160ns 100mb                         |L2.1257|                                                        "
    - "L2.1258[56982,57962] 160ns 100mb                         |L2.1258|                                                        "
    - "L2.1259[57963,57999] 160ns 4mb                          |L2.1259|                                                       "
    - "L2.1260[58000,58981] 160ns 100mb                          |L2.1260|                                                       "
    - "L2.1261[58982,59962] 160ns 100mb                          |L2.1261|                                                       "
    - "L2.1262[59963,59999] 160ns 4mb                          |L2.1262|                                                       "
    - "L2.1263[60000,60980] 165ns 100mb                           |L2.1263|                                                      "
    - "L2.1264[60981,61960] 165ns 100mb                           |L2.1264|                                                      "
    - "L2.1265[61961,61999] 165ns 4mb                           |L2.1265|                                                      "
    - "L2.1266[62000,62981] 165ns 100mb                           |L2.1266|                                                      "
    - "L2.1267[62982,63962] 165ns 100mb                            |L2.1267|                                                     "
    - "L2.1268[63963,63999] 165ns 4mb                            |L2.1268|                                                     "
    - "L2.1269[64000,64980] 165ns 100mb                            |L2.1269|                                                     "
    - "L2.1270[64981,65960] 165ns 100mb                             |L2.1270|                                                    "
    - "L2.1271[65961,65999] 165ns 4mb                             |L2.1271|                                                    "
    - "L2.1272[66000,66981] 169ns 100mb                             |L2.1272|                                                    "
    - "L2.1273[66982,67962] 169ns 100mb                              |L2.1273|                                                   "
    - "L2.1274[67963,67999] 169ns 4mb                              |L2.1274|                                                   "
    - "L2.1275[68000,68980] 169ns 100mb                              |L2.1275|                                                   "
    - "L2.1276[68981,69960] 169ns 100mb                               |L2.1276|                                                  "
    - "L2.1277[69961,69999] 169ns 4mb                               |L2.1277|                                                  "
    - "L2.1278[70000,70981] 175ns 100mb                               |L2.1278|                                                  "
    - "L2.1279[70982,71962] 175ns 100mb                               |L2.1279|                                                  "
    - "L2.1280[71963,71999] 175ns 4mb                                |L2.1280|                                                 "
    - "L2.1281[72000,72981] 175ns 100mb                                |L2.1281|                                                 "
    - "L2.1282[72982,73962] 175ns 100mb                                |L2.1282|                                                 "
    - "L2.1283[73963,73999] 175ns 4mb                                 |L2.1283|                                                "
    - "L2.1284[74000,74981] 179ns 100mb                                 |L2.1284|                                                "
    - "L2.1285[74982,75962] 179ns 100mb                                 |L2.1285|                                                "
    - "L2.1286[75963,75999] 179ns 4mb                                  |L2.1286|                                               "
    - "L2.1287[76000,76984] 179ns 100mb                                  |L2.1287|                                               "
    - "L2.1288[76985,77968] 179ns 100mb                                  |L2.1288|                                               "
    - "L2.1289[77969,77999] 179ns 3mb                                   |L2.1289|                                              "
    - "L2.1290[78000,78982] 184ns 100mb                                   |L2.1290|                                              "
    - "L2.1291[78983,79964] 184ns 100mb                                   |L2.1291|                                              "
    - "L2.1292[79965,79999] 184ns 4mb                                   |L2.1292|                                              "
    - "L2.1293[80000,80977] 184ns 100mb                                    |L2.1293|                                             "
    - "L2.1294[80978,81954] 184ns 100mb                                    |L2.1294|                                             "
    - "L2.1295[81955,81999] 184ns 5mb                                    |L2.1295|                                             "
    - "L2.1296[82000,82977] 184ns 100mb                                    |L2.1296|                                             "
    - "L2.1297[82978,83954] 184ns 100mb                                     |L2.1297|                                            "
    - "L2.1298[83955,83999] 184ns 5mb                                     |L2.1298|                                            "
    - "L2.1299[84000,84983] 188ns 100mb                                     |L2.1299|                                            "
    - "L2.1300[84984,85966] 188ns 100mb                                      |L2.1300|                                           "
    - "L2.1301[85967,85999] 188ns 3mb                                      |L2.1301|                                           "
    - "L2.1302[86000,86983] 192ns 100mb                                      |L2.1302|                                           "
    - "L2.1303[86984,87966] 192ns 100mb                                       |L2.1303|                                          "
    - "L2.1304[87967,87999] 192ns 3mb                                       |L2.1304|                                          "
    - "L2.1305[88000,88978] 192ns 100mb                                       |L2.1305|                                          "
    - "L2.1306[88979,89956] 192ns 100mb                                        |L2.1306|                                         "
    - "L2.1307[89957,89999] 192ns 5mb                                        |L2.1307|                                         "
    - "L2.1308[90000,90978] 192ns 100mb                                        |L2.1308|                                         "
    - "L2.1309[90979,91956] 192ns 100mb                                        |L2.1309|                                         "
    - "L2.1310[91957,91999] 192ns 5mb                                         |L2.1310|                                        "
    - "L2.1311[92000,92979] 197ns 100mb                                         |L2.1311|                                        "
    - "L2.1312[92980,93958] 197ns 100mb                                         |L2.1312|                                        "
    - "L2.1313[93959,93999] 197ns 4mb                                          |L2.1313|                                       "
    - "L2.1314[94000,94980] 197ns 100mb                                          |L2.1314|                                       "
    - "L2.1315[94981,95960] 197ns 100mb                                          |L2.1315|                                       "
    - "L2.1316[95961,95999] 197ns 4mb                                           |L2.1316|                                      "
    - "L2.1317[96000,96980] 197ns 100mb                                           |L2.1317|                                      "
    - "L2.1318[96981,97960] 197ns 100mb                                           |L2.1318|                                      "
    - "L2.1319[97961,97999] 197ns 4mb                                            |L2.1319|                                     "
    - "L2.1320[98000,98981] 201ns 100mb                                            |L2.1320|                                     "
    - "L2.1321[98982,99962] 201ns 100mb                                            |L2.1321|                                     "
    - "L2.1322[99963,99999] 201ns 4mb                                            |L2.1322|                                     "
    - "L2.1323[100000,100980] 201ns 100mb                                             |L2.1323|                                    "
    - "L2.1324[100981,101960] 201ns 100mb                                             |L2.1324|                                    "
    - "L2.1325[101961,101999] 201ns 4mb                                             |L2.1325|                                    "
    - "L2.1326[102000,102981] 207ns 100mb                                             |L2.1326|                                    "
    - "L2.1327[102982,103962] 207ns 100mb                                              |L2.1327|                                   "
    - "L2.1328[103963,103999] 207ns 4mb                                              |L2.1328|                                   "
    - "L2.1329[104000,104981] 207ns 100mb                                              |L2.1329|                                   "
    - "L2.1330[104982,105962] 207ns 100mb                                               |L2.1330|                                  "
    - "L2.1331[105963,105999] 207ns 4mb                                               |L2.1331|                                  "
    - "L2.1332[106000,106981] 211ns 100mb                                               |L2.1332|                                  "
    - "L2.1333[106982,107962] 211ns 100mb                                                |L2.1333|                                 "
    - "L2.1334[107963,107999] 211ns 4mb                                                |L2.1334|                                 "
    - "L2.1335[108000,108984] 211ns 100mb                                                |L2.1335|                                 "
    - "L2.1336[108985,109968] 211ns 100mb                                                 |L2.1336|                                "
    - "L2.1337[109969,109999] 211ns 3mb                                                 |L2.1337|                                "
    - "L2.1338[110000,110981] 215ns 100mb                                                 |L2.1338|                                "
    - "L2.1339[110982,111962] 215ns 100mb                                                 |L2.1339|                                "
    - "L2.1340[111963,111999] 215ns 4mb                                                  |L2.1340|                               "
    - "L2.1341[112000,112978] 215ns 100mb                                                  |L2.1341|                               "
    - "L2.1342[112979,113956] 215ns 100mb                                                  |L2.1342|                               "
    - "L2.1343[113957,113999] 215ns 5mb                                                   |L2.1343|                              "
    - "L2.1344[114000,114977] 215ns 100mb                                                   |L2.1344|                              "
    - "L2.1345[114978,115954] 215ns 100mb                                                   |L2.1345|                              "
    - "L2.1346[115955,115999] 215ns 5mb                                                    |L2.1346|                             "
    - "L2.1347[116000,116981] 221ns 100mb                                                    |L2.1347|                             "
    - "L2.1348[116982,117962] 221ns 100mb                                                    |L2.1348|                             "
    - "L2.1349[117963,117999] 221ns 4mb                                                     |L2.1349|                            "
    - "L2.1350[118000,118981] 221ns 100mb                                                     |L2.1350|                            "
    - "L2.1351[118982,119962] 221ns 100mb                                                     |L2.1351|                            "
    - "L2.1352[119963,119999] 221ns 4mb                                                     |L2.1352|                            "
    - "L2.1353[120000,120981] 225ns 100mb                                                      |L2.1353|                           "
    - "L2.1354[120982,121962] 225ns 100mb                                                      |L2.1354|                           "
    - "L2.1355[121963,121999] 225ns 4mb                                                      |L2.1355|                           "
    - "L2.1356[122000,122986] 225ns 100mb                                                      |L2.1356|                           "
    - "L2.1357[122987,123972] 225ns 100mb                                                       |L2.1357|                          "
    - "L2.1358[123973,123999] 225ns 3mb                                                       |L2.1358|                          "
    - "L2.1359[124000,124984] 230ns 100mb                                                       |L2.1359|                          "
    - "L2.1360[124985,125968] 230ns 100mb                                                        |L2.1360|                         "
    - "L2.1361[125969,125999] 230ns 3mb                                                        |L2.1361|                         "
    - "L2.1362[126000,126976] 230ns 100mb                                                        |L2.1362|                         "
    - "L2.1363[126977,127952] 230ns 100mb                                                         |L2.1363|                        "
    - "L2.1364[127953,127999] 230ns 5mb                                                         |L2.1364|                        "
    - "L2.1365[128000,128976] 230ns 100mb                                                         |L2.1365|                        "
    - "L2.1366[128977,129952] 230ns 100mb                                                          |L2.1366|                       "
    - "L2.1367[129953,129999] 230ns 5mb                                                          |L2.1367|                       "
    - "L2.1368[130000,130979] 235ns 100mb                                                          |L2.1368|                       "
    - "L2.1369[130980,131958] 235ns 100mb                                                          |L2.1369|                       "
    - "L2.1370[131959,131999] 235ns 4mb                                                           |L2.1370|                      "
    - "L2.1371[132000,132980] 235ns 100mb                                                           |L2.1371|                      "
    - "L2.1372[132981,133960] 235ns 100mb                                                           |L2.1372|                      "
    - "L2.1373[133961,133999] 235ns 4mb                                                            |L2.1373|                     "
    - "L2.1374[134000,134980] 235ns 100mb                                                            |L2.1374|                     "
    - "L2.1375[134981,135960] 235ns 100mb                                                            |L2.1375|                     "
    - "L2.1376[135961,135999] 235ns 4mb                                                             |L2.1376|                    "
    - "L2.1377[136000,136981] 239ns 100mb                                                             |L2.1377|                    "
    - "L2.1378[136982,137962] 239ns 100mb                                                             |L2.1378|                    "
    - "L2.1379[137963,137999] 239ns 4mb                                                              |L2.1379|                   "
    - "L2.1380[138000,138980] 239ns 100mb                                                              |L2.1380|                   "
    - "L2.1381[138981,139960] 239ns 100mb                                                              |L2.1381|                   "
    - "L2.1382[139961,139999] 239ns 4mb                                                              |L2.1382|                   "
    - "L2.1383[140000,140981] 244ns 100mb                                                               |L2.1383|                  "
    - "L2.1384[140982,141962] 244ns 100mb                                                               |L2.1384|                  "
    - "L2.1385[141963,141999] 244ns 4mb                                                               |L2.1385|                  "
    - "L2.1386[142000,142981] 244ns 100mb                                                               |L2.1386|                  "
    - "L2.1387[142982,143962] 244ns 100mb                                                                |L2.1387|                 "
    - "L2.1388[143963,143999] 244ns 4mb                                                                |L2.1388|                 "
    - "L2.1389[144000,144981] 249ns 100mb                                                                |L2.1389|                 "
    - "L2.1390[144982,145962] 249ns 100mb                                                                 |L2.1390|                "
    - "L2.1391[145963,145999] 249ns 4mb                                                                 |L2.1391|                "
    - "L2.1392[146000,146983] 249ns 100mb                                                                 |L2.1392|                "
    - "L2.1393[146984,147966] 249ns 100mb                                                                  |L2.1393|               "
    - "L2.1394[147967,147999] 249ns 3mb                                                                  |L2.1394|               "
    - "L2.1395[148000,148980] 253ns 100mb                                                                  |L2.1395|               "
    - "L2.1396[148981,149960] 253ns 100mb                                                                   |L2.1396|              "
    - "L2.1397[149961,149999] 253ns 4mb                                                                   |L2.1397|              "
    - "L2.1398[150000,150979] 253ns 100mb                                                                   |L2.1398|              "
    - "L2.1399[150980,151958] 253ns 100mb                                                                   |L2.1399|              "
    - "L2.1400[151959,151999] 253ns 4mb                                                                    |L2.1400|             "
    - "L2.1401[152000,152978] 253ns 100mb                                                                    |L2.1401|             "
    - "L2.1402[152979,153956] 253ns 100mb                                                                    |L2.1402|             "
    - "L2.1403[153957,153999] 253ns 5mb                                                                     |L2.1403|            "
    - "L2.1404[154000,154981] 259ns 100mb                                                                     |L2.1404|            "
    - "L2.1405[154982,155962] 259ns 100mb                                                                     |L2.1405|            "
    - "L2.1406[155963,155999] 259ns 4mb                                                                      |L2.1406|           "
    - "L2.1407[156000,156981] 259ns 100mb                                                                      |L2.1407|           "
    - "L2.1408[156982,157962] 259ns 100mb                                                                      |L2.1408|           "
    - "L2.1409[157963,157999] 259ns 4mb                                                                       |L2.1409|          "
    - "L2.1410[158000,158981] 263ns 100mb                                                                       |L2.1410|          "
    - "L2.1411[158982,159962] 263ns 100mb                                                                       |L2.1411|          "
    - "L2.1412[159963,159999] 263ns 4mb                                                                       |L2.1412|          "
    - "L2.1413[160000,160985] 263ns 100mb                                                                        |L2.1413|         "
    - "L2.1414[160986,161970] 263ns 100mb                                                                        |L2.1414|         "
    - "L2.1415[161971,161999] 263ns 3mb                                                                        |L2.1415|         "
    - "L2.1416[162000,162982] 268ns 100mb                                                                        |L2.1416|         "
    - "L2.1417[162983,163964] 268ns 100mb                                                                         |L2.1417|        "
    - "L2.1418[163965,163999] 268ns 4mb                                                                         |L2.1418|        "
    - "L2.1419[164000,164977] 268ns 100mb                                                                         |L2.1419|        "
    - "L2.1420[164978,165954] 268ns 100mb                                                                          |L2.1420|       "
    - "L2.1421[165955,165999] 268ns 5mb                                                                          |L2.1421|       "
    - "L2.1422[166000,166977] 268ns 100mb                                                                          |L2.1422|       "
    - "L2.1423[166978,167954] 268ns 100mb                                                                           |L2.1423|      "
    - "L2.1424[167955,167999] 268ns 5mb                                                                           |L2.1424|      "
    - "L2.1425[168000,168983] 272ns 100mb                                                                           |L2.1425|      "
    - "L2.1426[168984,169966] 272ns 100mb                                                                            |L2.1426|     "
    - "L2.1427[169967,169999] 272ns 3mb                                                                            |L2.1427|     "
    - "L2.1428[170000,170984] 276ns 100mb                                                                            |L2.1428|     "
    - "L2.1429[170985,171968] 276ns 100mb                                                                            |L2.1429|     "
    - "L2.1430[171969,171999] 276ns 3mb                                                                             |L2.1430|    "
    - "L2.1431[172000,172978] 276ns 100mb                                                                             |L2.1431|    "
    - "L2.1432[172979,173956] 276ns 100mb                                                                             |L2.1432|    "
    - "L2.1433[173957,173999] 276ns 5mb                                                                              |L2.1433|   "
    - "L2.1434[174000,174978] 276ns 100mb                                                                              |L2.1434|   "
    - "L2.1435[174979,175956] 276ns 100mb                                                                              |L2.1435|   "
    - "L2.1436[175957,175999] 276ns 5mb                                                                               |L2.1436|  "
    - "L2.1437[176000,176979] 281ns 100mb                                                                               |L2.1437|  "
    - "L2.1438[176980,177958] 281ns 100mb                                                                               |L2.1438|  "
    - "L2.1439[177959,177999] 281ns 4mb                                                                                |L2.1439| "
    - "L2.1440[178000,178980] 281ns 100mb                                                                                |L2.1440| "
    - "L2.1441[178981,179960] 281ns 100mb                                                                                |L2.1441| "
    - "L2.1442[179961,179999] 281ns 4mb                                                                                |L2.1442| "
    - "L2.1443[180000,180980] 281ns 100mb                                                                                 |L2.1443|"
    - "L2.1444[180981,181960] 281ns 100mb                                                                                 |L2.1444|"
    - "L2.1445[181961,181999] 281ns 4mb                                                                                 |L2.1445|"
    - "L2.1446[182000,182981] 285ns 100mb                                                                                 |L2.1446|"
    - "L2.1447[182982,183962] 285ns 100mb                                                                                  |L2.1447|"
    - "L2.1448[183963,183999] 285ns 4mb                                                                                  |L2.1448|"
    - "L2.1449[184000,184980] 285ns 100mb                                                                                  |L2.1449|"
    - "L2.1450[184981,185960] 285ns 100mb                                                                                   |L2.1450|"
    - "L2.1451[185961,185999] 285ns 4mb                                                                                   |L2.1451|"
    - "L2.1452[186000,186981] 291ns 100mb                                                                                   |L2.1452|"
    - "L2.1453[186982,187962] 291ns 100mb                                                                                    |L2.1453|"
    - "L2.1454[187963,187999] 291ns 4mb                                                                                    |L2.1454|"
    - "L2.1455[188000,188981] 291ns 100mb                                                                                    |L2.1455|"
    - "L2.1456[188982,189962] 291ns 100mb                                                                                     |L2.1456|"
    - "L2.1457[189963,189999] 291ns 4mb                                                                                     |L2.1457|"
    - "L2.1458[190000,190980] 295ns 100mb                                                                                     |L2.1458|"
    - "L2.1459[190981,191960] 295ns 100mb                                                                                     |L2.1459|"
    - "L2.1460[191961,191999] 295ns 4mb                                                                                      |L2.1460|"
    - "L2.1461[192000,192981] 295ns 100mb                                                                                      |L2.1461|"
    - "L2.1462[192982,193962] 295ns 100mb                                                                                      |L2.1462|"
    - "L2.1463[193963,193999] 295ns 4mb                                                                                       |L2.1463|"
    - "L2.1464[194000,194980] 295ns 100mb                                                                                       |L2.1464|"
    - "L2.1465[194981,195960] 295ns 100mb                                                                                       |L2.1465|"
    - "L2.1466[195961,195999] 295ns 4mb                                                                                        |L2.1466|"
    - "L2.1467[196000,196981] 299ns 100mb                                                                                        |L2.1467|"
    - "L2.1468[196982,197962] 299ns 100mb                                                                                        |L2.1468|"
    - "L2.1470[197963,198943] 299ns 100mb                                                                                         |L2.1470|"
    - "L2.1471[198944,199923] 299ns 100mb                                                                                         |L2.1471|"
    - "L2.1472[199924,200000] 299ns 8mb                                                                                         |L2.1472|"
    "###
    );
}
