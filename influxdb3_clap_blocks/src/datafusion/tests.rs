use clap::Parser;
use iox_query::{config::IoxConfigExt, exec::Executor};

use super::IoxQueryDatafusionConfig;

#[test_log::test]
fn max_parquet_fanout() {
    let datafusion_config =
        IoxQueryDatafusionConfig::parse_from(["", "--datafusion-max-parquet-fanout", "5"]).build();
    let exec = Executor::new_testing();
    let mut session_config = exec.new_session_config();
    for (k, v) in &datafusion_config {
        session_config = session_config.with_config_option(k, v);
    }
    let ctx = session_config.build();
    let inner_ctx = ctx.inner().state();
    let config = inner_ctx.config();
    let iox_config_ext = config.options().extensions.get::<IoxConfigExt>().unwrap();
    assert_eq!(5, iox_config_ext.max_parquet_fanout);
}
