use clap::Parser;
use datafusion::config::ExtensionOptions;
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

#[test_log::test]
fn udf_http_permissions() {
    let datafusion_config = IoxQueryDatafusionConfig::parse_from([
        "",
        "--datafusion-config",
        "iox.udfs_http_permissions.host.[api.example.com].allow_subnets:203.0.113.0/24,\
         iox.udfs_http_permissions.host.[api.example.com].port.443.methods:GET|POST",
    ])
    .build();
    let exec = Executor::new_testing();
    let mut session_config = exec.new_session_config();
    for (key, value) in &datafusion_config {
        session_config = session_config.with_config_option(key, value);
    }
    let ctx = session_config.build();
    let inner_ctx = ctx.inner().state();
    let config = inner_ctx.config();
    let iox_config_ext = config.options().extensions.get::<IoxConfigExt>().unwrap();
    let entries = iox_config_ext.entries();

    assert_eq!(
        entries
            .iter()
            .find(|entry| {
                entry.key == "udfs_http_permissions.host.[api.example.com].allow_subnets"
            })
            .and_then(|entry| entry.value.as_deref()),
        Some("203.0.113.0/24")
    );
    assert_eq!(
        entries
            .iter()
            .find(|entry| {
                entry.key == "udfs_http_permissions.host.[api.example.com].port.443.methods"
            })
            .and_then(|entry| entry.value.as_deref()),
        Some("GET|POST")
    );
}
