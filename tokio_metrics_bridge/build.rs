fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tokio_unstable = std::env::var("CARGO_CFG_TOKIO_UNSTABLE").is_ok();
    if !tokio_unstable {
        return Err("\
RUSTFLAGS got overwritten -- potentially by your environment -- and does not \
contain `--cfg tokio_unstable` anymore. Don't do that. If you want to adjust \
build configs, either edit the repo-level `Cargo.toml` or the repo-level \
`.cargo/config`. Ensure that it contains `--cfg tokio_unstable`."
            .to_owned()
            .into());
    }

    Ok(())
}
