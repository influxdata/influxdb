# Release instructions

## Step 1: Update `README.md` file

Update the `README.md` file (copies the rustdoc comments) using the [`cargo rmde`](https://crates.io/crates/cargo-rdme) tool (install via `cargo install cargo-rdme`):

```shell
cargo rdme
```

## Step 2: Update versions
Update the version in Cargo.toml, like:

```diff
--- a/influxdb_line_protocol/Cargo.toml
+++ b/influxdb_line_protocol/Cargo.toml
@@ -1,6 +1,6 @@
 [package]
 name = "influxdb_line_protocol"
-version = "1.0.0"
+version = "2.0.0"
 authors = ["InfluxDB IOx Project Developers"]
 edition = "2021"
 license = "MIT OR Apache-2.0"
```

## Step 3: Make a PR and merge

## Step 4: Publish to crates.io

Test it out with a dry run first:
```shell
cargo publish --dry-run
```

Publish to crates.io:
```shell
cargo publish
```
