# Platform-Specific Crypto Configuration

This document explains how to configure different cryptographic backends for different platforms in InfluxDB3.

## Background

The project uses `rustls` for TLS support, which can use different crypto backends:
- **aws-lc-rs** (default in rustls 0.23+): AWS's cryptographic library, optimized for performance on Linux
- **ring**: A pure Rust crypto library that works well across all platforms

## Current Configuration (Automatic)

The crypto backend is now automatically selected during the build process based on the target platform:

### Linux Targets
- Automatically uses aws-lc-rs for better performance
- Applies to any target triple containing "linux" (e.g., `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`)

### Windows/macOS/Other Targets
- Automatically uses ring for better compatibility
- Applies to all non-Linux targets (e.g., `x86_64-pc-windows-gnu`, `x86_64-apple-darwin`)

## How It Works

The `influxdb3_process/build.rs` script automatically detects the target platform during compilation and configures the appropriate crypto backend. This works correctly even when cross-compiling.

## Manual Override Options

While the build system automatically selects the appropriate backend, you can override this behavior:

### Method 1: Environment Variable

Set the `INFLUXDB_CRYPTO_BACKEND` environment variable before building:

```bash
# Force ring on any platform
export INFLUXDB_CRYPTO_BACKEND=ring
cargo build

# Force aws-lc-rs on any platform (requires aws-lc-sys to build)
export INFLUXDB_CRYPTO_BACKEND=aws-lc-rs
cargo build
```

### Method 2: Cargo.toml Patches (Manual)

For cases where you need to force specific rustls versions or features, you can still use the patch approach:

1. **Edit Cargo.toml**: Uncomment the platform-specific crypto patches:
   ```toml
   [patch.crates-io]
   rustls = { version = "0.23", default-features = false, features = ["ring", "logging", "std", "tls12"] }
   rustls-webpki = { version = "0.102", default-features = false, features = ["ring", "std"] }
   ```

2. **Build as normal**:
   ```bash
   cargo build
   ```

### Method 3: CircleCI Override

In CircleCI, you can override the default by setting the environment variable in the build step:

```yaml
- run:
    name: Configure crypto backend
    command: |
      # Force ring for all builds
      export INFLUXDB_CRYPTO_BACKEND=ring
```

## Configuration Files

- **influxdb3_process/build.rs**: Automatically detects target platform and configures crypto backend
- **Cargo.toml**: Contains optional patches for manual override
- **.cargo/config.toml**: Sets platform-specific rustflags
- **.circleci/config.yml**: Build configuration (crypto backend is now automatic)

## Build Output

During compilation, you'll see warnings indicating which crypto backend is being used:

```
warning: Building for target: x86_64-unknown-linux-gnu, using crypto backend: aws-lc-rs
```

Or when overriding:

```
warning: Using crypto backend from INFLUXDB_CRYPTO_BACKEND env: ring
warning: Building for target: x86_64-unknown-linux-gnu, using crypto backend: ring
```

## Troubleshooting

If you encounter build issues:

1. **aws-lc-sys build failures on non-Linux**: The build system should automatically use ring. If not, set `INFLUXDB_CRYPTO_BACKEND=ring`
2. **Force a specific backend**: Use the environment variable approach
3. **Clear build cache**: Run `cargo clean` if switching between backends
4. **Check build output**: Look for the warning messages to confirm which backend is being used

## Legacy Scripts

The following scripts are kept for reference but are no longer needed:
- `.circleci/scripts/apply-crypto-patch.sh`: Previously used to patch Cargo.toml
- `.circleci/scripts/setup-crypto-env.sh`: Alternative environment-based approach