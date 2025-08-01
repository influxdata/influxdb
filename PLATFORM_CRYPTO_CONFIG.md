# Platform-Specific Crypto Configuration

This document explains how to configure different cryptographic backends for different platforms in InfluxDB3.

## Background

The project uses `rustls` for TLS support, which can use different crypto backends:
- **aws-lc-rs** (default in rustls 0.23+): AWS's cryptographic library, optimized for performance on Linux
- **ring**: A pure Rust crypto library that works well across all platforms

## Current Configuration

### Linux
- Uses aws-lc-rs (default) for better performance
- No special configuration needed

### Windows/macOS
- Should use ring to avoid potential build issues with aws-lc-sys
- Requires applying patches to force ring usage

## How to Apply Platform-Specific Configuration

### Method 1: CircleCI Builds (Automated)

The CircleCI configuration automatically applies the correct patches based on the target platform:

1. The `.circleci/scripts/apply-crypto-patch.sh` script is run before each build
2. For non-Linux targets, it uncomments the rustls patch lines in `Cargo.toml`
3. This forces rustls to use ring instead of aws-lc-rs

### Method 2: Local Development

For local development on Windows or macOS:

1. **Edit Cargo.toml**: Uncomment the platform-specific crypto patches:
   ```toml
   [patch.crates-io]
   rustls = { version = "0.23", default-features = false, features = ["ring", "logging", "std", "tls12"] }
   rustls-webpki = { version = "0.102", default-features = false, features = ["ring", "std"] }
   ```

2. **Use the apply script**:
   ```bash
   .circleci/scripts/apply-crypto-patch.sh x86_64-apple-darwin
   ```

3. **Build as normal**:
   ```bash
   cargo build
   ```

### Method 3: Environment Variables

You can also use environment variables (though this requires additional build.rs support):

```bash
# For Windows/macOS
export INFLUXDB_CRYPTO_BACKEND=ring
cargo build
```

## Configuration Files

- **Cargo.toml**: Contains commented-out patches for ring crypto
- **.cargo/config.toml**: Sets platform-specific rustflags
- **.circleci/scripts/apply-crypto-patch.sh**: Applies patches based on target
- **.circleci/scripts/setup-crypto-env.sh**: Sets up environment variables (alternative approach)

## Troubleshooting

If you encounter build issues related to aws-lc-sys on non-Linux platforms:

1. Ensure the ring patches are uncommented in Cargo.toml
2. Run `cargo clean` to clear any cached dependencies
3. Check that you have the required build tools for your platform

## Future Improvements

Consider adding a build.rs script that automatically selects the appropriate crypto backend based on the target platform, eliminating the need for manual patches.