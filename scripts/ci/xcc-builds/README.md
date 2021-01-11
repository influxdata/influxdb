# Cross-compiler Builds
The scripts in this directory are used to build cross-compilers for InfluxDB from source.
We build & cache these artifacts separately to speed up cross-builds in CI.

## Building archives
The build scripts are stand-alone, any required variables are defined as constants within
their shell code. Running a script will produce a new `.tar.gz` archive under `out/` in this directory.
Archives are named after the version(s) of the software they contain + a build timestamp.

## Uploading builds
After building a new archive, follow these steps to add it to our CI image:
1. Log into the Errplane AWS console. Credentials are hosted in 1Password, within the `Engineering` vault.
2. Navigate to [dl.influxdata.com/influxdb-ci](https://s3.console.aws.amazon.com/s3/buckets/dl.influxdata.com?region=us-east-1&prefix=influxdb-ci/)
   in the S3 console.
3. Navigate to the appropriate sub-directory of `influxdb-ci` for the archive you're uploading. The path varies by cross-compiler.
   * Native AMD64 `musl-gcc` is hosted under `musl/<musl-version>/`
   * Cross-compilers for `musl-gcc` (i.e. ARM64) are hosted under `musl/<musl-version>/musl-cross/<musl-cross-make-version>/`
   * Cross-compilers for macOS `clang` are hosted under `osxcross/<osxcross-hash>/`
4. Use the S3 console to upload the `.tar.gz` archive into the directory.
5. Update our CircleCI config to point at the new archive.
