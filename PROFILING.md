# Profiling `influxdb3`

This document explains several profiling strategies.

## Preparation

### Choosing a profile

You will need to choose a profile to build and run the `influxdb3` binary. Available profiles are 
configured in [Cargo.toml] and are listed here:

- `release`: this is the profile used for release builds, and will produce a fully-optimized
production quality release binary. The compile time for this profile can be quite long, so for
rapid iteration, this profile is not recommended.
- `quick-release`: this is a modified version of `release` intended to produce a close-to-production
binary, but with a faster build time. So, it is more suitable to rapid iterations and testing out
changes.
- `bench`: this is the same as `release`, but will compile the binary with `debuginfo` turned on
for more readable symbols in generated profiles.
- `quick-bench`: a modified version of `quick-release` that compiles the binary with `debuginfo`
turned on.
- `dev`: this profile produces an unoptimized binary with `debuginfo` turned on.

If you are getting started, we recommend using either the `quick-release` or `quick-bench` profiles 
to get up and running faster, then once you are ready, and need to get as much performance as 
possible out of the binary, use `release` or `bench`.

### Building and running the binary

Once you have chosen a profile, you can build the `influxdb3` binary using cargo:

```
cargo build -p influxdb3 --profile <profile>
```

This will build the binary and place it in the `target` folder in your working directory, e.g.,
`target/<profile>/influxdb3`.

You can then prfile the `influxdb3` server, or whichever command, using the fully-qualified path
and the profiling tools of your choice. For example,

```
<path_to_working_directory>/target/<profile>/influxdb3 serve
```

## Profiling Tools

### macOS

#### Instruments: CPU/Performance Profiling

Instruments is a versatile profiling tool packaged with XCode on macOS and comes with several built-in
profiling tools that are useful with `influxdb3`:

- Time Profiler (sample-based CPU profiler)
- CPU Profiler (cycle-based CPU profiler)
- Filesystem Activity (file system and disk I/O activity)
- System Call Trace (system calls and CPU scheduling)

#### Instruments: Allocations (macOS Only)

The allocations instrument is a powerful tool for tracking heap allocations on macOS and recording call stacks.

It can be used with Rust and `influxdb3`, but requires some additional steps on aarch64 and later versions of macOS
due to increased security.

##### Preparing binary

You must compile `influxdb3` with `--no-default-features` to ensure the default system allocator is
used. Following the compilation step,
[you must codesign the binary](https://developer.apple.com/forums/thread/685964?answerId=683365022#683365022)
with the `get-task-allow` entitlement set to `true`. Without the codesign step, the Allocations instrument will fail to
start with an error similar to the following:

> Required Kernel Recording Resources Are in Use

First, generate a temporary entitlements plist file, named `tmp.entitlements`:

```sh
/usr/libexec/PlistBuddy -c "Add :com.apple.security.get-task-allow bool true" tmp.entitlements
```

Then codesign the file with the `tmp.entitlements` file:

```sh
codesign -s - --entitlements tmp.entitlements -f target/release/influxdb3
```

You can verify the file is correctly code-signed as follows:

```sh
codesign --display --entitlements - target/release/influxdb3
```
```
Executable=<path_to_working_dir>/target/release/influxdb3
[Dict]
	[Key] com.apple.security.get-task-allow
	[Value]
		[Bool] true
```

or the running `influxdb3` process using its PID:

```sh
codesign --display --entitlements - +<PID>
```

