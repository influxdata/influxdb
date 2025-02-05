# Contributing

Thank you for thinking of contributing! We very much welcome contributions from the community.
To make the process easier and more valuable for everyone involved we have a few rules and guidelines to follow.

Anyone with a Github account is free to file issues on the project.
However, if you want to contribute documentation or code then you will need to sign InfluxData's Individual Contributor License Agreement (CLA), which can be found with more information [on our website].

[on our website]: https://www.influxdata.com/legal/cla/

## Submitting Issues and Feature Requests

Before you file an [issue], please search existing issues in case the same or similar issues have already been filed.
If you find an existing open ticket covering your issue then please avoid adding "👍" or "me too" comments; Github notifications can cause a lot of noise for the project maintainers who triage the back-log.
However, if you have a new piece of information for an existing ticket and you think it may help the investigation or resolution, then please do add it as a comment!
You can signal to the team that you're experiencing an existing issue with one of Github's emoji reactions (these are a good way to add "weight" to an issue from a prioritisation perspective).

### Submitting an Issue

The [New Issue] page has templates for both bug reports and feature requests.
Please fill one of them out!
The issue templates provide details on what information we will find useful to help us fix an issue.
In short though, the more information you can provide us about your environment and what behaviour you're seeing, the easier we can fix the issue.
If you can push a PR with test cases that trigger a defect or bug, even better!
P.S, if you have never written a bug report before, or if you want to brush up on your bug reporting skills, we recommend reading Simon Tatham's essay [How to Report Bugs Effectively].

As well as bug reports we also welcome feature requests (there is a dedicated issue template for these).
Typically, the maintainers will periodically review community feature requests and make decisions about if we want to add them.
For features we don't plan to support we will close the feature request ticket (so, again, please check closed tickets for feature requests before submitting them).

[issue]: https://github.com/influxdata/influxdb/issues/new
[New Issue]: https://github.com/influxdata/influxdb/issues/new
[How to Report Bugs Effectively]: https://www.chiark.greenend.org.uk/~sgtatham/bugs.html

## Contributing Changes

InfluxDB 3 is written mostly in idiomatic Rust. Please refer to the [Rust API Guidelines][rust-api-guide] for more details.
All code must adhere to the `rustfmt` format, and pass all of the `clippy` checks we run in CI (there are more details further down this README).

[rust-api-guide]: https://rust-lang.github.io/api-guidelines/about.html

### Finding Issues To Work On

The [good first issue](https://github.com/influxdata/influxdb/issues?q=is%3Aopen+is%3Aissue+label%3Agood-first-issue) and the [help wanted](https://github.com/influxdata/influxdb/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted) labels are used to identify issues where we encourage community contributions.
They both indicate issues for which we would welcome independent community contributions, but the former indicates a sub-set of these that are especially good for first-time contributors.
If you want some clarifications or guidance for working on one of these issues, or you simply want to let others know that you're working on one, please leave a comment on the ticket.

[good first issue]: https://github.com/influxdata/influxdb/issues?q=is%3Aopen+is%3Aissue+label%3Agood-first-issue
[help wanted]: https://github.com/influxdata/influxdb/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted

### Bigger Changes

If you're planning to submit significant changes, even if it relates to existing tickets **please** talk to the project maintainers first!
The easiest way to do this is to open up a new ticket, describing the changes you plan to make and *why* you plan to make them. Changes that may seem obviously good to you, are not always obvious to everyone else.
Example of changes where we would encourage up-front communication:

* new InfluxDB 3 features;
* significant refactors that move code between modules/crates etc;
* performance improvements involving new concurrency patterns or the use of `unsafe` code;
* API-breaking changes, or changes that require a data migration;
* any changes that risk the durability or correctness of data.

We are always excited to have community involvement but we can't accept everything.
To avoid having your hard work rejected the best approach to start a discussion first.
Further, please don't expect us to accept significant changes without new test coverage, and/or in the case of performance changes benchmarks that show the improvements.

### Making a PR

To open a PR you will need to have a Github account.
Fork the `influxdb` repo and work on a branch on your fork.
When you have completed your changes, or you want some incremental feedback make a Pull Request to InfluxDB [here].

If you want to discuss some work in progress then please prefix `[WIP]` to the
PR title.

For PRs that you consider ready for review, verify the following locally before you submit it:

* you have a coherent set of logical commits, with messages conforming to the [Conventional Commits] specification;
* all the tests and/or benchmarks pass, including documentation tests;
* the code is correctly formatted and all `clippy` checks pass; and
* you haven't left any "code cruft" (commented out code blocks etc).

There are some tips on verifying the above in the [next section](#running-tests).

**After** submitting a PR, you should:

* verify that all CI status checks pass and the PR is 💚;
* ask for help on the PR if any of the status checks are 🔴, and you don't know why;
* wait patiently for one of the team to review your PR, which could take a few days.

[here]: https://github.com/influxdata/influxdb/compare
[Conventional Commits]: https://www.conventionalcommits.org/en/v1.0.0/

## Running Tests

Testing `influxdb3` requires the use of [`cargo-nextest`](https://nexte.st/) which can be installed
via:
```
cargo install cargo-nextest --locked
```

You can then run all tests in the workspace:

```shell
cargo nextest run --workspace
```

### Enabling logging in tests

To enable logging to stdout/stderr during a run of `cargo nextest` set the Rust `TEST_LOG`, `RUST_LOG`,
and `RUST_LOG_SPAN_EVENTS` environment variables. For example, to see all `INFO` messages:

```shell
TEST_LOG= RUST_LOG=info RUST_LOG_SPAN_EVENTS=full cargo nextest run --workspace --nocapture
```

`TEST_LOG` will emit logs from the running test server (see [End-to-End Tests](#end-to-end-tests)).

Since this feature uses [`EnvFilter`][env-filter] internally, you can use all the features of that
crate. For example, to disable the (somewhat noisy) logs in some h2 modules, you can use a value of
`RUST_LOG` such as:

```shell
RUST_LOG=debug,hyper::proto::h1=info,h2=info RUST_LOG_SPAN_EVENTS=full cargo nextest run --workspace --nocapture
```

Many tests use the [`test-log`](https://crates.io/crates/test-log) crate to enable this logging
behaviour, which requires the use of the `RUST_LOG_SPAN_EVENTS` environment variable.

[env-filter]: https://docs.rs/tracing-subscriber/0.2.15/tracing_subscriber/filter/struct.EnvFilter.html

### End-to-End Tests

There are end-to-end tests that spin up a server and make requests via the client library and API.
They can be found in `influxdb3/tests/`

When running these tests, you can have the logs of the running server emitted to stdout by adding
the `TEST_LOG` environment variable, for example:

```shell
TEST_LOG= cargo nextest run -p influxdb3 --nocapture
```

## Running `rustfmt` and `clippy`

CI will check the code formatting with [`rustfmt`] and Rust best practices with [`clippy`].

To automatically format your code according to `rustfmt` style, first make sure `rustfmt` is installed using `rustup`:

```shell
rustup component add rustfmt
```

Then, whenever you make a change and want to reformat, run:

```shell
cargo fmt --all
```

Similarly with `clippy`, install with:

```shell
rustup component add clippy
```

And run with:

```shell
cargo clippy --all-targets --workspace -- -D warnings
```

## Multi-process load generating setup

This section outlines the usage of a workflow based on `just` and `pueue` that
enables streamlined setup for multi-process load generating scenarios.

It was initially intended for reproducing WAL snapshotting and compaction
scenarios under load. The defaults at the outset reflect this goal, but may be
made configurable over time [with `just` CLI
overrides](https://just.systems/man/en/setting-variables-from-the-command-line.html).

### Requirements

Using this requires the the following CLI tools:

* https://just.systems
* https://github.com/Nukesor/pueue

### Setup

With this tools installed you will need to run the `pueue` daemon:

```
pueued -d
```

This daemon is what actually manages process lifetimes via commands from the
`pueue` CLI.

#### Linux Optional Setup

For Linux users who are concerned about the impact of repeatedly writing and
deleting load generator to their development, on option is to set up a `tmpfs`
as follows:

```
sudo mkdir -p /mnt/test-data
sudo mount -o size=8G -t tmpfs none /mnt/test-data
export LOADTEST_DATA_DIR
```

The environment variable `LOADTEST_DATA_DIR` is used to set a global variable
that recipes use to build paths for storing component data directories and
process stdout/stderr.

### Usage

To see what `just` targets are available, you can run:

```
just list
```

Targets are grouped to make it easier to figure out which to use. The
`workloads` group is intended for the multi-process just targets.

Workloads can be initiated with the following command:

```
just compaction-workload <testid> <load-generator-worker-count>
```

This will initiate a set of background processes managed by `pueue` that look
like this:

```
$ pueue status

Group "default" (5 parallel): running
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
 Id   Status    Command                                                        Path                                                      Start      End
════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
 0    Running   just run-writer writer /mnt/test-data/<testid>         /home/wayne/projects/github.com/influxdata/influxdb_pro   18:32:51
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
 1    Running   just run-compactor compactor /mnt/test-data/<testid>   /home/wayne/projects/github.com/influxdata/influxdb_pro   18:32:51
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
 2    Running   just run-load-generator testdb <load-generator-worker-count>  /home/wayne/projects/github.com/influxdata/influxdb_pro   18:33:01
────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
```

All running processes can be killed with:

```
just kill <testid>
```

Data directory and log files can be archived with:

```
just archive <testid>
```

All process and data directories for a given test id can be cleaned up with:

```
just clean <testid>
```

[`rustfmt`]: https://github.com/rust-lang/rustfmt
[`clippy`]: https://github.com/rust-lang/rust-clippy
