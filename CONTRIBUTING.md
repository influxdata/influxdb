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

[issue]: https://github.com/influxdata/influxdb_iox/issues/new
[New Issue]: https://github.com/influxdata/influxdb_iox/issues/new
[How to Report Bugs Effectively]: https://www.chiark.greenend.org.uk/~sgtatham/bugs.html

## Contributing Changes

InfluxDB IOx is written mostly in idiomatic Rust—please see the [Style Guide] for more details.
All code must adhere to the `rustfmt` format, and pass all of the `clippy` checks we run in CI (there are more details further down this README).

[Style Guide]: docs/style_guide.md

### Finding Issues To Work On

The [good first issue](https://github.com/influxdata/influxdb_iox/labels/good%20first%20issue) and the [help wanted](https://github.com/influxdata/influxdb_iox/labels/help%20wanted) labels are used to identify issues where we encourage community contributions.
They both indicate issues for which we would welcome independent community contributions, but the former indicates a sub-set of these that are especially good for first-time contributors.
If you want some clarifications or guidance for working on one of these issues, or you simply want to let others know that you're working on one, please leave a comment on the ticket.

[good first issue]: https://github.com/influxdata/influxdb_iox/labels/good%20first%20issue
[help wanted]: https://github.com/influxdata/influxdb_iox/labels/help%20wanted

### Bigger Changes

If you're planning to submit significant changes, even if it relates to existing tickets **please** talk to the project maintainers first!
The easiest way to do this is to open up a new ticket, describing the changes you plan to make and *why* you plan to make them. Changes that may seem obviously good to you, are not always obvious to everyone else.
Example of changes where we would encourage up-front communication:

* new IOx features;
* significant refactors that move code between modules/crates etc;
* performance improvements involving new concurrency patterns or the use of `unsafe` code;
* API-breaking changes, or changes that require a data migration;
* any changes that risk the durability or correctness of data.

We are always excited to have community involvement but we can't accept everything.
To avoid having your hard work rejected the best approach to start a discussion first.
Further, please don't expect us to accept significant changes without new test coverage, and/or in the case of performance changes benchmarks that show the improvements.

### Making a PR

To open a PR you will need to have a Github account.
Fork the `influxdb_iox` repo and work on a branch on your fork.
When you have completed your changes, or you want some incremental feedback make a Pull Request to InfluxDB IOx [here].

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

[here]: https://github.com/influxdata/influxdb_iox/compare
[Conventional Commits]: https://www.conventionalcommits.org/en/v1.0.0/

## Running Tests

The `cargo` build tool runs tests as well. Run:

```shell
cargo test --workspace
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

[`rustfmt`]: https://github.com/rust-lang/rustfmt
[`clippy`]: https://github.com/rust-lang/rust-clippy
