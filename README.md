# InfluxDB [![CircleCI](https://circleci.com/gh/influxdata/influxdb.svg?style=svg)](https://circleci.com/gh/influxdata/influxdb)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://www.influxdata.com/slack)


InfluxDB is an open source time series platform. This includes APIs for storing and querying data, processing it in the background for ETL or monitoring and alerting purposes, user dashboards, and visualizing and exploring the data and more. The master branch on this repo now represents the latest InfluxDB, which now includes functionality for Kapacitor (background processing) and Chronograf (the UI) all in a single binary.

The list of InfluxDB Client Libraries that are compatible with the latest version can be found in [our documentation](https://v2.docs.influxdata.com/v2.0/reference/api/client-libraries/).

If you are looking for the 1.x line of releases, there are branches for each minor version as well as a `master-1.x` branch that will contain the code for the next 1.x release. The master-1.x [working branch is here](https://github.com/influxdata/influxdb/tree/master-1.x). The [InfluxDB 1.x Go Client can be found here](https://github.com/influxdata/influxdb1-client).

## State of the Project

The latest InfluxDB 1.x is the stable release and recommended for production use. The InfluxDB that is on the master branch is currently in the beta stage. This means that it is still **NOT** recommended for production usage. There may be breaking API changes, breaking changes in the [Flux language](https://github.com/influxdata/flux), changes in the underlying storage format that will require you to delete all your data, and significant changes to the UI. The beta is intended for feature exploration and gathering feedback on the available feature set. It **SHOULD NOT** be used for performance testing, benchmarks, or other stress tests.

Additional features will arrive during the beta period until we reach general availability (GA). We will be cutting versioned releases at least every two weeks starting in the first release. There will also be nightly builds based off the latest code in master.

Once we close on the final feature set of what will be in the first GA release of InfluxDB in the 2.x line, we will move into the release candidate (RC) phase. At that point, we do not expect there to be breaking changes to the API or Flux language. We may still need to make a breaking change prior to GA due to some unforseen circumstance, but it would need to be extremely important and will be clearly communicated via the changelog and all available channels.

Our current plans are to release RCs suitable for production usage, but we will re-evaluate in consultation with the community as the cycle progresses. During the RC period, we will focus on feedback from users, bug fixes, performance, and additive features (where time permits).

### What you can expect in the Beta and RC Phases

#### Beta
**Releases every two weeks or as needed**

Planned additions include:
- Compatibility layer with 1.x including: 1.x HTTP Write API  and HTTP Read API support for InfluxQL
- Import Bulk Data from 1.x - convert TSM from 1.x to 2.x
- Performance tuning, stability improvements, and fine tuning based on community feedback.
- Finalization of supported client libraries starting with JavaScript and Go.

#### RC
**As needed**

Planned activities include:
- Performance tuning, stability improvements, and fine-tuning based on community feedback.

### What is **NOT** planned?
- Migration of users/security permissions from InfluxDB v1.x to 2.x.  ACTION REQUIRED: Re-establish users and permissions within the new unified security model which now spans the underlying database and user interface.
- Migration of Continuous Queries.  ACTION REQUIRED: These will need to be re-implemented as Flux tasks.
- Direct support by InfluxDB for CollectD, StatsD, Graphite, or UDP.  ACTION REQUIRED: Leverage Telegraf 1.9+ along with the InfluxDB v2.0 output plugin to translate these protocols/formats.

## Installing from Source

We have nightly and weekly versioned Docker images, Debian packages, RPM packages, and tarballs of InfluxDB available at the [InfluxData downloads page](https://portal.influxdata.com/downloads/).

## Building From Source

This project requires Go 1.13 and Go module support.

Set `GO111MODULE=on` or build the project outside of your `GOPATH` for it to succeed.

The project also requires a recent stable version of Rust. We recommend using [rustup](https://rustup.rs/) to install Rust.

If you are getting an `error loading module requirements` error with `bzr executable file not found in $PATH”` on `make`, then you need to ensure you have `bazaar`, `protobuf`, and `yarn` installed.

- OSX: `brew install bazaar protobuf yarn`
- Linux (Arch): `pacman -S bzr protobuf yarn`
- Linux (Ubuntu): `apt install bzr protobuf-compiler yarnpkg`

**NB:** For RedHat, there are some extra steps:

1. You must enable the [EPEL](https://fedoraproject.org/wiki/EPEL)
2. You must add the `yarn` [repository](https://yarnpkg.com/lang/en/docs/install/#centos-stable)

For information about modules, please refer to the [wiki](https://github.com/golang/go/wiki/Modules).

A successful `make` run results in two binaries, with platform-dependent paths:

```
$ make
...
env GO111MODULE=on go build -tags 'assets ' -o bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx ./cmd/influx
env GO111MODULE=on go build -tags 'assets ' -o bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influxd ./cmd/influxd
```

`influxd` is the InfluxDB service.
`influx` is the CLI management tool.

Start the service.
Logs to stdout by default:

```
$ bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influxd
```

### Building with the go command

The `Makefile` provides a wrapper around configuring the utilities for building influxdb. For those wanting to use the `go` command directly, one of two things can be done.

First, the `env` script is located in the root of the repository. This script can be used to execute `go` commands with the appropriate environment configuration.

```bash
$ ./env go build ./cmd/influxd
```

Another method is to configure the `pkg-config` utility. Follow the instructions [here](https://github.com/influxdata/flux#getting-started) to install and configure `pkg-config` and then the normal go commands will work.

The first step is to install the `pkg-config` command.

```bash
# On Debian/Ubuntu
$ sudo apt-get install -y clang pkg-config
# On Mac OS X with Homebrew
$ brew install pkg-config
```

Install the `pkg-config` wrapper utility of the same name to a different path that is earlier in the PATH.

```bash
# Install the pkg-config wrapper utility
$ go build -o ~/go/bin/ github.com/influxdata/pkg-config
# Ensure the GOBIN directory is on your PATH
$ export PATH=$HOME/go/bin:${PATH}
$ which -a pkg-config
/home/user/go/bin/pkg-config
/usr/bin/pkg-config
```

Then all `go` build commands should work.

```bash
$ go build ./cmd/influxd
$ go test ./...
```

## Getting Started

For a complete getting started guide, please see our full [online documentation site](https://v2.docs.influxdata.com/v2.0/). 

To write and query data or use the API in any way, you'll need to first create a user, credentials, organization and bucket.
Everything in InfluxDB is organized under a concept of an organization. The API is designed to be multi-tenant.
Buckets represent where you store time series data.
They're synonymous with what was previously in InfluxDB 1.x a database and retention policy.

The simplest way to get set up is to point your browser to [http://localhost:9999](http://localhost:9999) and go through the prompts.

**Note**: Port 9999 will be used during the beta phases of development of InfluxDB v2.0.
This should allow a v2.0-beta instance to be run alongside a v1.x instance without interfering on port 8086.
InfluxDB will thereafter continue to use 8086.

You can also get set up from the CLI using the subcommands `influx user`, `influx auth`, `influx org` and `influx bucket`,
or do it all in one breath with `influx setup`:


```
$ bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx setup
Welcome to InfluxDB 2.0!
Please type your primary username: marty

Please type your password: 

Please type your password again: 

Please type your primary organization name.: InfluxData

Please type your primary bucket name.: telegraf

Please type your retention period in hours.
Or press ENTER for infinite.: 72


You have entered:
  Username:          marty
  Organization:      InfluxData
  Bucket:            telegraf
  Retention Period:  72 hrs
Confirm? (y/n): y

UserID                  Username        Organization    Bucket
033a3f2c5ccaa000        marty           InfluxData      Telegraf
Your token has been stored in /Users/marty/.influxdbv2/credentials
```

You may get into a development loop where `influx setup` becomes tedious.
Some added flags can help:
```
$ bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx setup --username marty --password F1uxKapacit0r85 --org InfluxData --bucket telegraf --retention 168 --token where-were-going-we-dont-need-roads --force
```

`~/.influxdbv2/credentials` contains your auth token.
Most `influx` commands read the token from this file path by default.

You may need the organization ID and bucket ID later:

```
$ influx org find
ID                      Name
033a3f2c708aa000        InfluxData
```

```
$ influx bucket find
ID                      Name            Retention       Organization    OrganizationID
033a3f2c710aa000        telegraf        72h0m0s         InfluxData      033a3f2c708aa000
```

Write to measurement `m`, with tag `v=2`, in bucket `telegraf`, which belongs to organization `InfluxData`:

```
$ bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx write --org InfluxData --bucket telegraf --precision s "m v=2 $(date +%s)"
```

Write the same point using `curl`:

```
curl --header "Authorization: Token $(cat ~/.influxdbv2/credentials)" --data-raw "m v=2 $(date +%s)" "http://localhost:9999/api/v2/write?org=InfluxData&bucket=telegraf&precision=s"
```

Read that back with a simple Flux query:

```
$ bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx query -o InfluxData 'from(bucket:"telegraf") |> range(start:-1h)'
Result: _result
Table: keys: [_start, _stop, _field, _measurement]
                   _start:time                      _stop:time           _field:string     _measurement:string                      _time:time                  _value:float
------------------------------  ------------------------------  ----------------------  ----------------------  ------------------------------  ----------------------------
2019-12-30T22:19:39.043918000Z  2019-12-30T23:19:39.043918000Z                       v                       m  2019-12-30T23:17:02.000000000Z                             2
```

Use the fancy REPL:

```
$ bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx repl -o InfluxData
> from(bucket:"telegraf") |> range(start:-1h)
Result: _result
Table: keys: [_start, _stop, _field, _measurement]
                   _start:time                      _stop:time           _field:string     _measurement:string                      _time:time                  _value:float
------------------------------  ------------------------------  ----------------------  ----------------------  ------------------------------  ----------------------------
2019-12-30T22:22:44.776351000Z  2019-12-30T23:22:44.776351000Z                       v                       m  2019-12-30T23:17:02.000000000Z                             2
>
```


## Introducing Flux

Flux is an MIT-licensed data scripting language (previously named IFQL) used for querying time series data from InfluxDB. The source for Flux is [available on GitHub](https://github.com/influxdata/flux). Learn more about Flux from [CTO Paul Dix's presentation](https://speakerdeck.com/pauldix/flux-number-fluxlang-a-new-time-series-data-scripting-language).

## Contributing to the Project

InfluxDB is an [MIT licensed](LICENSE) open source project and we love our community. The fastest way to get something fixed is to open a PR. Check out our [contributing](CONTRIBUTING.md) guide if you're interested in helping out. Also, join us on our [Community Slack Workspace](https://influxdata.com/slack) if you have questions or comments for our engineering teams.

## CI and Static Analysis

### CI

All pull requests will run through CI, which is currently hosted by Circle.
Community contributors should be able to see the outcome of this process by looking at the checks on their PR.
Please fix any issues to ensure a prompt review from members of the team.

The InfluxDB project is used internally in a number of proprietary InfluxData products, and as such, PRs and changes need to be tested internally.
This can take some time, and is not really visible to community contributors.

### Static Analysis

This project uses the following static analysis tools.
Failure during the running of any of these tools results in a failed build.
Generally, code must be adjusted to satisfy these tools, though there are exceptions.

- [go vet](https://golang.org/cmd/vet/) checks for Go code that should be considered incorrect.
- [go fmt](https://golang.org/cmd/gofmt/) checks that Go code is correctly formatted.
- [go mod tidy](https://tip.golang.org/cmd/go/#hdr-Add_missing_and_remove_unused_modules) ensures that the source code and go.mod agree.
- [staticcheck](http://next.staticcheck.io/docs/) checks for things like: unused code, code that can be simplified, code that is incorrect and code that will have performance issues.

### staticcheck

If your PR fails `staticcheck` it is easy to dig into why it failed, and also to fix the problem.
First, take a look at the error message in Circle under the `staticcheck` build section, e.g.,

```
tsdb/tsm1/encoding.gen.go:1445:24: func BooleanValues.assertOrdered is unused (U1000)
tsdb/tsm1/encoding.go:172:7: receiver name should not be an underscore, omit the name if it is unused (ST1006)
```

Next, go and take a [look here](http://next.staticcheck.io/docs/checks) for some clarification on the error code that you have received, e.g., `U1000`.
The docs will tell you what's wrong, and often what you need to do to fix the issue.

#### Generated Code

Sometimes generated code will contain unused code or occasionally that will fail a different check.
`staticcheck` allows for [entire files](http://next.staticcheck.io/docs/#ignoring-problems) to be ignored, though it's not ideal.
A linter directive, in the form of a comment, must be placed within the generated file.
This is problematic because it will be erased if the file is re-generated.
Until a better solution comes about, below is the list of generated files that need an ignores comment.
If you re-generate a file and find that `staticcheck` has failed, please see this list below for what you need to put back:

|          File          |                             Comment                              |
| :--------------------: | :--------------------------------------------------------------: |
| query/promql/promql.go | //lint:file-ignore SA6001 Ignore all unused code, it's generated |

#### End-to-End Tests

CI also runs end-to-end tests. These test the integration between the influx server the ui. You can run them locally in two steps:

- Start the server in "testing mode" by running `make run-e2e`.
- Run the tests with `make e2e`.
nothing
