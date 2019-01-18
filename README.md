# InfluxDB [![CircleCI](https://circleci.com/gh/influxdata/influxdb.svg?style=svg)](https://circleci.com/gh/influxdata/influxdb)

This is the for InfluxDB 2.0 OSS, which includes parts that used to be known as Chronograf and Kapacitor.

If you are looking for the [InfluxDB 1.x Go Client, we've created a new repo](https://github.com/influxdata/influxdb1-client) for that.

## Installation

This project requires Go 1.11 and Go module support.

Set `GO111MODULE=on` or build the project outside of your `GOPATH` for it to succeed.

If you are getting an `error loading module requirements` error with `bzr executable file not found in $PATH”` on `make`, `brew install bazaar` (on macOS) before continuing. This error will also be returned if you have not installed `npm`.  On macOS, `brew install npm` will install `npm`.

For information about modules, please refer to the [wiki](https://github.com/golang/go/wiki/Modules).

## Basic Usage

A successful `make` run results in two binaries, with platform-dependent paths:
```
$ make
...
env GO111MODULE=on go build -tags 'assets ' -o bin/darwin/influx ./cmd/influx
env GO111MODULE=on go build -tags 'assets ' -o bin/darwin/influxd ./cmd/influxd
```

`influxd` is the InfluxDB service. `influx` is the CLI management tool.


Start the service. Logs to stdout by default:
```
$ bin/darwin/influxd
```

To write and read fancy timeseries data, you'll need to first create a user, credentials, organization and bucket.
Use the subcommands `influx user`, `influx auth`, `influx org` and `influx bucket`, or do it all in one breath with `influx setup`:
```
$ bin/darwin/influx setup
Welcome to InfluxDB 2.0!
Please type your primary username: user

Please type your password: s3^R#tpA$s

Please type your password again: s3^R#tpA$s

Please type your primary organization name.: my-org

Please type your primary bucket name.: my-bucket

Please type your retention period in hours (exp 168 for 1 week).
Or press ENTER for infinite.: 72


You have entered:
  Username:          user
  Organization:      my-org
  Bucket:            my-bucket
  Retention Period:  72 hrs
Confirm? (y/n): y

UserID                  Username        Organization    Bucket
033a3f2c5ccaa000        user            my-org          my-bucket
Your token has been stored in /Users/you/.influxdbv2/credentials
```

`~/.influxdbv2/credentials` contains your auth token.
Most `influx` commands read the token from this file path by default.

You may need the organization ID and bucket ID later:
```
$ influx org find
ID                      Name
033a3f2c708aa000        my-org
```

```
$ influx bucket find
ID                      Name            Retention       Organization    OrganizationID
033a3f2c710aa000        my-bucket       72h0m0s         my-org          033a3f2c708aa000
```

Write to measurement `m`, with tag `v=2`, in bucket `my-bucket`, which belongs to organization `my-org`:
```
$ bin/darwin/influx write --org my-org --bucket my-bucket --precision s "m v=2 $(date +%s)"
```

Write the same point using `curl`:
```
curl --header "Authorization: Token $(cat ~/.influxdbv2/credentials)" --data-raw "m v=2 $(date +%s)" "http://localhost:9999/api/v2/write?org=033a3f2c708aa000&bucket=033a3f2c710aa000&precision=s"
```

Read that back with a simple Flux query (currently, the `query` subcommand does not have a `--org` flag):
```
$ bin/darwin/influx query --org-id 033a3f2c708aa000 'from(bucket:"my-bucket") |> range(start:-1h)'
Result: _result
Table: keys: [_start, _stop, _field, _measurement]
                   _start:time                      _stop:time           _field:string     _measurement:string                      _time:time                  _value:float
------------------------------  ------------------------------  ----------------------  ----------------------  ------------------------------  ----------------------------
2019-01-10T19:24:06.806244000Z  2019-01-10T20:24:06.806244000Z                       v                       m  2019-01-10T20:04:09.000000000Z                             2
```

Use the fancy REPL:
```
$ bin/darwin/influx repl --org my-org
> from(bucket:"my-bucket") |> range(start:-1h)
Result: _result
Table: keys: [_start, _stop, _field, _measurement]
                   _start:time                      _stop:time           _field:string     _measurement:string                      _time:time                  _value:float
------------------------------  ------------------------------  ----------------------  ----------------------  ------------------------------  ----------------------------
2019-01-10T19:36:23.361220000Z  2019-01-10T20:36:23.361220000Z                       v                       m  2019-01-10T20:04:09.000000000Z                             2
>
```

## Introducing Flux

We recently announced Flux, the MIT-licensed data scripting language (and rename for IFQL). The source for Flux is [available on GitHub](https://github.com/influxdata/flux). Learn more about Flux from [CTO Paul Dix's presentation](https://speakerdeck.com/pauldix/flux-number-fluxlang-a-new-time-series-data-scripting-language).

## CI and Static Analysis

### CI

All pull requests will run through CI, which is currently hosted by Circle.
Community contributors should be able to see the outcome of this process by looking at the checks on their PR.
Please fix any issues to ensure a prompt review from members of the team.

The InfluxDB project is used internally in a number of proprietary InfluxData products, and as such, PRs and changes need to be tested internally.
This can take some time, and is not really visible to community contributors.

### Static Analysis

This project uses the following static analysis tools. Failure during the running of any of these tools results in a failed build.
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

| File  | Comment  |
|:-:|:-:|
| query/promql/promql.go  | //lint:file-ignore SA6001 Ignore all unused code, it's generated  |
| proto/bin_gen.go  | //lint:file-ignore ST1005 Ignore error strings should not be capitalized |

