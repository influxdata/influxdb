# InfluxDB
<div align="center">
  <img  src="assets/influxdb-logo.png" width="600" alt="InfluxDB Logo">
</div>

<p align="center">
  <a href="https://circleci.com/gh/influxdata/influxdb">
  <img alt="CircleCI" src="https://circleci.com/gh/influxdata/influxdb.svg?style=svg" />
  </a>
  
  <a href="https://www.influxdata.com/slack">
  <img alt="Slack Status" src="https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social" />
  </a>
  
  <a href="https://docs.influxdata.com/influxdb/v2.4/install/?t=Docker">
  <img alt="Docker Pulls" src="https://img.shields.io/docker/pulls/_/influxdb" />
  </a>
  
  <a href="https://github.com/influxdata/influxdb/blob/master/LICENSE">
  <img alt="Docker Pulls" src="https://img.shields.io/github/license/influxdata/influxdb" />
  </a>
</p>
<h3 align="center">
    <b><a href="https://www.influxdata.com/">Website</a></b>
    •
    <a href="https://docs.influxdata.com/">Documentation</a>
    •
    <a href="https://university.influxdata.com/">InfluxDB University</a>
    •
    <a href="https://www.influxdata.com/blog/">Blog</a>
</h3>

---

InfluxDB is an open source time series platform. This includes APIs for storing and querying data, processing it in the background for ETL or monitoring and alerting purposes, user dashboards, and visualizing and exploring the data and more. The master branch on this repo now represents the latest InfluxDB, which now includes functionality for Kapacitor (background processing) and Chronograf (the UI) all in a single binary.

The list of InfluxDB Client Libraries that are compatible with the latest version can be found in [our documentation](https://docs.influxdata.com/influxdb/latest/tools/client-libraries/).

If you are looking for the 1.x line of releases, there are branches for each minor version as well as a `master-1.x` branch that will contain the code for the next 1.x release. The master-1.x [working branch is here](https://github.com/influxdata/influxdb/tree/master-1.x). The [InfluxDB 1.x Go Client can be found here](https://github.com/influxdata/influxdb1-client).

| Try **InfluxDB Cloud** for free and get started fast with no local setup required. Click [**here**](https://cloud2.influxdata.com/signup) to start building your application on InfluxDB Cloud. |
|:------|

## Install

We have nightly and versioned Docker images, Debian packages, RPM packages, and tarballs of InfluxDB available at the [InfluxData downloads page](https://portal.influxdata.com/downloads/). We also provide the `influx` command line interface (CLI) client as a separate binary available at the same location.

If you are interested in building from source, see the [building from source](CONTRIBUTING.md#building-from-source) guide for contributors.

<a href="https://university.influxdata.com/catalog/">
  <img src="assets/influxdbU-banner.png" width="600"/>
</a>

## Get Started

For a complete getting started guide, please see our full [online documentation site](https://docs.influxdata.com/influxdb/latest/).

To write and query data or use the API in any way, you'll need to first create a user, credentials, organization and bucket.
Everything in InfluxDB is organized under a concept of an organization. The API is designed to be multi-tenant.
Buckets represent where you store time series data.
They're synonymous with what was previously in InfluxDB 1.x a database and retention policy.

The simplest way to get set up is to point your browser to [http://localhost:8086](http://localhost:8086) and go through the prompts.

You can also get set up from the CLI using the command `influx setup`:


```bash
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

You can run this command non-interactively using the `-f, --force` flag if you are automating the setup.
Some added flags can help:
```bash
$ bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx setup \
--username marty \
--password F1uxKapacit0r85 \
--org InfluxData \
--bucket telegraf \
--retention 168 \
--token where-were-going-we-dont-need-roads \
--force
```

Once setup is complete, a configuration profile is created to allow you to interact with your local InfluxDB without passing in credentials each time. You can list and manage those profiles using the `influx config` command.
```bash
$ bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx config
Active	Name	URL			            Org
*	    default	http://localhost:8086	InfluxData
```

## Write Data
Write to measurement `m`, with tag `v=2`, in bucket `telegraf`, which belongs to organization `InfluxData`:

```bash
$ bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx write --bucket telegraf --precision s "m v=2 $(date +%s)"
```

Since you have a default profile set up, you can omit the Organization and Token from the command.

Write the same point using `curl`:

```bash
curl --header "Authorization: Token $(bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx auth list --json | jq -r '.[0].token')" \
--data-raw "m v=2 $(date +%s)" \
"http://localhost:8086/api/v2/write?org=InfluxData&bucket=telegraf&precision=s"
```

Read that back with a simple Flux query:

```bash
$ bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influx query 'from(bucket:"telegraf") |> range(start:-1h)'
Result: _result
Table: keys: [_start, _stop, _field, _measurement]
                   _start:time                      _stop:time           _field:string     _measurement:string                      _time:time                  _value:float
------------------------------  ------------------------------  ----------------------  ----------------------  ------------------------------  ----------------------------
2019-12-30T22:19:39.043918000Z  2019-12-30T23:19:39.043918000Z                       v                       m  2019-12-30T23:17:02.000000000Z                             2
```

Use the `-r, --raw` option to return the raw flux response from the query. This is useful for moving data from one instance to another as the `influx write` command can accept the Flux response using the `--format csv` option.

## Script with Flux

Flux (previously named IFQL) is an open source functional data scripting language designed for querying, analyzing, and acting on data. Flux supports multiple data source types, including:

- Time series databases (such as InfluxDB)
- Relational SQL databases (such as MySQL and PostgreSQL)
- CSV

The source for Flux is [available on GitHub](https://github.com/influxdata/flux).
To learn more about Flux, see the latest [InfluxData Flux documentation](https://docs.influxdata.com/flux/) and [CTO Paul Dix's presentation](https://speakerdeck.com/pauldix/flux-number-fluxlang-a-new-time-series-data-scripting-language).

## Contribute to the Project

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
- [staticcheck](https://staticcheck.io/docs/) checks for things like: unused code, code that can be simplified, code that is incorrect and code that will have performance issues.

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

CI also runs end-to-end tests. These test the integration between the `influxd` server the UI.
Since the UI is used by interal repositories as well as the `influxdb` repository, the
end-to-end tests cannot be run on forked pull requests or run locally. The extent of end-to-end
testing required for forked pull requests will be determined as part of the review process.

## Additional Resources
- [InfluxDB Tips and Tutorials](https://www.influxdata.com/blog/category/tech/influxdb/)
- [InfluxDB Essentials Course](https://university.influxdata.com/courses/influxdb-essentials-tutorial/)
- [Exploring InfluxDB Cloud Course](https://university.influxdata.com/courses/exploring-influxdb-cloud-tutorial/)