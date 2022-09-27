# Contributing to InfluxDB v2

## How to report a bug

Before you report an issue, please [search existing issues](https://github.com/influxdata/influxdb/issues) to check whether it's
already been reported, or perhaps even fixed.
If you choose to report an issue, please include the following in your report:

- Full details of your operating system (or distribution)--for example, `64bit Ubuntu 18.04`.
  To get your operating system details, run the following command in your terminal
  and copy-paste the output into your report:

  ```sh
  uname -srm
  ```
- How you installed InfluxDB. Did you use a pre-built package or did you build from source?
- The version of InfluxDB you're running.
  If you installed InfluxDB using a pre-built package, run the following command in your terminal and then copy-paste the output into your report:

  ```sh
  influxd version
  ```

  If you built and ran `influxd` from source, run the following command from your *influxdb* directory and then copy-paste the output into your report:

  ```sh
  bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influxd version
  ```
- [Clear steps to reproduce the issue](#how-to-provide-steps-to-reproduce-an-issue)

### How to provide steps for reproducing an issue

The easier we can reproduce the problem, the easier we can fix it.
To learn how to write an effective bug report, we recommend reading [Simon Tatham's essay, "How to Report Bugs Effectively."](http://www.chiark.greenend.org.uk/~sgtatham/bugs.html).

When describing how to reproduce the issue,
please provide test cases in the form of `curl` commands--for example:

```bash
# write data
curl -XPOST "http://localhost:8086/api/v2/write?org=YOUR_ORG&bucket=YOUR_BUCKET&precision=s" \
  --header "Authorization: Token YOURAUTHTOKEN" \
  --data-raw "mem,host=host1 used_percent=23.43234543 1556896326"

# query data
# Bug: expected it to return no data, but data comes back.
curl http://localhost:8086/api/v2/query?org=my-org -XPOST -sS \
  -H 'Authorization: Token YOURAUTHTOKEN' \
  -H 'Accept: application/csv' \
  -H 'Content-type: application/vnd.flux' \
  -d 'from(bucket:"example-bucket")
    |> range(start:-1000h)
    |> group(columns:["_measurement"], mode:"by")
    |> sum()'
```

Test cases with `influx` CLI commands are also helpful--for example:

```
# write data
influx write -o YOUR_ORG -b YOUR_BUCKET -p s -t YOURAUTHTOKEN \
  "mem,host=host1 used_percent=23.43234543 1556896326"

# query data
# Bug: expected it to return no data, but data comes back.
influx query -o YOUR_ORG -t YOURAUTHTOKEN 'from(bucket:"example-bucket")
  |> range(start:-1000h)
  |> group(columns:["_measurement"], mode:"by")
  |> sum()'
```

If you don't provide clear test cases like the examples above, then investigating your issue will be very difficult for us.
If you have trouble including data in your report, please zip up your data directory and include a link to it in your bug report.

Note that issues are _not the place to file general support requests_ such as "How do I use `collectd` with InfluxDB?"
Please submit requests for help to the [InfluxData Community](https://community.influxdata.com/) - don't report them as issues in the repo.

## How to request a feature

We encourage you to submit feature requests as they help us prioritize our work.

In your feature request, please include the following:
- Clear requirements and goals.
- What you would like to see added to InfluxDB.
- Examples.
- Why the feature is important to you.

If you find your request already exists in a Github issue,
please indicate your support for the existing issue by using the "thumbs up" reaction.

## How to submit a pull (change) request

To submit a change for code or documentation in this repository, please [create a pull request](https://github.com/influxdata/influxdb/compare) and follow the instructions in the pull request template to help us review your PR.
After you complete the template steps and submit the PR, expect some deliberation as we review and finalize the change.
Once your PR is approved, you can merge it.

## How to report security vulnerabilities

InfluxData takes security and our users' trust very seriously.
If you believe you have found a security issue in any of our open source projects, please responsibly disclose it by contacting security@influxdata.com.
More details about security vulnerability reporting, including our GPG key, [can be found here](https://www.influxdata.com/how-to-report-security-vulnerabilities/).

## Signing the CLA

Before you contribute to InfluxDB, please sign our [Individual Contributor License Agreement (CLA)](https://influxdata.com/community/cla/).

## How to build InfluxDB from source

### Install Go

InfluxDB requires Go 1.18.

At InfluxData we find `gvm`, a Go version manager, useful for installing Go.
For instructions on how to install it see [the gvm page on github](https://github.com/moovweb/gvm).

After installing `gvm` you can install and set the default Go version by running the following:

```bash
$ gvm install go1.18
$ gvm use go1.18 --default
```

InfluxDB requires Go module support. Set `GO111MODULE=on` or build the project outside of your `GOPATH` for it to succeed. For information about modules, please refer to the [wiki](https://github.com/golang/go/wiki/Modules).

### Install revision control systems

Go has the ability to import remote packages via revision control systems with the `go get` command.
To ensure that you can retrieve any remote package, install `git` and `bzr` revision control software, following the instructions for your system:

- [Install Git](http://git-scm.com/book/en/Getting-Started-Installing-Git)
- [Install Bazaar](http://doc.bazaar.canonical.com/latest/en/user-guide/installing_bazaar.html)

### Install additional dependencies

In addition to `go`, `git`, and `bzr`, you will need the following prerequisites
installed on your system:

- Rust (a recent stable version, 1.60 or higher).
  To install Rust, we recommend using [rustup](https://rustup.rs/).
- `clang`
- `make`
- `pkg-config`
- `protobuf`
- Go protobuf plugin. To use Go to install the plugin, enter the following command in your terminal:

  `go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28`

To install prerequisites, use the following example command for your system:

- OSX: `brew install pkg-config protobuf`
  - For OSX, you must have [HomeBrew](https://brew.sh) installed.
  - You will also need the [Developer Tools](https://webkit.org/build-tools/), which includes `make`.
- Linux (Arch): `pacman -S clang make pkgconf protobuf`
- Linux (Ubuntu): `sudo apt install make clang pkg-config protobuf-compiler libprotobuf-dev build-essential`
- Linux (RHEL): see the [RedHat-specific instructions](#redhat-specific-instructions).

#### RedHat-specific instructions

For RedHat, you must enable the [EPEL](https://fedoraproject.org/wiki/EPEL)

### Build influxd with make

`influxd` is the InfluxDB service.

For `influx`, the InfluxDB CLI tool, see the [influx-cli repository on Github](https://github.com/influxdata/influx-cli).

Once you've installed the dependencies,
follow these steps to build `influxd` from source and start the service:

1. Clone this repo (influxdb).
2. In your influxdb directory, run `make` to generate the influxd binary:

   ```sh
   make
   ```

   If successful,  `make` installs the binary to a platform-specific path for your system.
   The output is the following:

   ```sh
   env GO111MODULE=on go build -tags 'assets ' -o bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influxd ./cmd/influxd
   ```

3. To start the `influxd` service that runs InfluxDB, enter the following command
   to run the platform-specific binary:

   ```
   bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influxd
   ```

   `influxd` logs to `stdout` by default.

**Troubleshooting**

- If you've changed Go or Rust versions and have trouble building, try running `go clean -r -x -cache -testcache -modcache ./` to clear out old build artifacts that may be incompatible.

### Run tests

This project is built from various languages.
To run tests for all languages and components, enter the following command in
your terminal:

```bash
make test
```

To run tests for only Go and Rust components, enter the following command in your terminal:

```bash
make test-go
```

## Generated Google Protobuf code

Most changes to the source don't require changes to the generated protocol buffer code.
If you need to modify the protocol buffer code, you'll first need to install the protocol buffers toolchain.

First install the [protocol buffer compiler](https://developers.google.com/protocol-buffers/) 3.17.3 or later for your OS.

Then run `go generate` after updating any `*.proto` file:

```bash
go generate ./...
```

**How to troubleshoot protobuf**

If generating the protobuf code is failing for you, check each of the following:

- Ensure the protobuf library can be found. Make sure that `LD_LIBRARY_PATH` includes the directory in which the library `libprotoc.so` has been installed.
- Ensure the command `protoc-gen-go`, found in `GOPATH/bin`, is on your path. This can be done by adding `GOPATH/bin` to `PATH`.

## Generated Go Templates

The query engine requires optimized data structures for each data type so instead of writing each implementation several times we use templates.
_Do not change code that ends in a `.gen.go` extension!_
Instead you must edit the `.gen.go.tmpl` file that was used to generate it.

Once you've edited the template file, you'll need the [`tmpl`][tmpl] utility to generate the code:

```sh
$ go get github.com/benbjohnson/tmpl
```

Then you can regenerate all templates in the project:

```sh
$ go generate ./...
```

[tmpl]: https://github.com/benbjohnson/tmpl

## Profiling

When troubleshooting problems with CPU or memory the Go toolchain can be helpful.
You can start InfluxDB with CPU and memory profiling turned on.
For example:

```bash
# start influx with profiling

$ ./influxd -cpuprofile influxdcpu.prof -memprof influxdmem.prof

# run queries, writes, whatever you're testing
# Quit out of influxd and influxd.prof will then be written.
# open up pprof to examine the profiling data.

$ go tool pprof ./influxd influxd.prof

# once inside run "web", opens up browser with the CPU graph
# can also run "web <function name>" to zoom in. Or "list <function name>" to see specific lines
```

Note that when you pass the binary to `go tool pprof` _you must specify the path to the binary_.

If you are profiling benchmarks built with the `testing` package, you may wish
to use the [`github.com/pkg/profile`](github.com/pkg/profile) package to limit
the code being profiled:

```go
func BenchmarkSomething(b *testing.B) {
  // do something intensive like fill database with data...
  defer profile.Start(profile.ProfilePath("/tmp"), profile.MemProfile).Stop()
  // do something that you want to profile...
}
```
