# Contributing to InfluxDB v2

## Report a bug

Before you file an issue, please search existing issues to see if it's been filed (or fixed).
If you file an issue, please include the following:

- Full details of your operating system (or distribution) e.g. `64bit Ubuntu 18.04`.
- The version of InfluxDB you're running.
- Whether you installed InfluxDB from a pre-built package, or built it from source.
- Clear steps to reproduce the issue described, if possible.

The easier for us to reproduce the problem, the easier we can fix it.
If you've never written a bug report before, or want to brush up on your bug reporting skills,
we recommend reading [Simon Tatham's essay "How to Report Bugs Effectively."](http://www.chiark.greenend.org.uk/~sgtatham/bugs.html)

Please provide tests in the form of `curl` commands, as in the following example:

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

Test cases with `influx` commands are also helpful, as in the following example:

```bash
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

If you don't include a clear test case like the example above,
we won't be able to investigate your issue as quickly.
If writing the data is too difficult, please zip up your data directory and include a link to it in your bug report.

Please note that issues are *not* the place to file general support requests.
Please send support questions like *"How do I use collectd with InfluxDB?"* to the [InfluxData Community](https://community.influxdata.com/), not to Github issues.

## Request a feature

We welcome your feature requests as they help us prioritize our work.
When submitting feature requests, please keep the following in mind:

- Be clear about your requirements and goals.
- Provide examples of the enhancement that you would like to see and tell us why it's important to you.
- If you find your feature request already exists as a Github issue, please add a "thumbs up" reaction to indicate your support for that feature.

## Submit a pull request

To submit a pull request, follow these steps:

1. Fork the `influxdata/influxdb` repository.
2. Checkout a feature branch in your fork.
3. In your feature branch, make your change and run the test suite (given you won't be able to merge if tests fail).
4. Commit your change.
5. Generate a pull request (PR) from your branch against the **master** branch of `influxdata/influxdb`.
6. In your PR, provide details of your change--the why *and* the how--as well as the testing you performed.
7. Expect some deliberation as we finalize the change, but once that completes it may be merged.

To help our review, please add the following in your PR comment:

```md
- [ ] CHANGELOG.md updated
- [ ] Rebased/mergable
- [ ] Tests pass
- [ ] Sign [CLA](https://influxdata.com/community/cla/) (if not already signed)
```

## Report security vulnerabilities

InfluxDB takes security and our users' trust very seriously.
If you believe you have found a security issue in any of our open source projects, please responsibly disclose it by contacting security@influxdata.com.
For more information, see [how to report security vulnerabilities](https://www.influxdata.com/how-to-report-security-vulnerabilities/).

## Sign the CLA

Before you contribute to InfluxDB, take a second to sign our CLA, which can be found [on our website](https://influxdata.com/community/cla/).

## Build InfluxDB from Source

### Install Go

InfluxDB requires Go 1.18.

At InfluxData, we use the `gvm` Go version manager to install Go.
For installation instructions, see [the gvm page on github](https://github.com/moovweb/gvm).

With `gvm` installed, run the following commands to install the required go version and set it as the default.

```bash
gvm install go1.18
gvm use go1.18 --default
```

InfluxDB requires Go module support. Set `GO111MODULE=on` or build the project outside your `GOPATH` for it to succeed.
For more information about modules, see the [Go wiki](https://github.com/golang/go/wiki/Modules).

### Install revision control systems

With the `go get` command, you can import remote Go packages from revision control systems (RCS).
To install `git` and `bzr` RCS for InfluxDB, follow the instructions:

- [Install Git](http://git-scm.com/book/en/Getting-Started-Installing-Git)
- [Install Bazaar](http://doc.bazaar.canonical.com/latest/en/user-guide/installing_bazaar.html)

### Install additional dependencies

With the [revision control systems](#install-revision-control-systems) installed,
you're ready to install the remaining dependencies.

You need a recent stable version of Rust.
We recommend using [rustup](https://rustup.rs/) to install Rust.

You also need `clang`, `make`, `pkg-config`, and `protobuf` installed.

- macOS:
  1. Install [HomeBrew](https://brew.sh) (which provides `brew`) and [Developer Tools](https://webkit.org/build-tools/) (which provides `make`).
  2. Run `brew install pkg-config protobuf`
- Linux (Arch): `pacman -S clang make pkgconf protobuf`
- Linux (Ubuntu): `sudo apt install make clang pkg-config protobuf-compiler libprotobuf-dev`
- Linux (RHEL): See below

#### RedHat-specific instructions

For RedHat, you must enable the [EPEL](https://fedoraproject.org/wiki/EPEL)

### Build influxd with make

`influxd` is the InfluxDB service.

For `influx`, the InfluxDB CLI tool, see the [influx-cli repository on Github](https://github.com/influxdata/influx-cli).

Once you've installed the dependencies,
follow these steps to build `influxd` from source and start the service:

1. Clone this repo (influxdb).
2. In your influxdb directory, run `make` to encode dependencies.

   ```bash
   make
   ```

3. Run the following `go build` command to compile the influxd binary at a platform-dependent path.

   ```bash
   env GO111MODULE=on go build -tags 'assets ' -o bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influxd ./cmd/influxd
   ```

4. Start the `influxd` service.

   ```bash
   bin/$(uname -s | tr '[:upper:]' '[:lower:]')/influxd
   ```

   `influxd` logs to stdout by default.

### Run tests

This project is built from various languages.
To run tests for all languages and components, run the following:

```bash
make test
```

To run tests for only Go/Rust components, run the following:

```bash
make test-go
```

## Generated Google Protobuf code

Most changes to the source do not require that the generated protocol buffer code be changed.
But if you need to modify the protocol buffer code, you'll first need to install the protocol buffers toolchain.

First install the [protocol buffer compiler](https://developers.google.com/protocol-buffers/) 3.17.3 or later for your OS.

Then run `go generate` after updating any `*.proto` file:

```bash
go generate ./...
```

**Troubleshoot protobuf**

If generating the protobuf code is failing for you, check the following:

- Ensure Go can find the protobuf library on your system--check that `LD_LIBRARY_PATH` includes the directory where the `libprotoc.so` library is installed.
- Ensure the `protoc-gen-go` executable in `GOPATH/bin` is on your system path by adding `GOPATH/bin` to `PATH`.

## Generated Go Templates

The query engine requires optimized data structures for each data type so instead of writing each implementation several times, we use templates.
_Do not change code that ends in a `.gen.go` extension!_
Instead, edit the `.gen.go.tmpl` file that was used to generate it.

Once you've edited the template file, you'll need the [`tmpl`][tmpl] utility to generate the code:

```sh
go get github.com/benbjohnson/tmpl
```

Then you can regenerate all templates in the project:

```sh
go generate ./...
```

[tmpl]: https://github.com/benbjohnson/tmpl

## Profiling

Go profiling tools can help you troubleshoot problems with CPU or memory.
To start InfluxDB with CPU and memory profiling enabled, pass the profiling flags, as in the following example:

```bash
# Start influx with profiling.

$ ./influxd -cpuprofile influxdcpu.prof -memprof influxdmem.prof

# Run queries, writes, or whatever you're testing.
# Quit out of influxd and influxd.prof will then be written.
# Run pprof to examine the profiling data.

$ go tool pprof ./influxd influxd.prof

# At the prompt, run "web" to view the CPU graph in a browser.
# To zoom in, run "web <function name>".
# To see specific lines, run "list <function name>".
```

Note that when you pass the binary to `go tool pprof` *you must specify the path to the binary*.

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

To learn more about profiling and debugging in InfluxDB, see [InfluxDB OSS runtime](https://docs.influxdata.com/influxdb/latest/reference/internals/runtime/).
