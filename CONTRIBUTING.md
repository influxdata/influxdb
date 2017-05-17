# Contributing to InfluxDB

Welcome prospective Influx contributor!

The following guide will help you get started contributing to InfluxDB, to make this process as smooth as possible, we've prepared this short guide for you. It's split it into three sections for your convenience:

* [Getting started](#getting-started) contains instructions for setting up your development environment and links to any pre-required reading material.
* [Guidelines](#guidelines) outlines steps for submitting bugs, feature requests and pull-request.
* [Advanced Topics](#advanced-topics) for experienced contributors and developers, information about building packages etc.
* [Assistance](#assistance) have further questions or need help? Checkout this section.

## Getting Started

Before contributing to InfluxDB please take a second to sign our CLA, which can be found [on our website](https://influxdata.com/community/cla/).

### Prerequisite Knowledge

InfluxDB is written in the [Go](https://golang.org/) programming language, also known as Golang.

Before proceeding to work with the codebase it's strongly advised to make sure you're familiar with Go and have a pretty solid understanding Golang paradigms. A great place to get started with Go is the [official docs homepage](https://golang.org/doc/)

We use Git source control system, so having some basic Git skills is important, see the official getting started guide [here](https://git-scm.com/book/en/v1/Getting-Started).

### Setting up your environment

#### Installing Go

Currently the recommended version of Go used for building InfluxDB is `1.8.1`

Instructions for installing Go can be found in the official [installation guide](https://golang.org/doc/install); However the InfluxDB team likes to ensure all developers run the same version of Go. To help manage different versions of Go on any workspace, we recommend the use of a Go version manager such [gvm](https://github.com/moovweb/gvm).

If you're using `gvm` simply run the following commands to run the correct version:

```bash
gvm install go1.8.1
gvm use go1.8.1 --default
```

#### Version Control Systems

Go has the ability to fetch remote packages via revision control systems using the `go get` tool.  Before using `go get`,be sure to install the following revision control software packages on your system:

* [Install Git](http://git-scm.com/book/en/Getting-Started-Installing-Git)
* [Install Mercurial](http://mercurial.selenic.com/wiki/Download)

On Mac or Linux these packages can be installed using package managers such as [brew](http://linuxbrew.sh/), [apt](https://wiki.debian.org/Apt), [pacman](https://wiki.archlinux.org/index.php/Pacman) etc,

#### GNU Make

[GNU Make](https://www.gnu.org/software/make) is used for automating the installation of tooling, building packages and running tests, it's commonly used in conjunction with Golang projects  for these tasks.

Nearly all (semi-)compliant POSIX operating systems (Mac, Linux) ship with `make` installed out of the box.

#### Getting the source

The best way to get the source is using `go get`, see the [go get docs](https://golang.org/cmd/go/) to learn more about this process:

```bash
go get github.com/influxdata/influxdb
```

#### Working with a Fork

To make code contributions you will need to work on your own copy of the codebase and send us a pull request. This is typically done by working off a local copy of the project called a fork.

Create a [fork](https://help.github.com/articles/fork-a-repo/) of the official [repository](https://github.com/influxdata/influxdb) and check it out locally using `go get`:

```bash
git clone git@github.com:<github username>/influxdb
cd $GOPATH/src/<github username>/influxdb
```

### Test and Build

You should now have he correct version of Go installed and the InfluxDB source code available locally, all that's left to do is run unit tests and build.

Let's run out first build using `make`, if you you're interested in understanding the commands used to produce a build, take a look inside the `Makefile`:

```bash
cd $GOPATH/src/github.com/influxdata/influxdb
make
```

At a high level, the `make` build performed the following steps:

1. Installed all required Go tooling to produce a build.
2. Checkouts and packages used by InfluxDB at the appropriate version using [gdm](https://github.com/sparrc/gdm).
3. Runs the `go clean ./...` and `go build ./...` commands to produce all binaries, finally the `go install ./...` command install the InfluxDB binaries to `$GOPATH/bin` on your filesytsem.

_Note: The actual InfluxDB binary is named `influxd`, not `influxdb`.`_

## Guidelines

### Bug Reports

Before you file an issue, please search existing issues in case it has already been filed, or perhaps even fixed. If you proceed to create an issue on Github, you will presented with an issue template and we will ask questions like:

* Full details of your operating system (or distribution) e.g. 64-bit Ubuntu 14.04.
* The version of InfluxDB you are running
* Whether you installed it using a pre-built package, or built it from source.
* A small test case, if applicable, that demonstrates the issues.

Remember the golden rule of bug reports: **The easier you make it for us to reproduce the problem, the faster it will get fixed.**

If you have never written a bug report before, or if you want to brush up on your bug reporting skills, we recommend reading [Simon Tatham's essay "How to Report Bugs Effectively."](http://www.chiark.greenend.org.uk/~sgtatham/bugs.html)

Please note that issues are *not the place to file general questions* such as "How do I use collectd with InfluxDB?" please ask questions of this nature on the [InfluxData Community](https://community.influxdata.com/) site after reading through the official [docs](https://docs.influxdata.com/influxdb/v1.2/)

### Feature requests

We really like to receive feature requests, as it helps us prioritize our work and build our users a better product. Please be clear about your requirements, as incomplete feature requests may simply be closed if we don't understand what you would like to see added to InfluxDB.

### Submitting a pull request

To submit a pull request you should fork the InfluxDB repository, make any changes required then, commit and push your changes to a feature branch of your fork. Generate a pull request from your branch against *master* of the InfluxDB repository.

Include in your pull request and commit message the details of your change, we're after the not just the how, but the why.  Just like the issues, when a pull request is created, you will be presented with a template to fill out, please take the time to fill this out carefully as it will help us merge you work faster.

Please be sure to run the test suite with your change in place by running `make test`. Changes that cause tests to fail cannot be merged.ion out carefully to have your code merged faster.

## Advanced Topics

### Generated Google Protobuf code

Most changes to the source do not require that the generated protocol buffer code be changed. But if you need to modify the protocol buffer code, you'll first need to install the protocol buffers toolchain.

First install the [protocol buffer compiler](https://developers.google.com/protocol-buffers/
) 2.6.1 or later for your OS:

Then install the go plugins:

```bash
go get github.com/gogo/protobuf/proto
go get github.com/gogo/protobuf/protoc-gen-gogo
go get github.com/gogo/protobuf/gogoproto
```

Finally run, `go generate` after updating any `*.proto` file:

```bash
go generate ./...
```

#### Troubleshooting Protobuf

If generating the protobuf code is failing for you, check each of the following:

* Ensure the protobuf library can be found. Make sure that `LD_LIBRRARY_PATH` includes the directory in which the library `libprotoc.so` has been installed.
* Ensure the command `protoc-gen-gogo`, found in `GOPATH/bin`, is on your path. This can be done by adding `GOPATH/bin` to `PATH`.

### Generated Go Templates

The query engine requires optimized data structures for each data type so
instead of writing each implementation several times we use templates. _Do not
change code that ends in a `.gen.go` extension!_ Instead you must edit the
`.gen.go.tmpl` file that was used to generate it.

Once you've edited the template file, you'll need the [`tmpl`][tmpl] utility to generate the code:

```bash
$ go get github.com/benbjohnson/tmpl
```

Then you can regenerate all templates in the project:

```bash
    $ go generate ./...
```

[tmpl]: https://github.com/benbjohnson/tmpl

### Pre-commit checks / hooks

We have a pre-commit hook to make sure code is formatted properly and vetted before you commit any changes. We strongly recommend using the pre-commit hook to guard against accidentally committing unformatted code. To use the pre-commit hook, run the following:

```bash
cd $GOPATH/src/github.com/influxdata/influxdb
make tools
cp .hooks/pre-commit .git/hooks/
```

In case the commit is rejected because it's not formatted correctly you can run the following to format the code:

```bash
go fmt ./...
go vet ./...
```

To run [go vet](https://golang.org/cmd/vet/), run the following command:

```bash
    go vet ./...
```

### Profiling

When troubleshooting problems with CPU or memory the Go toolchain can be helpful. You can start InfluxDB with CPU and memory profiling turned on. For example:

```bash
# start influx with profiling
./influxd -cpuprofile influxdcpu.prof -memprof influxdmem.prof
# run queries, writes, whatever you're testing
# Quit out of influxd and influxd.prof will then be written.
# open up pprof to examine the profiling data.
go tool pprof ./influxd influxd.prof
# once inside run "web", opens up browser with the CPU graph
# can also run "web <function name>" to zoom in. Or "list <function name>" to see specific lines
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

### Continuous Integration testing

InfluxDB uses CircleCI for continuous integration testing. To see how the code is built and tested, check out [this file](https://github.com/influxdata/influxdb/blob/master/circle-test.sh). It closely follows the build and test process outlined above. You can see the exact version of Go InfluxDB uses for testing by consulting that file.

### Building specific versions

To set the version and commit flags during the build process one can set the following options **install** command:

```bash
LD_FLAGS="-X main.version=$VERSION -X main.branch=$BRANCH -X main.commit=$COMMIT" make
```

Where `$VERSION` is the version, `$BRANCH` is the branch, and `$COMMIT` is the git commit hash.

If you would like to build packages, see `build.py` usage information:

```bash
python build.py --help
```

## Assistance

If you have any questions about contributing, this guide, experience errors during building of packages or need further assistance, the best place to ask for help is the [InfluxData Community Site](https://community.influxdata.com/).
