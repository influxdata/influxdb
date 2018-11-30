# InfluxData Platform [![CircleCI](https://circleci.com/gh/influxdata/platform.svg?style=svg)](https://circleci.com/gh/influxdata/platform)

This is the [monorepo](https://danluu.com/monorepo/) for InfluxData Platform, a.k.a. Influx 2.0 OSS.

## Installation

This project requires Go 1.11 and Go module support. Set `GO111MODULE=on` or build the project outside of your `GOPATH` for it to succeed.

For information about modules, please refer to the [wiki](https://github.com/golang/go/wiki/Modules).

## Introducing Flux

We recently announced Flux, the MIT-licensed data scripting language (and rename for IFQL). The source for Flux is [in this repository under `query`](query#flux---influx-data-language). Learn more about Flux from [CTO Paul Dix's presentation](https://speakerdeck.com/pauldix/flux-number-fluxlang-a-new-time-series-data-scripting-language).

## CI and Static Analysis

### CI

All pull requests will run through CI, which is currently hosted by Circle.
Community contributors should be able to see the outcome of this process by looking at the checks on their PR.
Please fix any issues to ensure a prompt review from members of the team.

The Platform project is used internally in a number of proprietary InfluxData products, and as such, PRs and changes need to be tested internally.
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

