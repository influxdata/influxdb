# InfluxData Platform [![CircleCI](https://circleci.com/gh/influxdata/platform.svg?style=svg)](https://circleci.com/gh/influxdata/platform)

This is the [monorepo](https://danluu.com/monorepo/) for InfluxData Platform, a.k.a. Influx 2.0 OSS.

## Installation

This project requires Go 1.11 and Go module support. Set `GO111MODULE=on` or build the project outside of your `GOPATH` for it to succeed.

For information about modules, please refer to the [wiki](https://github.com/golang/go/wiki/Modules).

## Introducing Flux

We recently announced Flux, the MIT-licensed data scripting language (and rename for IFQL). The source for Flux is [in this repository under `query`](query#flux---influx-data-language). Learn more about Flux from [CTO Paul Dix's presentation](https://speakerdeck.com/pauldix/flux-number-fluxlang-a-new-time-series-data-scripting-language).
