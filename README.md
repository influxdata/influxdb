# InfluxData Platform [![CircleCI](https://circleci.com/gh/influxdata/platform.svg?style=svg)](https://circleci.com/gh/influxdata/platform)

This is the [monorepo](https://danluu.com/monorepo/) for InfluxData Platform, a.k.a. Influx 2.0 OSS.

## Vendoring

This project is using [`dep`](https://golang.github.io/dep/docs/introduction.html) for vendoring.

Use `dep ensure -vendor-only` when you only need to populate the `vendor` directory to run `go build` successfully,
or run `dep ensure` to both populate the `vendor` directory and update `Gopkg.lock` with any newer constraints.

## Introducing Flux 

We recently announced Flux, the MIT-licensed data scripting language (and rename for IFQL). The source for Flux is [in this repository under `query`](query#flux---influx-data-language). Learn more about Flux from [CTO Paul Dix's presentation](https://speakerdeck.com/pauldix/flux-number-fluxlang-a-new-time-series-data-scripting-language).
