# InfluxData Platform

This is the [monorepo](https://danluu.com/monorepo/) for InfluxData Platform, a.k.a. Influx 2.0 OSS.

## Vendoring

This project is using [`dep`](https://golang.github.io/dep/docs/introduction.html) for vendoring.

Use `dep ensure -vendor-only` when you only need to populate the `vendor` directory to run `go build` successfully,
or run `dep ensure` to both populate the `vendor` directory and update `Gopkg.lock` with any newer constraints.
