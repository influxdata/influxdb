# Local Development and Writing Code for Influxdb

**Table of Contents**
* [Quickstart](#quickstart)
* [Full Length Guide](#full-length-guide)
* [Getting Some Graphs](#getting-some-graphs)
* [Getting Help](#getting-help)

## Quickstart

## Docker

We've provided `make` targets that provide an "interactive" development experience using Docker.


```console
make dshell
```

This command builds a development container image and puts you inside a container with all the tooling you require to develop and build InfluxDB.
You can use the "Local" instructions once inside this container and work on the premise that you have everything installed.

Other container runtimes should work, but we've only tested with Docker and Podman (`alias docker=podman`).

## Local

Assuming you have Go 1.13, Node LTS, and yarn installed, and some means of ingesting data locally (e.g. telegraf):

You'll need two terminal tabs to run influxdb from source: one to run the go application server, the other to run the development server that will listen for front-end changes, rebuild the bundle, serve the new bundle, then reload your webpage for you.

Tab 1:

```sh
go run ./cmd/influxd --assets-path=ui/build
```

Tab 2:

```sh
cd ui
yarn && yarn start
```

If there are no errors, hit [localhost:8080](http://localhost:8080) and follow the prompts to setup your username and password. *Note the port difference: `8080` vs the production `8086`*

You're set up to develop Influx locally. Any changes you make to front-end code under the `ui/` directory will be updated after the watcher process (that was initiated by running `yarn start`) sees them and rebuilds the bundle. Any changes to go code will need to be re-compiled by re-running the `go run` command above.

See [Getting some Graphs](#getting-some-graphs) for next steps.

## Full-Length Guide

To get started with Influx, you'll need to install these tools if you don't already have them:

1. [Install go](https://golang.org/doc/install)
1. [Install nodejs](https://nodejs.org/en/download/package-manager/)
1. [Install yarn](https://yarnpkg.com/lang/en/docs/install/)

Yarn is a package manager for nodejs and an alternative to npm.

To run Influx locally, you'll need two terminal tabs: one to run the go application server, the other to run the development server that will listen for front-end changes, rebuild the bundle, serve the new bundle, then reload your webpage for you.

Tab 1:

```sh
go run ./cmd/influxd --assets-path=ui/build
```

This starts the influxdb application server. It handles API requests and can be reached via `localhost:8086`. Any changes to go code will need to be re-compiled by re-running the `go run` command above.

Tab 2:

```sh
cd ui
yarn install
yarn start
```

This installs front-end dependencies and starts the front-end build server. It will listen to changes to TypeScript and JavaScript files, rebuild the front-end bundle, serve that bundle, then auto reload any pages with changes. If everything went smoothly without errors, you should be able to go to [localhost:8080.](http://localhost:8080) and follow the prompts to login or to setup your username and password.

If you're setting things up for the first time, be sure to check out the [the official getting started guide](https://v2.docs.influxdata.com/v2.0/get-started/) to get make sure you configure everything properly.

### Testing Changes

To make sure everything got wired up properly, we'll want to make a minor change on the frontend and see that it's added.

Add a newline and following log statement to the [entry point to the app:](https://github.com/influxdata/influxdb/blob/master/ui/src/index.tsx#L468)

```js
console.log('hello, world!')
```

Your browser should reload the page after you save your changes (sometimes this happens quickly and is hard to spot). Open your browser console and you should see your message after the page reloads.

## Getting some Graphs

If you haven't set up telegraf yet, [following the official telegraf documentation](https://v2.docs.influxdata.com/v2.0/write-data/no-code/use-telegraf/) is the quickest and most straightforward and hassle-free way of getting some data into your local instance. The documentation there will be kept fresher and and more current than this tutorial.

Learning how to input Line protocol data is a great tool if you need to debug with arbitrary adhoc data. Check out a quick intro to the [Line protocol](https://v2.docs.influxdata.com/v2.0/write-data/#what-you-ll-need), and learn how to [input it via the ui.](https://v2.docs.influxdata.com/v2.0/write-data/#user-interface) *Since we're running `influxd` locally, you can skip step 1.*

## Getting Help

If you get stuck, the following resources might help:

* [Influx Community Slack #V2 channel](https://app.slack.com/client/TH8RGQX5Z/CH8RV8PK5)
* [InfluxData subreddit](https://www.reddit.com/r/InfluxData/)
