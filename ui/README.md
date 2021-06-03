## InfluxDB UI

UI assets for InfluxDB are automatically downloaded and embedded in the `influxd` binary
when using the top-level `Makefile`. The UI assets are built and made available from
the [`influxdata/ui` repository](https://github.com/influxdata/ui). All of the UI source code
has been removed from this directory, and now lives in the [`influxdata/ui` repository](https://github.com/influxdata/ui).
Please submit all PRs and issues related to the InfluxDB UI to the [`influxdata/ui` repository](https://github.com/influxdata/ui).

### Starting a Local Development Environment

It is possible to run a frontend development server with hot reloading using the UI from
[`influxdata/ui`](https://github.com/influxdata/ui) in front of the InfluxDB backend:

Start `influxd` listening on the default port (`8086`):

`$ ./bin/darwin/influxd`

Clone (if needed) & start the UI development server from the `ui` repository:

```
$ git clone https://github.com/influxdata/ui.git
$ cd ui
$ yarn start
```

The UI development server runs at [`http://localhost:8080`](http://localhost:8080/)

### Running InfluxDB with Local UI Assets

To run InfluxDB with local UI assets, first build the assets:

```
$ git clone https://github.com/influxdata/ui.git
$ cd ui
$ yarn build
```

Start `influxd` using the local UI assets via the `--assets-path` flag. For example,
if the `ui` folder containing built assets is at the same level as the `influxdb` folder
and the `influxd` binary is at `influxdb/bin/darwin/influxd`:

`$ ./bin/darwin/influxd --assets-path=../ui/build`
