# influxdb/releng

This directory and its subdirectories contain release engineering scripts to build source tarballs and packages, run unit tests in an isolated environment, and so on.
The directory layout typically looks like:

```
├── Dockerfile
├── build.bash
└── fs
    └── usr
        └── local
            └── bin
                └── influxdb_tarball.bash
```

Where you only need to run `build.bash` (or other shell scripts in the root directory) with valid arguments to complete the step.
All scripts in the root folders accept the `-h` flag to explain usage.

The `fs` folder is overlaid on the Docker image so that is clear where each script for the Docker containers reside.
Those scripts make assumptions about the environment which are controlled in the outer scripts (i.e. `build.bash`), so the scripts not intended to be run outside of Docker.

By default, these scripts will use the "current" Go version as determined by `_go_versions.sh`.
To use the "next" version of Go, set the environment variable GO_NEXT to a non-empty value.

## source-tarball

Generates a source tarball of influxdb that can be extracted to a new `GOPATH` such that you can `go build github.com/influxdata/influxdb/cmd/influxd`, etc., without manually setting linker flags or anything.

## raw-binaries

Builds the raw binaries for the various influxdb commands, and stores them in OS/architecture-specific tarballs in the provided destination path.

## packages

Given a source tarball and an archive of raw binaries, generates OS/architecture-specific packages (i.e. .deb and .rpm files).

## unit-tests

Given a source tarball, runs the influxdb unit tests in a clean Docker environment.
