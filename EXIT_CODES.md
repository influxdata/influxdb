# `influxd` exit codes

A reference and operator guide for the process exit status `influxd` returns
when it fails to start.

## Overview

`influxd` reports the **category** of a startup failure in its exit status, so a
supervisor can tell a misconfiguration apart from a busy port or a full disk.
The one question the status is designed to answer is **will restarting help?**

The vocabulary is [`sysexits.h`](https://www.man7.org/linux/man-pages/man3/sysexits.h.3head.html),
whose 64–78 block is the conventional home for an application's own statuses. It
sits inside the 1–125 "standard errors" range and clear of every band something
else has already claimed — 126–127 for the shell, 128–165 for signals (128 + the
signal number), and 255 for overflow.

The status says *what kind* of failure it was, not *which subsystem* failed.
Subsystem attribution is reported by `/health` and `/ready`, and
`--startup-error-linger` exists to keep those endpoints answering long enough
for a scraper to read it — see [HEALTH_READY.md](HEALTH_READY.md). The error
`influxd` prints to stderr and the `subsystem` field on its log line carry the
same identity. A single integer cannot carry both, so it carries the half those
other channels do not.

## The codes

| Code | Name | Meaning | Restart helps? |
|-----:|------|---------|----------------|
| 0 | `EX_OK` | Startup succeeded, and so did shutdown — including a shutdown a signal asked for. | — |
| 1 | — | An error with no category. Every subcommand (`inspect`, `upgrade`, `downgrade`, `recovery`, `print-config`) reports its failures this way, as does any startup failure that could not be classified. | unknown |
| 64 | `EX_USAGE` | The command line is wrong: an unknown flag, an unparseable flag value, or an unexpected positional argument. Only the server commands — `influxd`, `run` and `print-config` — report it; the subcommands still report 1. | **no** |
| 65 | `EX_DATAERR` | The data on disk is not something this build can read. Today this is the incompatible-prior-version check: 1.x-era `_series` or `index` directories under `--engine-path`. | **no** |
| 66 | `EX_NOINPUT` | A path given to `influxd` does not exist, or is not the kind of object it was used as — `--engine-path` pointing at a regular file, or a parent directory that is missing. | **no** |
| 69 | `EX_UNAVAILABLE` | Something `influxd` needs is held by someone else: the `--http-bind-address` port is in use, a `--pid-file` already exists, or a dependency refused the connection. | only once the conflict clears |
| 70 | `EX_SOFTWARE` | Startup failed with no operating-system cause to point at. Read the error and the log. | **no** |
| 71 | `EX_OSERR` | The operating system is out of a resource: file descriptors (`EMFILE`, `ENFILE`) or memory. | after raising limits |
| 73 | `EX_CANTCREAT` | A file cannot be created or extended: the disk is full, a quota is exhausted, or the filesystem is read-only. | only once space is freed |
| 74 | `EX_IOERR` | An I/O error while reading or writing. Usually hardware or a failing filesystem. | **no** |
| 75 | `EX_TEMPFAIL` | Something did not get to finish: a dependency timed out, a `SIGINT` interrupted startup, or a stop cut off requests still running when the two-second shutdown budget expired. Nothing is wrong with the configuration or the machine. | **yes** |
| 77 | `EX_NOPERM` | Permission denied on a data directory, a PID file, a TLS certificate, or a privileged port. | **no** |
| 78 | `EX_CONFIG` | A configured value is wrong: 1.x keys in a 2.x config file, an unreadable or unparseable config file, an `INFLUXD_*` or config-file value the option it sets will not accept, an unknown `--store` or `--secret-store`, an unsupported `--tls-min-version`, or a certificate that will not parse. | **no** |

`EX_NOUSER` (67), `EX_NOHOST` (68), `EX_OSFILE` (72) and `EX_PROTOCOL` (76) are
part of `sysexits.h` but `influxd` does not emit them.

### What did not change

Anything `influxd` did not classify still exits **1**, exactly as it always has.
That includes every failure of a subcommand that was actually selected and run:
`influxd inspect dump-wal /missing.wal` exits 1 today and exits 1 now, and so
does `influxd inspect --bogus-flag`. Cobra would have inherited the server's
usage status down the whole tree, so each subcommand is given a pass-through of
its own to stop that — see `newRootCommand` in `cmd/influxd/main.go`. Of the
statuses in the table above, only the server's own startup path and command line
produce one — with the single exception below.

### Failures before a command is selected

`influxd` reads the config file and the `INFLUXD_*` environment while it
assembles its command tree, which happens before cobra has looked at the command
line at all. Three failures therefore exit **78** for *any* command line,
`influxd inspect` and `influxd version` included:

- a config file that cannot be read, or will not parse;
- a config file holding 1.x keys;
- an `INFLUXD_*` or config-file value the option it sets will not accept.

This is not a subcommand acquiring a status it never opted into. No subcommand
has been selected yet, and none of them will run: what failed is `influxd`
reading the configuration that every command in the tree shares, so `EX_CONFIG`
describes it as accurately for `influxd inspect` as for `influxd run`. All three
already aborted every invocation before these statuses existed — same message,
exit 1 — so the status is the only thing about them that changed.

Worth knowing when this fires: with no `INFLUXD_CONFIG_PATH` set, `influxd`
looks for a `config.{json|toml|yaml|yml}` in the working directory, so a broken
one sitting in the directory you happen to run from produces it too.

A clean stop is still **0**. A `SIGINT` that leads to a successful shutdown exits
0, so `systemctl stop` does not read as a failure under `Restart=on-failure`.

Two stops do not exit 0, and the second is not rare. A `SIGINT` that arrives
*before* startup finished exits **75**: nothing came up, so there is nothing to
call a success, but the interruption was asked for and a later start may well
work. And a shutdown that itself fails reports the category of the teardown
error — which includes the ordinary case of stopping a busy server, because
teardown gives in-flight requests only two seconds and `http.Server.Shutdown`
reports that deadline as an error. A `SIGINT` while a longer query is running
therefore exits **75** as well.

## Using the codes with systemd

The unit shipped in the InfluxDB packages sets `Restart=on-failure`, which
restarts on **any** non-zero exit. A misconfigured server therefore restarts
forever, failing the same way each time. `RestartPreventExitStatus=` fixes that:

```ini
[Service]
Restart=on-failure
# Do not restart into a failure that cannot resolve on its own.
RestartPreventExitStatus=64 65 66 70 74 77 78
```

With that in place, a bad config file, a wrong path, a permissions problem or
incompatible on-disk data stops the unit and leaves it in `failed` state, where
`systemctl status influxdb` shows the reason. A busy port (69), a full disk (73),
exhausted file descriptors (71) and an interrupted or timed-out start (75) still
retry, because those can clear without anyone editing anything.

### The shipped unit cannot see these codes yet

**This is a limitation to know about before relying on the above.** The packaged
unit is `Type=forking` and runs `influxd` through a wrapper,
`/usr/lib/influxdb/scripts/influxd-systemd-start.sh`. That script starts
`influxd` in the background and never `wait`s on it, so the exit status is
discarded. Its readiness loop polls `/ready` without an attempt limit, and the
unit sets `TimeoutStartSec=0`, so a failed start makes `systemctl start` wait
indefinitely rather than report anything.

Until that wrapper propagates the child's status, these codes are observable
from an interactive shell, from a container runtime, and from any supervisor
that execs `influxd` directly — but not through the packaged systemd unit. The
SysV `init.sh` backgrounds the daemon the same way, and has its own unrelated
`exit 1`/`2`/`5` vocabulary.

## Using the codes with Docker and Kubernetes

`docker/influxd/entrypoint.sh` ends in `exec "$@"`, so the status passes through
unchanged and `docker inspect --format '{{.State.ExitCode}}'` reports it.

Kubernetes does not branch on exit codes in a `restartPolicy`, but the code is
recorded in the container's `lastState.terminated.exitCode` and shown by
`kubectl describe pod`, which makes a `CrashLoopBackOff` diagnosable without
reading the logs: a pod looping on 78 is misconfigured, one looping on 69 is
fighting over a port or a PID file.

## Checking a code

```console
$ influxd --engine-path=/etc/hosts
Error: mkdir /etc/hosts: not a directory
$ echo $?
66
```

```console
$ influxd --store=bogus
Error: unknown store type bogus; expected disk or memory
$ echo $?
78
```

Note that `$?` must be read immediately: any other command in between replaces
it.

## For developers

The mapping lives in [`kit/exit`](kit/exit). Two functions, deliberately
separate:

- `exit.Classify(err)` chooses a status from the cause underneath an error — the
  `syscall` errno, or the `io/fs` and `context` sentinels. It runs **once**, in
  `Launcher.run`'s deferred hook, and its result is pinned onto the error with
  `exit.WithCode`.
- `exit.Code(err)` only reads a pinned status, and reports `1` for an error that
  has none. It never classifies. That is what keeps every path which has not
  opted in exiting exactly as it did before.

To give a new failure a status, pin one at the site **only if the error has no
operating-system cause underneath it** — a sentinel, or a `fmt.Errorf` with no
`%w` of a syscall error:

```go
return exit.WithCode(exit.CodeConfig,
	fmt.Errorf("unknown store type %s; expected disk or memory", opts.StoreType))
```

If the error does wrap an OS error, pin nothing: `Classify` reads the errno and
gets a more specific answer than a fixed tag could. `net.Listen`'s failure is the
example — the same call yields `EADDRINUSE` (69) and `EACCES` (77), and only the
error knows which.

A site that has to override the classifier — because it knows a category the OS
cannot express — should ask it first and tag only when it answers `EX_SOFTWARE`,
which is `Classify` saying it found no cause it could place:

```go
if exit.Classify(err) == exit.CodeSoftware {
	err = exit.WithCode(exit.CodeConfig, err)
}
```

The TLS key pair in `runHTTP` is the only such site: a pair that will not parse
is a configuration error, but a pair the OS refused to hand over keeps whatever
its errno says. Re-checking a couple of `io/fs` sentinels instead of asking would
report `ENOTDIR`, `EMFILE` and `EIO` as 78 as well.

`Classify` searches the whole error tree for an errno it has a status for, not
just the first one it meets. The teardown error is an `errors.Join` of one error
per failing closer, so a mapped `ENOSPC` frequently sits behind an unmapped
`EINVAL` from a listener something else already closed; `errors.As` alone would
stop at the `EINVAL` and report a full disk as 70.

The errno tables are per-platform (`errno_unix.go`, `errno_windows.go`) because
Windows reports Win32 and Winsock numbers: a busy socket there is
`WSAEADDRINUSE` (10048), which shares no value with `syscall.EADDRINUSE`.

## Known gaps

- **The packaged systemd wrapper discards the status.** See above.
- **A failure *after* startup still exits 0.** If the HTTP server stops serving
  once the server is up, `influxd` shuts down cleanly and reports success.
- **`SIGTERM` is not trapped.** `kit/signals` registers `os.Interrupt` and
  `os.Kill` only, so a `systemctl stop` terminates the process where it stands
  and no status of `influxd`'s own is produced.
